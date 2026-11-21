package authzrpc_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/server/authzrpc"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/internal/storage/postgres"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
	"github.com/buildbetter-app/plugs/internal/testclock"
)

type quietFixture struct {
	*fixture
	clock *testclock.Clock
}

func newQuietFixture(t *testing.T, engine string, postgresDSN func(*testing.T) string) *quietFixture {
	t.Helper()
	return newQuietFixtureWrapped(t, engine, postgresDSN, nil)
}

// newQuietFixtureWrapped lets a test interpose on the opened real store before
// the composition binds it. wrap must return a store that delegates to the real
// one; nil keeps the store unchanged.
func newQuietFixtureWrapped(t *testing.T, engine string, postgresDSN func(*testing.T) string, wrap func(*testing.T, driver.Store) driver.Store) *quietFixture {
	t.Helper()
	return newQuietFixtureConfigured(t, engine, postgresDSN, wrap, nil)
}

// newQuietFixtureConfigured additionally lets a test adjust the composition
// config (for example to install the hierarchy authorizer); nil keeps it.
func newQuietFixtureConfigured(t *testing.T, engine string, postgresDSN func(*testing.T) string, wrap func(*testing.T, driver.Store) driver.Store, configure func(*authzrpc.Config)) *quietFixture {
	t.Helper()
	clock := testclock.New(t, time.Date(2026, 11, 3, 6, 0, 0, 0, time.UTC))
	var store driver.Store
	var err error
	switch engine {
	case "memory":
		store, err = memory.NewWithOptions(memory.Options{Clock: clock.Now})
	case "sqlite":
		store, err = (sqlite.Driver{Clock: clock.Now}).Open(context.Background(), driver.Config{DSN: filepath.Join(t.TempDir(), "quiet.db")})
	case "postgres":
		dsn := clock.BindPostgres(t, postgresDSN(t))
		store, err = postgres.Open(context.Background(), driver.Config{DSN: dsn})
		if err == nil && !testclock.PostgresNow(t, dsn).Equal(clock.Now()) {
			t.Fatal("actual SQL clock differs from the application seed")
		}
	default:
		t.Fatalf("unknown real engine %q", engine)
	}
	if err != nil {
		t.Fatal(err)
	}
	if wrap != nil {
		store = wrap(t, store)
	}
	f := newFixtureWith(t, store, nil, func(ctx context.Context, config authzrpc.Config) (*authzrpc.Composition, error) {
		if configure != nil {
			configure(&config)
		}
		return authzrpc.NewWithClockForTest(ctx, config, clock.Now)
	})
	return &quietFixture{fixture: f, clock: clock}
}

func quietRef() model.Ref { return model.RefByID(beadID, model.Natural64(0)) }

func quietParams() map[string]any { return map[string]any{"id": quietRef()} }

func quietOverlayKey() string {
	digest := sha256.Sum256([]byte("plug/lease/" + beadID))
	return "plug/lease/" + hex.EncodeToString(digest[:])
}

func quietReceiptKey(t *testing.T, by model.PrincipalRef, key int) string {
	t.Helper()
	principal, err := driver.PrincipalKey(by)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256([]byte(principal + "\x00\x00bead.heartbeat\x00" + fmt.Sprintf("%032x", key)))
	return "plug/heartbeat/" + hex.EncodeToString(digest[:])
}

type quietState struct {
	Head, Incarnation, Revision, Body string
	Versions, Changes, Facts          string
	OrdinaryReceipt                   *driver.IdempotencyRecord
	Overlay, Replay                   driver.Checkpoint
}

func quietJSON(t *testing.T, value any) string {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func (f *quietFixture) state(t *testing.T, by model.PrincipalRef, key int) quietState {
	t.Helper()
	ctx := context.Background()
	meta, err := f.store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	r, err := f.store.Read(ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	rows, err := r.Get(ctx, []model.Ref{quietRef()})
	if err != nil || len(rows) != 1 {
		t.Fatalf("actual stored target: rows=%d err=%v", len(rows), err)
	}
	versions, err := r.Versions(ctx, quietRef(), driver.PageReq{Limit: 1000})
	if err != nil || versions.NextCursor != nil {
		t.Fatalf("complete versions: %v", err)
	}
	changes, err := r.Changesets(ctx, nil, 1000)
	if err != nil {
		t.Fatal(err)
	}
	ref := quietRef()
	keys := []driver.LockKey{{Kind: driver.LockSentinel, Sentinel: driver.SentinelGraphBlocking}, {Kind: driver.LockRecord, Ref: &ref}}
	for _, dependency := range []authz.Dependency{authz.ACLDependency(f.server.Namespace), authz.ContextDependency(f.server.Namespace)} {
		fact, err := driver.NewReadFact(dependency.Key)
		if err != nil {
			t.Fatal(err)
		}
		keys = append(keys, driver.LockKey{Kind: driver.LockReadFact, Fact: fact})
	}
	encoded := make(map[driver.LockKey]string, len(keys))
	for _, key := range keys {
		value, err := driver.EncodeLockKey(key)
		if err != nil {
			t.Fatal(err)
		}
		encoded[key] = value
	}
	sort.Slice(keys, func(i, j int) bool { return encoded[keys[i]] < encoded[keys[j]] })
	if err := driver.ValidateLockKeys(keys); err != nil {
		t.Fatal(err)
	}
	facts, err := r.Observe(ctx, keys)
	if err != nil || len(facts) != len(keys) {
		t.Fatalf("graph/record/authority facts: %v", err)
	}
	principal, err := driver.PrincipalKey(by)
	if err != nil {
		t.Fatal(err)
	}
	ordinary, err := r.IdempotencyByKey(ctx, principal, "", "bead.heartbeat", fmt.Sprintf("%032x", key))
	if err != nil {
		t.Fatal(err)
	}
	checkpoints, ok := f.store.(driver.CheckpointStore)
	if !ok {
		t.Fatal("real driver lacks checkpoint storage")
	}
	overlay, err := checkpoints.LoadCheckpoint(ctx, quietOverlayKey())
	if err != nil {
		t.Fatal(err)
	}
	replay, err := checkpoints.LoadCheckpoint(ctx, quietReceiptKey(t, by, key))
	if err != nil {
		t.Fatal(err)
	}
	return quietState{
		Head: meta.Head.String(), Incarnation: meta.Incarnation, Revision: rows[0].Revision.String(), Body: string(rows[0].Body),
		Versions: quietJSON(t, versions), Changes: quietJSON(t, changes), Facts: quietJSON(t, facts),
		OrdinaryReceipt: ordinary, Overlay: overlay, Replay: replay,
	}
}

func requireQuietGraph(t *testing.T, before, after quietState) {
	t.Helper()
	before.Overlay, before.Replay = driver.Checkpoint{}, driver.Checkpoint{}
	after.Overlay, after.Replay = driver.Checkpoint{}, driver.Checkpoint{}
	if !reflect.DeepEqual(before, after) {
		t.Fatal("quiet call changed durable bytes, revision/head, versions/events, graph/authority facts or ordinary receipt")
	}
}

func requireQuietRefusal(t *testing.T, got reply, code model.ErrorCode, hint string) {
	t.Helper()
	if got.Error == nil || got.Error.Data.Code != code || len(got.Result) != 0 {
		t.Fatalf("expected exactly %s without a success result; error=%+v", code, got.Error)
	}
	if hint != "" && got.Error.Data.Hint != hint {
		t.Fatalf("refusal reached the wrong guard: hint=%q want=%q", got.Error.Data.Hint, hint)
	}
}

func quietDeadline(t *testing.T, got reply) time.Time {
	t.Helper()
	success(t, got)
	var result struct{ LeaseExpiresAt time.Time }
	if err := json.Unmarshal(got.Result, &result); err != nil || result.LeaseExpiresAt.IsZero() {
		t.Fatalf("actual heartbeat deadline: %v", err)
	}
	return result.LeaseExpiresAt
}

func (f *quietFixture) claim(t *testing.T, by model.PrincipalRef) {
	t.Helper()
	success(t, f.call(t, by, "bead.claim", quietParams(), 7100))
}

func (f *quietFixture) alterClaimFixture(t *testing.T, alter func(*pipeline.Document)) {
	t.Helper()
	state := f.state(t, worker("user:operator"), 7101)
	doc, err := pipeline.DecodeDocument([]byte(state.Body))
	if err != nil {
		t.Fatal(err)
	}
	alter(&doc)
	body := quietJSON(t, doc)
	_, err = conformance.Seed(context.Background(), f.store, f.lease, driver.ChangesetHeader{}, []conformance.FixtureChange{{
		Ref: quietRef(), Kind: "bead", Op: model.OpPatch, Body: []byte(body),
	}})
	if err != nil {
		t.Fatal(err)
	}
	if persisted := f.state(t, worker("user:operator"), 7101); persisted.Body != body {
		t.Fatal("actual persisted fixture bytes differ from the seeded document")
	}
}

func TestQuietHeartbeatHTTPAcrossDrivers(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			var dsn func(*testing.T) string
			if engine == "postgres" {
				dsn = postgresDSNFactory(t)
			}
			t.Run("overlay_same_second_and_replay", func(t *testing.T) {
				quietOverlayHTTP(t, newQuietFixture(t, engine, dsn))
			})
			for _, row := range []struct {
				name    string
				foreign bool
				prepare func(*testing.T, *quietFixture, model.PrincipalRef)
				code    model.ErrorCode
				hint    string
			}{
				{"missing_base", false, func(t *testing.T, f *quietFixture, _ model.PrincipalRef) {
					f.alterClaimFixture(t, func(doc *pipeline.Document) { doc.Lease = nil })
				}, "lease_not_held", "bead has no active claim"},
				{"missing_base_with_orphan_overlay", false, func(t *testing.T, f *quietFixture, by model.PrincipalRef) {
					f.alterClaimFixture(t, func(doc *pipeline.Document) { doc.Lease = nil })
					payload := quietJSON(t, map[string]any{
						"principal": by, "expiresAt": f.clock.Now().Add(5 * time.Minute), "ttlMs": "300000",
						"epoch": model.Natural64(f.lease.Epoch()), "lastHeartbeat": f.clock.Now(),
					})
					if _, err := f.store.(driver.CheckpointStore).SaveCheckpoint(context.Background(), f.lease, quietOverlayKey(), 0, []byte(payload)); err != nil {
						t.Fatal(err)
					}
				}, "lease_not_held", "bead has no active claim"},
				{"missing_holder", false, func(t *testing.T, f *quietFixture, _ model.PrincipalRef) {
					f.alterClaimFixture(t, func(doc *pipeline.Document) { doc.Lease.Principal.ID = "" })
					stored := f.state(t, worker("user:operator"), 7101)
					var actual struct {
						Lease struct{ Principal model.PrincipalRef }
					}
					if err := json.Unmarshal([]byte(stored.Body), &actual); err != nil || actual.Lease.Principal.Kind != "agent" || actual.Lease.Principal.ID != "" {
						t.Fatalf("missing-ID Agent fixture was not persisted: %v", err)
					}
				}, "lease_not_held", "bead is claimed by another principal"},
				{"zero_base_ttl", false, func(t *testing.T, f *quietFixture, _ model.PrincipalRef) {
					f.alterClaimFixture(t, func(doc *pipeline.Document) { doc.Lease.TtlMs = model.Natural64(0) })
				}, "lease_not_held", "claim has an invalid lease TTL"},
				{"overflow_base_ttl", false, func(t *testing.T, f *quietFixture, _ model.PrincipalRef) {
					f.alterClaimFixture(t, func(doc *pipeline.Document) {
						doc.Lease.TtlMs = model.Natural64(uint64((1<<63-1)/time.Millisecond) + 1)
					})
				}, "lease_not_held", "claim has an invalid lease TTL"},
				{"foreign_holder", true, nil, "lease_not_held", "bead is claimed by another principal"},
				{"released", false, func(t *testing.T, f *quietFixture, by model.PrincipalRef) {
					params := quietParams()
					params["reason"] = "holder release"
					success(t, f.call(t, by, "bead.release", params, 7120))
					stored := f.state(t, by, 7120)
					var actual struct {
						Lease    *json.RawMessage
						Comments []struct {
							Body      string
							Principal model.PrincipalRef
						}
					}
					if err := json.Unmarshal([]byte(stored.Body), &actual); err != nil || actual.Lease != nil || len(actual.Comments) != 1 ||
						actual.Comments[0].Body != "holder release" || !samePrincipal(t, actual.Comments[0].Principal, by) {
						t.Fatalf("public release did not persist its comment and nil lease: %v", err)
					}
				}, "lease_not_held", "bead has no active claim"},
				{"closed", false, func(t *testing.T, f *quietFixture, _ model.PrincipalRef) {
					// Seed an actual closed row; this tests heartbeat admission,
					// not the separately owned close/transition authorization.
					f.alterClaimFixture(t, func(doc *pipeline.Document) { doc.Fields["status"], doc.Lease = "done", nil })
				}, "lease_not_held", "bead has no active claim"},
				{"expiry_equality", false, func(t *testing.T, f *quietFixture, _ model.PrincipalRef) {
					f.alterClaimFixture(t, func(doc *pipeline.Document) { doc.Lease.ExpiresAt = f.clock.Now() })
				}, "lease_expired", "bead claim has expired"},
				{"foreign_project", false, func(t *testing.T, f *quietFixture, _ model.PrincipalRef) {
					f.alterClaimFixture(t, func(doc *pipeline.Document) { doc.Project = "unregistered" })
				}, "forbidden", ""},
			} {
				t.Run(row.name, func(t *testing.T) {
					f := newQuietFixture(t, engine, dsn)
					by := worker("user:operator")
					holder := by
					if row.foreign {
						holder.ID = "agent:other"
					}
					f.claim(t, holder)
					if row.prepare != nil {
						row.prepare(t, f, by)
					}
					before := f.state(t, by, 7110)
					if row.name == "zero_base_ttl" || row.name == "overflow_base_ttl" {
						var actual struct{ Lease struct{ TtlMs model.Natural } }
						if err := json.Unmarshal([]byte(before.Body), &actual); err != nil {
							t.Fatal(err)
						}
						want := model.Natural64(0)
						if row.name == "overflow_base_ttl" {
							want = model.Natural64(uint64((1<<63-1)/time.Millisecond) + 1)
						}
						if actual.Lease.TtlMs.String() != want.String() || before.Replay.Generation != 0 || len(before.Replay.Payload) != 0 {
							t.Fatal("invalid TTL fixture or absent fresh-key receipt was not independently observed")
						}
					}
					got := f.call(t, by, "bead.heartbeat", quietParams(), 7110)
					requireQuietRefusal(t, got, row.code, row.hint)
					if after := f.state(t, by, 7110); !reflect.DeepEqual(before, after) {
						t.Fatal("refusal changed durable state, graph facts, lease checkpoint or either receipt plane")
					}
				})
			}
			t.Run("current_authority_and_replay", func(t *testing.T) {
				quietRevocationHTTP(t, newQuietFixture(t, engine, dsn))
			})
			for _, state := range []string{"missing_base", "released", "closed", "foreign_holder", "expired"} {
				t.Run("published_replay_"+state, func(t *testing.T) {
					quietPastReplayHTTP(t, newQuietFixture(t, engine, dsn), state)
				})
			}
			t.Run("published_replay_reclaimed_same_holder", func(t *testing.T) {
				quietReclaimedReplayHTTP(t, newQuietFixture(t, engine, dsn))
			})
			t.Run("lost_storage_lease", func(t *testing.T) {
				quietLeaseLossHTTP(t, newQuietFixture(t, engine, dsn))
			})
			t.Run("public_recovery_refused", func(t *testing.T) {
				quietPublicRecoveryHTTP(t, newQuietFixture(t, engine, dsn))
			})
			t.Run("original_method_forbids", func(t *testing.T) {
				quietPolicyForbidsHTTP(t, newQuietFixture(t, engine, dsn))
			})
		})
	}
}

func quietOverlayHTTP(t *testing.T, f *quietFixture) {
	t.Helper()
	by := worker("user:operator")
	f.claim(t, by)
	before := f.state(t, by, 7200)
	if before.Overlay.Generation != 0 || len(before.Overlay.Payload) != 0 || before.Replay.Generation != 0 {
		t.Fatal("claim unexpectedly supplied a quiet overlay/receipt")
	}
	f.clock.Advance(t, 2*time.Second)
	want := f.clock.Now().Add(5 * time.Minute)
	originalDeadline := want
	got := f.call(t, by, "bead.heartbeat", quietParams(), 7200)
	if got.Meta.Replayed || !quietDeadline(t, got).Equal(want) {
		t.Fatal("fresh overlay did not use the independently scheduled five-minute deadline")
	}
	after := f.state(t, by, 7200)
	requireQuietGraph(t, before, after)
	var overlay struct {
		Principal     model.PrincipalRef
		ExpiresAt     time.Time
		LastHeartbeat time.Time
		TtlMs, Epoch  model.Natural
	}
	if err := json.Unmarshal(after.Overlay.Payload, &overlay); err != nil {
		t.Fatal(err)
	}
	if after.Overlay.Generation == 0 || !overlay.ExpiresAt.Equal(want) || !overlay.LastHeartbeat.Equal(f.clock.Now()) ||
		overlay.TtlMs.String() != "300000" || overlay.Epoch.String() != model.Natural64(f.lease.Epoch()).String() || !samePrincipal(t, overlay.Principal, by) {
		t.Fatal("actual checkpoint does not contain the authenticated holder and exact scheduled deadline")
	}
	var receipt struct {
		ID, State      string
		LeaseExpiresAt time.Time
	}
	if err := json.Unmarshal(after.Replay.Payload, &receipt); err != nil || receipt.ID != beadID || receipt.State != "published" || !receipt.LeaseExpiresAt.Equal(want) {
		t.Fatalf("actual published checkpoint receipt differs from the response: %v", err)
	}

	f.clock.Advance(t, 250*time.Millisecond)
	beforeSame := f.state(t, by, 7201)
	if !quietDeadline(t, f.call(t, by, "bead.heartbeat", quietParams(), 7201)).Equal(want) {
		t.Fatal("same-second fresh key extended the deadline")
	}
	afterSame := f.state(t, by, 7201)
	requireQuietGraph(t, beforeSame, afterSame)
	if !reflect.DeepEqual(beforeSame.Overlay, afterSame.Overlay) || afterSame.Replay.Generation == 0 {
		t.Fatal("same-second heartbeat changed the overlay or omitted its keyed response")
	}

	f.clock.Advance(t, 2*time.Second)
	beforeReplay := f.state(t, by, 7200)
	replay := f.call(t, by, "bead.heartbeat", quietParams(), 7200)
	if !replay.Meta.Replayed || !quietDeadline(t, replay).Equal(want) || !reflect.DeepEqual(beforeReplay, f.state(t, by, 7200)) {
		t.Fatal("same-key replay changed the actual persisted deadline or any state")
	}
	beforeRefresh := f.state(t, by, 7202)
	want = f.clock.Now().Add(5 * time.Minute)
	if !quietDeadline(t, f.call(t, by, "bead.heartbeat", quietParams(), 7202)).Equal(want) {
		t.Fatal("fresh later heartbeat did not refresh the deadline")
	}
	afterRefresh := f.state(t, by, 7202)
	requireQuietGraph(t, beforeRefresh, afterRefresh)
	if afterRefresh.Overlay.Generation <= beforeRefresh.Overlay.Generation {
		t.Fatal("later heartbeat did not persist an overlay refresh")
	}
	if err := json.Unmarshal(afterRefresh.Overlay.Payload, &overlay); err != nil || !overlay.ExpiresAt.Equal(want) {
		t.Fatalf("refreshed stored deadline differs from the independent schedule: %v", err)
	}
	if err := json.Unmarshal(afterRefresh.Replay.Payload, &receipt); err != nil || receipt.State != "published" || !receipt.LeaseExpiresAt.Equal(want) {
		t.Fatalf("refreshed stored response differs from the public deadline: %v", err)
	}
	beforeOldReplay := f.state(t, by, 7200)
	oldReplay := f.call(t, by, "bead.heartbeat", quietParams(), 7200)
	if !oldReplay.Meta.Replayed || !quietDeadline(t, oldReplay).Equal(originalDeadline) {
		t.Fatal("same-claim replay did not preserve the original response after a newer heartbeat")
	}
	if !reflect.DeepEqual(beforeOldReplay, f.state(t, by, 7200)) || !reflect.DeepEqual(afterRefresh, f.state(t, by, 7202)) {
		t.Fatal("same-claim old-key replay rewrote a receipt or the newer overlay")
	}
}

func quietReclaimedReplayHTTP(t *testing.T, f *quietFixture) {
	t.Helper()
	by := worker("user:operator")
	f.claim(t, by)
	_ = quietDeadline(t, f.call(t, by, "bead.heartbeat", quietParams(), 7800))
	original := f.state(t, by, 7800)
	var oldReceipt struct{ ClaimExpiresAt time.Time }
	if err := json.Unmarshal(original.Replay.Payload, &oldReceipt); err != nil || oldReceipt.ClaimExpiresAt.IsZero() {
		t.Fatalf("original receipt claim boundary: %v", err)
	}
	success(t, f.call(t, by, "bead.release", quietParams(), 7801))
	f.clock.Advance(t, time.Second)
	success(t, f.call(t, by, "bead.claim", quietParams(), 7802))
	before := f.state(t, by, 7800)
	var actual struct{ Lease pipeline.ClaimLease }
	if err := json.Unmarshal([]byte(before.Body), &actual); err != nil ||
		!samePrincipal(t, actual.Lease.Principal, by) || actual.Lease.Epoch.String() != model.Natural64(f.lease.Epoch()).String() ||
		!actual.Lease.ExpiresAt.Equal(f.clock.Now().Add(5*time.Minute)) || actual.Lease.ExpiresAt.Equal(oldReceipt.ClaimExpiresAt) ||
		!reflect.DeepEqual(original.Replay, before.Replay) {
		t.Fatalf("public reacquisition did not establish a later claim with the original receipt intact: %v", err)
	}
	requireQuietRefusal(t, f.call(t, by, "bead.heartbeat", quietParams(), 7800), "lease_not_held", "heartbeat reservation belongs to a different claim")
	if !reflect.DeepEqual(before, f.state(t, by, 7800)) {
		t.Fatal("old-key replay after public reacquisition changed durable/checkpoint/receipt state")
	}
}

func quietRevocationHTTP(t *testing.T, f *quietFixture) {
	t.Helper()
	by := worker("user:operator")
	f.claim(t, by)
	f.clock.Advance(t, 2*time.Second)
	_ = quietDeadline(t, f.call(t, by, "bead.heartbeat", quietParams(), 7300))
	revoked := f.base
	revoked.Grants = nil
	if err := f.facts.Replace(revoked); err != nil {
		t.Fatal(err)
	}
	// Capture after the real competing ACL commit. Neither an old published
	// heartbeat receipt nor a fresh key may retain the revoked permit.
	for _, key := range []int{7300, 7301} {
		before := f.state(t, by, key)
		requireQuietRefusal(t, f.call(t, by, "bead.heartbeat", quietParams(), key), "forbidden", "")
		if !reflect.DeepEqual(before, f.state(t, by, key)) {
			t.Fatal("revoked current authority changed graph or checkpoint state")
		}
	}
}

func quietPastReplayHTTP(t *testing.T, f *quietFixture, state string) {
	t.Helper()
	by := worker("user:operator")
	f.claim(t, by)
	_ = quietDeadline(t, f.call(t, by, "bead.heartbeat", quietParams(), 7700))
	code, hint := model.ErrorCode("lease_not_held"), "bead has no active claim"
	switch state {
	case "missing_base":
		f.alterClaimFixture(t, func(doc *pipeline.Document) { doc.Lease = nil })
	case "released", "foreign_holder":
		success(t, f.call(t, by, "bead.release", quietParams(), 7702))
		if state == "foreign_holder" {
			other := by
			other.ID = "agent:next-holder"
			success(t, f.call(t, other, "bead.claim", quietParams(), 7703))
			hint = "bead is claimed by another principal"
		}
	case "closed":
		f.alterClaimFixture(t, func(doc *pipeline.Document) { doc.Fields["status"], doc.Lease = "done", nil })
	case "expired":
		// Expire the actual claim/overlay fixture without expiring or extending
		// the independent ten-second storage-writer lease.
		f.alterClaimFixture(t, func(doc *pipeline.Document) { doc.Lease.ExpiresAt = f.clock.Now() })
		checkpoint := f.state(t, by, 7700).Overlay
		if _, err := f.store.(driver.CheckpointStore).SaveCheckpoint(context.Background(), f.lease, quietOverlayKey(), checkpoint.Generation, nil); err != nil {
			t.Fatal(err)
		}
		code, hint = "lease_expired", "bead claim has expired"
	default:
		t.Fatalf("unknown ended claim %q", state)
	}
	beforeOriginal := f.state(t, by, 7700)
	for _, key := range []int{7700, 7701} {
		before := f.state(t, by, key)
		requireQuietRefusal(t, f.call(t, by, "bead.heartbeat", quietParams(), key), code, hint)
		if !reflect.DeepEqual(before, f.state(t, by, key)) || !reflect.DeepEqual(beforeOriginal, f.state(t, by, 7700)) {
			t.Fatal("ended claim reused a heartbeat receipt or published an overlay/rearm")
		}
	}
}

func quietLeaseLossHTTP(t *testing.T, f *quietFixture) {
	t.Helper()
	by := worker("user:operator")
	f.claim(t, by)
	_ = quietDeadline(t, f.call(t, by, "bead.heartbeat", quietParams(), 7400))
	if err := f.lease.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, row := range []struct {
		method string
		key    int
	}{{"bead.heartbeat", 7400}, {"bead.heartbeat", 7401}, {"bead.release", 7402}} {
		before := f.state(t, by, row.key)
		got := f.call(t, by, row.method, quietParams(), row.key)
		requireQuietRefusal(t, got, "busy", "active storage lease was lost")
		if !got.Error.Data.Retryable || !reflect.DeepEqual(before, f.state(t, by, row.key)) {
			t.Fatal("lost storage lease retained a public permit or published state")
		}
		if row.method == "bead.release" && f.receipt(t, by, row.method, row.key) != nil {
			t.Fatal("lost-lease release published an ordinary receipt")
		}
	}
}

func quietPublicRecoveryHTTP(t *testing.T, f *quietFixture) {
	t.Helper()
	by := worker("user:operator")
	f.claim(t, by)
	requireForeignLiveClaim := func(state quietState, caller model.PrincipalRef, key int) {
		t.Helper()
		select {
		case <-f.lease.Lost():
			t.Fatal("public recovery fixture lost its storage lease")
		default:
		}
		var actual struct {
			ID    string
			Lease *pipeline.ClaimLease
		}
		if err := json.Unmarshal([]byte(state.Body), &actual); err != nil || actual.ID != beadID || actual.Lease == nil {
			t.Fatalf("public recovery fixture has no actual base claim: %v", err)
		}
		holder, err := driver.PrincipalKey(actual.Lease.Principal)
		if err != nil {
			t.Fatal(err)
		}
		principal, err := driver.PrincipalKey(caller)
		if err != nil || holder == principal || !samePrincipal(t, actual.Lease.Principal, by) {
			t.Fatalf("public recovery fixture is not held by the original, different Agent: %v", err)
		}
		epoch, err := actual.Lease.Epoch.Uint64()
		if err != nil || epoch == 0 || epoch != f.lease.Epoch() || !f.clock.Now().Before(actual.Lease.ExpiresAt) {
			t.Fatalf("public recovery fixture claim is not live under the current storage lease: %v", err)
		}
		if state.Overlay.Generation != 0 || len(state.Overlay.Payload) != 0 || f.receipt(t, caller, "bead.release", key) != nil {
			t.Fatal("public recovery fixture has an overlay or an existing release receipt")
		}
	}
	if _, err := f.keys.Issue(model.PrincipalRef{Kind: "system", ID: "core"}, time.Now().Add(time.Hour)); err == nil {
		t.Fatal("public API-key provider issued the trusted recovery principal")
	}
	other := worker("user:operator")
	other.ID = "agent:core"
	params := quietParams()
	params["_meta"] = map[string]any{"agent": "core"}
	before := f.state(t, other, 7500)
	requireForeignLiveClaim(before, other, 7500)
	got := f.call(t, other, "bead.release", params, 7500)
	requireQuietRefusal(t, got, "lease_not_held", "")
	// Ordinary release puts its diagnostic in Error.Message. The RPC catalog
	// supplies the public message and retry metadata without inventing a hint.
	if got.Error.Data.Hint != "" || !got.Error.Data.Retryable || len(got.Error.Data.Details) != 0 {
		t.Fatal("ordinary release did not preserve its public lease_not_held projection")
	}
	if !reflect.DeepEqual(before, f.state(t, other, 7500)) || f.receipt(t, other, "bead.release", 7500) != nil {
		t.Fatal("agent label or ID impersonated recovery or released a foreign claim")
	}
	user := model.PrincipalRef{Kind: "user", ID: "user:core"}
	before = f.state(t, user, 7501)
	requireForeignLiveClaim(before, user, 7501)
	requireQuietRefusal(t, f.call(t, user, "bead.release", quietParams(), 7501), "forbidden", "claim maintenance requires an authenticated sponsored agent")
	if !reflect.DeepEqual(before, f.state(t, user, 7501)) || f.receipt(t, user, "bead.release", 7501) != nil {
		t.Fatal("public User acquired a recovery/deputy write")
	}
}

type quietForbidRules struct{}

func (quietForbidRules) Eval(_ context.Context, _ authz.Slice, requests []authz.CheckRequest) ([]authz.Decision, error) {
	decisions := make([]authz.Decision, len(requests))
	for i, request := range requests {
		decisions[i].Effect = authz.NoDecision
		if request.Method == "bead.heartbeat" || request.Method == "bead.release" {
			decisions[i] = authz.Decision{Effect: authz.Deny, Reasons: []string{"actual claim method forbidden"}}
		}
	}
	return decisions, nil
}

func (quietForbidRules) Compile(authz.ListRequest) (authz.FilterTemplate, error) {
	return authz.FilterTemplate{}, authz.ErrCheckOnly
}

func quietPolicyForbidsHTTP(t *testing.T, f *quietFixture) {
	t.Helper()
	by := worker("user:operator")
	f.claim(t, by)
	authzrpc.AddRuleForTest(f.server, quietForbidRules{})
	for i, method := range []string{"bead.heartbeat", "bead.release"} {
		key := 7600 + i
		before := f.state(t, by, key)
		requireQuietRefusal(t, f.call(t, by, method, quietParams(), key), "forbidden", "")
		if !reflect.DeepEqual(before, f.state(t, by, key)) || f.receipt(t, by, method, key) != nil {
			t.Fatal("fixed membership mapping hid an actual-method forbid or published a receipt")
		}
	}
}
