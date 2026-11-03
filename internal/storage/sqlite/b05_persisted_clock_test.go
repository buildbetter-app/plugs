package sqlite

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/testclock"
)

// These tests use the real service and Writer in explicit no-plugin mode.
// They do not supply claim JSON or SQL timestamps to the driver.
func b05Service(t *testing.T, s *Store, clock *testclock.Clock) (*service.Service, driver.ActiveLease) {
	t.Helper()
	lease, err := s.Lease(context.Background(), "b05-clock-host", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := lease.Release(context.Background()); err != nil {
			t.Error(err)
		}
	})
	ontology := service.NewOntology(nil)
	writer := pipeline.New(s, lease, pipeline.WithOntology(ontology), pipeline.WithClock(clock.Now),
		pipeline.WithIDGenerator(func() (string, error) { return "b05-clock-bead", nil }))
	return service.New(s, lease, writer, service.WithOntology(ontology), service.WithClock(clock.Now)), lease
}

func b05Digest(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}

func b05Meta(label string) model.WriteMeta {
	return model.WriteMeta{Project: "b05-clock", Agent: "b05-worker", AgentLabel: "b05-worker",
		IdempotencyKey: b05Digest("b05-clock/" + label)}
}

func b05Document(t *testing.T, s *Store, ref model.Ref) (driver.Record, pipeline.Document) {
	t.Helper()
	ctx := context.Background()
	meta, err := s.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	r, err := s.Read(ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	rows, err := r.Get(ctx, []model.Ref{ref})
	if err != nil || len(rows) != 1 || rows[0].Kind != "bead" {
		t.Fatalf("persisted bead rows=%+v err=%v", rows, err)
	}
	doc, err := pipeline.DecodeDocument(rows[0].Body)
	if err != nil {
		t.Fatal(err)
	}
	return rows[0], doc
}

// Read every logical row, including raw bodies, history, predicate/derived
// states, receipts and checkpoint generations/payloads. Only storage lease
// ownership and PostgreSQL's lock bookkeeping are excluded: a full reopen
// intentionally changes the server epoch, and neither is B05 graph state.
type b05State struct{ Graph, Checkpoints string }

func b05Rows(t *testing.T, s *Store, query string) string {
	t.Helper()
	rows, err := s.db.QueryContext(context.Background(), query)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	data := []any{columns}
	for rows.Next() {
		values := make([]any, len(columns))
		pointers := make([]any, len(columns))
		for i := range values {
			pointers[i] = &values[i]
		}
		if err := rows.Scan(pointers...); err != nil {
			t.Fatal(err)
		}
		for i, value := range values {
			if body, ok := value.([]byte); ok {
				values[i] = bytes.Clone(body)
			}
		}
		data = append(data, values)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(data)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

func b05PersistedState(t *testing.T, s *Store) b05State {
	t.Helper()
	var graph []string
	for _, query := range []string{
		"SELECT * FROM meta ORDER BY singleton",
		"SELECT * FROM objects ORDER BY key",
		"SELECT * FROM records ORDER BY key",
		"SELECT * FROM versions ORDER BY key,rev",
		"SELECT * FROM key_history ORDER BY key,rev",
		"SELECT * FROM blobs ORDER BY hash",
		"SELECT * FROM changesets ORDER BY rev",
		"SELECT * FROM replays ORDER BY scope",
		"SELECT * FROM allocator_states ORDER BY prefix",
		"SELECT * FROM core_sessions ORDER BY id",
	} {
		graph = append(graph, b05Rows(t, s, query))
	}
	raw, err := json.Marshal(graph)
	if err != nil {
		t.Fatal(err)
	}
	return b05State{Graph: string(raw), Checkpoints: b05Rows(t, s, "SELECT * FROM checkpoints ORDER BY key")}
}

type b05LeaseRow struct {
	Epoch         uint64
	Owner, Holder string
	Expires       time.Time
}

func b05StorageLease(t *testing.T, s *Store) b05LeaseRow {
	t.Helper()
	row, err := readLease(context.Background(), s.db)
	if err != nil {
		t.Fatal(err)
	}
	return b05LeaseRow{Epoch: row.epoch, Owner: row.token, Holder: row.holder, Expires: time.Unix(0, row.expires).UTC()}
}

func b05OpenClockStore(t *testing.T, path string, clock *testclock.Clock) *Store {
	t.Helper()
	value, err := (Driver{Clock: clock.Now}).Open(context.Background(), driver.Config{DSN: path})
	if err != nil {
		t.Fatal(err)
	}
	s := value.(*Store)
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	return s
}

// The storage-server lease is the 10s epoch fence, separate from the 5m
// application ClaimLease. Only PostgreSQL generates this expiry inside SQL.
func TestB05SQLitePersistedServerLeaseReopenRenewalAndTakeover(t *testing.T) {
	for _, point := range []struct {
		name  string
		delta time.Duration
	}{
		{"exactly", driver.LeaseTTL},
		{"after", driver.LeaseTTL + time.Nanosecond},
	} {
		t.Run(point.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			clock := testclock.New(t, time.Date(2037, 11, 1, 4, 29, 59, 123456000, time.UTC))
			path := filepath.Join(t.TempDir(), "b05-server-lease.db")
			open := func() *Store { return b05OpenClockStore(t, path, clock) }
			owner := open()
			lease, err := owner.Lease(ctx, "b05-old-host", driver.LeaseTTL)
			if err != nil {
				t.Fatal(err)
			}
			observer := open()
			initial := b05StorageLease(t, observer)
			if initial.Epoch != lease.Epoch() || initial.Owner == "" || initial.Holder != "b05-old-host" ||
				!initial.Expires.Equal(clock.Now().Add(driver.LeaseTTL)) {
				t.Fatalf("actual Go-generated lease row=%+v", initial)
			}
			if err := observer.Close(); err != nil {
				t.Fatal(err)
			}
			clock.Advance(t, time.Second)
			if err := lease.Renew(ctx); err != nil {
				t.Fatal(err)
			}
			observer = open()
			renewed := initial
			renewed.Expires = clock.Now().Add(driver.LeaseTTL)
			if got := b05StorageLease(t, observer); got != renewed {
				t.Fatalf("renewed row across independent pool reopen=%+v want=%+v", got, renewed)
			}
			if err := observer.Close(); err != nil {
				t.Fatal(err)
			}
			clock.Advance(t, point.delta)
			observer = open()
			if got := b05StorageLease(t, observer); got != renewed {
				t.Fatalf("reopen rewrote persisted expiry at %s boundary: %+v", point.name, got)
			}

			before := b05PersistedState(t, observer)
			var fenced driver.ErrFenced
			if _, err := owner.SaveCheckpoint(ctx, lease, "b05/expired-owner", 0, []byte("refuse")); !errors.As(err, &fenced) {
				t.Fatalf("expired old owner publication=%v", err)
			}
			if err := lease.Renew(ctx); !errors.As(err, &fenced) {
				t.Fatalf("expired old owner renewal=%v", err)
			}
			if b05PersistedState(t, observer) != before || b05StorageLease(t, observer) != renewed {
				t.Fatal("expired owner changed logical rows or generated lease timestamp")
			}
			next, err := observer.Lease(ctx, "b05-next-host", driver.LeaseTTL)
			if err != nil {
				t.Fatal(err)
			}
			taken := b05StorageLease(t, observer)
			if next.Epoch() != lease.Epoch()+1 || taken.Epoch != next.Epoch() ||
				taken.Owner == "" || taken.Owner == initial.Owner || taken.Holder != "b05-next-host" ||
				!taken.Expires.Equal(clock.Now().Add(driver.LeaseTTL)) {
				t.Fatalf("actual takeover-generated lease=%+v", taken)
			}
			if err := lease.Release(ctx); err != nil {
				t.Fatal(err)
			}
			if err := owner.Close(); err != nil {
				t.Fatal(err)
			}
			if b05StorageLease(t, observer) != taken || b05PersistedState(t, observer) != before {
				t.Fatal("old owner release/close changed successor lease or logical state")
			}
			saved, err := observer.SaveCheckpoint(ctx, next, "b05/new-owner", 0, []byte("new epoch"))
			if err != nil || saved.Generation != 1 || string(saved.Payload) != "new epoch" {
				t.Fatalf("successor publication=%+v err=%v", saved, err)
			}
			after := b05PersistedState(t, observer)
			if after.Graph != before.Graph {
				t.Fatal("server lease takeover/checkpoint publication changed graph history")
			}
			if err := observer.Close(); err != nil {
				t.Fatal(err)
			}
			reopened := open()
			cleared := b05StorageLease(t, reopened)
			if cleared.Epoch != next.Epoch() || cleared.Owner != "" || cleared.Holder != "" ||
				!cleared.Expires.Equal(time.Unix(0, 0).UTC()) || b05PersistedState(t, reopened) != after {
				t.Fatalf("full reopen lost successor epoch/checkpoint or graceful release: %+v", cleared)
			}
			t.Log("initial/renewed/takeover Go-generated expiries read from independent reopened pools; old owner cannot erase successor; full close/reopen retains epoch and checkpoint")
		})
	}
}

// Deliberately separate the application clock from the real driver's clock.
// Persisted claim/heartbeat JSON must follow Go time even when the store clock
// has crossed the claim's expiry and grace. This is not a Beads SQL-clock twin.
func TestB05SQLiteApplicationClaimReclaimClockBoundaryAcrossReopen(t *testing.T) {
	ctx := context.Background()
	storeClock := testclock.New(t, time.Date(2037, 3, 29, 1, 59, 59, 123456000, time.UTC))
	path := filepath.Join(t.TempDir(), "b05-application.db")
	open := func() *Store { return b05OpenClockStore(t, path, storeClock) }
	appClock := testclock.New(t, storeClock.Now().Add(2*time.Hour))
	s := open()
	svc, lease := b05Service(t, s, appClock)
	by := model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}
	core := model.PrincipalRef{Kind: "system", ID: "core"}
	created, err := svc.Create(ctx, by, service.CreateRequest{Type: "issue", Fields: map[string]any{"title": "persisted B05 clocks"}}, b05Meta("create"))
	if err != nil || len(created.Results) != 1 {
		t.Fatalf("create=%+v err=%v", created, err)
	}
	ref := created.Results[0].Ref
	baseExpiry := appClock.Now().Add(5 * time.Minute)
	claimed, err := svc.Claim(ctx, by, service.ClaimRequest{ID: ref}, b05Meta("claim"))
	if err != nil || !claimed.LeaseExpiresAt.Equal(baseExpiry) {
		t.Fatalf("claim=%+v err=%v", claimed, err)
	}
	baseRecord, doc := b05Document(t, s, ref)
	if doc.Lease == nil || !doc.Lease.ExpiresAt.Equal(baseExpiry) ||
		doc.Lease.ExpiresAt.Equal(storeClock.Now().Add(5*time.Minute)) ||
		doc.Lease.TtlMs.String() != "300000" || doc.Lease.Epoch.String() != model.Natural64(lease.Epoch()).String() ||
		!reflect.DeepEqual(doc.Lease.Principal, by) || doc.Lease.Agent != "b05-worker" {
		t.Fatalf("actual Go-generated persisted claim=%+v", doc.Lease)
	}

	appClock.Advance(t, 2*time.Second)
	heartbeatAt := appClock.Now()
	overlayExpiry := heartbeatAt.Add(5 * time.Minute)
	heartbeatMeta := b05Meta("heartbeat")
	beforeHeartbeat := b05PersistedState(t, s)
	heartbeat, err := svc.HeartbeatWithOutcome(ctx, by, service.HeartbeatRequest{ID: ref}, heartbeatMeta)
	if err != nil || heartbeat.Replayed || !heartbeat.LeaseExpiresAt.Equal(overlayExpiry) {
		t.Fatalf("heartbeat=%+v err=%v", heartbeat, err)
	}
	afterHeartbeat := b05PersistedState(t, s)
	record, _ := b05Document(t, s, ref)
	if afterHeartbeat.Graph != beforeHeartbeat.Graph || !reflect.DeepEqual(record, baseRecord) {
		t.Fatal("quiet heartbeat changed graph, raw bead, history, predicates or ordinary receipts")
	}
	overlayKey := "plug/lease/" + b05Digest("plug/lease/"+ref.ID)
	overlay, err := s.LoadCheckpoint(ctx, overlayKey)
	if err != nil || overlay.Generation == 0 {
		t.Fatalf("actual persisted heartbeat overlay=%+v err=%v", overlay, err)
	}
	var stamp struct {
		pipeline.ClaimLease
		LastHeartbeat time.Time `json:"lastHeartbeat"`
	}
	if err := json.Unmarshal(overlay.Payload, &stamp); err != nil ||
		!stamp.ExpiresAt.Equal(overlayExpiry) || !stamp.LastHeartbeat.Equal(heartbeatAt) ||
		stamp.LastHeartbeat.Equal(storeClock.Now()) || !reflect.DeepEqual(stamp.Principal, by) ||
		stamp.Agent != "b05-worker" || stamp.TtlMs.String() != "300000" || stamp.Epoch.String() != doc.Lease.Epoch.String() {
		t.Fatalf("application-generated persisted heartbeat=%+v err=%v", stamp, err)
	}

	// Advance only the store/SQL clock beyond BOTH domain expiry and grace.
	// Reopen all service/store state and acquire a genuinely new server epoch;
	// the persisted claim and overlay remain the old claim image.
	storeClock.Advance(t, 4*time.Hour)
	oldEpoch := lease.Epoch()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = open()
	svc, lease = b05Service(t, s, appClock)
	if lease.Epoch() != oldEpoch+1 || b05PersistedState(t, s) != afterHeartbeat {
		t.Fatal("full reopen changed logical state or failed to advance durable server epoch")
	}
	_, reopened := b05Document(t, s, ref)
	effective, err := pipeline.EffectiveLease(ctx, s, ref.ID, reopened.Lease)
	if err != nil || effective == nil || !effective.ExpiresAt.Equal(overlayExpiry) ||
		effective.Epoch.String() != model.Natural64(oldEpoch).String() {
		t.Fatalf("reopened persisted effective claim=%+v err=%v", effective, err)
	}
	replayedHeartbeat, err := svc.HeartbeatWithOutcome(ctx, by, service.HeartbeatRequest{ID: ref}, heartbeatMeta)
	if err != nil || !replayedHeartbeat.Replayed || !replayedHeartbeat.LeaseExpiresAt.Equal(overlayExpiry) ||
		b05PersistedState(t, s) != afterHeartbeat {
		t.Fatalf("reopened heartbeat replay=%+v err=%v", replayedHeartbeat, err)
	}

	recoveryMeta := b05Meta("reclaim")
	request := service.ReleaseRequest{ID: ref, Reason: "expired worker recovered"}
	grace := 10 * time.Minute // The unchanged production grace is 2 * the 5m claim TTL.
	for _, point := range []struct {
		name string
		at   time.Time
	}{
		{"application_live_after_store_clock_jump", appClock.Now()},
		{"overlay_expiry_before", overlayExpiry.Add(-time.Nanosecond)},
		{"overlay_expiry_exactly", overlayExpiry},
		{"base_grace_passed_overlay_still_protected", baseExpiry.Add(grace + time.Nanosecond)},
		{"overlay_grace_before", overlayExpiry.Add(grace - time.Nanosecond)},
		{"overlay_grace_exactly", overlayExpiry.Add(grace)},
	} {
		if !t.Run(point.name, func(t *testing.T) {
			appClock.Advance(t, point.at.Sub(appClock.Now()))
			before := b05PersistedState(t, s)
			_, err := svc.ReleaseWithOutcome(ctx, core, request, recoveryMeta)
			var typed *model.Error
			if !errors.As(err, &typed) || typed.Data.Code != "lease_expired" ||
				typed.Message != "bead claim is inside the reclaim grace window" {
				t.Fatalf("reclaim boundary=%s err=%v", point.name, err)
			}
			if b05PersistedState(t, s) != before {
				t.Fatal("refused reclaim changed complete logical rows, receipts or checkpoint bytes")
			}
		}) {
			return
		}
	}

	t.Run("overlay_grace_after_with_event_and_reopen_replay", func(t *testing.T) {
		appClock.Advance(t, time.Nanosecond)
		out, err := svc.ReleaseWithOutcome(ctx, core, request, recoveryMeta)
		if err != nil || out.Replayed {
			t.Fatalf("strictly after overlay grace reclaim=%+v err=%v", out, err)
		}
		recoveredRecord, recovered := b05Document(t, s, ref)
		if recovered.Lease != nil || recovered.Fields["status"] != "todo" ||
			len(recovered.Comments) != 1 || recovered.Comments[0].Body != request.Reason ||
			!reflect.DeepEqual(recovered.Comments[0].Principal, core) ||
			!recovered.Comments[0].Time.Equal(appClock.Now()) || recovered.Comments[0].Time.Equal(storeClock.Now()) ||
			recoveredRecord.Revision.String() != out.Commit.Revision.String() {
			t.Fatalf("persisted recovered document=%+v", recovered)
		}
		meta, err := s.Meta(ctx)
		if err != nil {
			t.Fatal(err)
		}
		r, err := s.Read(ctx, driver.View{Incarnation: meta.Incarnation})
		if err != nil {
			t.Fatal(err)
		}
		entries, logErr := r.Changesets(ctx, nil, 100)
		versions, versionErr := r.Versions(ctx, ref, driver.PageReq{Limit: 100})
		if err := r.Close(); err != nil {
			t.Fatal(err)
		}
		if logErr != nil || versionErr != nil || len(entries) != 3 || len(versions.Items) != 3 || versions.NextCursor != nil {
			t.Fatalf("actual atomic recovery history: changes=%d versions=%d logErr=%v versionErr=%v", len(entries), len(versions.Items), logErr, versionErr)
		}
		// The Writer supplies nonzero ChangesetHeader.Time from appClock.
		// The driver store/SQL clock is only the zero-header fallback, which
		// this domain path does not reach. Read back the supplied timestamp.
		entry := entries[2]
		if entry.Revision.String() != out.Commit.Revision.String() || !reflect.DeepEqual(entry.Principal, core) ||
			!entry.Time.Equal(appClock.Now()) || entry.Time.Equal(storeClock.Now()) ||
			len(entry.Events) != 1 || entry.Events[0].Type != model.EventBeadLeaseReclaimed ||
			entry.Events[0].Provenance.Method != "bead.release" || entry.Events[0].Provenance.Cause.Root != recoveryMeta.IdempotencyKey {
			t.Fatalf("persisted reclaim event/application time=%+v", entry)
		}

		// The accepted recovery and its ordinary receipt survive another full
		// reopen. A replay must preserve every logical row and both checkpoints.
		recoveredState := b05PersistedState(t, s)
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		s = open()
		svc, _ = b05Service(t, s, appClock)
		if b05PersistedState(t, s) != recoveredState {
			t.Fatal("recovery or receipt bytes changed across second full reopen")
		}
		replay, err := svc.ReleaseWithOutcome(ctx, core, request, recoveryMeta)
		if err != nil || !replay.Replayed || !reflect.DeepEqual(replay.Commit, out.Commit) ||
			b05PersistedState(t, s) != recoveredState {
			t.Fatalf("reopened recovery replay=%+v err=%v", replay, err)
		}
		t.Log("real persisted claim/heartbeat/comment/changeset fields follow application Go time; SQLite server-lease expiry follows the injected Go store clock; no Beads SQL-generated claim twin")
	})
}
