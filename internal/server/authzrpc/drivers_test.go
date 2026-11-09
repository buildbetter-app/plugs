package authzrpc_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/plugins/authnapikey"
	"github.com/buildbetter-app/plugs/internal/plugins/authzfixed"
	"github.com/buildbetter-app/plugs/internal/server/authzrpc"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/internal/storage/postgres"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

type storeFactory func(*testing.T) driver.Store

// These tests use actual drivers and the authenticated HTTP handler. PostgreSQL
// is always an owned disposable engine on the runner's local Docker socket;
// no caller-supplied DSN or Docker context can select an external service.
func TestFixedPermissionsAcrossDrivers(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			var open storeFactory
			switch engine {
			case "memory":
				open = func(*testing.T) driver.Store { return memory.New() }
			case "sqlite":
				open = func(t *testing.T) driver.Store {
					s, err := (sqlite.Driver{}).Open(context.Background(), driver.Config{DSN: filepath.Join(t.TempDir(), "authz.db")})
					if err != nil {
						t.Fatal(err)
					}
					return s
				}
			case "postgres":
				open = postgresFactory(t)
			}
			t.Run("immutable_registry", func(t *testing.T) { immutableRegistry(t, open) })
			t.Run("compiled_lists_equal_point_reads", func(t *testing.T) { listMembership(t, open) })
			t.Run("ready_order_cursor", func(t *testing.T) { readyMembershipCursorInvalidation(t, open) })
			t.Run("ready_unknown_creation_authorization", func(t *testing.T) { readyUnknownCreationAuthorization(t, open) })
			t.Run("history_uses_current_permissions", func(t *testing.T) { currentHistory(t, open) })
			t.Run("post_image", func(t *testing.T) { postImage(t, open) })
			t.Run("authorization_races", func(t *testing.T) { authorizationRaces(t, open) })
			t.Run("source_publication_failure", func(t *testing.T) { sourcePublicationFailure(t, open) })
			t.Run("initial_source_publication", func(t *testing.T) { initialSourcePublication(t, open) })
			t.Run("writer_boolean_presence_and_restamping", func(t *testing.T) { writerPublishesKernelBooleanAndSourceOnlyRestamps(t, open) })
			t.Run("ready_facts_carry_forward", func(t *testing.T) { readyFactsCarryForward(t, open) })
		})
	}
}

func readyFactsCarryForward(t *testing.T, open storeFactory) {
	for _, mode := range []string{"maintained", "unmaintained", "corrupt", "drift"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			store := &authorizationRaceStore{Store: open(t)}
			t.Cleanup(func() { _ = store.Close() })
			lease, err := store.Lease(ctx, "ready-facts", driver.LeaseTTL)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = lease.Release(ctx) })
			meta, err := store.Meta(ctx)
			if err != nil {
				t.Fatal(err)
			}
			base := authzfixed.Facts{Namespace: authz.Namespace{StoreID: string(meta.StoreID), Incarnation: meta.Incarnation, InstallationID: "fixed", Authority: model.RefByID("authority", model.Natural64(0))}}
			facts, err := authzrpc.NewMutableFacts(base)
			if err != nil {
				t.Fatal(err)
			}
			keys := authnapikey.New(authnapikey.Config{AuthorizationInstalled: func() bool { return true }})
			if _, err := authzrpc.New(ctx, authzrpc.Config{Store: store, Lease: lease, Provider: keys, Facts: facts, Namespace: base.Namespace}); err != nil {
				t.Fatal(err)
			}
			if err := facts.Replace(base); err != nil {
				t.Fatal(err)
			}
			old, err := store.Read(ctx, driver.View{Incarnation: meta.Incarnation})
			if err != nil {
				t.Fatal(err)
			}
			defer old.Close()
			rows, err := old.DerivedRows(ctx, driver.DerivedReadyState, "")
			if err != nil || len(rows) != 1 || rows[0].Key != "plug.ready.v2/coverage" {
				t.Fatalf("empty initial publication lacks its exact Ready marker: %+v/%v", rows, err)
			}
			var marker struct {
				Version     int
				Incarnation string
				Through     model.Revision
				Unsafe      uint64
			}
			if err := json.Unmarshal(rows[0].Value, &marker); err != nil || marker.Version != 2 || marker.Incarnation != meta.Incarnation || marker.Through.String() != old.Snapshot().At.String() {
				t.Fatal("initial Ready marker is not bound to the fact publication")
			}
			want := []driver.DerivedRow{rows[0].Clone()}
			if mode == "unmaintained" || mode == "corrupt" {
				tx, err := store.Write(ctx, lease)
				if err != nil {
					t.Fatal(err)
				}
				defer tx.Close()
				var locks driver.LockSet
				var sentinel driver.Sentinel
				if mode == "corrupt" {
					sentinel, err = driver.NewReadySentinel("*", "*")
					if err != nil {
						t.Fatal(err)
					}
					rowKey := driver.DerivedRowKey{Table: driver.DerivedReadyState, Key: rows[0].Key}
					locks = driver.LockSet{{Key: driver.LockKey{Kind: driver.LockDerived, Derived: &rowKey}, Mode: driver.LockUpdate}, {Key: driver.LockKey{Kind: driver.LockSentinel, Sentinel: sentinel}, Mode: driver.LockUpdate}}
					sort.Slice(locks, func(i, j int) bool {
						a, _ := driver.EncodeLockKey(locks[i].Key)
						b, _ := driver.EncodeLockKey(locks[j].Key)
						return a < b
					})
				}
				if _, err := tx.Lock(ctx, locks); err != nil {
					t.Fatal(err)
				}
				if _, err := tx.Head(ctx); err != nil {
					t.Fatal(err)
				}
				if mode == "corrupt" {
					want[0].Value = []byte("malformed optional Ready metadata")
					if err := tx.PutDerived(ctx, driver.DerivedReadyState, nil, want); err != nil {
						t.Fatal(err)
					}
					if err := tx.BumpSentinels(ctx, []driver.Sentinel{sentinel}); err != nil {
						t.Fatal(err)
					}
				}
				lockKeys := make([]driver.LockKey, len(locks))
				for i, lock := range locks {
					lockKeys[i] = lock.Key
				}
				if _, err := tx.Commit(ctx, driver.ChangesetHeader{Incarnation: meta.Incarnation, Kind: model.ChangesetDerived, Principal: model.PrincipalRef{Kind: "system", ID: "ready-gap"}, LockKeys: lockKeys}); err != nil {
					t.Fatal(err)
				}
			}
			before, err := store.Meta(ctx)
			if err != nil {
				t.Fatal(err)
			}
			var competing model.Revision
			var competingRef model.Ref
			var calls int
			if mode == "drift" {
				// The fact publisher has already pinned its Ready observations
				// when Store.Write runs. Commit a real maintained source mutation
				// before its lock census, without changing the ACL/source guards.
				hook := func() error {
					calls++
					o := service.NewOntology(nil)
					w := pipeline.New(store.Store, lease, pipeline.WithOntology(o))
					svc := service.New(store.Store, lease, w, service.WithOntology(o))
					commit, err := svc.MemorySave(ctx, model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}, service.MemorySaveRequest{Key: "ready-fact-drift", Content: "competing maintained source"}, model.WriteMeta{Project: "local", Agent: "ready-test", AgentLabel: "ready-test", IdempotencyKey: fmt.Sprintf("%064x", 1)})
					if err != nil {
						return err
					}
					if len(commit.Results) != 1 {
						return errors.New("competing source did not return its record")
					}
					competingRef = commit.Results[0].Ref
					current, err := store.Store.Meta(ctx)
					competing = current.Head
					return err
				}
				store.beforeWrite.Store(&hook)
			}
			if err := facts.Replace(base); err != nil {
				t.Fatalf("optional Ready state refused an ordinary fact publication: %v", err)
			}
			after, err := store.Read(ctx, driver.View{Incarnation: meta.Incarnation})
			if err != nil {
				t.Fatal(err)
			}
			defer after.Close()
			n, _ := before.Head.Uint64()
			if mode == "drift" {
				if calls != 1 || competing.String() != model.Natural64(n+1).String() {
					t.Fatalf("competing source did not commit exactly once: calls=%d head=%s before=%s", calls, competing, before.Head)
				}
				n++
				records, err := after.Get(ctx, []model.Ref{competingRef})
				if err != nil || len(records) != 1 || records[0].Revision.String() != competing.String() || !strings.Contains(string(records[0].Body), "competing maintained source") {
					t.Fatalf("competing source was lost or restamped: %+v/%v", records, err)
				}
				marker.Through = competing
				want[0].Value, err = json.Marshal(marker)
				if err != nil {
					t.Fatal(err)
				}
			}
			if after.Snapshot().At.String() != model.Natural64(n+1).String() {
				t.Fatal("fact publication did not retain one durable H+1 commit")
			}
			if mode == "maintained" {
				marker.Through = *after.Snapshot().At
				want[0].Value, err = json.Marshal(marker)
				if err != nil {
					t.Fatal(err)
				}
			}
			got, err := after.DerivedRows(ctx, driver.DerivedReadyState, "")
			if err != nil || !reflect.DeepEqual(want, got) {
				t.Fatalf("Ready carry-forward or fallback changed the wrong bytes: want%+v got%+v/%v", want, got, err)
			}
			pinned, err := old.DerivedRows(ctx, driver.DerivedReadyState, "")
			if err != nil || !reflect.DeepEqual(rows, pinned) {
				t.Fatal("fact maintenance rewrote a pinned Ready marker")
			}
			if _, err := facts.Load(ctx, after); err != nil {
				t.Fatal("Ready maintenance lost durable fact/source/ACL binding", err)
			}
		})
	}
}

func fixtureRef(n int) model.Ref {
	return model.RefByID(fmt.Sprintf("00000000-0000-4000-8000-%012d", n), model.Natural64(0))
}

func matrixFixture(t *testing.T, open storeFactory) *fixture {
	t.Helper()
	f := newFixtureOn(t, open(t), nil)
	next := f.base
	next.Projects = append(append([]authzfixed.Project(nil), next.Projects...),
		authzfixed.Project{Ref: model.RefByID("inactive", model.Natural64(0)), Name: "inactive", InstallationID: "fixed"},
		authzfixed.Project{Ref: model.RefByID("other", model.Natural64(0)), Name: "other", InstallationID: "fixed", Active: true})
	if err := f.facts.Replace(next); err != nil {
		t.Fatal(err)
	}
	f.base = next
	var changes []conformance.FixtureChange
	for i, project := range []string{"unregistered", "unregistered", "inactive", "other", "project", "project"} {
		ref := fixtureRef(i + 2)
		body, err := json.Marshal(map[string]any{"id": ref.ID, "type": "issue", "project": project, "createdAt": "2026-10-01T00:00:00Z", "fields": map[string]any{"title": "historical", "status": "todo", "priority": min(i+1, 4)}})
		if err != nil {
			t.Fatal(err)
		}
		changes = append(changes, conformance.FixtureChange{Ref: ref, Kind: "bead", Op: model.OpCreate, Body: body})
	}
	if _, err := conformance.Seed(context.Background(), f.store, f.lease, driver.ChangesetHeader{}, changes); err != nil {
		t.Fatal(err)
	}
	return f
}

type rpcPage struct {
	Items []struct {
		ID     string         `json:"id"`
		Fields map[string]any `json:"fields"`
	} `json:"items"`
	NextCursor string `json:"nextCursor"`
	AsOf       struct {
		Revision    model.Revision `json:"revision"`
		Incarnation string         `json:"incarnation"`
	} `json:"asOf"`
}

func page(t *testing.T, got reply) rpcPage {
	t.Helper()
	success(t, got)
	var p rpcPage
	if err := json.Unmarshal(got.Result, &p); err != nil {
		t.Fatal(err)
	}
	return p
}

func listMembership(t *testing.T, open storeFactory) {
	f := matrixFixture(t, open)
	// Expected membership is fixture-authored, not computed by Eval, Compile,
	// any SQL projection or the point-read response being compared with lists.
	for _, row := range []struct {
		name string
		by   model.PrincipalRef
		ids  []int
	}{
		{"user", model.PrincipalRef{Kind: "user", ID: "user:reader"}, []int{1, 2, 3, 4, 5, 6, 7}},
		{"user_refs", model.PrincipalRef{Kind: "user", ID: "user:scoped", Caveats: map[string]any{"refs": []model.Ref{fixtureRef(1), fixtureRef(3), fixtureRef(7)}}}, []int{1, 3, 7}},
		{"user_projects", model.PrincipalRef{Kind: "user", ID: "user:projects", Caveats: map[string]any{"projects": []model.Ref{model.RefByID("project", model.Natural64(0)), model.RefByID("inactive", model.Natural64(0))}}}, []int{1, 4, 6, 7}},
		{"agent_with_involved_sponsor", worker("user:operator"), nil},
		{"user_empty_caveat", model.PrincipalRef{Kind: "user", ID: "user:empty", Caveats: map[string]any{"refs": []model.Ref{}}}, nil},
	} {
		t.Run(row.name, func(t *testing.T) {
			want := map[string]bool{}
			for _, n := range row.ids {
				want[fixtureRef(n).ID] = true
			}
			for n := 1; n <= 7; n++ {
				got := f.call(t, row.by, "bead.get", map[string]any{"id": fixtureRef(n)}, 1000+n)
				if want[fixtureRef(n).ID] {
					success(t, got)
				} else if got.Error == nil || got.Error.Data.Code != "not_found" {
					t.Fatalf("denied point read leaked a result: %+v", got)
				}
			}
			for _, scope := range []struct {
				name string
				ids  []int
			}{{"project", []int{1, 6, 7}}, {"unregistered", []int{2, 3}}, {"inactive", []int{4}}, {"other", []int{5}}} {
				params := map[string]any{"limit": 1, "_meta": map[string]any{"project": scope.name}}
				var ids []string
				for pages := 0; ; pages++ {
					if pages > 7 {
						t.Fatal("authorized pagination failed to terminate")
					}
					got := page(t, f.call(t, row.by, "bead.ready", params, 1100))
					for _, item := range got.Items {
						ids = append(ids, item.ID)
					}
					if got.NextCursor == "" {
						break
					}
					params["cursor"] = got.NextCursor
				}
				var expected []string
				for _, n := range scope.ids {
					if want[fixtureRef(n).ID] {
						expected = append(expected, fixtureRef(n).ID)
					}
				}
				if !reflect.DeepEqual(ids, expected) {
					t.Fatalf("compiled list differs from independent per-item membership: project=%s got=%v want=%v", scope.name, ids, expected)
				}
			}
		})
	}
}

// readyMembershipCursorInvalidation is the guarded counterpart to the core
// ready-order fixtures. The complete authorized sequence is captured in the
// signed cursor; changing a recent row's priority reorders that sequence and
// must refuse the old offset without disclosing a partial page.
func readyMembershipCursorInvalidation(t *testing.T, open storeFactory) {
	f := newFixtureOn(t, open(t), nil)
	by := model.PrincipalRef{Kind: "user", ID: "user:reader"}
	createdAt := time.Now().UTC().Add(-time.Minute)
	changes := make([]conformance.FixtureChange, 0, 2)
	for i, n := range []int{8, 9} {
		ref := fixtureRef(n)
		body, err := json.Marshal(map[string]any{
			"id": ref.ID, "type": "issue", "project": "project", "createdAt": createdAt,
			"fields": map[string]any{"title": "ready-order", "status": "todo", "priority": i},
		})
		if err != nil {
			t.Fatal(err)
		}
		changes = append(changes, conformance.FixtureChange{Ref: ref, Kind: "bead", Op: model.OpCreate, Body: body})
	}
	if _, err := conformance.Seed(context.Background(), f.store, f.lease, driver.ChangesetHeader{}, changes); err != nil {
		t.Fatal(err)
	}
	params := map[string]any{"limit": 1, "_meta": map[string]any{"project": "project"}}
	first := page(t, f.call(t, by, "bead.ready", params, 7010))
	if len(first.Items) != 1 || first.Items[0].ID != fixtureRef(8).ID || first.NextCursor == "" {
		t.Fatalf("recent ready order did not produce a continuation: %+v", first)
	}
	updated, err := json.Marshal(map[string]any{
		"id": fixtureRef(8).ID, "type": "issue", "project": "project", "createdAt": createdAt,
		"fields": map[string]any{"title": "ready-order", "status": "todo", "priority": 4},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conformance.Seed(context.Background(), f.store, f.lease, driver.ChangesetHeader{}, []conformance.FixtureChange{{
		Ref: fixtureRef(8), Kind: "bead", Op: model.OpPatch, Body: updated,
	}}); err != nil {
		t.Fatal(err)
	}
	params["cursor"] = first.NextCursor
	refused := f.call(t, by, "bead.ready", params, 7011)
	if refused.Error == nil || refused.Error.Data.Code != "cursor_invalid" || len(refused.Result) != 0 {
		t.Fatalf("ordered ready membership change was not an atomic cursor refusal: %+v", refused)
	}
}

func readyUnknownCreationAuthorization(t *testing.T, open storeFactory) {
	f := newFixtureOn(t, open(t), nil)
	known := []byte(fmt.Sprintf(`{"id":%q,"type":"issue","project":"project","createdAt":"2026-11-03T00:00:00Z","fields":{"title":"known","status":"todo","priority":0}}`, fixtureRef(8).ID))
	unknown := []byte(fmt.Sprintf(`{"id":%q,"type":"issue","project":"project","fields":{"title":"unknown legacy creation","status":"todo","priority":1}}`, fixtureRef(9).ID))
	if _, err := conformance.Seed(context.Background(), f.store, f.lease, driver.ChangesetHeader{}, []conformance.FixtureChange{
		{Ref: fixtureRef(8), Kind: "bead", Op: model.OpCreate, Body: known, Graph: &conformance.FixtureGraphSource{}},
		{Ref: fixtureRef(9), Kind: "bead", Op: model.OpCreate, Body: unknown, Graph: &conformance.FixtureGraphSource{}},
	}); err != nil {
		t.Fatal(err)
	}
	before := f.reader(t).Snapshot().At.String()
	t.Run("denied_unknown_does_not_refuse_known_set", func(t *testing.T) {
		by := model.PrincipalRef{Kind: "user", ID: "user:known-only", Caveats: map[string]any{"refs": []model.Ref{fixtureRef(8)}}}
		got := page(t, f.call(t, by, "bead.ready", map[string]any{"limit": 1}, 7020))
		if len(got.Items) != 1 || got.Items[0].ID != fixtureRef(8).ID || got.NextCursor != "" {
			t.Fatalf("denied unknown creation changed the full authorized set: %+v", got)
		}
	})
	t.Run("authorized_unknown_still_refuses_atomically", func(t *testing.T) {
		by := model.PrincipalRef{Kind: "user", ID: "user:unknown-reader", Caveats: map[string]any{"refs": []model.Ref{fixtureRef(8), fixtureRef(9)}}}
		got := f.call(t, by, "bead.ready", map[string]any{"limit": 1}, 7021)
		if got.Error == nil || got.Error.Data.Code != "internal_error" || got.Error.Data.Details["reason"] != "ready_creation_time_unavailable" || len(got.Result) != 0 {
			t.Fatalf("authorized unknown creation lost its typed atomic refusal: %+v", got)
		}
	})
	if after := f.reader(t).Snapshot().At.String(); after != before {
		t.Fatal("known/unknown creation reads published a changeset")
	}
}

func TestB01GuardedReadyCutoffCursorAcrossDrivers(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			var dsn func(*testing.T) string
			if engine == "postgres" {
				dsn = postgresDSNFactory(t)
			}
			f := newQuietFixture(t, engine, dsn)
			var changes []conformance.FixtureChange
			for i, n := range []int{8, 9} {
				at, priority := f.clock.Now(), 4
				if i == 0 {
					at, priority = at.Add(-48*time.Hour), 0
				}
				body, err := json.Marshal(map[string]any{"id": fixtureRef(n).ID, "type": "issue", "project": "project", "createdAt": at, "fields": map[string]any{"title": "cutoff", "status": "todo", "priority": priority}})
				if err != nil {
					t.Fatal(err)
				}
				changes = append(changes, conformance.FixtureChange{Ref: fixtureRef(n), Kind: "bead", Op: model.OpCreate, Body: body, Graph: &conformance.FixtureGraphSource{}})
			}
			if _, err := conformance.Seed(context.Background(), f.store, f.lease, driver.ChangesetHeader{}, changes); err != nil {
				t.Fatal(err)
			}
			by := model.PrincipalRef{Kind: "user", ID: "user:cutoff-reader", Caveats: map[string]any{"refs": []model.Ref{fixtureRef(8), fixtureRef(9)}}}
			first := page(t, f.call(t, by, "bead.ready", map[string]any{"limit": 1}, 7030))
			if len(first.Items) != 1 || first.Items[0].ID != fixtureRef(8).ID || first.NextCursor == "" {
				t.Fatalf("exactly-48h row did not lead the recent bucket: %+v", first)
			}
			params := map[string]any{"limit": 1, "cursor": first.NextCursor}
			stable := page(t, f.call(t, by, "bead.ready", params, 7031))
			if len(stable.Items) != 1 || stable.Items[0].ID != fixtureRef(9).ID || stable.NextCursor != "" {
				t.Fatalf("unchanged guarded order rejected or changed continuation: %+v", stable)
			}
			before := f.reader(t).Snapshot().At.String()
			// Keep the same handler, signing key, authority, incarnation and
			// unexpired storage lease. One SQL-representable microsecond crosses
			// the cutoff without a write or a constructor/lease reacquisition.
			f.clock.Advance(t, time.Microsecond)
			got := f.call(t, by, "bead.ready", params, 7032)
			if got.Error == nil || got.Error.Data.Code != "cursor_invalid" || len(got.Result) != 0 {
				t.Fatalf("no-write guarded cutoff reorder accepted an old offset: %+v", got)
			}
			fresh := page(t, f.call(t, by, "bead.ready", map[string]any{"limit": 1}, 7033))
			if len(fresh.Items) != 1 || fresh.Items[0].ID != fixtureRef(9).ID || fresh.AsOf.Revision.String() != first.AsOf.Revision.String() || fresh.AsOf.Incarnation != first.AsOf.Incarnation {
				t.Fatalf("cutoff did not reverse the authorized order at the same source: %+v", fresh)
			}
			if after := f.reader(t).Snapshot().At.String(); after != before {
				t.Fatal("cutoff check changed durable source or authorization")
			}
		})
	}
}

type readyCheckOnlyRules struct{ deniedID string }

func (r readyCheckOnlyRules) Eval(_ context.Context, _ authz.Slice, requests []authz.CheckRequest) ([]authz.Decision, error) {
	decisions := make([]authz.Decision, len(requests))
	for i, request := range requests {
		decisions[i].Effect = authz.NoDecision
		if request.Resource.Ref.ID == r.deniedID {
			decisions[i].Effect = authz.Deny
		}
	}
	return decisions, nil
}

func (readyCheckOnlyRules) Compile(authz.ListRequest) (authz.FilterTemplate, error) {
	return authz.FilterTemplate{}, authz.ErrCheckOnly
}

func TestB01CheckOnlyReadyOrderingAcrossDrivers(t *testing.T) {
	countDrivers(t, func(t *testing.T, open storeFactory) {
		f := newFixtureOn(t, open(t), nil)
		createdAt := time.Now().UTC()
		body := func(n, priority int) []byte {
			raw, err := json.Marshal(map[string]any{"id": fixtureRef(n).ID, "type": "issue", "project": "project", "createdAt": createdAt, "fields": map[string]any{"title": "check-only-order", "status": "todo", "priority": priority}})
			if err != nil {
				t.Fatal(err)
			}
			return raw
		}
		var changes []conformance.FixtureChange
		for i, n := range []int{8, 9, 10} {
			changes = append(changes, conformance.FixtureChange{Ref: fixtureRef(n), Kind: "bead", Op: model.OpCreate, Body: body(n, i), Graph: &conformance.FixtureGraphSource{}})
		}
		if _, err := conformance.Seed(context.Background(), f.store, f.lease, driver.ChangesetHeader{}, changes); err != nil {
			t.Fatal(err)
		}
		authzrpc.AddRuleForTest(f.server, readyCheckOnlyRules{deniedID: fixtureRef(8).ID})
		by := model.PrincipalRef{Kind: "user", ID: "user:bounded-ready", Caveats: map[string]any{"refs": []model.Ref{fixtureRef(8), fixtureRef(9), fixtureRef(10)}}}
		first := page(t, f.call(t, by, "bead.ready", map[string]any{"limit": 1}, 7080))
		if len(first.Items) != 1 || first.Items[0].ID != fixtureRef(9).ID || first.NextCursor == "" {
			t.Fatalf("check-only denial consumed the first authorized page: %+v", first)
		}
		params := map[string]any{"limit": 1, "cursor": first.NextCursor}
		assertTail := func(key int) {
			t.Helper()
			got := page(t, f.call(t, by, "bead.ready", params, key))
			if len(got.Items) != 1 || got.Items[0].ID != fixtureRef(10).ID || got.NextCursor != "" {
				t.Fatalf("check-only continuation lost authorized order or leaked denial: %+v", got)
			}
		}
		assertTail(7081)
		if _, err := conformance.Seed(context.Background(), f.store, f.lease, driver.ChangesetHeader{}, []conformance.FixtureChange{{Ref: fixtureRef(8), Kind: "bead", Op: model.OpPatch, Body: body(8, 4), Graph: &conformance.FixtureGraphSource{}}}); err != nil {
			t.Fatal(err)
		}
		// A denied row's reorder and revision must not change the fingerprint
		// or expose its existence through an otherwise stable continuation.
		assertTail(7082)
	})
}

func TestB01CompiledReadyFilterCapBoundaryAcrossDrivers(t *testing.T) {
	countDrivers(t, func(t *testing.T, open storeFactory) {
		store := &readyQueryStore{Store: open(t)}
		f := newFixtureOn(t, store, nil)
		renewCountFixtureLease(t, f.lease)
		const denied, allowed = 20001, 3
		createdAt := time.Now().UTC()
		body := func(n, priority int) []byte {
			raw, err := json.Marshal(map[string]any{"id": fixtureRef(n).ID, "type": "issue", "project": "project", "createdAt": createdAt, "fields": map[string]any{"title": "finite-ready-cap", "status": "todo", "priority": priority}})
			if err != nil {
				t.Fatal(err)
			}
			return raw
		}
		for start := 0; start < denied+allowed; start += 1000 {
			var changes []conformance.FixtureChange
			for i := start; i < min(start+1000, denied+allowed); i++ {
				changes = append(changes, conformance.FixtureChange{Ref: fixtureRef(100 + i), Kind: "bead", Op: model.OpCreate, Body: body(100+i, 1), Graph: &conformance.FixtureGraphSource{}})
			}
			if _, err := conformance.Seed(context.Background(), f.store, f.lease, driver.ChangesetHeader{}, changes); err != nil {
				t.Fatal(err)
			}
		}
		callReady := func(by model.PrincipalRef, params map[string]any, key int) reply {
			t.Helper()
			store.pages.Store(0)
			store.rows.Store(0)
			got := f.call(t, by, "bead.ready", params, key)
			// The complete real source is read once, even when the trusted
			// guard traverses more than20 internal1000-row ready pages. Each
			// subsequent page still owns a fresh reader and policy evaluation.
			const records = denied + allowed + 1 // includes the original seed
			if pages, rows := store.pages.Load(), store.rows.Load(); pages != (records+999)/1000 || rows != records {
				t.Fatalf("ready reread or truncated the full source: pages=%d rows=%d error=%+v result=%s", pages, rows, got.Error, got.Result)
			}
			return got
		}
		t.Run("denied_prefix_does_not_consume_cap_or_pages", func(t *testing.T) {
			refs := []model.Ref{fixtureRef(100 + denied), fixtureRef(101 + denied), fixtureRef(102 + denied)}
			by := model.PrincipalRef{Kind: "user", ID: "user:finite-ready-reader", Caveats: map[string]any{"refs": refs}}
			params := map[string]any{"limit": 1}
			for i, ref := range refs {
				got := page(t, callReady(by, params, 7040+i))
				if len(got.Items) != 1 || got.Items[0].ID != ref.ID || (i == len(refs)-1) != (got.NextCursor == "") {
					t.Fatalf("finite filter lost an authorized tail after %d denied rows: page=%d %+v", denied, i, got)
				}
				params["cursor"] = got.NextCursor
			}
		})
		t.Run("full_authorized_set_exceeds_cap_and_binds_tail", func(t *testing.T) {
			by := model.PrincipalRef{Kind: "user", ID: "user:all-ready-reader"}
			first := page(t, callReady(by, map[string]any{"limit": 1}, 7050))
			if len(first.Items) != 1 || first.Items[0].ID != fixtureRef(100).ID || first.NextCursor == "" {
				t.Fatalf("complete compiled set above 20,000 was capped or reordered: %+v", first)
			}
			last := 100 + denied + allowed - 1
			if _, err := conformance.Seed(context.Background(), f.store, f.lease, driver.ChangesetHeader{}, []conformance.FixtureChange{{Ref: fixtureRef(last), Kind: "bead", Op: model.OpPatch, Body: body(last, 0), Graph: &conformance.FixtureGraphSource{}}}); err != nil {
				t.Fatal(err)
			}
			got := callReady(by, map[string]any{"limit": 1, "cursor": first.NextCursor}, 7051)
			if got.Error == nil || got.Error.Data.Code != "cursor_invalid" || len(got.Result) != 0 {
				t.Fatalf("authorized tail beyond 20,000 was missing from full fingerprint: %+v", got)
			}
			fresh := page(t, callReady(by, map[string]any{"limit": 1}, 7052))
			if len(fresh.Items) != 1 || fresh.Items[0].ID != fixtureRef(last).ID {
				t.Fatalf("authorized tail was discarded rather than reordered: %+v", fresh)
			}
		})
		t.Run("genuine_check_only_still_refuses_without_partial_result", func(t *testing.T) {
			before := f.reader(t).Snapshot().At.String()
			authzrpc.AddRuleForTest(f.server, readyCheckOnlyRules{})
			got := callReady(model.PrincipalRef{Kind: "user", ID: "user:check-only-ready"}, map[string]any{"limit": 1}, 7060)
			if got.Error == nil || got.Error.Data.Code != "authz_postfilter_cap" || len(got.Result) != 0 {
				t.Fatalf("check-only candidate cap returned a partial or unbounded result: error=%+v result=%s", got.Error, got.Result)
			}
			if after := f.reader(t).Snapshot().At.String(); after != before {
				t.Fatal("check-only refusal changed durable source")
			}
		})
	})
}

type readyQueryStore struct {
	driver.Store
	pages atomic.Int64
	rows  atomic.Int64
}

func (s *readyQueryStore) Read(ctx context.Context, view driver.View) (driver.Reader, error) {
	r, err := s.Store.Read(ctx, view)
	if err != nil {
		return nil, err
	}
	return &readyQueryReader{Reader: r, store: s}, nil
}

type readyQueryReader struct {
	driver.Reader
	store *readyQueryStore
}

func (r *readyQueryReader) Query(ctx context.Context, query driver.Query) (driver.Page[driver.Record], error) {
	page, err := r.Reader.Query(ctx, query)
	if query.Filter["kind"] == "bead" {
		r.store.pages.Add(1)
		r.store.rows.Add(int64(len(page.Items)))
	}
	return page, err
}

func TestB01ReadySnapshotDependencyRefusalsAcrossDrivers(t *testing.T) {
	countDrivers(t, func(t *testing.T, open storeFactory) {
		store := &countAuthorityStore{Store: open(t)}
		f := newFixtureOn(t, store, nil)
		for _, dep := range []authz.Dependency{
			authz.ContextDependency(f.server.Namespace), authz.ACLDependency(f.server.Namespace),
			{Kind: "sentinel", Key: "plugins"}, {Kind: "sentinel", Key: "ontology"},
		} {
			t.Run(dep.Kind+"_"+dep.Key, func(t *testing.T) {
				var key driver.LockKey
				if dep.Kind == "read_fact" {
					fact, err := driver.NewReadFact(dep.Key)
					if err != nil {
						t.Fatal(err)
					}
					key = driver.LockKey{Kind: driver.LockReadFact, Fact: fact}
				} else {
					sentinel, err := driver.NewSentinel(dep.Key)
					if err != nil {
						t.Fatal(err)
					}
					key = driver.LockKey{Kind: driver.LockSentinel, Sentinel: sentinel}
				}
				encoded, err := driver.EncodeLockKey(key)
				if err != nil {
					t.Fatal(err)
				}
				store.failKey = encoded
				defer func() { store.failKey = "" }()
				got := f.call(t, model.PrincipalRef{Kind: "user", ID: "user:ready-dependency"}, "bead.ready", map[string]any{"limit": 1}, 7070)
				if got.Error == nil || got.Error.Data.Code != "authz_unavailable" || len(got.Result) != 0 {
					t.Fatalf("Ready ignored an unavailable real snapshot dependency: %+v", got)
				}
			})
		}
	})
}

func TestB01GuardedReadyInternalPagesAcrossDrivers(t *testing.T) {
	countDrivers(t, func(t *testing.T, open storeFactory) {
		for _, mode := range []string{"project_caveat_stable", "compiled_mutation", "check_only_mutation"} {
			t.Run(mode, func(t *testing.T) {
				f := newFixtureOn(t, open(t), nil)
				renewCountFixtureLease(t, f.lease)
				createdAt := time.Now().UTC()
				for start := 0; start < 1001; start += 1000 {
					var changes []conformance.FixtureChange
					for i := start; i < min(start+1000, 1001); i++ {
						ref := fixtureRef(100 + i)
						body, err := json.Marshal(map[string]any{"id": ref.ID, "type": "issue", "project": "project", "createdAt": createdAt, "fields": map[string]any{"title": "internal-pages", "status": "todo", "priority": 1}})
						if err != nil {
							t.Fatal(err)
						}
						changes = append(changes, conformance.FixtureChange{Ref: ref, Kind: "bead", Op: model.OpCreate, Body: body, Graph: &conformance.FixtureGraphSource{}})
					}
					if _, err := conformance.Seed(context.Background(), f.store, f.lease, driver.ChangesetHeader{}, changes); err != nil {
						t.Fatal(err)
					}
				}
				by := model.PrincipalRef{Kind: "user", ID: "user:internal-project-reader", Caveats: map[string]any{"projects": []model.Ref{model.RefByID("project", model.Natural64(0))}}}
				beforeHead, beforeACL := publicationState(t, f.store, f.server.Namespace)
				if mode != "project_caveat_stable" {
					if mode == "check_only_mutation" {
						authzrpc.AddRuleForTest(f.server, readyCheckOnlyRules{})
					}
					barriers := 0
					authzrpc.AfterFirstReadyPageForTest(f.server, func(first service.BeadPage) {
						barriers++
						if len(first.Items) != 1000 || first.Next == nil || first.Items[0].ID != fixtureRef(100).ID {
							t.Fatalf("barrier did not follow an actual first1000-row domain page: %+v", first)
						}
						// A real public Writer update moves the prefix to the tail.
						// Keeping offset1000 would omit row1100 and repeat row100.
						success(t, f.call(t, worker("user:operator"), "bead.update", map[string]any{"id": fixtureRef(100), "fields": map[string]any{"priority": 4}}, 7090))
					})
					got := f.call(t, by, "bead.ready", map[string]any{"limit": 1}, 7091)
					if barriers != 1 || got.Error == nil || got.Error.Data.Code != "cursor_invalid" || len(got.Result) != 0 {
						t.Fatalf("mixed internal durable pages produced a partial or fingerprinted result: barriers=%d %+v", barriers, got)
					}
					afterHead, afterACL := publicationState(t, f.store, f.server.Namespace)
					if afterHead.String() == beforeHead.String() || !reflect.DeepEqual(beforeACL, afterACL) {
						t.Fatal("barrier failed to make a real content-only write under the same authority")
					}
					beforeHead = afterHead
				}
				first := page(t, f.call(t, by, "bead.ready", map[string]any{"limit": 1000}, 7092))
				if len(first.Items) != 1000 || first.NextCursor == "" {
					t.Fatalf("stable internal-reader traversal lost a full authorized page: %+v", first)
				}
				start := 100
				if mode != "project_caveat_stable" {
					start++
				}
				for i, item := range first.Items {
					if item.ID != fixtureRef(start+i).ID {
						t.Fatalf("complete stable order at%d: %s", i, item.ID)
					}
				}
				last := page(t, f.call(t, by, "bead.ready", map[string]any{"limit": 1000, "cursor": first.NextCursor}, 7093))
				tail := fixtureRef(1100).ID
				if mode != "project_caveat_stable" {
					tail = fixtureRef(100).ID
				}
				if len(last.Items) != 2 || last.Items[0].ID != tail || last.Items[1].ID != beadID || last.NextCursor != "" || last.AsOf.Revision.String() != beforeHead.String() {
					t.Fatalf("stable continuation did not retain the exact complete authorized tail: %+v", last)
				}
				afterHead, afterACL := publicationState(t, f.store, f.server.Namespace)
				if afterHead.String() != beforeHead.String() || !reflect.DeepEqual(beforeACL, afterACL) {
					t.Fatal("stable internal paging changed source or authority")
				}
			})
		}
	})
}

func TestB01GuardedReadyInternalCutoffAcrossDrivers(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			var dsn func(*testing.T) string
			if engine == "postgres" {
				dsn = postgresDSNFactory(t)
			}
			f := newQuietFixture(t, engine, dsn)
			renewCountFixtureLease(t, f.lease)
			// Publish the setup baseline's complete typed root post-image in
			// backlog, retaining its identity, creation time and other fields.
			baseline := []byte(`{"id":"` + beadID + `","type":"issue","project":"project","createdAt":"2026-10-01T00:00:00Z","fields":{"title":"seed","status":"backlog","priority":0}}`)
			if _, err := conformance.Seed(context.Background(), f.store, f.lease, driver.ChangesetHeader{}, []conformance.FixtureChange{{
				Ref: quietRef(), Kind: "bead", Op: model.OpPatch, Body: baseline, Graph: &conformance.FixtureGraphSource{},
			}}); err != nil {
				t.Fatal(err)
			}
			for start := 0; start < 1002; start += 1000 {
				var changes []conformance.FixtureChange
				for i := start; i < min(start+1000, 1002); i++ {
					ref := fixtureRef(100 + i)
					at, priority := f.clock.Now(), 4
					if i == 0 {
						at, priority = at.Add(-48*time.Hour), 0
					}
					body, err := json.Marshal(map[string]any{"id": ref.ID, "type": "issue", "project": "project", "createdAt": at, "fields": map[string]any{"title": "internal-cutoff", "status": "todo", "priority": priority}})
					if err != nil {
						t.Fatal(err)
					}
					changes = append(changes, conformance.FixtureChange{Ref: ref, Kind: "bead", Op: model.OpCreate, Body: body, Graph: &conformance.FixtureGraphSource{}})
				}
				if _, err := conformance.Seed(context.Background(), f.store, f.lease, driver.ChangesetHeader{}, changes); err != nil {
					t.Fatal(err)
				}
			}
			by := model.PrincipalRef{Kind: "user", ID: "user:internal-cutoff", Caveats: map[string]any{"projects": []model.Ref{model.RefByID("project", model.Natural64(0))}}}
			beforeHead, beforeACL := publicationState(t, f.store, f.server.Namespace)
			control := page(t, f.call(t, by, "bead.ready", map[string]any{"limit": 1000}, 7101))
			if len(control.Items) != 1000 || control.Items[0].ID != fixtureRef(100).ID || control.NextCursor == "" {
				t.Fatal("stable internal cutoff control did not produce its exact first page")
			}
			controlTail := page(t, f.call(t, by, "bead.ready", map[string]any{"limit": 1000, "cursor": control.NextCursor}, 7102))
			if len(controlTail.Items) != 2 || controlTail.Items[0].ID != fixtureRef(1100).ID || controlTail.Items[1].ID != fixtureRef(1101).ID || controlTail.NextCursor != "" {
				t.Fatalf("unchanged-order internal continuation failed: %+v", controlTail)
			}
			barriers := 0
			authzrpc.AfterFirstReadyPageForTest(f.server, func(first service.BeadPage) {
				barriers++
				if len(first.Items) != 1000 || first.Items[0].ID != fixtureRef(100).ID || first.Next == nil {
					t.Fatal("cutoff barrier missed the actual first1000-row page")
				}
				// Same handler/signing key, live lease, incarnation, head and
				// authority. No write occurs; the actual SQL clock moves1us.
				f.clock.Advance(t, time.Microsecond)
			})
			got := f.call(t, by, "bead.ready", map[string]any{"limit": 1}, 7103)
			if barriers != 1 || got.Error == nil || got.Error.Data.Code != "cursor_invalid" || len(got.Result) != 0 {
				t.Fatalf("same-head internal cutoff splice produced a partial/fingerprinted result: barriers=%d %+v", barriers, got)
			}
			fresh := page(t, f.call(t, by, "bead.ready", map[string]any{"limit": 1000}, 7104))
			if len(fresh.Items) != 1000 || fresh.NextCursor == "" {
				t.Fatal("fresh post-cutoff first page is incomplete")
			}
			for i, item := range fresh.Items {
				if item.ID != fixtureRef(101+i).ID {
					t.Fatalf("fresh cutoff order at%d: %s", i, item.ID)
				}
			}
			last := page(t, f.call(t, by, "bead.ready", map[string]any{"limit": 1000, "cursor": fresh.NextCursor}, 7105))
			if len(last.Items) != 2 || last.Items[0].ID != fixtureRef(1101).ID || last.Items[1].ID != fixtureRef(100).ID || last.NextCursor != "" {
				t.Fatalf("fresh cutoff continuation omitted or duplicated a member: %+v", last)
			}
			afterHead, afterACL := publicationState(t, f.store, f.server.Namespace)
			if afterHead.String() != beforeHead.String() || !reflect.DeepEqual(beforeACL, afterACL) || fresh.AsOf.Revision.String() != control.AsOf.Revision.String() || fresh.AsOf.Incarnation != control.AsOf.Incarnation {
				t.Fatal("internal cutoff check changed durable source, incarnation or authority")
			}
		})
	}
}

func currentHistory(t *testing.T, open storeFactory) {
	f := matrixFixture(t, open)
	view := f.reader(t).Snapshot()
	by := model.PrincipalRef{Kind: "user", ID: "user:history", Caveats: map[string]any{"projects": []model.Ref{model.RefByID("newproject", model.Natural64(0))}}}
	params := map[string]any{"asOf": map[string]any{"revision": *view.At, "incarnation": view.Incarnation}, "_meta": map[string]any{"project": "unregistered"}}
	if got := page(t, f.call(t, by, "history.query", params, 1200)); len(got.Items) != 0 {
		t.Fatal("unknown current assignment granted historical read")
	}
	next := f.base
	next.Projects = append(append([]authzfixed.Project(nil), next.Projects...), authzfixed.Project{
		Ref: model.RefByID("newproject", model.Natural64(0)), Name: "unregistered", InstallationID: "fixed", Active: true,
	})
	if err := f.facts.Replace(next); err != nil {
		t.Fatal(err)
	}
	got := page(t, f.call(t, by, "history.query", params, 1201))
	if len(got.Items) != 2 || got.Items[0].ID != fixtureRef(2).ID || got.Items[1].ID != fixtureRef(3).ID || got.Items[0].Fields["title"] != "historical" || got.Items[1].Fields["title"] != "historical" || got.AsOf.Revision.String() != view.At.String() {
		t.Fatalf("history did not combine current read permission with the requested content snapshot: %+v", got)
	}
	if got := page(t, f.call(t, worker("user:operator"), "history.query", params, 1202)); len(got.Items) != 0 {
		t.Fatal("Agent borrowed its sponsor's history read permit")
	}
	// A separate current restricted principal cannot use an old content
	// revision to bypass its intersecting caveat.
	by.Caveats["refs"] = []model.Ref{fixtureRef(1)}
	if got := page(t, f.call(t, by, "history.query", params, 1203)); len(got.Items) != 0 {
		t.Fatal("historical content bypassed current principal restrictions")
	}
	unrestricted := model.PrincipalRef{Kind: "user", ID: "user:history-cursor"}
	cursorParams := map[string]any{"asOf": params["asOf"], "limit": 1}
	first := page(t, f.call(t, unrestricted, "history.query", cursorParams, 1204))
	if len(first.Items) != 1 || first.NextCursor == "" {
		t.Fatal("history fixture did not produce a continuation")
	}
	// Guarded history pages use the history cursor contract; Ready-only
	// workflow membership metadata must never be attached to this token.
	parts := strings.Split(first.NextCursor, ".")
	if len(parts) != 2 {
		t.Fatal("history cursor was not signed as two segments")
	}
	body, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		t.Fatal(err)
	}
	var cursorFields map[string]json.RawMessage
	if err := json.Unmarshal(body, &cursorFields); err != nil {
		t.Fatal(err)
	}
	if _, ok := cursorFields["readyMembership"]; ok {
		t.Fatal("history cursor carried Ready membership metadata")
	}
	next.Grants = nil
	if err := f.facts.Replace(next); err != nil {
		t.Fatal(err)
	}
	cursorParams["cursor"] = first.NextCursor
	denied := f.call(t, unrestricted, "history.query", cursorParams, 1205)
	if denied.Error == nil || denied.Error.Data.Code != "cursor_invalid" || len(denied.Result) != 0 {
		t.Fatal("history cursor did not invalidate after current ACL changed")
	}
}

func postImage(t *testing.T, open storeFactory) {
	f := newFixtureOn(t, open(t), nil)
	ctx := context.Background()
	reader := f.reader(t)
	rows, err := reader.Get(ctx, []model.Ref{fixtureRef(1)})
	if err != nil || len(rows) != 1 {
		t.Fatalf("stored pre-image: %v", err)
	}
	proposal := port.ProposedChange{Principal: worker("user:operator"), Meta: model.WriteMeta{Method: "bead.update"}, Changes: []port.Change{{Resource: model.ResourceRef{Kind: "bead", Ref: fixtureRef(1)}, Before: rows[0].Body, After: rows[0].Body}}}
	if err := f.server.Checker.Authorize(ctx, port.AuthorizeAfter, proposal, reader); err != nil {
		t.Fatalf("unchanged supported image: %#v", err)
	}
	// The native caller and the same caller detached by authentication/freezing
	// must agree. Keep the typed Ref caveat above to cover object-key ordering.
	wire, err := json.Marshal(proposal.Principal)
	if err != nil {
		t.Fatal(err)
	}
	wireProposal := proposal
	if err := json.Unmarshal(wire, &wireProposal.Principal); err != nil {
		t.Fatal(err)
	}
	if err := f.server.Checker.Authorize(ctx, port.AuthorizeAfter, wireProposal, reader); err != nil {
		t.Fatalf("unchanged wire image: %#v", err)
	}
	for _, field := range []string{"project", "parent", "owner", "assignee", "type", "labels", "restricted"} {
		t.Run(field, func(t *testing.T) {
			var body map[string]any
			if err := json.Unmarshal(rows[0].Body, &body); err != nil {
				t.Fatal(err)
			}
			body[field] = "changed"
			after, err := json.Marshal(body)
			if err != nil {
				t.Fatal(err)
			}
			change := proposal
			change.Changes = append([]port.Change(nil), proposal.Changes...)
			change.Changes[0].After = after
			if err := f.server.Checker.Authorize(ctx, port.AuthorizeAfter, change, reader); err == nil {
				t.Fatal("permission-bearing post-image was allowed")
			}
			change.Changes[0].Before = after
			if err := f.server.Checker.Authorize(ctx, port.AuthorizeAfter, change, reader); err == nil {
				t.Fatal("fabricated pre-image bypassed the stored source")
			}
		})
	}
}

type authorizationRaceStore struct {
	driver.Store
	beforeWrite atomic.Pointer[func() error]
}

func (s *authorizationRaceStore) Write(ctx context.Context, lease driver.ActiveLease) (driver.WriteTx, error) {
	if hook := s.beforeWrite.Swap(nil); hook != nil {
		if err := (*hook)(); err != nil {
			return nil, err
		}
	}
	return s.Store.Write(ctx, lease)
}

func authorizationRaces(t *testing.T, open storeFactory) {
	for _, change := range []string{"direct_grant", "team_edge", "membership", "team_active", "project_active", "bead_assignment"} {
		t.Run(change, func(t *testing.T) {
			store := &authorizationRaceStore{Store: open(t)}
			f := newFixtureOn(t, store, nil)
			if change == "direct_grant" {
				f.base.Grants = []authz.Grant{{Subject: authz.SubjectRef{Kind: "user", ID: "user:operator"}, Scope: authz.Scope{Kind: authz.ProjectScope, Ref: model.RefByID("project", model.Natural64(0))}, Role: "involved"}}
				if err := f.facts.Replace(f.base); err != nil {
					t.Fatal(err)
				}
			}
			var changed model.Revision
			var calls int
			hook := func() error {
				calls++
				next := f.base
				switch change {
				case "direct_grant":
					next.Grants = nil
				case "team_edge":
					next.Grants = append([]authz.Grant(nil), next.Grants[:1]...)
				case "membership":
					next.Grants = append([]authz.Grant(nil), next.Grants[1:]...)
				case "team_active":
					next.Teams = append([]authzfixed.Team(nil), next.Teams...)
					next.Teams[0].Active = false
				case "project_active":
					next.Projects = append([]authzfixed.Project(nil), next.Projects...)
					next.Projects[0].Active = false
				case "bead_assignment":
					_, err := conformance.Seed(context.Background(), store, f.lease, driver.ChangesetHeader{}, []conformance.FixtureChange{{
						Ref: fixtureRef(1), Kind: "bead", Op: model.OpPatch,
						Body: []byte(`{"id":"` + beadID + `","type":"issue","project":"unregistered","fields":{"title":"seed","status":"todo","priority":0}}`),
					}})
					if err != nil {
						return err
					}
				}
				if change != "bead_assignment" {
					if err := f.facts.Replace(next); err != nil {
						return err
					}
				}
				meta, err := store.Meta(context.Background())
				changed = meta.Head
				return err
			}
			// Writer preparation (including both authorization stages) runs
			// before Store.Write. This real competing commit races its recorded
			// dependencies, rather than manufacturing a stale-snapshot error.
			store.beforeWrite.Store(&hook)
			by := worker("user:operator")
			got := f.call(t, by, "bead.update", map[string]any{"id": fixtureRef(1), "fields": map[string]any{"title": "must not commit"}}, 1300)
			if calls != 1 || got.Error == nil || got.Error.Data.Code != "forbidden" || f.receipt(t, by, "bead.update", 1300) != nil || f.reader(t).Snapshot().At.String() != changed.String() {
				t.Fatalf("raced authorization committed or failed to reauthorize: calls=%d error=%+v head=%s competing=%s", calls, got.Error, f.reader(t).Snapshot().At, changed)
			}
			rows, err := f.reader(t).Get(context.Background(), []model.Ref{fixtureRef(1)})
			if err != nil || len(rows) != 1 {
				t.Fatalf("raced source disappeared: %v", err)
			}
			var body struct{ Fields map[string]any }
			if err := json.Unmarshal(rows[0].Body, &body); err != nil || body.Fields["title"] != "seed" {
				t.Fatal("denied race published a partial post-image")
			}
		})
	}
}

func immutableRegistry(t *testing.T, open storeFactory) {
	for _, change := range []string{"rename", "remap", "delete", "authority"} {
		t.Run(change, func(t *testing.T) {
			f := newFixtureOn(t, open(t), nil)
			next := f.base
			next.Projects = append([]authzfixed.Project(nil), f.base.Projects...)
			switch change {
			case "rename":
				next.Projects[0].Name = "renamed"
			case "remap":
				next.Projects[0].Ref = model.RefByID("replacement", model.Natural64(0))
				// Keep the candidate internally valid: the old team edge would
				// otherwise fail validation before reaching the immutable registry.
				next.Grants = nil
			case "delete":
				next.Projects, next.Grants = nil, nil
			case "authority":
				next.Namespace.Authority = model.RefByID("replacement", model.Natural64(0))
			}
			before := *f.reader(t).Snapshot().At
			if err := f.facts.Replace(next); err == nil {
				t.Errorf("installed source accepted forbidden registry/authority %s", change)
			}
			if got := f.reader(t).Snapshot().At; got.String() != before.String() {
				t.Errorf("rejected registry change advanced head: %s -> %s", before, got)
			}
			got, err := f.facts.Load(context.Background(), f.reader(t))
			if err != nil || !reflect.DeepEqual(got, f.base) {
				t.Errorf("rejected registry change replaced the installed facts: %v", err)
			}
			// Refusal leaves the previously authorized runtime usable.
			success(t, f.call(t, worker("user:operator"), "bead.update", map[string]any{
				"id": model.RefByID(beadID, model.Natural64(0)), "fields": map[string]any{"title": "still authorized"},
			}, 100))
		})
	}
}

var errBeforeFactsCommit = errors.New("authorization publication failed before commit")

// Only the next real transaction is wrapped. Lock, Head, staging, rollback and
// persistence remain driver-owned; the control fires at Commit, after staging.
type factsPublicationStore struct {
	driver.Store
	next *factsPublicationFault
}

type factsPublicationFault struct {
	lostAck     bool
	beforeLock  func()
	headCalls   int
	commitCalls int
	bumped      []driver.ReadFact
	committed   model.Changeset
	rows        map[driver.DerivedRowKey]driver.DerivedRow
	typed       bool
}

func (s *factsPublicationStore) Write(ctx context.Context, lease driver.ActiveLease) (driver.WriteTx, error) {
	tx, err := s.Store.Write(ctx, lease)
	if err != nil || s.next == nil {
		return tx, err
	}
	fault := s.next
	s.next = nil
	_, fault.typed = tx.(driver.SelectionIndexWriter)
	fault.rows = map[driver.DerivedRowKey]driver.DerivedRow{}
	return &factsPublicationTx{WriteTx: tx, fault: fault}, nil
}

type factsPublicationTx struct {
	driver.WriteTx
	fault *factsPublicationFault
}

func (tx *factsPublicationTx) Lock(ctx context.Context, locks []driver.LockRequest) ([]driver.LockState, error) {
	if tx.fault.beforeLock != nil {
		f := tx.fault.beforeLock
		tx.fault.beforeLock = nil
		f()
	}
	return tx.WriteTx.Lock(ctx, locks)
}

func (tx *factsPublicationTx) Head(ctx context.Context) (model.Revision, error) {
	tx.fault.headCalls++
	return tx.WriteTx.Head(ctx)
}

// Preserve the optional typed writer on the wrapped real transaction. The
// injection must fire after actual indexed delta staging, not on capability
// loss before reaching the tested transaction boundary.
func (tx *factsPublicationTx) PutSelectionDerived(ctx context.Context, table driver.DerivedTable, del, add []driver.DerivedRow) error {
	if typed, ok := tx.WriteTx.(driver.SelectionIndexWriter); ok {
		if err := typed.PutSelectionDerived(ctx, table, del, add); err != nil {
			return err
		}
	} else {
		copy := make([]driver.DerivedRow, len(add))
		for i, row := range add {
			copy[i] = driver.DerivedRow{Key: row.Key, Value: append([]byte(nil), row.Value...)}
		}
		if err := tx.WriteTx.PutDerived(ctx, table, del, copy); err != nil {
			return err
		}
		add = copy
	}
	for _, row := range add {
		tx.fault.rows[driver.DerivedRowKey{Table: table, Key: row.Key}] = row.Clone()
	}
	return nil
}

func (tx *factsPublicationTx) PutDerived(ctx context.Context, table driver.DerivedTable, del, add []driver.DerivedRow) error {
	if err := tx.WriteTx.PutDerived(ctx, table, del, add); err != nil {
		return err
	}
	for _, row := range add {
		tx.fault.rows[driver.DerivedRowKey{Table: table, Key: row.Key}] = row.Clone()
	}
	return nil
}

func (tx *factsPublicationTx) BumpReadFacts(ctx context.Context, facts []driver.ReadFact) error {
	if err := tx.WriteTx.BumpReadFacts(ctx, facts); err != nil {
		return err
	}
	tx.fault.bumped = append(tx.fault.bumped, facts...)
	return nil
}

func (tx *factsPublicationTx) Commit(ctx context.Context, header driver.ChangesetHeader) (model.Changeset, error) {
	tx.fault.commitCalls++
	if !tx.fault.lostAck {
		return model.Changeset{}, errBeforeFactsCommit
	}
	entry, err := tx.WriteTx.Commit(ctx, header)
	if err != nil {
		return model.Changeset{}, err
	}
	tx.fault.committed = entry
	// The real commit is complete. Hide its result just as a lost COMMIT
	// acknowledgement does; deferred Rollback/Close cannot undo persistence.
	return model.Changeset{}, driver.ErrIndeterminate{IdempotencyKey: header.IdempotencyKey}
}

func publicationState(t *testing.T, store driver.Store, namespace authz.Namespace) (model.Revision, driver.LockState) {
	t.Helper()
	ctx := context.Background()
	meta, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := store.Read(ctx, driver.View{Incarnation: namespace.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	fact, err := driver.NewReadFact(authz.ACLDependency(namespace).Key)
	if err != nil {
		t.Fatal(err)
	}
	states, err := reader.Observe(ctx, []driver.LockKey{{Kind: driver.LockReadFact, Fact: fact}})
	if err != nil || len(states) != 1 || reader.Snapshot().At.String() != meta.Head.String() {
		t.Fatalf("independent durable ACL/head observation: states=%+v head=%s error=%v", states, meta.Head, err)
	}
	return meta.Head, states[0]
}

func publicationProjections(t *testing.T, store driver.Store, namespace authz.Namespace) []driver.LockState {
	t.Helper()
	rows, err := authzfixed.SelectionRows(namespace)
	if err != nil {
		t.Fatal(err)
	}
	keys := []driver.LockKey{}
	for _, row := range []driver.DerivedRowKey{rows.Source, rows.Grants, rows.Projects, rows.Teams} {
		row := row
		keys = append(keys, driver.LockKey{Kind: driver.LockDerived, Derived: &row})
	}
	sort.Slice(keys, func(i, j int) bool {
		a, _ := driver.EncodeLockKey(keys[i])
		b, _ := driver.EncodeLockKey(keys[j])
		return a < b
	})
	r, err := store.Read(context.Background(), driver.View{Incarnation: namespace.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	states, err := r.Observe(context.Background(), keys)
	if err != nil || len(states) != 4 {
		t.Fatalf("durable projection observation: %+v %v", states, err)
	}
	for _, state := range states {
		if !state.Present || state.Derived == nil {
			t.Fatalf("missing explicit complete fact domain: %+v", state)
		}
	}
	return states
}

func sourcePublicationFailure(t *testing.T, open storeFactory) {
	for _, mode := range []string{"before_commit", "commit_lost_ack"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			store := &factsPublicationStore{Store: open(t)}
			f := newFixtureOn(t, store, nil)
			// Begin with a nonzero, genuinely committed ACL generation. The
			// successful bead write then separates storage head from that fact.
			if err := f.facts.Replace(f.base); err != nil {
				t.Fatal(err)
			}
			old, replacement := worker("user:operator"), worker("user:replacement")
			update := func(by model.PrincipalRef, title string, key int) reply {
				return f.call(t, by, "bead.update", map[string]any{"id": fixtureRef(1), "fields": map[string]any{"title": title}}, key)
			}
			success(t, update(old, "last published", 1400))
			beforeRows, err := f.reader(t).Get(ctx, []model.Ref{fixtureRef(1)})
			if err != nil || len(beforeRows) != 1 {
				t.Fatalf("committed protected fixture: %v", err)
			}
			beforeHead, beforeACL := publicationState(t, store.Store, f.base.Namespace)
			beforeProjection := publicationProjections(t, store.Store, f.base.Namespace)
			if !beforeACL.Present || beforeACL.Revision.IsZero() || beforeACL.Revision.String() == beforeHead.String() {
				t.Fatal("fixture must distinguish committed ACL generation from current head")
			}
			next := f.base
			next.Grants = []authz.Grant{{Subject: authz.SubjectRef{Kind: "user", ID: "user:replacement"}, Scope: authz.Scope{Kind: authz.ProjectScope, Ref: f.base.Projects[0].Ref}, Role: "involved"}}
			fault := &factsPublicationFault{lostAck: mode == "commit_lost_ack"}
			store.next = fault
			err = f.facts.Replace(next)
			var unknown driver.ErrIndeterminate
			if fault.lostAck && !errors.As(err, &unknown) || !fault.lostAck && !errors.Is(err, errBeforeFactsCommit) {
				t.Fatalf("wrong publication failure: %v", err)
			}
			if fault.commitCalls != 1 || !reflect.DeepEqual(fault.bumped, []driver.ReadFact{beforeACL.Key.Fact}) || store.next != nil {
				t.Fatalf("fault did not reach Commit after real ACL staging: %+v", fault)
			}
			keys, err := authzfixed.SelectionRows(next.Namespace)
			if err != nil {
				t.Fatal(err)
			}
			sourceBytes, err := authzfixed.FactsSourceBytes(next)
			if err != nil {
				t.Fatal(err)
			}
			if len(fault.rows) != 4 || !reflect.DeepEqual(fault.rows[keys.Source].Value, sourceBytes) {
				t.Fatalf("failure occurred before complete source/index staging: %+v", fault.rows)
			}
			for _, key := range []driver.DerivedRowKey{keys.Grants, keys.Projects, keys.Teams} {
				row := fault.rows[key]
				if row.Key != key.Key || fault.typed && (row.Index == nil || row.Index.Source.Digest != driver.SelectionDigest(sourceBytes) || row.Index.Facts == nil) {
					t.Fatalf("missing actually staged fact domain: %+v", row)
				}
			}
			afterHead, afterACL := publicationState(t, store.Store, f.base.Namespace)
			afterProjection := publicationProjections(t, store.Store, f.base.Namespace)
			entries, err := f.reader(t).Changesets(ctx, &driver.Cursor{Incarnation: f.base.Namespace.Incarnation, Revision: beforeHead}, 10)
			if err != nil {
				t.Fatal(err)
			}
			if fault.lostAck {
				if afterHead.String() == beforeHead.String() || !afterACL.Present || afterACL.Revision.String() != afterHead.String() || fault.committed.Revision.String() != afterHead.String() || len(entries) != 1 || !reflect.DeepEqual(entries[0], fault.committed) || entries[0].Kind != model.ChangesetDerived || entries[0].Principal.Kind != "system" || entries[0].Principal.ID != "authzrpc" || len(entries[0].Changes) != 0 {
					t.Fatalf("lost acknowledgement lacked a real atomic ACL commit: head=%s ACL=%+v entries=%+v", afterHead, afterACL, entries)
				}
				for _, state := range afterProjection {
					if state.Revision.String() != afterHead.String() {
						t.Fatal("lost acknowledgement split source/index/ACL publication")
					}
					if *state.Key.Derived == keys.Source {
						if !reflect.DeepEqual(state.Derived.Value, sourceBytes) {
							t.Fatal("lost acknowledgement omitted source bytes")
						}
					} else if fault.typed && (state.Derived.Index == nil || state.Derived.Index.Source.Revision.String() != afterHead.String() || state.Derived.Index.Source.Digest != driver.SelectionDigest(sourceBytes)) {
						t.Fatal("committed fact index lost its final source binding")
					}
				}
				loaded, err := f.facts.Load(ctx, f.reader(t))
				if err == nil || !reflect.DeepEqual(loaded, authzfixed.Facts{}) {
					t.Fatal("generation mismatch returned stale authorization facts")
				}
				got := f.call(t, model.PrincipalRef{Kind: "user", ID: "user:reader"}, "bead.get", map[string]any{"id": fixtureRef(1)}, 1401)
				if got.Error == nil || got.Error.Data.Code != "authz_unavailable" || !got.Error.Data.Retryable || len(got.Result) != 0 {
					t.Fatalf("uncertain source allowed a protected read: %+v", got)
				}
			} else {
				if afterHead.String() != beforeHead.String() || !reflect.DeepEqual(afterACL, beforeACL) || !reflect.DeepEqual(beforeProjection, afterProjection) || len(entries) != 0 || !fault.committed.Revision.IsZero() {
					t.Fatal("failed commit published an ACL generation or changeset")
				}
				loaded, err := f.facts.Load(ctx, f.reader(t))
				if err != nil || !reflect.DeepEqual(loaded, f.base) {
					t.Fatalf("failed commit replaced the previously published source: %v", err)
				}
				success(t, f.call(t, model.PrincipalRef{Kind: "user", ID: "user:reader"}, "bead.get", map[string]any{"id": fixtureRef(1)}, 1401))
			}
			// Neither a tentative grant nor the old grant after a durable
			// revocation may authorize a write. Check actual content and receipts.
			for i, by := range []model.PrincipalRef{replacement, old} {
				if !fault.lostAck && i == 1 {
					continue // The old grant is still the committed source here.
				}
				want := "forbidden"
				if fault.lostAck {
					want = "authz_unavailable"
				}
				got := update(by, "must not commit", 1410+i)
				if got.Error == nil || got.Error.Data.Code != model.ErrorCode(want) || fault.lostAck && !got.Error.Data.Retryable || len(got.Result) != 0 || f.receipt(t, by, "bead.update", 1410+i) != nil {
					t.Fatalf("unpublished source authorized a write: %+v", got)
				}
			}
			rows, err := f.reader(t).Get(ctx, []model.Ref{fixtureRef(1)})
			if err != nil || !reflect.DeepEqual(rows, beforeRows) {
				t.Fatal("publication failure or denied write changed protected content")
			}
			if head, acl := publicationState(t, store.Store, f.base.Namespace); head.String() != afterHead.String() || !reflect.DeepEqual(acl, afterACL) {
				t.Fatal("protected reads or denied writes changed durable ACL/head")
			}
			if !fault.lostAck {
				success(t, update(old, "previous source remains valid", 1420))
			}
			// Recover by publishing the intended current source through Replace,
			// never by rebinding stale grants to the observed newer generation.
			if err := f.facts.Replace(next); err != nil {
				t.Fatal(err)
			}
			recoveredHead, recoveredACL := publicationState(t, store.Store, f.base.Namespace)
			loaded, err := f.facts.Load(ctx, f.reader(t))
			if err != nil || !reflect.DeepEqual(loaded, next) || recoveredHead.String() == afterHead.String() || !recoveredACL.Present || recoveredACL.Revision.String() != recoveredHead.String() {
				t.Fatalf("legitimate publication did not recover the current source: %v", err)
			}
			success(t, f.call(t, model.PrincipalRef{Kind: "user", ID: "user:reader"}, "bead.get", map[string]any{"id": fixtureRef(1)}, 1430))
			denied := update(old, "stale grant", 1431)
			if denied.Error == nil || denied.Error.Data.Code != "forbidden" || len(denied.Result) != 0 || f.receipt(t, old, "bead.update", 1431) != nil || f.reader(t).Snapshot().At.String() != recoveredHead.String() {
				t.Fatal("recovered source accepted the revoked grant")
			}
			success(t, update(replacement, "current source", 1432))
			if f.receipt(t, replacement, "bead.update", 1432) == nil {
				t.Fatal("current source write omitted its durable receipt")
			}
			t.Logf("publication control=%s Commit_calls=%d before_head=%s before_acl=%s after_head=%s after_acl=%s recovery_head=%s recovery_acl=%s", mode, fault.commitCalls, beforeHead, beforeACL.Revision, afterHead, afterACL.Revision, recoveredHead, recoveredACL.Revision)
		})
	}
}

func localDocker(args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "docker", append([]string{"--host=unix:///var/run/docker.sock"}, args...)...)
	for _, value := range os.Environ() {
		key, _, _ := strings.Cut(value, "=")
		switch key {
		case "DOCKER_HOST", "DOCKER_CONTEXT", "DOCKER_TLS", "DOCKER_TLS_VERIFY", "DOCKER_CERT_PATH":
		default:
			cmd.Env = append(cmd.Env, value)
		}
	}
	return cmd.CombinedOutput()
}

func postgresFactory(t *testing.T) storeFactory {
	t.Helper()
	dsn := postgresDSNFactory(t)
	return func(t *testing.T) driver.Store {
		t.Helper()
		s, err := postgres.Open(context.Background(), driver.Config{DSN: dsn(t)})
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
}

func postgresDSNFactory(t *testing.T) func(*testing.T) string {
	t.Helper()
	const image = "postgres@sha256:2d2b8998d31037bf721cfdf764d76ba74171b4fab3431b7f72c27c56ddbdf9e3"
	name := fmt.Sprintf("plug-swarm-authz-i5-%d-%d", os.Getpid(), time.Now().UnixNano())
	if out, err := localDocker("run", "-d", "--rm", "--name", name, "-e", "POSTGRES_HOST_AUTH_METHOD=trust", "-p", "127.0.0.1::5432", image); err != nil {
		t.Fatalf("owned PostgreSQL start: %v %s", err, out)
	}
	t.Cleanup(func() {
		if out, err := localDocker("rm", "-f", name); err != nil {
			t.Errorf("owned PostgreSQL cleanup: %v %s", err, out)
		}
	})
	out, err := localDocker("port", name, "5432/tcp")
	address := strings.TrimSpace(string(out))
	host, _, parseErr := net.SplitHostPort(address)
	if err != nil || parseErr != nil || host != "127.0.0.1" {
		t.Fatalf("PostgreSQL requires a single loopback binding: %q %v", address, err)
	}
	deadline := time.Now().Add(40 * time.Second)
	for {
		if _, err := localDocker("exec", name, "pg_isready", "-h", "127.0.0.1", "-U", "postgres"); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("owned PostgreSQL readiness timed out")
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Logf("real PostgreSQL image=%s loopback_only=true owned_cleanup=true", image)
	var sequence atomic.Uint64
	return func(t *testing.T) string {
		t.Helper()
		database := fmt.Sprintf("authz_%d", sequence.Add(1))
		if out, err := localDocker("exec", name, "createdb", "-h", "127.0.0.1", "-U", "postgres", database); err != nil {
			t.Fatalf("owned test database: %v %s", err, out)
		}
		return "postgres://postgres@" + address + "/" + database + "?sslmode=disable"
	}
}

func TestMutableFactsBindsCanonicalDurableSource(t *testing.T) {
	f := newFixture(t, nil)
	beforeHead, _ := publicationState(t, f.store, f.base.Namespace)
	before := publicationProjections(t, f.store, f.base.Namespace)
	for _, mismatch := range []bool{false, true} {
		facts := f.base
		facts.Grants = append([]authz.Grant(nil), facts.Grants...)
		if mismatch {
			facts.Grants = nil
		} else {
			for i, j := 0, len(facts.Grants)-1; i < j; i, j = i+1, j-1 {
				facts.Grants[i], facts.Grants[j] = facts.Grants[j], facts.Grants[i]
			}
		}
		source, err := authzrpc.NewMutableFacts(facts)
		if err != nil {
			t.Fatal(err)
		}
		_, err = authzrpc.New(context.Background(), authzrpc.Config{Store: f.store, Lease: f.lease, Provider: f.keys, Facts: source, Namespace: f.base.Namespace, DefaultProject: "project"})
		if mismatch && err == nil || !mismatch && err != nil {
			t.Fatalf("fresh bind mismatch=%t: %v", mismatch, err)
		}
		afterHead, _ := publicationState(t, f.store, f.base.Namespace)
		if afterHead.String() != beforeHead.String() || !reflect.DeepEqual(before, publicationProjections(t, f.store, f.base.Namespace)) {
			t.Fatal("fresh source binding changed durable authority")
		}
	}
}

func TestMutableFactsRejectsStaleRegistryPublication(t *testing.T) {
	f := newFixture(t, nil)
	stale, err := authzrpc.NewMutableFacts(f.base)
	if err != nil {
		t.Fatal(err)
	}
	_, err = authzrpc.New(context.Background(), authzrpc.Config{Store: f.store, Lease: f.lease, Provider: f.keys, Facts: stale, Namespace: f.base.Namespace, DefaultProject: "project"})
	if err != nil {
		t.Fatal(err)
	}
	next := f.base
	next.Projects = append(append([]authzfixed.Project(nil), next.Projects...), authzfixed.Project{Ref: model.RefByID("new-project", model.Natural64(0)), InstallationID: "fixed", Name: "new-project", Active: true})
	if err := f.facts.Replace(next); err != nil {
		t.Fatal(err)
	}
	beforeHead, beforeACL := publicationState(t, f.store, next.Namespace)
	before := publicationProjections(t, f.store, next.Namespace)
	if err := stale.Replace(f.base); err == nil {
		t.Fatal("stale source deleted an actually installed project registry binding")
	}
	afterHead, afterACL := publicationState(t, f.store, next.Namespace)
	if afterHead.String() != beforeHead.String() || !reflect.DeepEqual(beforeACL, afterACL) || !reflect.DeepEqual(before, publicationProjections(t, f.store, next.Namespace)) {
		t.Fatal("stale replacement changed durable source/index/ACL")
	}
	if _, err := stale.Load(context.Background(), f.reader(t)); err == nil {
		t.Fatal("stale source retained authorization after competing publication")
	}
	// Recovery must retain the actual durable registry, including after an
	// uncertain acknowledgement. It is not limited to the stale local copy.
	if err := stale.Replace(next); err != nil {
		t.Fatal(err)
	}
	if _, err := stale.Load(context.Background(), f.reader(t)); err != nil {
		t.Fatal(err)
	}
}

// Explicit initial Replace is a real H+1 authority publication. A failed commit
// keeps the target empty; a lost acknowledgement is durably occupied and is
// recovered by exact rebind without a second initialization commit.
func initialSourcePublication(t *testing.T, open storeFactory) {
	for _, mode := range []string{"success", "before_commit", "commit_lost_ack"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			store := &factsPublicationStore{Store: open(t)}
			t.Cleanup(func() { _ = store.Close() })
			lease, err := store.Lease(ctx, "initial-facts", driver.LeaseTTL)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = lease.Release(ctx) })
			probe, err := store.Store.Write(ctx, lease)
			if err != nil {
				t.Fatal(err)
			}
			_, supportsSelection := probe.(driver.SelectionIndexWriter)
			_ = probe.Close()
			if _, memoryEngine := store.Store.(*memory.Store); memoryEngine && !supportsSelection {
				t.Fatal("W3 memory optional typed writer is required for this positive oracle")
			}
			before, err := store.Meta(ctx)
			if err != nil || !before.Head.IsZero() {
				t.Fatalf("nonempty initial fixture: %+v %v", before, err)
			}
			namespace := authz.Namespace{StoreID: string(before.StoreID), Incarnation: before.Incarnation, InstallationID: "fixed", Authority: model.RefByID("authority", model.Natural64(0))}
			facts := authzfixed.Facts{Namespace: namespace}
			sourceBytes, err := authzfixed.FactsSourceBytes(facts)
			if err != nil {
				t.Fatal(err)
			}
			stagedRows := initialPublicationRows(t, namespace, sourceBytes, model.Natural64(0), supportsSelection)
			durableRows := initialPublicationRows(t, namespace, sourceBytes, model.Natural64(1), supportsSelection)
			source, err := authzrpc.NewMutableFacts(facts)
			if err != nil {
				t.Fatal(err)
			}
			provider := authnapikey.New(authnapikey.Config{AuthorizationInstalled: func() bool { return true }})
			config := authzrpc.Config{Store: store, Lease: lease, Facts: source, Provider: provider, Namespace: namespace, DefaultProject: "project"}
			var fault *factsPublicationFault
			if mode != "success" {
				fault = &factsPublicationFault{lostAck: mode == "commit_lost_ack"}
				store.next = fault
			}
			composition, err := authzrpc.New(ctx, config)
			if err != nil || composition == nil {
				t.Fatal(err)
			}
			constructed, constructedACL := publicationState(t, store.Store, namespace)
			if constructed.String() != before.Head.String() || constructedACL.Present || !constructedACL.Revision.IsZero() || fault != nil && (fault.headCalls != 0 || fault.commitCalls != 0) {
				t.Fatal("host construction published initial authority")
			}
			err = source.Replace(facts)
			if mode == "success" && err != nil {
				t.Fatal(err)
			}
			if mode != "success" {
				var lost driver.ErrIndeterminate
				if fault.commitCalls != 1 || fault.headCalls != 1 || !reflect.DeepEqual(fault.rows, stagedRows) || len(fault.bumped) != 1 || (fault.lostAck && !errors.As(err, &lost)) || (!fault.lostAck && !errors.Is(err, errBeforeFactsCommit)) {
					t.Fatalf("initial fault did not follow complete source/fact/ACL staging: %+v error=%v", fault, err)
				}
			}
			after, acl := publicationState(t, store.Store, namespace)
			if mode == "before_commit" {
				if !after.IsZero() || acl.Present || !acl.Revision.IsZero() {
					t.Fatal("failed initialization occupied the empty target")
				}
				r, err := store.Read(ctx, driver.View{Incarnation: namespace.Incarnation})
				if err != nil {
					t.Fatal(err)
				}
				for key := range stagedRows {
					states, err := r.Observe(ctx, []driver.LockKey{{Kind: driver.LockDerived, Derived: &key}})
					if err != nil || len(states) != 1 || states[0].Present || !states[0].Revision.IsZero() {
						t.Fatal("failed initialization leaked a projection")
					}
				}
				if rows := publicationInventory(t, r); len(rows) != 0 {
					t.Fatalf("failed initialization leaked derived inventory: %+v", rows)
				}
				_ = r.Close()
			} else {
				if after.String() != "1" || !acl.Present || acl.Revision.String() != after.String() {
					t.Fatal("initial publication did not atomically advance head and ACL")
				}
				keys, _ := authzfixed.SelectionRows(namespace)
				sourceBytes, err := authzfixed.FactsSourceBytes(facts)
				if err != nil {
					t.Fatal(err)
				}
				for _, state := range publicationProjections(t, store.Store, namespace) {
					if state.Revision.String() != after.String() {
						t.Fatal("initial source/domain publication split")
					}
					if *state.Key.Derived == keys.Source {
						if state.Derived.Index != nil || string(state.Derived.Value) != string(sourceBytes) {
							t.Fatal("initial root source bytes were not preserved")
						}
						continue
					}
					index := state.Derived.Index
					if !supportsSelection {
						if index != nil {
							t.Fatal("unsupported writer certified initial facts")
						}
						continue
					}
					kind := map[driver.DerivedRowKey]driver.SelectionFactKind{keys.Grants: driver.SelectionScopedGrantFact, keys.Projects: driver.SelectionProjectFact, keys.Teams: driver.SelectionTeamFact}[*state.Key.Derived]
					if index == nil || index.Facts == nil || kind == "" || index.Facts.Kind != kind || len(index.Facts.Rows) != 0 || index.Source.Key.Kind != driver.LockDerived || index.Source.Key.Derived == nil || *index.Source.Key.Derived != keys.Source || !index.Source.Present || index.Source.Revision.String() != after.String() || index.Source.Digest != driver.SelectionDigest(sourceBytes) || index.Incarnation != namespace.Incarnation || index.Validate(state.Key.Derived.Table, *state.Derived) != nil {
						t.Fatal("capable writer lost explicit complete empty domain/source binding")
					}
				}
			}
			initial, err := store.Read(ctx, driver.View{Incarnation: namespace.Incarnation})
			if err != nil {
				t.Fatal(err)
			}
			defer initial.Close()
			if mode != "before_commit" && !reflect.DeepEqual(publicationInventory(t, initial), durableRows) {
				t.Fatal("initial publication lost exact complete source/fact/Ready inventory")
			}
			if _, err := authzrpc.New(ctx, config); err != nil {
				t.Fatal("legitimate initial recovery/rebind failed:", err)
			}
			if mode == "before_commit" {
				if err := source.Replace(facts); err != nil {
					t.Fatal("explicit initial publication after rollback:", err)
				}
			}
			finalHead, finalACL := publicationState(t, store.Store, namespace)
			if finalHead.String() != "1" || finalACL.Revision.String() != "1" {
				t.Fatal("initial recovery/rebind published twice")
			}
			r, err := store.Read(ctx, driver.View{Incarnation: namespace.Incarnation})
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			loaded, err := source.Load(ctx, r)
			if err != nil || loaded.Namespace != namespace || len(loaded.Grants)+len(loaded.Projects)+len(loaded.Teams) != 0 {
				t.Fatal("rebound initial source unavailable:", err)
			}
			entries, err := r.Changesets(ctx, nil, 10)
			if err != nil || len(entries) != 1 || entries[0].Kind != model.ChangesetDerived {
				t.Fatal("initial publication/recovery history is not exactly one commit")
			}
			if !reflect.DeepEqual(publicationInventory(t, r), durableRows) {
				t.Fatal("initial recovery changed the exact five-row source/fact/Ready inventory")
			}
			pinned := publicationInventory(t, initial)
			if mode == "before_commit" && len(pinned) != 0 || mode != "before_commit" && !reflect.DeepEqual(pinned, durableRows) {
				t.Fatal("initial recovery rewrote its pinned inventory")
			}
		})
	}
}

// Independently enumerate the complete empty authority publication: one raw
// source, three explicit empty typed domains and the exact H+1 Ready marker.
// This oracle does not call either publication producer.
func initialPublicationRows(t *testing.T, namespace authz.Namespace, body []byte, sourceRevision model.Revision, typed bool) map[driver.DerivedRowKey]driver.DerivedRow {
	t.Helper()
	keys, err := authzfixed.SelectionRows(namespace)
	if err != nil {
		t.Fatal(err)
	}
	want := map[driver.DerivedRowKey]driver.DerivedRow{keys.Source: {Key: keys.Source.Key, Value: append([]byte(nil), body...)}}
	for i, key := range []driver.DerivedRowKey{keys.Grants, keys.Projects, keys.Teams} {
		facts := driver.SelectionFacts{Kind: []driver.SelectionFactKind{driver.SelectionScopedGrantFact, driver.SelectionProjectFact, driver.SelectionTeamFact}[i], Rows: []driver.SelectionFact{}}
		value, err := json.Marshal(facts)
		if err != nil {
			t.Fatal(err)
		}
		row := driver.DerivedRow{Key: key.Key, Value: value}
		if typed {
			row.Index = &driver.SelectionIndex{Version: driver.SelectionIndexVersion, Row: key, Incarnation: namespace.Incarnation, Branch: model.Natural64(0), ValueDigest: driver.SelectionDigest(value), Source: driver.SelectionSource{Key: driver.LockKey{Kind: driver.LockDerived, Derived: &keys.Source}, Revision: sourceRevision, Present: true, Digest: driver.SelectionDigest(body)}, Facts: &facts}
		}
		// Own the expected annotation at the same boundary as driver reads and
		// the fault capture; retain the explicit [] in the logical Value bytes.
		want[key] = row.Clone()
	}
	marker, err := json.Marshal(struct {
		Version     int
		Incarnation string
		Through     model.Revision
		Unsafe      uint64
	}{2, namespace.Incarnation, model.Natural64(1), 0})
	if err != nil {
		t.Fatal(err)
	}
	want[driver.DerivedRowKey{Table: driver.DerivedReadyState, Key: "plug.ready.v2/coverage"}] = driver.DerivedRow{Key: "plug.ready.v2/coverage", Value: marker}
	return want
}

func publicationInventory(t *testing.T, r driver.Reader) map[driver.DerivedRowKey]driver.DerivedRow {
	t.Helper()
	rows := map[driver.DerivedRowKey]driver.DerivedRow{}
	for _, table := range []driver.DerivedTable{driver.DerivedReadyState, driver.DerivedAncestorClosure, driver.DerivedCutClosure, driver.DerivedAttribute, driver.DerivedSortKey, driver.DerivedReferenceIndex, driver.DerivedGrant, driver.DerivedOntology, driver.DerivedPlugins} {
		values, err := r.DerivedRows(context.Background(), table, "")
		if err != nil {
			t.Fatal(err)
		}
		for _, row := range values {
			key := driver.DerivedRowKey{Table: table, Key: row.Key}
			if _, duplicate := rows[key]; duplicate {
				t.Fatal("duplicate publication inventory row")
			}
			rows[key] = row.Clone()
		}
	}
	return rows
}

func TestMutableFactsRejectsPublicationDriftBeforeHead(t *testing.T) {
	store := &factsPublicationStore{Store: memory.New()}
	f := newFixtureOn(t, store, nil)
	competitor, err := authzrpc.NewMutableFacts(f.base)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := authzrpc.New(context.Background(), authzrpc.Config{Store: store, Lease: f.lease, Provider: f.keys, Facts: competitor, Namespace: f.base.Namespace, DefaultProject: "project"}); err != nil {
		t.Fatal(err)
	}
	next := f.base
	next.Grants = nil
	var winningHead model.Revision
	var winningACL driver.LockState
	var winningRows []driver.LockState
	fault := &factsPublicationFault{beforeLock: func() {
		if err := competitor.Replace(next); err != nil {
			t.Fatal(err)
		}
		winningHead, winningACL = publicationState(t, store.Store, f.base.Namespace)
		winningRows = publicationProjections(t, store.Store, f.base.Namespace)
	}}
	store.next = fault
	if err := f.facts.Replace(f.base); err == nil {
		t.Fatal("publication ignored source/ACL drift after census")
	}
	if fault.beforeLock != nil || fault.headCalls != 0 || fault.commitCalls != 0 || len(fault.rows) != 0 {
		t.Fatal("stale authority reached Head or staged projections")
	}
	head, acl := publicationState(t, store.Store, f.base.Namespace)
	if head.String() != winningHead.String() || !reflect.DeepEqual(acl, winningACL) || !reflect.DeepEqual(publicationProjections(t, store.Store, f.base.Namespace), winningRows) {
		t.Fatal("losing publication changed the winning source/index/ACL")
	}
	if _, err := f.facts.Load(context.Background(), f.reader(t)); err == nil {
		t.Fatal("stale binding remained usable after competing commit")
	}
	if _, err := competitor.Load(context.Background(), f.reader(t)); err != nil {
		t.Fatal("winning binding unavailable:", err)
	}
}

// This producer control runs the real fixed-kernel service ontology through
// Writer.Apply. Missing template on a new create is normalized to BOOLEAN false;
// a legacy source's absent template stays absent on an ordinary body-only write.
func TestWriterPublishesKernelBooleanAndSourceOnlyRestamps(t *testing.T) {
	writerPublishesKernelBooleanAndSourceOnlyRestamps(t, func(*testing.T) driver.Store { return memory.New() })
}

func writerPublishesKernelBooleanAndSourceOnlyRestamps(t *testing.T, open storeFactory) {
	t.Helper()
	ctx := context.Background()
	store := open(t)
	defer store.Close()
	lease, err := store.Lease(ctx, "real-projection-writer", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release(ctx)
	writer := pipeline.New(store, lease, pipeline.WithOntology(service.NewOntology(nil)))
	principal := model.PrincipalRef{Kind: "user", ID: "projection-owner"}
	read := func(at *model.Revision) driver.Reader {
		t.Helper()
		meta, err := store.Meta(ctx)
		if err != nil {
			t.Fatal(err)
		}
		r, err := store.Read(ctx, driver.View{Incarnation: meta.Incarnation, At: at})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = r.Close() })
		return r
	}
	observe := func(r driver.Reader, ref model.Ref) []driver.LockState {
		t.Helper()
		rows, err := pipeline.SourceSelectionRows(ref)
		if err != nil {
			t.Fatal(err)
		}
		closure, err := pipeline.AncestorRow(ref.ID, ref.ID, 0)
		if err != nil {
			t.Fatal(err)
		}
		rows = append(rows, driver.DerivedRowKey{Table: driver.DerivedAncestorClosure, Key: closure.Key})
		keys := make([]driver.LockKey, len(rows))
		for i, row := range rows {
			row := row
			keys[i] = driver.LockKey{Kind: driver.LockDerived, Derived: &row}
		}
		sort.Slice(keys, func(i, j int) bool {
			a, _ := driver.EncodeLockKey(keys[i])
			b, _ := driver.EncodeLockKey(keys[j])
			return a < b
		})
		states, err := r.Observe(ctx, keys)
		if err != nil || len(states) != 4 {
			t.Fatal("real Writer projection observations:", err)
		}
		return states
	}
	for i, input := range []any{nil, false, true} {
		fields := map[string]any{"title": "real boolean", "priority": 2}
		if input != nil {
			fields["template"] = input
		}
		m := model.WriteMeta{Method: "bead.create", Project: "test", IdempotencyKey: fmt.Sprintf("%032x", i+1)}
		created, err := writer.Apply(ctx, principal, model.ChangeSet{Ops: []model.Op{{Kind: model.OpCreate, Create: &model.CreateOp{Type: "issue", Fields: fields}}}}, m)
		if err != nil || len(created.Results) != 1 {
			t.Fatal("real ontology create:", err)
		}
		ref := created.Results[0].Ref
		r := read(nil)
		records, err := r.Get(ctx, []model.Ref{ref})
		if err != nil || len(records) != 1 {
			t.Fatal(err)
		}
		doc, err := pipeline.DecodeDocument(records[0].Body)
		if err != nil {
			t.Fatal(err)
		}
		want := input == true
		if value, ok := doc.Fields["template"].(bool); !ok || value != want {
			t.Fatal("kernel did not publish the actual boolean template")
		}
		before := observe(r, ref)
		beforeInventory := selectionFamilyInventory(t, r)
		for _, state := range before {
			index := state.Derived
			if !state.Present || index == nil || index.Index == nil || index.Index.Source.Digest != driver.SelectionDigest(records[0].Body) || index.Index.Source.Revision.String() != created.Revision.String() {
				t.Fatal("real Writer did not publish complete source-bound projection")
			}
			if state.Key.Derived.Table == driver.DerivedSortKey && (index.Index.Bead == nil || index.Index.Bead.Template != want || len(index.Index.Bead.Absent) != 0) {
				t.Fatal("real boolean producer annotation mismatch")
			}
		}
		m.Method, m.IdempotencyKey = "tx.apply", fmt.Sprintf("%032x", i+11)
		changed, err := writer.Apply(ctx, principal, model.ChangeSet{Ops: []model.Op{{Kind: model.OpSetMeta, SetMeta: &model.SetMetaOp{Target: ref, Key: "note", Value: json.RawMessage(`"changed source only"`)}}}}, m)
		if err != nil {
			t.Fatal(err)
		}
		afterReader := read(nil)
		afterRecords, err := afterReader.Get(ctx, []model.Ref{ref})
		if err != nil || len(afterRecords) != 1 || afterRecords[0].Revision.String() != changed.Revision.String() {
			t.Fatal(err)
		}
		after := observe(afterReader, ref)
		wantInventory := sourceRestampedInventory(t, beforeInventory, records[0], afterRecords[0])
		afterInventory := selectionFamilyInventory(t, afterReader)
		for j, state := range after {
			want := before[j]
			want.Revision = changed.Revision
			key := *want.Key.Derived
			row, ok := wantInventory[key]
			if !ok {
				t.Fatal("source-only expected inventory lost a selected row")
			}
			if key.Table == driver.DerivedAncestorClosure {
				if state.Derived == nil {
					t.Fatal("source-only restamp lost the selected ancestor row")
				}
				inventoryRow, ok := afterInventory[key]
				if !ok {
					t.Fatal("source-only inventory lost the selected ancestor row")
				}
				row = canonicalRestampAncestorOwners(t, row, afterRecords[0])
				observedRow := canonicalRestampAncestorOwners(t, *state.Derived, afterRecords[0])
				state.Derived = &observedRow
				wantInventory[key] = row
				afterInventory[key] = canonicalRestampAncestorOwners(t, inventoryRow, afterRecords[0])
			}
			want.Derived = &row
			if !reflect.DeepEqual(state, want) {
				wantJSON, _ := json.Marshal(want)
				gotJSON, _ := json.Marshal(state)
				t.Fatalf("source-only restamp changed fields outside exact provenance and semantic leaf: key=%+v want=%s got=%s expected_source_ref=%#v actual_source_ref=%#v", *want.Key.Derived, wantJSON, gotJSON, want.Derived.Index.Source.Key.Ref, state.Derived.Index.Source.Key.Ref)
			}
		}
		if !reflect.DeepEqual(afterInventory, wantInventory) {
			t.Fatal("source-only restamp changed an unselected row or the complete selection inventory")
		}
		pinned := read(&created.Revision)
		if !reflect.DeepEqual(observe(pinned, ref), before) || !reflect.DeepEqual(selectionFamilyInventory(t, pinned), beforeInventory) {
			t.Fatal("source-only update changed historical annotations")
		}
	}
	legacyRef := model.RefByID("pl-legacy-absent-template", model.Natural64(0))
	legacyBody := []byte(`{"id":"pl-legacy-absent-template","type":"issue","project":"test","fields":{"title":"unchanged legacy","status":"todo","priority":0}}`)
	if _, err := conformance.Seed(ctx, store, lease, driver.ChangesetHeader{}, []conformance.FixtureChange{{Ref: legacyRef, Kind: "bead", Op: model.OpCreate, Body: legacyBody, Graph: &conformance.FixtureGraphSource{}}}); err != nil {
		t.Fatal(err)
	}
	_, err = writer.Apply(ctx, principal, model.ChangeSet{Ops: []model.Op{{Kind: model.OpSetMeta, SetMeta: &model.SetMetaOp{Target: legacyRef, Key: "note", Value: json.RawMessage(`"body only"`)}}}}, model.WriteMeta{Method: "tx.apply", Project: "test", IdempotencyKey: strings.Repeat("b", 32)})
	if err != nil {
		t.Fatal("real Writer rejected supported absent template:", err)
	}
	states := observe(read(nil), legacyRef)
	for _, state := range states {
		if state.Key.Derived.Table == driver.DerivedSortKey {
			if state.Derived == nil || state.Derived.Index == nil || state.Derived.Index.Bead == nil || !reflect.DeepEqual(state.Derived.Index.Bead.Absent, []driver.SelectionColumn{driver.SelectionTemplate}) || state.Derived.Index.Bead.Template {
				t.Fatal("real Writer conflated absent template with present false")
			}
		}
	}
}

// Ancestor owners borrow Record.Ref, whose private arm discriminator differs
// between native literals and JSON-decoded records. On detached comparison
// copies, canonicalize only the selected ancestor's two owner references after
// validating their complete lock identity and exact public fields against the
// committed source. All logical bytes, other annotations and rows remain exact.
func canonicalRestampAncestorOwners(t *testing.T, row driver.DerivedRow, source driver.Record) driver.DerivedRow {
	t.Helper()
	row = row.Clone()
	index := row.Index
	if index == nil || index.Facts == nil || index.Facts.Kind != driver.SelectionAncestorFact || len(index.Facts.Rows) != 1 {
		t.Fatal("selected ancestor has an incomplete typed owner annotation")
	}
	if err := index.Validate(driver.DerivedAncestorClosure, row); err != nil {
		t.Fatal("selected ancestor annotation is invalid:", err)
	}
	if err := driver.ValidateHierarchyRef(source.Ref); err != nil {
		t.Fatal("committed ancestor source identity is invalid:", err)
	}
	canonicalize := func(name string, ref *model.Ref) {
		t.Helper()
		if ref == nil {
			t.Fatal("selected ancestor owner reference is missing:", name)
		}
		if err := driver.ValidateHierarchyRef(*ref); err != nil {
			t.Fatal("selected ancestor owner reference is invalid:", name, err)
		}
		if !driver.SelectionSameRef(*ref, source.Ref) || ref.ID != source.Ref.ID || ref.Branch != source.Ref.Branch || ref.Key != source.Ref.Key {
			t.Fatalf("selected ancestor %s differs from committed source: owner=%#v source=%#v", name, *ref, source.Ref)
		}
		canonical := model.RefByID(ref.ID, ref.Branch)
		if !reflect.DeepEqual(*ref, canonical) {
			t.Logf("selected ancestor %s canonical identity preserves public fields: before=%#v after=%#v", name, *ref, canonical)
		}
		*ref = canonical
	}
	canonicalize("Source.Key.Ref", index.Source.Key.Ref)
	canonicalize("Facts.Rows[0].Ref", index.Facts.Rows[0].Ref)
	return row
}

func selectionFamilyInventory(t *testing.T, r driver.Reader) map[driver.DerivedRowKey]driver.DerivedRow {
	t.Helper()
	rows := map[driver.DerivedRowKey]driver.DerivedRow{}
	for _, table := range []driver.DerivedTable{driver.DerivedSortKey, driver.DerivedAttribute, driver.DerivedReferenceIndex, driver.DerivedAncestorClosure} {
		values, err := r.DerivedRows(context.Background(), table, "")
		if err != nil {
			t.Fatal(err)
		}
		for _, row := range values {
			key := driver.DerivedRowKey{Table: table, Key: row.Key}
			if _, duplicate := rows[key]; duplicate {
				t.Fatal("duplicate selection inventory identity")
			}
			rows[key] = row.Clone()
		}
	}
	return rows
}

// Start with every before row. Only the selected source's exact provenance and
// revision-bound semantic leaf may change; all other bytes and Index fields
// remain part of the expected full inventory. Reconstruct the original Ready
// JSON from actual source bytes, independently of the publication helpers.
func sourceRestampedInventory(t *testing.T, before map[driver.DerivedRowKey]driver.DerivedRow, oldSource, source driver.Record) map[driver.DerivedRowKey]driver.DerivedRow {
	t.Helper()
	if !driver.SelectionSameRef(oldSource.Ref, source.Ref) || oldSource.Revision.String() == source.Revision.String() || string(oldSource.Body) == string(source.Body) {
		t.Fatal("source-only control did not publish the selected changed source")
	}
	membership := func(record driver.Record) string {
		var doc struct {
			ID, Type, Project string
			Fields            map[string]any
		}
		decoder := json.NewDecoder(strings.NewReader(string(record.Body)))
		decoder.UseNumber()
		if err := decoder.Decode(&doc); err != nil {
			t.Fatal(err)
		}
		status, _ := doc.Fields["status"].(string)
		body, err := json.Marshal(struct {
			ID       string         `json:"id"`
			Type     string         `json:"type"`
			Project  string         `json:"project"`
			Status   string         `json:"status"`
			Revision model.Revision `json:"revision"`
			Fields   map[string]any `json:"fields,omitempty"`
		}{doc.ID, doc.Type, doc.Project, status, record.Revision, doc.Fields})
		if err != nil {
			t.Fatal(err)
		}
		return driver.SelectionDigest(body)
	}
	want := make(map[driver.DerivedRowKey]driver.DerivedRow, len(before))
	refreshed := map[driver.DerivedTable]int{}
	for key, row := range before {
		row = row.Clone()
		index := row.Index
		if index != nil && index.Source.Key.Kind == driver.LockRecord && index.Source.Key.Ref != nil && driver.SelectionSameRef(*index.Source.Key.Ref, oldSource.Ref) {
			if index.Validate(key.Table, row) != nil || !index.Source.Present || index.Source.Revision.String() != oldSource.Revision.String() || index.Source.Digest != driver.SelectionDigest(oldSource.Body) {
				t.Fatal("baseline selected annotation does not bind its exact old source")
			}
			index.Source.Revision, index.Source.Digest = source.Revision, driver.SelectionDigest(source.Body)
			if index.Bead != nil {
				if index.Bead.MembershipLeaf != membership(oldSource) {
					t.Fatal("baseline selected leaf does not bind the exact source")
				}
				index.Bead.MembershipLeaf = membership(source)
				body, err := json.Marshal(index.Bead)
				if err != nil {
					t.Fatal(err)
				}
				row.Value, index.ValueDigest = body, driver.SelectionDigest(body)
			}
			refreshed[key.Table]++
		}
		want[key] = row
	}
	if !reflect.DeepEqual(refreshed, map[driver.DerivedTable]int{driver.DerivedSortKey: 1, driver.DerivedAttribute: 1, driver.DerivedReferenceIndex: 1, driver.DerivedAncestorClosure: 1}) {
		t.Fatalf("incomplete selected restamp census: %+v", refreshed)
	}
	return want
}
