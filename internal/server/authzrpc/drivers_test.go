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
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/core/service"
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
				} else if got.Error == nil {
					t.Fatalf("denied point read returned no error: result=%s; want not_found", got.Result)
				} else if got.Error.Data.Code != "not_found" {
					t.Fatalf("denied point read returned code=%q message=%q retryable=%t; want not_found", got.Error.Data.Code, got.Error.Message, got.Error.Data.Retryable)
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
	commitCalls int
	bumped      []driver.ReadFact
	committed   model.Changeset
}

func (s *factsPublicationStore) Write(ctx context.Context, lease driver.ActiveLease) (driver.WriteTx, error) {
	tx, err := s.Store.Write(ctx, lease)
	if err != nil || s.next == nil {
		return tx, err
	}
	fault := s.next
	s.next = nil
	return &factsPublicationTx{WriteTx: tx, fault: fault}, nil
}

type factsPublicationTx struct {
	driver.WriteTx
	fault *factsPublicationFault
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
			afterHead, afterACL := publicationState(t, store.Store, f.base.Namespace)
			entries, err := f.reader(t).Changesets(ctx, &driver.Cursor{Incarnation: f.base.Namespace.Incarnation, Revision: beforeHead}, 10)
			if err != nil {
				t.Fatal(err)
			}
			if fault.lostAck {
				if afterHead.String() == beforeHead.String() || !afterACL.Present || afterACL.Revision.String() != afterHead.String() || fault.committed.Revision.String() != afterHead.String() || len(entries) != 1 || !reflect.DeepEqual(entries[0], fault.committed) || entries[0].Kind != model.ChangesetDerived || entries[0].Principal.Kind != "system" || entries[0].Principal.ID != "authzrpc" || len(entries[0].Changes) != 0 {
					t.Fatalf("lost acknowledgement lacked a real atomic ACL commit: head=%s ACL=%+v entries=%+v", afterHead, afterACL, entries)
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
				if afterHead.String() != beforeHead.String() || !reflect.DeepEqual(afterACL, beforeACL) || len(entries) != 0 || !fault.committed.Revision.IsZero() {
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
