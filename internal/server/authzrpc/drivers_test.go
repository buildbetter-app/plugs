package authzrpc_test

import (
	"context"
	"encoding/json"
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
	"github.com/buildbetter-app/plugs/internal/plugins/authzfixed"
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
			t.Run("history_uses_current_permissions", func(t *testing.T) { currentHistory(t, open) })
			t.Run("post_image", func(t *testing.T) { postImage(t, open) })
			t.Run("authorization_races", func(t *testing.T) { authorizationRaces(t, open) })
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
	for i, project := range []string{"", "unregistered", "inactive", "other", "project", "project"} {
		ref := fixtureRef(i + 2)
		body, err := json.Marshal(map[string]any{"id": ref.ID, "type": "issue", "project": project, "fields": map[string]any{"title": "historical", "status": "todo", "priority": i + 1}})
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
			}{{"project", []int{1, 6, 7}}, {"unregistered", []int{3}}, {"inactive", []int{4}}, {"other", []int{5}}} {
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
	if len(got.Items) != 1 || got.Items[0].ID != fixtureRef(3).ID || got.Items[0].Fields["title"] != "historical" || got.AsOf.Revision.String() != view.At.String() {
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
		t.Fatal(err)
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
	return func(t *testing.T) driver.Store {
		t.Helper()
		database := fmt.Sprintf("authz_%d", sequence.Add(1))
		if out, err := localDocker("exec", name, "createdb", "-h", "127.0.0.1", "-U", "postgres", database); err != nil {
			t.Fatalf("owned test database: %v %s", err, out)
		}
		s, err := postgres.Open(context.Background(), driver.Config{DSN: "postgres://postgres@" + address + "/" + database + "?sslmode=disable"})
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
}
