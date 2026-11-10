package authzrpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/viewops"
	"github.com/buildbetter-app/plugs/internal/plugins/authnapikey"
	"github.com/buildbetter-app/plugs/internal/plugins/authzfixed"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/internal/storage/postgres"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

type w9ObservedStore struct {
	driver.Store
	selects, queries, gets int
	allowScan              bool
	afterSelect            func(driver.SelectionResult)
	afterQuery             func()
}
type w9ObservedReader struct {
	driver.Reader
	store *w9ObservedStore
}

func (s *w9ObservedStore) Read(ctx context.Context, v driver.View) (driver.Reader, error) {
	r, err := s.Store.Read(ctx, v)
	if err != nil {
		return nil, err
	}
	return &w9ObservedReader{r, s}, nil
}
func (r *w9ObservedReader) Query(ctx context.Context, q driver.Query) (driver.Page[driver.Record], error) {
	r.store.queries++
	if !r.store.allowScan {
		return driver.Page[driver.Record]{}, errors.New("W9 compiled view attempted payload scan")
	}
	got, err := r.Reader.Query(ctx, q)
	if hook := r.store.afterQuery; hook != nil && err == nil {
		r.store.afterQuery = nil
		hook()
	}
	return got, err
}
func (r *w9ObservedReader) Get(ctx context.Context, refs []model.Ref) ([]driver.Record, error) {
	r.store.gets += len(refs)
	return r.Reader.Get(ctx, refs)
}
func (r *w9ObservedReader) Select(ctx context.Context, p driver.SelectionPlan) (driver.SelectionResult, error) {
	r.store.selects++
	selector, ok := r.Reader.(driver.SelectionReader)
	if !ok {
		return driver.SelectionResult{}, errors.New("W9 engine lacks selection")
	}
	got, err := selector.Select(ctx, p)
	if hook := r.store.afterSelect; hook != nil && err == nil {
		r.store.afterSelect = nil
		hook(got)
	}
	return got, err
}

type w9Fixture struct {
	store              *w9ObservedStore
	lease              driver.ActiveLease
	app                *Composition
	facts              *MutableFacts
	base               authzfixed.Facts
	started            time.Time
	traceSQLiteCommits bool
}

func w9Ref(id string) model.Ref { return model.RefByID(id, model.Natural64(0)) }

var w9Reader = model.PrincipalRef{Kind: "user", ID: "user:w9-reader"}

func newW9Fixture(t *testing.T, store driver.Store) *w9Fixture {
	t.Helper()
	ctx := context.Background()
	f := &w9Fixture{store: &w9ObservedStore{Store: store}, started: time.Now()}
	t.Cleanup(func() { _ = store.Close() })
	var err error
	f.lease, err = store.Lease(ctx, "w9-view-selection", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.lease.Release(ctx) })
	meta, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	ns := authz.Namespace{StoreID: string(meta.StoreID), Incarnation: meta.Incarnation, InstallationID: "fixed", Authority: w9Ref("authority")}
	f.base = authzfixed.Facts{Namespace: ns, Projects: []authzfixed.Project{
		{Ref: w9Ref("project-canonical-visible"), Name: "visible-name", InstallationID: "fixed", Active: true},
		{Ref: w9Ref("project-canonical-hidden"), Name: "hidden-name", InstallationID: "fixed", Active: true},
	}, Grants: []authz.Grant{{Subject: authz.SubjectRef{Kind: "user", ID: w9Reader.ID}, Scope: authz.Scope{Kind: authz.ProjectScope, Ref: w9Ref("project-canonical-hidden")}, Role: "involved"}}}
	f.facts, err = NewMutableFacts(f.base)
	if err != nil {
		t.Fatal(err)
	}
	provider := authnapikey.New(authnapikey.Config{AuthorizationInstalled: func() bool { return true }})
	f.app, err = New(ctx, Config{Store: f.store, Lease: f.lease, Provider: provider, Facts: f.facts, Namespace: ns})
	if err != nil {
		t.Fatal(err)
	}
	// Host construction is read-only; this fixture explicitly installs the
	// authority domains required by its grant/project selection assertions.
	if err := f.facts.Replace(f.base); err != nil {
		t.Fatal(err)
	}
	f.app.Checker.chain.Rules = append(f.app.Checker.chain.Rules, w9ProjectForbid{ns})
	return f
}

// The installed rule's interpreter uses current source assignment and exact
// registry refs. It does not invoke the lowering, selector, or view matcher.
type w9ProjectForbid struct{ namespace authz.Namespace }

func (r w9ProjectForbid) Compile(q authz.ListRequest) (authz.FilterTemplate, error) {
	return authz.FilterTemplate{Namespace: r.namespace, Request: q, Permit: authz.FilterExpr{Op: authz.False}, Forbid: authz.FilterExpr{Op: authz.GrantExists, Subject: authz.CallerSubject, Scope: authz.ProjectBindingScope, Role: "involved"}}, nil
}
func (r w9ProjectForbid) Eval(ctx context.Context, s authz.Slice, qs []authz.CheckRequest) ([]authz.Decision, error) {
	out := make([]authz.Decision, len(qs))
	for i, q := range qs {
		out[i].Effect = authz.NoDecision
		project, err := s.ProjectForBead(ctx, q.Resource.Ref)
		if err != nil {
			return nil, err
		}
		if !project.Found || q.Caller.Principal.Kind != "user" {
			continue
		}
		denied, err := s.HasGrant(ctx, authz.SubjectRef{Kind: "user", ID: q.Caller.Principal.ID}, authz.Scope{Kind: authz.ProjectScope, Ref: project.Ref}, "involved")
		if err != nil {
			return nil, err
		}
		if denied {
			out[i].Effect = authz.Deny
		}
	}
	return out, nil
}

func (f *w9Fixture) views(t *testing.T, cfg viewops.Config) *viewops.Service {
	t.Helper()
	cfg.Selection = f.app.Service.ViewSelection()
	cfg.OntologyRevision = model.Natural64(1)
	v, err := viewops.New(f.store, f.lease, cfg)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

// Publish exact source bytes and actual core-produced annotations in bounded
// batches, with one complete lock census and one call per projection table.
// The selector still receives the complete 100k input. No fixture fabricates
// annotations or coverage markers, or borrows another run's proof.
func (f *w9Fixture) seed(t *testing.T, records []driver.Record) model.Revision {
	t.Helper()
	var revision model.Revision
	total, completed, batch := len(records), 0, 0
	for len(records) != 0 {
		n := len(records)
		if n > 1000 {
			n = 1000
		}
		batch++
		revision = f.seedBatch(t, records[:n], batch, completed, total)
		completed += n
		records = records[n:]
	}
	return revision
}

func (f *w9Fixture) seedBatch(t *testing.T, records []driver.Record, batch, completed, total int) model.Revision {
	t.Helper()
	ctx := context.Background()
	started, stageStarted := time.Now(), time.Now()
	stage, recordIndex, committed := "prepare", -1, false
	mark := func(name string, index int) {
		stage, recordIndex, stageStarted = name, index, time.Now()
	}
	defer func() {
		if !committed {
			// Keep the original fatal error and add the operation it came from.
			t.Logf("W9 seed batch=%d size=%d completed_batches=%d completed_records=%d total_records=%d stage=%s record_index=%d elapsed=%s batch_elapsed=%s stage_elapsed=%s", batch, len(records), batch-1, completed, total, stage, recordIndex, time.Since(f.started), time.Since(started), time.Since(stageStarted))
		}
	}()
	plans := make([]*pipeline.SourceProjectionPlan, len(records))
	keys := map[string]driver.LockKey{}
	bumps := map[driver.Sentinel]bool{}
	for i, record := range records {
		mark("PrepareSource", i)
		var p *pipeline.SourceProjectionPlan
		var err error
		if record.Body == nil {
			p, err = pipeline.PrepareSourceDelete(record.Ref, record.Kind)
		} else {
			p, err = pipeline.PrepareSourceRecord(record)
		}
		if err != nil {
			t.Fatal(err)
		}
		plans[i] = p
		for _, key := range p.LockKeys() {
			mark("EncodeLockKey", i)
			id, err := driver.EncodeLockKey(key)
			if err != nil {
				t.Fatal(err)
			}
			keys[id] = key
		}
		for _, sentinel := range p.Sentinels() {
			bumps[sentinel] = true
		}
	}
	ids := make([]string, 0, len(keys))
	for id := range keys {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	requests := make([]driver.LockRequest, len(ids))
	lockKeys := make([]driver.LockKey, len(ids))
	for i, id := range ids {
		requests[i] = driver.LockRequest{Key: keys[id], Mode: driver.LockUpdate}
		lockKeys[i] = keys[id]
	}
	mark("Write", -1)
	tx, err := f.store.Write(ctx, f.lease)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Close()
	mark("Lock", -1)
	states, err := tx.Lock(ctx, requests)
	if err != nil {
		t.Fatal(err)
	}
	previous := map[string]driver.LockState{}
	for _, state := range states {
		if state.Key.Kind == driver.LockRecord {
			previous[state.Key.Ref.ID] = state
		}
	}
	mark("Head", -1)
	if _, err := tx.Head(ctx); err != nil {
		t.Fatal(err)
	}
	header := driver.ChangesetHeader{Incarnation: tx.Snapshot().Incarnation, Kind: model.ChangesetWrite, Principal: model.PrincipalRef{Kind: "system", ID: "w9-fixture"}, LockKeys: lockKeys}
	writer, ok := tx.(driver.GraphSourceWriter)
	if !ok {
		t.Fatal("W9 engine lacks complete source publication")
	}
	add := map[driver.DerivedTable][]driver.DerivedRow{}
	remove := map[driver.DerivedTable][]driver.DerivedRow{}
	for i, p := range plans {
		record := records[i]
		before := previous[record.Ref.ID]
		mark("SourceSelectionRows", i)
		rowKeys, err := pipeline.SourceSelectionRows(record.Ref)
		if err != nil {
			t.Fatal(err)
		}
		if record.Body == nil {
			mark("Delete", i)
			if err := tx.Delete(ctx, record.Ref); err != nil {
				t.Fatal(err)
			}
			for _, key := range rowKeys {
				remove[key.Table] = append(remove[key.Table], driver.DerivedRow{Key: key.Key})
			}
		} else {
			var doc pipeline.Document
			mark("DecodeSource", i)
			if err := json.Unmarshal(record.Body, &doc); err != nil {
				t.Fatal(err)
			}
			links := make([]driver.SourceLink, len(doc.Links))
			for i, link := range doc.Links {
				links[i] = driver.SourceLink{Kind: link.Kind, To: link.To}
			}
			mark("PutGraphRecord", i)
			if err := writer.PutGraphRecord(ctx, record, doc.Parent, links); err != nil {
				t.Fatal(err)
			}
			mark("SelectionRows", i)
			rows, err := p.SelectionRows(tx.Snapshot().Incarnation, before.Revision)
			if err != nil || len(rows) != len(rowKeys) {
				t.Fatalf("actual producer rows=%d keys=%d err=%v", len(rows), len(rowKeys), err)
			}
			for i, row := range rows {
				add[rowKeys[i].Table] = append(add[rowKeys[i].Table], row)
			}
		}
		op := model.OpCreate
		if before.Present {
			op = model.OpPatch
		}
		if records[i].Body == nil {
			op = model.OpDelete
		}
		header.Changes = append(header.Changes, model.ChangeRecord{Ref: records[i].Ref, Op: op, FromRevision: before.Revision})
	}
	for _, table := range []driver.DerivedTable{driver.DerivedSortKey, driver.DerivedAttribute, driver.DerivedReferenceIndex} {
		if len(remove[table])+len(add[table]) != 0 {
			mark("PutSelectionDerived/"+string(table), -1)
			if err := pipeline.PutSelectionDerived(ctx, tx, table, remove[table], add[table]); err != nil {
				t.Fatal(err)
			}
		}
	}
	sentinels := make([]driver.Sentinel, 0, len(bumps))
	for sentinel := range bumps {
		sentinels = append(sentinels, sentinel)
	}
	mark("BumpSentinels", -1)
	if err := tx.BumpSentinels(ctx, sentinels); err != nil {
		t.Fatal(err)
	}
	commitCtx := ctx
	var trace *sqlite.CommitTrace
	if f.traceSQLiteCommits {
		trace = &sqlite.CommitTrace{}
		commitCtx = sqlite.WithCommitTrace(ctx, trace)
	}
	mark("Commit", -1)
	t.Logf("W9 seed Commit start batch=%d size=%d completed_records=%d elapsed=%s", batch, len(records), completed, time.Since(f.started))
	cs, err := tx.Commit(commitCtx, header)
	t.Logf("W9 seed Commit end batch=%d elapsed=%s duration=%s error_type=%T error_value=%#v error=%v", batch, time.Since(f.started), time.Since(stageStarted), err, err, err)
	if trace != nil {
		// Staged counts describe inputs; nested phase durations overlap.
		samples := trace.Snapshot()
		encoded, encodeErr := json.Marshal(samples)
		if encodeErr != nil {
			t.Errorf("SQLite Commit trace encoding: %v", encodeErr)
		}
		t.Logf("W9_SQLITE_COMMIT_PHASES batch=%d size=%d completed_records=%d sample_count=%d samples=%s", batch, len(records), completed, len(samples), encoded)
	}
	if err != nil {
		t.Fatal(err)
	}
	committed = true
	return cs.Revision
}
func w9Record(id, typ, project, title, status string, labels ...string) driver.Record {
	raw, err := json.Marshal(pipeline.Document{ID: id, Type: typ, Project: project, Fields: map[string]any{"title": title, "status": status}, Labels: labels})
	if err != nil {
		panic(err)
	}
	return driver.Record{Ref: w9Ref(id), Kind: "bead", Body: raw}
}
func w9Code(t *testing.T, err error, want model.ErrorCode) {
	t.Helper()
	var typed *model.Error
	if !errors.As(err, &typed) || typed.Data.Code != want {
		t.Fatalf("error=%T %v want=%s", err, err, want)
	}
}
func w9IDs(result viewops.Result) []string {
	out := []string{}
	for _, item := range result.Items {
		out = append(out, item.ID)
	}
	for _, group := range result.Groups {
		for _, item := range group.Items {
			out = append(out, item.ID)
		}
	}
	sort.Strings(out)
	return out
}
func w9Run(t *testing.T, views *viewops.Service, req viewops.RunRequest) viewops.Result {
	t.Helper()
	got, err := views.Run(context.Background(), w9Reader, req)
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func w9Engines(t *testing.T, run func(*testing.T, driver.Store)) {
	t.Helper()
	t.Run("memory", func(t *testing.T) { run(t, memory.New()) })
	t.Run("sqlite", func(t *testing.T) {
		s, err := (sqlite.Driver{}).Open(context.Background(), driver.Config{DSN: t.TempDir() + "/w9.db"})
		if err != nil {
			t.Fatal(err)
		}
		run(t, s)
	})
	t.Run("postgres", func(t *testing.T) {
		name := fmt.Sprintf("plug-swarm-w9-%d-%d", os.Getpid(), time.Now().UnixNano())
		docker := func(args ...string) ([]byte, error) {
			ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
			defer cancel()
			return exec.CommandContext(ctx, "docker", append([]string{"--host=unix:///var/run/docker.sock"}, args...)...).CombinedOutput()
		}
		const image = "postgres@sha256:2d2b8998d31037bf721cfdf764d76ba74171b4fab3431b7f72c27c56ddbdf9e3"
		if out, err := docker("run", "-d", "--rm", "--name", name, "-e", "POSTGRES_HOST_AUTH_METHOD=trust", "-p", "127.0.0.1::5432", image); err != nil {
			t.Fatalf("PostgreSQL start: %v %s", err, out)
		}
		t.Cleanup(func() {
			if out, err := docker("rm", "-f", name); err != nil {
				t.Errorf("PostgreSQL cleanup: %v %s", err, out)
			}
		})
		out, err := docker("port", name, "5432/tcp")
		address := strings.TrimSpace(string(out))
		host, _, parseErr := net.SplitHostPort(address)
		if err != nil || parseErr != nil || host != "127.0.0.1" {
			t.Fatalf("PostgreSQL loopback: %q %v", address, err)
		}
		deadline := time.Now().Add(40 * time.Second)
		for {
			if _, err := docker("exec", name, "pg_isready", "-h", "127.0.0.1", "-U", "postgres"); err == nil {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("PostgreSQL readiness deadline")
			}
			time.Sleep(25 * time.Millisecond)
		}
		s, err := postgres.Open(context.Background(), driver.Config{DSN: "postgres://postgres@" + address + "/postgres?sslmode=disable"})
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("actual PostgreSQL image=%s loopback=true", image)
		run(t, s)
	})
}

func TestW9StorageMembershipTotalsSuffixesAndOpaqueBytesAcrossEngines(t *testing.T) {
	w9Engines(t, func(t *testing.T, store driver.Store) {
		f := newW9Fixture(t, store)
		records := []driver.Record{
			w9Record("a", "bug", "visible-name", "literal_%needle", "todo", "x", "x"),
			w9Record("b", "task", "visible-name", "literal_%needle", "todo", "y"),
			w9Record("c", "bug", "visible-name", "literal_%needle", "todo", "x", "y"),
			w9Record("d", "task", "visible-name", "literal_%needle", "done", "x"),
			w9Record("hidden", "secret-kind", "hidden-name", "literal_%needle", "todo", "x"),
			w9Record("case", "case-kind", "visible-name", "LITERAL_%needle", "todo", "x"),
		}
		// Whitespace, escape spelling and a number beyond binary64 precision
		// belong to the original logical source bytes, not a selector codec.
		records[0].Body = []byte("{ \"id\":\"a\",\"type\":\"bug\",\"project\":\"visible-name\",\"fields\":{\"title\":\"literal_%needle\",\"status\":\"todo\",\"exact\":9007199254740993},\"labels\":[\"x\",\"x\"] }")
		f.seed(t, records)
		views := f.views(t, viewops.Config{})
		budget := model.Natural64(1)
		def := &viewops.Definition{Filter: &viewops.Filter{Resources: []string{"a", "a", "b", "c", "d", "hidden", "case"}, Types: []string{"bug", "task", "secret-kind", "case-kind"}, Query: "title=literal_% AND status=todo AND (label=x OR label=y)"}, Fields: []string{"fields"}, GroupBy: []string{"type"}, Sort: []string{"-title"}, BudgetTokens: &budget}
		// This explicit independent oracle is fixture truth. It does not call
		// queryMatches, compileMembership, lowerViewFilter or storage.Select.
		wantAll := []string{"a", "b", "c"}
		seen := []string{}
		suffix := map[string]int{"bug": 2, "task": 1}
		req := viewops.RunRequest{Definition: def}
		for page := 0; page < 3; page++ {
			result := w9Run(t, views, req)
			ids := w9IDs(result)
			if len(ids) != 1 || ids[0] != wantAll[page] {
				t.Fatalf("page%d=%+v want=%s", page, result, wantAll[page])
			}
			seen = append(seen, ids...)
			if ids[0] == "a" && result.Groups[0].Items[0].Fields["exact"] != json.Number("9007199254740993") {
				t.Fatalf("exact number lost: %+v", result)
			}
			typ := "bug"
			if ids[0] == "b" {
				typ = "task"
			}
			suffix[typ]--
			for _, group := range result.Groups {
				want := 2
				if group.Key == "task" {
					want = 1
				}
				if group.Count != want {
					t.Fatalf("group total=%+v", group)
				}
				got := 0
				if group.Elided != nil {
					got = group.Elided.Count
					if group.Elided.Reason != "output_budget" {
						t.Fatal(group)
					}
				}
				if got != suffix[group.Key] {
					t.Fatalf("suffix page%d group=%+v want=%v", page, group, suffix)
				}
			}
			if page < 2 && (result.NextCursor == nil || result.NextCursor.AuthorityBinding == "") {
				t.Fatalf("missing bound continuation: %+v", result)
			}
			req.Cursor = result.NextCursor
		}
		if !reflect.DeepEqual(seen, wantAll) || req.Cursor != nil || f.store.selects != 3 || f.store.queries != 0 {
			t.Fatalf("membership=%v selects=%d scans=%d", seen, f.store.selects, f.store.queries)
		}
		negated := w9Run(t, views, viewops.RunRequest{CountOnly: true, Definition: &viewops.Definition{Filter: &viewops.Filter{Resources: []string{"a", "a", "b", "c", "d", "hidden"}, Query: "status=todo AND (label=x OR label=y) AND NOT label=y"}}})
		if negated.Count == nil || *negated.Count != 1 {
			t.Fatalf("label NOT/AND membership=%+v", negated)
		}
		raw, err := json.Marshal(w9Run(t, views, viewops.RunRequest{Definition: &viewops.Definition{Filter: def.Filter, Fields: []string{"fields"}}}))
		if err != nil {
			t.Fatal(err)
		}
		for _, secret := range []string{"hidden", "secret-kind", "case-kind", "hidden-name"} {
			if strings.Contains(string(raw), secret) {
				t.Fatalf("hidden ordinary trace %q: %s", secret, raw)
			}
		}
		meta, _ := store.Meta(context.Background())
		reader, err := store.Read(context.Background(), driver.View{Incarnation: meta.Incarnation})
		if err != nil {
			t.Fatal(err)
		}
		defer reader.Close()
		saved, err := reader.Get(context.Background(), []model.Ref{w9Ref("a")})
		if err != nil || len(saved) != 1 || string(saved[0].Body) != string(records[0].Body) {
			t.Fatalf("logical bytes changed: %v", err)
		}
	})
}

func TestW9CountOnlyZeroAndSummaryDoNoPayloadOrParentWorkAcrossEngines(t *testing.T) {
	w9Engines(t, func(t *testing.T, store driver.Store) {
		f := newW9Fixture(t, store)
		f.seed(t, []driver.Record{w9Record("hidden", "hidden-kind", "hidden-name", "secret", "todo")})
		calls := 0
		views := f.views(t, viewops.Config{Readiness: w9Readiness(func(context.Context, driver.Reader, []model.Ref) (map[string]viewops.ReadinessState, error) {
			calls++
			return nil, errors.New("payload readiness called")
		}), ParentProjection: &viewops.ParentProjectionConfig{Project: func(context.Context, model.PrincipalRef, driver.Reader, []model.Ref) (viewops.ParentProjectionAuthority, error) {
			calls++
			return viewops.ParentProjectionAuthority{}, errors.New("payload parents called")
		}, ValidateAuthority: func(context.Context, model.PrincipalRef, string) error {
			calls++
			return errors.New("payload parent fence called")
		}}})
		before := f.store.gets
		got := w9Run(t, views, viewops.RunRequest{CountOnly: true, Definition: &viewops.Definition{Filter: &viewops.Filter{}}})
		if got.Count == nil || *got.Count != 0 || got.Items == nil || len(got.Items) != 0 || len(got.Groups) != 0 || got.NextCursor != nil || calls != 0 || f.store.gets != before || f.store.queries != 0 {
			t.Fatalf("zero count=%+v payload_calls=%d gets=%d", got, calls, f.store.gets-before)
		}
		got = w9Run(t, views, viewops.RunRequest{Definition: &viewops.Definition{Filter: &viewops.Filter{}, Fields: []string{}, GroupBy: []string{"type"}}})
		if len(got.Groups) != 0 || len(got.Items) != 0 || got.Count != nil || calls != 0 || f.store.gets != before {
			t.Fatalf("zero summary=%+v calls=%d gets=%d", got, calls, f.store.gets-before)
		}
	})
}

type w9Readiness func(context.Context, driver.Reader, []model.Ref) (map[string]viewops.ReadinessState, error)

func (fn w9Readiness) Pending(ctx context.Context, r driver.Reader, refs []model.Ref) (map[string]viewops.ReadinessState, error) {
	return fn(ctx, r, refs)
}

func TestW9HistoricalContentCurrentAuthorityContinuationAndFinalFenceAcrossEngines(t *testing.T) {
	w9Engines(t, func(t *testing.T, store driver.Store) {
		f := newW9Fixture(t, store)
		old := f.seed(t, []driver.Record{w9Record("a", "bug", "visible-name", "old-title", "todo"), w9Record("b", "task", "visible-name", "old-title", "todo")})
		views := f.views(t, viewops.Config{})
		budget := model.Natural64(1)
		def := &viewops.Definition{Filter: &viewops.Filter{Query: "title=old-title"}, Fields: []string{"fields"}, BudgetTokens: &budget}
		first := w9Run(t, views, viewops.RunRequest{Definition: def, AsOf: &viewops.AsOf{Revision: old}})
		f.seed(t, []driver.Record{w9Record("a", "bug", "hidden-name", "new-secret-title", "secret-status")})
		count := w9Run(t, views, viewops.RunRequest{Definition: def, CountOnly: true, AsOf: &viewops.AsOf{Revision: old}})
		if count.Count == nil || *count.Count != 1 {
			t.Fatalf("history used old authority: %+v", count)
		}
		page := w9Run(t, views, viewops.RunRequest{Definition: def, AsOf: &viewops.AsOf{Revision: old}})
		if !reflect.DeepEqual(w9IDs(page), []string{"b"}) || page.Items[0].Fields["title"] != "old-title" {
			t.Fatalf("historical content=%+v", page)
		}
		got, err := views.Run(context.Background(), w9Reader, viewops.RunRequest{Definition: def, Cursor: first.NextCursor})
		w9Code(t, err, "cursor_invalid")
		if len(got.Items) != 0 || got.Count != nil {
			t.Fatal(got)
		}
		f.seed(t, []driver.Record{{Ref: w9Ref("b"), Kind: "bead"}})
		deleted := w9Run(t, views, viewops.RunRequest{Definition: def, CountOnly: true, AsOf: &viewops.AsOf{Revision: old}})
		if deleted.Count == nil || *deleted.Count != 0 {
			t.Fatalf("historical source bypassed current deletion: %+v", deleted)
		}
		f.seed(t, []driver.Record{w9Record("b", "task", "visible-name", "current-title", "todo")})
		// Revoke after actual Select, before payload/publication. Verify the
		// independent expected pre-fence total for both nonempty and zero
		// cases, then require full erasure in item, count and summary modes.
		for _, mode := range []string{"page", "count", "summary"} {
			for _, zero := range []bool{false, true} {
				t.Run(fmt.Sprintf("final_%s_zero_%t", mode, zero), func(t *testing.T) {
					query, want := "title=old-title", uint64(1)
					if zero {
						query, want = "title=missing", 0
					}
					f.store.afterSelect = func(selected driver.SelectionResult) {
						if selected.Total != want {
							t.Fatalf("pre-fence membership=%d want=%d", selected.Total, want)
						}
						after := f.base
						after.Grants = append(append([]authz.Grant(nil), f.base.Grants...), authz.Grant{Subject: authz.SubjectRef{Kind: "user", ID: w9Reader.ID}, Scope: authz.Scope{Kind: authz.ProjectScope, Ref: w9Ref("project-canonical-visible")}, Role: "involved"})
						if err := f.facts.Replace(after); err != nil {
							t.Fatal(err)
						}
					}
					def := &viewops.Definition{Filter: &viewops.Filter{Query: query}, Fields: []string{"fields"}}
					if mode == "summary" {
						def.Fields, def.GroupBy = []string{}, []string{"type"}
					}
					got, err := views.Run(context.Background(), w9Reader, viewops.RunRequest{CountOnly: mode == "count", Definition: def, AsOf: &viewops.AsOf{Revision: old}})
					w9Code(t, err, "authz_unavailable")
					if !reflect.DeepEqual(got, viewops.Result{}) {
						t.Fatalf("final fence leaked: %+v", got)
					}
					if err := f.facts.Replace(f.base); err != nil {
						t.Fatal(err)
					}
				})
			}
		}
	})
}

func TestW9OrdinaryAuthorizedCapVersusExplicit100kSummaryAcrossEngines(t *testing.T) {
	w9Engines(t, func(t *testing.T, store driver.Store) {
		f := newW9Fixture(t, store)
		_, f.traceSQLiteCommits = store.(*sqlite.Store)
		// Keep the existing lease alive during the genuine 100k producer and
		// engine work. Component budgets and test deadlines stay unchanged.
		stop := make(chan struct{})
		done := make(chan struct{})
		lostDone := make(chan struct{})
		renewCtx, cancelRenew := context.WithCancel(context.Background())
		go func() {
			defer close(lostDone)
			select {
			case <-f.lease.Lost():
				t.Logf("W9 lease Lost observed elapsed=%s", time.Since(f.started))
			case <-stop:
			}
		}()
		go func() {
			defer close(done)
			timer := time.NewTicker(driver.LeaseTTL / 3)
			defer timer.Stop()
			for {
				select {
				case <-stop:
					return
				case <-timer.C:
					started := time.Now()
					t.Logf("W9 lease renewal start elapsed=%s", time.Since(f.started))
					err := f.lease.Renew(renewCtx)
					t.Logf("W9 lease renewal end elapsed=%s duration=%s error_type=%T error_value=%#v error=%v", time.Since(f.started), time.Since(started), err, err, err)
					if err != nil {
						return
					}
				}
			}
		}()
		defer func() {
			t.Logf("W9 lease renewal cleanup elapsed=%s", time.Since(f.started))
			close(stop)
			cancelRenew()
			<-done
			<-lostDone
		}()
		records := make([]driver.Record, 100000)
		for i := range records {
			records[i] = w9Record(fmt.Sprintf("w9-%06d", i), fmt.Sprintf("kind-%02d", i%100), "visible-name", "cap", "todo")
		}
		f.seed(t, records)
		views := f.views(t, viewops.Config{})
		for _, req := range []viewops.RunRequest{
			{Definition: &viewops.Definition{Filter: &viewops.Filter{}, Fields: []string{"id"}}},
			{CountOnly: true, Definition: &viewops.Definition{Filter: &viewops.Filter{}}},
			{Definition: &viewops.Definition{Filter: &viewops.Filter{}, Fields: []string{"id"}, GroupBy: []string{"type"}}},
		} {
			got, err := views.Run(context.Background(), w9Reader, req)
			w9Code(t, err, "scope_too_large")
			if len(got.Items) != 0 || got.Count != nil {
				t.Fatalf("cap returned partial: %+v", got)
			}
		}
		before := f.store.gets
		summary := &viewops.Definition{Filter: &viewops.Filter{}, Fields: []string{}, GroupBy: []string{"type"}}
		got := w9Run(t, views, viewops.RunRequest{Definition: summary})
		if len(got.Groups) != 100 || len(got.Items) != 0 || f.store.gets != before {
			t.Fatalf("summary groups=%d payload_gets=%d", len(got.Groups), f.store.gets-before)
		}
		total := 0
		for i, g := range got.Groups {
			if g.Key != fmt.Sprintf("kind-%02d", i) || g.Count != 1000 || len(g.Items) != 0 || g.Elided != nil {
				t.Fatal(g)
			}
			total += g.Count
		}
		if total != 100000 {
			t.Fatal(total)
		}
		resources := make([]string, 20000, 20002)
		for i := range resources {
			resources[i] = records[i].Ref.ID
		}
		resources = append(resources, resources[0], resources[0])
		bounded := &viewops.Definition{Filter: &viewops.Filter{Resources: resources}, Fields: []string{"id"}}
		atCap := w9Run(t, views, viewops.RunRequest{CountOnly: true, Definition: bounded})
		if atCap.Count == nil || *atCap.Count != 20000 {
			t.Fatalf("exact ordinary cap counted duplicates: %+v", atCap)
		}
		largeBudget := model.Natural64(20000)
		bounded.BudgetTokens = &largeBudget
		atCap = w9Run(t, views, viewops.RunRequest{Definition: bounded})
		if len(atCap.Items) != 20000 || atCap.NextCursor != nil || atCap.Items[0].ID != "w9-000000" || atCap.Items[19999].ID != "w9-019999" {
			t.Fatalf("ordinary budget/cap changed: items=%d cursor=%+v", len(atCap.Items), atCap.NextCursor)
		}
		// 101 groups is a distinct refusal from cap+1 inputs.
		records[0] = w9Record("w9-000000", "kind-100", "visible-name", "cap", "todo")
		f.seed(t, records[:1])
		_, err := views.Run(context.Background(), w9Reader, viewops.RunRequest{Definition: summary})
		w9Code(t, err, "scope_too_large")
		records[0] = w9Record("w9-000000", "kind-00", "visible-name", "cap", "todo")
		f.seed(t, records[:1])
		f.seed(t, []driver.Record{w9Record("w9-100000", "kind-00", "visible-name", "cap", "todo")})
		_, err = views.Run(context.Background(), w9Reader, viewops.RunRequest{Definition: summary})
		w9Code(t, err, "scope_too_large")
		// A large hidden population never consumes the authorized cap or
		// appears as ordinary elision/count metadata.
		after := f.base
		after.Grants = append(append([]authz.Grant(nil), f.base.Grants...), authz.Grant{Subject: authz.SubjectRef{Kind: "user", ID: w9Reader.ID}, Scope: authz.Scope{Kind: authz.ProjectScope, Ref: w9Ref("project-canonical-visible")}, Role: "involved"})
		if err := f.facts.Replace(after); err != nil {
			t.Fatal(err)
		}
		zero := w9Run(t, views, viewops.RunRequest{CountOnly: true, Definition: &viewops.Definition{Filter: &viewops.Filter{}}})
		if zero.Count == nil || *zero.Count != 0 {
			t.Fatalf("hidden rows consumed cap: %+v", zero)
		}
		if f.store.queries != 0 {
			t.Fatalf("compiled cap controls scanned %d times", f.store.queries)
		}
	})
}

func TestW9ParentBlockerAndHistoricalRedactionAcrossEngines(t *testing.T) {
	w9Engines(t, func(t *testing.T, store driver.Store) {
		f := newW9Fixture(t, store)
		child := pipeline.Document{ID: "child", Type: "issue", Project: "visible-name", Parent: func() *model.Ref { v := w9Ref("secret-parent"); return &v }(), Fields: map[string]any{"title": "public-child", "status": "todo"}, Links: []pipeline.Link{{To: w9Ref("secret-parent"), Kind: "blocks"}, {To: w9Ref("visible-blocker"), Kind: "blocks"}}}
		raw, err := json.Marshal(child)
		if err != nil {
			t.Fatal(err)
		}
		old := f.seed(t, []driver.Record{{Ref: w9Ref("child"), Kind: "bead", Body: raw}, w9Record("secret-parent", "secret-type", "hidden-name", "secret-title", "secret-status"), w9Record("visible-blocker", "note", "visible-name", "blocker", "todo")})
		cfg := viewops.Config{ParentProjection: &viewops.ParentProjectionConfig{
			Project: func(ctx context.Context, by model.PrincipalRef, content driver.Reader, refs []model.Ref) (viewops.ParentProjectionAuthority, error) {
				auth, err := f.app.Checker.AuthorizeParentProjections(ctx, by, content, refs)
				if err != nil {
					return viewops.ParentProjectionAuthority{}, err
				}
				parents := map[string]viewops.ParentDisclosure{}
				for id, p := range auth.Parents {
					parents[id] = viewops.ParentDisclosure{Exists: p.Exists, Ref: p.Ref}
				}
				return viewops.ParentProjectionAuthority{Parents: parents, PolicyBinding: auth.PolicyBinding}, nil
			}, ValidateAuthority: f.app.Checker.ValidateReadAuthorityBinding,
		}}
		views := f.views(t, cfg)
		got := w9Run(t, views, viewops.RunRequest{Definition: &viewops.Definition{Filter: &viewops.Filter{Resources: []string{"child", "child"}}}, AsOf: &viewops.AsOf{Revision: old}})
		if len(got.Items) != 1 || !got.Items[0].ParentHidden || got.Items[0].Parent != "" || got.Items[0].HiddenBlockers != 1 {
			t.Fatalf("parent/blocker projection=%+v", got)
		}
		encoded, err := json.Marshal(got)
		if err != nil {
			t.Fatal(err)
		}
		for _, secret := range []string{"secret-parent", "secret-type", "secret-title", "secret-status", "hidden-name"} {
			if strings.Contains(string(encoded), secret) {
				t.Fatalf("hidden related trace %q: %s", secret, encoded)
			}
		}
		if !strings.Contains(string(encoded), `"parent":{"exists":true}`) {
			t.Fatalf("parent existence shape=%s", encoded)
		}
		visibleParent := w9Ref("visible-blocker")
		child = pipeline.Document{ID: "child-visible-parent", Type: "issue", Project: "visible-name", Parent: &visibleParent, Fields: map[string]any{"title": "public-child", "status": "todo"}}
		raw, err = json.Marshal(child)
		if err != nil {
			t.Fatal(err)
		}
		f.seed(t, []driver.Record{{Ref: w9Ref(child.ID), Kind: "bead", Body: raw}})
		visible := w9Run(t, views, viewops.RunRequest{Definition: &viewops.Definition{Filter: &viewops.Filter{Resources: []string{child.ID}}}})
		if len(visible.Items) != 1 || visible.Items[0].ParentHidden || visible.Items[0].Parent != visibleParent.ID {
			t.Fatalf("authorized parent absent from selected page=%+v", visible)
		}
		if f.store.queries != 0 {
			t.Fatalf("related projection scanned %d times", f.store.queries)
		}
	})
}

type w9FixedViewReader struct{ definition viewops.Definition }

func (r w9FixedViewReader) GetView(context.Context, model.PrincipalRef, string) (viewops.Definition, error) {
	return r.definition, nil
}

func TestW9UnsupportedMembershipCaveatsAndBothPolicyPlanesAcrossEngines(t *testing.T) {
	w9Engines(t, func(t *testing.T, store driver.Store) {
		f := newW9Fixture(t, store)
		f.seed(t, []driver.Record{w9Record("a", "bug", "visible-name", "public", "todo", "x"), w9Record("b", "bug", "hidden-name", "secret", "todo", "x")})
		f.store.allowScan = true
		for _, name := range []string{"unbounded_label", "saved_without_source_binding", "extra_host_restriction", "caveat_intersection", "unsupported_caveat"} {
			t.Run(name, func(t *testing.T) {
				cfg := viewops.Config{}
				def := viewops.Definition{Filter: &viewops.Filter{}}
				by := w9Reader
				req := viewops.RunRequest{CountOnly: true, Definition: &def}
				want := 1
				switch name {
				case "unbounded_label":
					def.Filter.Query = "label=x"
				case "saved_without_source_binding":
					cfg.Views = w9FixedViewReader{def}
					req.Definition = nil
					req.ViewID = "saved"
				case "extra_host_restriction":
					cfg.AuthorizeRead = func(context.Context, model.PrincipalRef, model.ResourceRef, string) error {
						return &model.Error{Data: model.ErrorData{Code: "forbidden"}}
					}
					want = 0
				case "caveat_intersection":
					by.Caveats = map[string]any{"refs": []model.Ref{w9Ref("b")}}
					want = 0
				case "unsupported_caveat":
					by.Caveats = map[string]any{"provider": true}
					want = 0
				}
				// Both the newly supported exact refs and every ordinary fallback
				// retain the same real current-authority final fence.
				cfg.Selection = f.app.Service.ViewSelection()
				cfg.OntologyRevision = model.Natural64(1)
				bind, fences := cfg.Selection.Bind, 0
				cfg.Selection.Bind = func(ctx context.Context, by model.PrincipalRef, inline string) (*viewops.SelectionAuthority, error) {
					authority, err := bind(ctx, by, inline)
					if err == nil && authority != nil {
						fence := authority.Fence
						authority.Fence = func(ctx context.Context) error { fences++; return fence(ctx) }
					}
					return authority, err
				}
				views, err := viewops.New(f.store, f.lease, cfg)
				if err != nil {
					t.Fatal(err)
				}
				before, beforeQueries := f.store.selects, f.store.queries
				got, err := views.Run(context.Background(), by, req)
				wantSelects := 0
				if name == "caveat_intersection" {
					// The same hidden ref stays forbidden by the full policy. Only
					// its former blanket check_only route changes with refs support.
					wantSelects = 1
				}
				if err != nil || got.Count == nil || *got.Count != want || len(got.Items) != 0 || len(got.Groups) != 0 || got.NextCursor != nil || f.store.selects-before != wantSelects || fences != 1 {
					t.Fatalf("restricted view %s=%+v err=%v selects_delta=%d want=%d fences=%d", name, got, err, f.store.selects-before, wantSelects, fences)
				}
				if wantSelects == 1 && f.store.queries != beforeQueries || name == "unsupported_caveat" && f.store.queries == beforeQueries {
					t.Fatalf("wrong compiled/fallback route %s: queries_delta=%d", name, f.store.queries-beforeQueries)
				}
			})
		}
		// Unsupported full policy remains unavailable even when a supported
		// sibling happens to be true. Empty facts cannot narrow the policy.
		f.app.Checker.chain.Rules = append(f.app.Checker.chain.Rules, w9UnsupportedTeam{f.base.Namespace})
		authority, err := f.app.Service.PrepareViewSelection(context.Background(), w9Reader, driver.SelectionDigest([]byte("actual-inline-test-definition")))
		if err != nil {
			t.Fatal(err)
		}
		defer authority.Reader.Close()
		if authority.Boundary != nil {
			t.Fatal("full TeamInvolvement received a compiled boundary")
		}
		if err := authority.Fence(context.Background()); err != nil {
			t.Fatal(err)
		}
		t.Run("no_plugins_current_existence_check_only", func(t *testing.T) {
			f.app.Checker.chain.Rules, f.app.Checker.chain.Links = nil, nil
			auth, err := f.app.Service.PrepareViewSelection(context.Background(), w9Reader, driver.SelectionDigest([]byte("actual-inline-no-plugin-definition")))
			if err != nil {
				t.Fatal(err)
			}
			defer auth.Reader.Close()
			if auth.Boundary != nil {
				t.Fatal("no-plugin authorization=true dropped current source existence")
			}
			if err := auth.Fence(context.Background()); err != nil {
				t.Fatal(err)
			}
		})
	})
}

type w9UnsupportedTeam struct{ namespace authz.Namespace }

func (r w9UnsupportedTeam) Compile(q authz.ListRequest) (authz.FilterTemplate, error) {
	return authz.FilterTemplate{Namespace: r.namespace, Request: q, Permit: authz.Or(authz.FilterExpr{Op: authz.True}, authz.FilterExpr{Op: authz.TeamInvolvement, Subject: authz.CallerSubject}), Forbid: authz.FilterExpr{Op: authz.False}}, nil
}
func (r w9UnsupportedTeam) Eval(context.Context, authz.Slice, []authz.CheckRequest) ([]authz.Decision, error) {
	return nil, errors.New("unsupported test policy must not execute")
}

// The compiler emits a valid, supported full policy whose size exceeds the
// storage budget. Eval is an independent project/grant oracle and never calls
// the lowering or expression interpreter.
type w9BudgetPolicy struct {
	namespace authz.Namespace
	position  string
	shape     string
	evalErr   error
}

func (r *w9BudgetPolicy) Compile(q authz.ListRequest) (authz.FilterTemplate, error) {
	user := authz.FilterExpr{Op: authz.PrincipalKind, Kind: "user"}
	denied := authz.FilterExpr{Op: authz.GrantExists, Subject: authz.CallerSubject, Scope: authz.ProjectBindingScope, Role: "involved"}
	large := func(leaf authz.FilterExpr) authz.FilterExpr {
		if r.shape == "depth" {
			for i := 0; i <= driver.SelectionMaxDepth; i++ {
				leaf = authz.And(leaf, authz.FilterExpr{Op: authz.True})
			}
			return leaf
		}
		children := make([]authz.FilterExpr, driver.SelectionMaxNodes+1)
		for i := range children {
			children[i] = authz.FilterExpr{Op: authz.True}
		}
		children = append(children, leaf)
		return authz.And(children...)
	}
	out := authz.FilterTemplate{Namespace: r.namespace, Request: q, Permit: user, Forbid: denied}
	switch r.position {
	case "permit":
		out.Permit = large(user)
	case "forbid":
		out.Forbid = large(denied)
	case "restriction":
		out.Restrictions = []authz.FilterExpr{large(user)}
	}
	return out, nil
}
func (r *w9BudgetPolicy) Eval(ctx context.Context, s authz.Slice, qs []authz.CheckRequest) ([]authz.Decision, error) {
	if r.evalErr != nil {
		return nil, r.evalErr
	}
	decisions, err := (w9ProjectForbid{r.namespace}).Eval(ctx, s, qs)
	if err != nil {
		return nil, err
	}
	for i, q := range qs {
		if q.Caller.Principal.Kind == "user" && decisions[i].Effect != authz.Deny {
			decisions[i].Effect = authz.Allow
		}
	}
	return decisions, nil
}

func TestW9OversizedPolicyKeepsExactOrdinaryAuthorizationAndFence(t *testing.T) {
	for _, position := range []string{"permit", "forbid", "restriction"} {
		for _, shape := range []string{"depth", "nodes"} {
			t.Run(position+"_"+shape, func(t *testing.T) {
				f := newW9Fixture(t, memory.New())
				policy := &w9BudgetPolicy{namespace: f.base.Namespace, position: position, shape: shape}
				f.app.Checker.chain.Rules = []authz.RuleSet{policy}
				f.store.allowScan = true
				f.seed(t, []driver.Record{w9Record("visible", "bug", "visible-name", "public", "todo"), w9Record("hidden", "secret", "hidden-name", "secret", "todo")})
				authority, err := f.app.Service.PrepareViewSelection(context.Background(), w9Reader, driver.SelectionDigest([]byte("oversized-actual-policy")))
				if err != nil {
					t.Fatal(err)
				}
				if authority.Boundary != nil {
					t.Fatal("over-budget policy received a storage boundary")
				}
				decisions, err := authority.Check(context.Background(), []model.Ref{w9Ref("visible"), w9Ref("hidden")})
				if err != nil || !reflect.DeepEqual(decisions, []bool{true, false}) {
					t.Fatalf("ordinary current decisions=%v err=%v", decisions, err)
				}
				if err := authority.Fence(context.Background()); err != nil {
					t.Fatal(err)
				}
				if err := authority.Reader.Close(); err != nil {
					t.Fatal(err)
				}
				views := f.views(t, viewops.Config{})
				page := w9Run(t, views, viewops.RunRequest{Definition: &viewops.Definition{Filter: &viewops.Filter{}, Fields: []string{"id"}}})
				if !reflect.DeepEqual(w9IDs(page), []string{"visible"}) {
					t.Fatalf("ordinary policy membership=%+v", page)
				}
				count := w9Run(t, views, viewops.RunRequest{CountOnly: true, Definition: &viewops.Definition{Filter: &viewops.Filter{}}})
				if count.Count == nil || *count.Count != 1 || f.store.selects != 0 || f.store.queries == 0 {
					t.Fatalf("ordinary policy count=%+v select=%d queries=%d", count, f.store.selects, f.store.queries)
				}
				// Revoke real facts after the content scan on a pinned authority.
				// The final fence must erase the otherwise successful count.
				f.store.afterQuery = func() {
					after := f.base
					after.Grants = append(append([]authz.Grant(nil), f.base.Grants...), authz.Grant{Subject: authz.SubjectRef{Kind: "user", ID: w9Reader.ID}, Scope: authz.Scope{Kind: authz.ProjectScope, Ref: w9Ref("project-canonical-visible")}, Role: "involved"})
					if err := f.facts.Replace(after); err != nil {
						t.Fatal(err)
					}
				}
				got, err := views.Run(context.Background(), w9Reader, viewops.RunRequest{CountOnly: true, Definition: &viewops.Definition{Filter: &viewops.Filter{}}})
				w9Code(t, err, "authz_unavailable")
				if !reflect.DeepEqual(got, viewops.Result{}) {
					t.Fatalf("over-budget policy fence leaked %+v", got)
				}
				if err := f.facts.Replace(f.base); err != nil {
					t.Fatal(err)
				}
				policy.evalErr = errors.New("actual policy evaluation failed")
				got, err = views.Run(context.Background(), w9Reader, viewops.RunRequest{CountOnly: true, Definition: &viewops.Definition{Filter: &viewops.Filter{}}})
				w9Code(t, err, "authz_unavailable")
				if !reflect.DeepEqual(got, viewops.Result{}) || f.store.selects != 0 {
					t.Fatalf("over-budget policy error leaked %+v", got)
				}
			})
		}
	}
	t.Run("authorized_20000_cap", func(t *testing.T) {
		f := newW9Fixture(t, memory.New())
		f.app.Checker.chain.Rules = []authz.RuleSet{&w9BudgetPolicy{namespace: f.base.Namespace, position: "restriction", shape: "nodes"}}
		f.store.allowScan = true
		records := make([]driver.Record, viewops.OrdinaryCandidateCap+1)
		for i := range records {
			records[i] = w9Record(fmt.Sprintf("policy-%05d", i), "bug", "visible-name", "ordinary", "todo")
		}
		f.seed(t, records)
		views := f.views(t, viewops.Config{})
		for _, mode := range []string{"page", "count", "summary"} {
			t.Run(mode, func(t *testing.T) {
				def := &viewops.Definition{Filter: &viewops.Filter{}, Fields: []string{"id"}}
				if mode == "summary" {
					def.Fields, def.GroupBy = []string{}, []string{"type"}
				}
				got, err := views.Run(context.Background(), w9Reader, viewops.RunRequest{Definition: def, CountOnly: mode == "count"})
				w9Code(t, err, "scope_too_large")
				if !reflect.DeepEqual(got, viewops.Result{}) || f.store.selects != 0 {
					t.Fatalf("over-budget policy bypassed ordinary cap: %+v", got)
				}
			})
		}
	})
}
