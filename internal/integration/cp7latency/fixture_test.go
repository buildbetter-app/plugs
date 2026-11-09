package cp7latency_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/core/viewops"
	"github.com/buildbetter-app/plugs/internal/plugins/authnapikey"
	"github.com/buildbetter-app/plugs/internal/plugins/authzfixed"
	"github.com/buildbetter-app/plugs/internal/server/authzrpc"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/postgres"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

const (
	corpusSize    = 100000
	seedBatch     = 32
	pageSize      = 100
	projectName   = "cp7-project"
	viewQuery     = "type=issue AND title=CP7"
	postgresImage = "postgres@sha256:2d2b8998d31037bf721cfdf764d76ba74171b4fab3431b7f72c27c56ddbdf9e3"
)

func beadRef(n int) model.Ref {
	return model.RefByID(fmt.Sprintf("00000000-0000-4000-8000-%012d", n), model.Natural64(0))
}

func document(n int) pipeline.Document {
	// Persist the same creation instant for every seed and reset body so
	// creation-time ordering retains the fixture's existing ID tie-break.
	createdAt := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	d := pipeline.Document{ID: beadRef(n).ID, Type: "issue", Project: projectName,
		Fields: map[string]any{"title": fmt.Sprintf("CP7 bead %06d", n), "status": "todo", "priority": 0}, CreatedAt: &createdAt}
	if (n-1)%11 != 0 {
		parent := beadRef(n - 1)
		d.Parent = &parent
	}
	return d
}

func body(t *testing.T, n int) []byte {
	t.Helper()
	b, err := json.Marshal(document(n))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func ancestors(t *testing.T, n int) []driver.DerivedRow {
	t.Helper()
	rows := []driver.DerivedRow{}
	for a := n; a >= n-(n-1)%11; a-- {
		row, err := pipeline.AncestorRow(beadRef(n).ID, beadRef(a).ID, uint64(n-a))
		if err != nil {
			t.Fatal(err)
		}
		rows = append(rows, row)
	}
	return rows
}

// This holder owns instrumentation, but product composition receives Store
// itself. Concrete optional reader/transaction/store capabilities stay intact.
type measuredStore struct {
	driver.Store
	counts counters
}

type fixture struct {
	store      *measuredStore
	lease      driver.ActiveLease
	facts      *authzrpc.MutableFacts
	base       authzfixed.Facts
	app        *authzrpc.Composition
	views      *viewops.Service
	n          int
	corpusHash string
	seededAt   service.AsOf
	viewPlan   func(driver.SelectionPlan)
	readyRows  map[string]driver.DerivedRow
}

// Dedicated route controls observe the real plan and delegate unchanged to
// the concrete SQL selector. Other fixture readers remain unwrapped.
type viewPlanReader struct {
	driver.Reader
	selector driver.SelectionReader
	observe  func(driver.SelectionPlan)
}

func (r *viewPlanReader) Select(ctx context.Context, plan driver.SelectionPlan) (driver.SelectionResult, error) {
	r.observe(plan)
	return r.selector.Select(ctx, plan)
}

func openStore(t *testing.T, engine string) driver.Store {
	t.Helper()
	ctx := context.Background()
	if engine == "sqlite" {
		s, err := (sqlite.Driver{}).Open(ctx, driver.Config{DSN: filepath.Join(t.TempDir(), "cp7.db")})
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	if engine != "postgres" {
		t.Fatalf("undeclared engine %q", engine)
	}
	name := fmt.Sprintf("plug-swarm-cp7-latency-%d-%d", os.Getpid(), time.Now().UnixNano())
	if out, err := docker("run", "-d", "--rm", "--name", name, "-e", "POSTGRES_HOST_AUTH_METHOD=trust", "-p", "127.0.0.1::5432", postgresImage); err != nil {
		t.Fatalf("owned PostgreSQL start: %v %s", err, out)
	}
	t.Cleanup(func() {
		if out, err := docker("rm", "-f", name); err != nil {
			t.Errorf("owned PostgreSQL cleanup: %v %s", err, out)
		}
	})
	out, err := docker("port", name, "5432/tcp")
	address := strings.TrimSpace(string(out))
	host, _, parseErr := net.SplitHostPort(address)
	if err != nil || parseErr != nil || host != "127.0.0.1" {
		t.Fatalf("non-loopback PostgreSQL binding: %q %v", address, err)
	}
	deadline := time.Now().Add(40 * time.Second)
	for {
		if _, err := docker("exec", name, "pg_isready", "-h", "127.0.0.1", "-U", "postgres"); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("owned PostgreSQL readiness deadline")
		}
		time.Sleep(25 * time.Millisecond)
	}
	s, err := postgres.Open(ctx, driver.Config{DSN: "postgres://postgres@" + address + "/postgres?sslmode=disable"})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("CP7_ENGINE postgres image=%s loopback_only=true", postgresImage)
	return s
}

func docker(args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
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

func newFixture(t *testing.T, engine string, n int) *fixture {
	t.Helper()
	started := time.Now()
	raw := openStore(t, engine)
	_, ok := raw.(driver.ResponseFinalizer)
	if !ok {
		raw.Close()
		t.Fatal("declared SQL driver lacks response finalization")
	}
	_, ok = raw.(driver.CheckpointStore)
	if !ok {
		raw.Close()
		t.Fatal("declared SQL driver lacks checkpoints")
	}
	s := &measuredStore{Store: raw}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	lease, err := s.Lease(context.Background(), "cp7-latency", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := lease.Release(context.Background()); err != nil {
			t.Error(err)
		}
	})
	stop := make(chan struct{})
	done := make(chan struct{})
	renewError := make(chan error, 1)
	go func() {
		defer close(done)
		ticker := time.NewTicker(driver.LeaseRenewEvery)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				if err := lease.Renew(context.Background()); err != nil {
					renewError <- err
					return
				}
			}
		}
	}()
	t.Cleanup(func() {
		close(stop)
		<-done
		select {
		case err := <-renewError:
			t.Errorf("active lease renewal: %v", err)
		default:
		}
	})
	f := &fixture{store: s, lease: lease, n: n}
	for start := 1; start <= n; start += seedBatch {
		f.seed(t, start, min(start+seedBatch-1, n))
		if start == 1 {
			// Coverage must already exist before any product composition.
			f.verifySourceCoverage(t, "first_batch")
		}
	}
	if !t.Run("TypedSourceCorpusReadback", func(t *testing.T) {
		f.verifySourceCoverage(t, "seeded")
		f.verifyCorpus(t)
	}) {
		t.FailNow()
	}
	meta, err := s.Meta(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	f.seededAt = service.AsOf{Revision: meta.Head, Incarnation: meta.Incarnation}
	ns := authz.Namespace{StoreID: string(meta.StoreID), Incarnation: meta.Incarnation, InstallationID: "fixed", Authority: model.RefByID("authority", model.Natural64(0))}
	project := model.RefByID(projectName, model.Natural64(0))
	team := model.RefByID("cp7-team", model.Natural64(0))
	f.base = authzfixed.Facts{Namespace: ns,
		Projects: []authzfixed.Project{{Ref: project, Name: projectName, InstallationID: "fixed", Active: true}},
		Teams:    []authzfixed.Team{{Ref: team, InstallationID: "fixed", Active: true}},
		Grants: []authz.Grant{
			{Subject: authz.SubjectRef{Kind: "user", ID: "user:direct"}, Scope: authz.Scope{Kind: authz.ProjectScope, Ref: project}, Role: "involved"},
			{Subject: authz.SubjectRef{Kind: "user", ID: "user:team"}, Scope: authz.Scope{Kind: authz.TeamScope, Ref: team}, Role: "member"},
			{Subject: authz.SubjectRef{Kind: "team", ID: team.ID}, Scope: authz.Scope{Kind: authz.ProjectScope, Ref: project}, Role: "involved"},
		}}
	f.facts, err = authzrpc.NewMutableFacts(f.base)
	if err != nil {
		t.Fatal(err)
	}
	f.compose(t)
	// Binding is read-only. Initial setup must explicitly publish the canonical
	// authority source; later recompositions must not republish or restore it.
	if err := f.facts.Replace(f.base); err != nil {
		t.Fatal(err)
	}
	f.verifySourceCoverage(t, "composed")
	t.Logf("CP7_CORPUS engine=%s beads=%d bodies_sha256=%s setup_ms=%.3f max_parent_depth=10 seed_batch=%d product_driver_defaults=true", engine, n, f.corpusHash, float64(time.Since(started).Microseconds())/1000, seedBatch)
	return f
}

func (f *fixture) compose(t *testing.T) {
	t.Helper()
	app, err := authzrpc.New(context.Background(), authzrpc.Config{Store: f.store.Store, Lease: f.lease,
		Provider: authnapikey.New(authnapikey.Config{AuthorizationInstalled: func() bool { return true }}),
		Facts:    f.facts, Namespace: f.base.Namespace, DefaultProject: projectName})
	if err != nil {
		t.Fatal(err)
	}
	f.app = app
	selection := app.Service.ViewSelection()
	if f.viewPlan != nil {
		bind := selection.Bind
		selection.Bind = func(ctx context.Context, p model.PrincipalRef, definition string) (*viewops.SelectionAuthority, error) {
			authority, err := bind(ctx, p, definition)
			if err != nil {
				return nil, err
			}
			selector, ok := authority.Reader.(driver.SelectionReader)
			if !ok {
				_ = authority.Reader.Close()
				return nil, fmt.Errorf("declared SQL driver lacks view selection")
			}
			authority.Reader = &viewPlanReader{Reader: authority.Reader, selector: selector, observe: f.viewPlan}
			return authority, nil
		}
	}
	f.configureViews(t, selection)
}

func (f *fixture) configureViews(t *testing.T, selection *viewops.SelectionConfig) {
	t.Helper()
	var err error
	f.views, err = viewops.New(f.store.Store, f.lease, viewops.Config{OntologyRevision: model.Natural64(1),
		Selection: selection,
		ParentProjection: &viewops.ParentProjectionConfig{
			Project: func(ctx context.Context, p model.PrincipalRef, r driver.Reader, refs []model.Ref) (viewops.ParentProjectionAuthority, error) {
				a, err := f.app.Checker.AuthorizeParentProjections(ctx, p, r, refs)
				out := viewops.ParentProjectionAuthority{PolicyBinding: a.PolicyBinding, Parents: make(map[string]viewops.ParentDisclosure, len(a.Parents))}
				for id, value := range a.Parents {
					out.Parents[id] = viewops.ParentDisclosure{Exists: value.Exists, Ref: value.Ref}
				}
				return out, err
			},
			ValidateAuthority: f.app.Checker.ValidateReadAuthorityBinding,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
}

// Corpus installation uses the production typed source adapter from its first
// batch, with exact original bodies and the real core ancestor codec/table.
func (f *fixture) seed(t *testing.T, first, last int) {
	t.Helper()
	ctx := context.Background()
	meta, err := f.store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	requests := map[string]driver.LockRequest{}
	add := func(k driver.LockKey) {
		id, err := driver.EncodeLockKey(k)
		if err != nil {
			t.Fatal(err)
		}
		requests[id] = driver.LockRequest{Key: k, Mode: driver.LockUpdate}
	}
	derived := []driver.DerivedRow{}
	plans := make([]*pipeline.SourceProjectionPlan, 0, last-first+1)
	bumps := []driver.Sentinel{driver.SentinelGraphBlocking}
	for n := first; n <= last; n++ {
		ref := beadRef(n)
		plan, err := pipeline.PrepareSourceRecord(driver.Record{Ref: ref, Kind: "bead", Body: body(t, n)})
		if err != nil {
			t.Fatal(err)
		}
		plans = append(plans, plan)
		for _, key := range plan.LockKeys() {
			add(key)
		}
		for _, sentinel := range plan.Sentinels() {
			if sentinel != driver.SentinelGraphBlocking {
				bumps = append(bumps, sentinel)
			}
		}
		for _, row := range ancestors(t, n) {
			derived = append(derived, row)
			add(driver.LockKey{Kind: driver.LockDerived, Derived: &driver.DerivedRowKey{Table: driver.DerivedAncestorClosure, Key: row.Key}})
		}
	}
	ids := make([]string, 0, len(requests))
	for id := range requests {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	locks := make([]driver.LockRequest, 0, len(ids))
	keys := make([]driver.LockKey, 0, len(ids))
	for _, id := range ids {
		locks = append(locks, requests[id])
		keys = append(keys, requests[id].Key)
	}
	tx, err := f.store.Write(ctx, f.lease)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Close()
	if _, ok := tx.(driver.GraphSourceWriter); !ok {
		t.Fatal("declared SQL driver lacks complete typed graph publication")
	}
	if _, ok := tx.(driver.SelectionIndexWriter); !ok {
		t.Fatal("declared SQL driver lacks typed selection publication")
	}
	if _, err := tx.Lock(ctx, locks); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Head(ctx); err != nil {
		t.Fatal(err)
	}
	changes := []model.ChangeRecord{}
	for n := first; n <= last; n++ {
		ref := beadRef(n)
		if err := plans[n-first].Publish(ctx, tx); err != nil {
			t.Fatal(err)
		}
		changes = append(changes, model.ChangeRecord{Ref: ref, Op: model.OpCreate, FromRevision: model.Natural64(0)})
	}
	if err := pipeline.PutAncestorProjections(ctx, tx, nil, derived); err != nil {
		t.Fatal(err)
	}
	if err := tx.BumpSentinels(ctx, bumps); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Commit(ctx, driver.ChangesetHeader{Incarnation: meta.Incarnation, Kind: model.ChangesetWrite, Principal: model.PrincipalRef{Kind: "system", ID: "cp7-corpus"}, LockKeys: keys, Changes: changes}); err != nil {
		t.Fatalf("corpus batch %d..%d: %v", first, last, err)
	}
}

func (f *fixture) reader(t *testing.T, at *model.Revision) driver.Reader {
	t.Helper()
	meta, err := f.store.Meta(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	r, err := f.store.Read(context.Background(), driver.View{Incarnation: meta.Incarnation, At: at})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func (f *fixture) verifyCorpus(t *testing.T) {
	t.Helper()
	r := f.reader(t, nil)
	defer r.Close()
	h := sha256.New()
	revisions := make(map[string]model.Revision, f.n)
	digests := make(map[string]string, f.n)
	for start := 1; start <= f.n; start += 128 {
		refs := []model.Ref{}
		for n := start; n <= min(start+127, f.n); n++ {
			refs = append(refs, beadRef(n))
		}
		rows, err := r.Get(context.Background(), refs)
		if err != nil || len(rows) != len(refs) {
			t.Fatalf("corpus readback: %d rows %v", len(rows), err)
		}
		for i, row := range rows {
			if row.Ref.ID != refs[i].ID || row.Kind != "bead" || !bytes.Equal(row.Body, body(t, start+i)) {
				t.Fatalf("corpus mismatch at %d", start+i)
			}
			h.Write(row.Body)
			h.Write([]byte{'\n'})
			revisions[row.Ref.ID] = row.Revision
			digests[row.Ref.ID] = driver.SelectionDigest(row.Body)
		}
	}
	f.corpusHash = fmt.Sprintf("%x", h.Sum(nil))
	// The source census shares tables with independently owned producers.
	// Enumerate their exact expected rows; never accept arbitrary extras or
	// relabel the full bead census after composition or a public claim.
	extra := map[driver.DerivedRowKey]driver.DerivedRow{}
	if f.app != nil {
		keys, err := authzfixed.SelectionRows(f.base.Namespace)
		if err != nil {
			t.Fatal(err)
		}
		key := driver.LockKey{Kind: driver.LockDerived, Derived: &keys.Source}
		states, err := r.Observe(context.Background(), []driver.LockKey{key})
		canonical, encodeErr := authzfixed.FactsSourceBytes(f.base)
		if err != nil || encodeErr != nil || len(states) != 1 || !states[0].Present || states[0].Derived == nil || !bytes.Equal(states[0].Derived.Value, canonical) {
			t.Fatalf("canonical authority source readback: %v %v", err, encodeErr)
		}
		source := driver.SelectionSource{Key: key, Revision: states[0].Revision, Present: true, Digest: driver.SelectionDigest(canonical)}
		rows, err := authzfixed.SelectionProjection(f.base, source)
		if err != nil {
			t.Fatal(err)
		}
		for _, row := range rows {
			if row.Index == nil {
				t.Fatal("fixture authority projection is not certified")
			}
			if row.Index.Row.Table == driver.DerivedAttribute {
				extra[row.Index.Row] = row
			}
		}
	}
	for key, row := range f.readyRows {
		extra[driver.DerivedRowKey{Table: driver.DerivedSortKey, Key: key}] = row
	}
	checkProjection := func(table driver.DerivedTable, row driver.DerivedRow, id string) {
		t.Helper()
		index := row.Index
		if index == nil || index.Validate(table, row) != nil || index.Incarnation != r.Snapshot().Incarnation ||
			index.Source.Key.Ref == nil || index.Source.Key.Ref.ID != id || !index.Source.Present ||
			index.Source.Revision.String() != revisions[id].String() || index.Source.Digest != digests[id] {
			t.Fatalf("source-bound %s projection differs from actual corpus image %s", table, id)
		}
	}
	// These are real persisted annotations, published in the same first and
	// subsequent batches as the unchanged bodies, not a retrospective build.
	for _, table := range []driver.DerivedTable{driver.DerivedSortKey, driver.DerivedAttribute, driver.DerivedReferenceIndex} {
		rows, err := r.DerivedRows(context.Background(), table, "")
		if err != nil {
			t.Fatalf("complete %s source projection readback: %v", table, err)
		}
		seen := make(map[string]bool, f.n)
		for _, row := range rows {
			key := driver.DerivedRowKey{Table: table, Key: row.Key}
			if expected, ok := extra[key]; ok {
				if !bytes.Equal(row.Value, expected.Value) || !reflect.DeepEqual(row.Index, expected.Index) {
					t.Fatalf("independent %s producer row changed: %s", table, row.Key)
				}
				delete(extra, key)
				continue
			}
			if _, ok := revisions[row.Key]; !ok || seen[row.Key] {
				t.Fatalf("duplicate or unknown %s source projection %s", table, row.Key)
			}
			seen[row.Key] = true
			checkProjection(table, row, row.Key)
			switch table {
			case driver.DerivedSortKey:
				bead := row.Index.Bead
				var n int
				if _, err := fmt.Sscanf(row.Key, "00000000-0000-4000-8000-%012d", &n); err != nil || bead == nil ||
					bead.Ref.ID != row.Key || bead.Type != "issue" || bead.Project == nil || *bead.Project != projectName ||
					bead.Status != "todo" || bead.Title != fmt.Sprintf("CP7 bead %06d", n) || bead.Priority != 0 || bead.Template ||
					len(bead.Absent) != 1 || bead.Absent[0] != driver.SelectionTemplate {
					t.Fatalf("counting source projection changed frozen document semantics at %s", row.Key)
				}
				parent := document(n).Parent
				if (bead.Parent == nil) != (parent == nil) || parent != nil && !driver.SelectionSameRef(*bead.Parent, *parent) {
					t.Fatalf("counting source projection changed canonical parent at %s", row.Key)
				}
			case driver.DerivedAttribute, driver.DerivedReferenceIndex:
				kind := driver.SelectionLabelFact
				if table == driver.DerivedReferenceIndex {
					kind = driver.SelectionLinkFact
				}
				if row.Index.Facts == nil || row.Index.Facts.Kind != kind || len(row.Index.Facts.Rows) != 0 {
					t.Fatalf("source projection invented corpus facts at %s", row.Key)
				}
			}
		}
		if len(seen) != f.n {
			t.Fatalf("complete %s source projection readback: rows=%d want=%d", table, len(seen), f.n)
		}
	}
	if len(extra) != 0 {
		t.Fatalf("independent producer rows missing: %+v", extra)
	}
	// Independently enumerate canonical children from the frozen eleven-row
	// chains. Check every indexed image against the exact source read above.
	children, ok := r.(driver.HierarchyChildrenExcludingReader)
	if !ok {
		t.Fatal("declared SQL reader lacks indexed canonical children")
	}
	links, ok := r.(driver.InverseSourceLinksReader)
	if !ok {
		t.Fatal("declared SQL reader lacks indexed inverse source links")
	}
	for first := 1; first <= f.n; first += driver.HierarchyMaxParents {
		last := min(first+driver.HierarchyMaxParents-1, f.n)
		parents := make([]model.Ref, 0, last-first+1)
		expected := []int{}
		for n := first; n <= last; n++ {
			parents = append(parents, beadRef(n))
			if n%11 != 0 && n < f.n {
				expected = append(expected, n+1)
			}
		}
		indexed, more, err := children.HierarchyChildren(context.Background(), parents, driver.HierarchyMaxParents)
		if err != nil || more || len(indexed) != len(expected) {
			t.Fatalf("complete indexed parent readback %d..%d: rows=%d more=%v err=%v", first, last, len(indexed), more, err)
		}
		for i, row := range indexed {
			n := expected[i]
			if row.Ref.ID != beadRef(n).ID || row.Kind != "bead" || row.Revision.String() != revisions[row.Ref.ID].String() || !bytes.Equal(row.Body, body(t, n)) {
				t.Fatalf("indexed source differs from exact corpus image at %d", n)
			}
		}
		incoming, more, err := links.InverseSourceLinks(context.Background(), parents, sourceLinkKinds(), nil, driver.SourceLinkMaxLimit)
		if err != nil || more || len(incoming) != 0 {
			t.Fatalf("link-free corpus has incomplete or invented incoming links: rows=%d more=%v err=%v", len(incoming), more, err)
		}
	}
	rows, err := r.DerivedRows(context.Background(), driver.DerivedAncestorClosure, "")
	if err != nil {
		t.Fatal(err)
	}
	// This total does not reuse ancestors(), which also builds the seed rows.
	completeChains, tail := f.n/11, f.n%11
	wantRows := completeChains*66 + tail*(tail+1)/2
	if len(rows) != wantRows || f.n == corpusSize && len(rows) != 599995 {
		t.Fatalf("independent closure total=%d want=%d (100k requires 599995)", len(rows), wantRows)
	}
	expected := map[string]string{}
	for n := 1; n <= f.n; n++ {
		for _, row := range ancestors(t, n) {
			expected[row.Key] = string(row.Value)
		}
	}
	if len(rows) != len(expected) {
		t.Fatalf("ancestor row count=%d want=%d", len(rows), len(expected))
	}
	for _, row := range rows {
		value, ok := expected[row.Key]
		if !ok || string(row.Value) != value {
			t.Fatal("ancestor row readback mismatch")
		}
		descendant, _, _, err := pipeline.DecodeAncestorRow(row)
		if err != nil {
			t.Fatal(err)
		}
		checkProjection(driver.DerivedAncestorClosure, row, descendant)
		delete(expected, row.Key)
	}
	if len(expected) != 0 {
		t.Fatal("ancestor readback missed rows")
	}
	t.Logf("CP7_CORPUS_READBACK beads=%d exact_body_images=%d indexed_canonical_children=%d inverse_links=0 closure_rows=%d source_projection_rows=%d source_head=%s bodies_sha256=%s", f.n, f.n, f.n-(f.n+10)/11, len(rows), 3*f.n+len(rows), r.Snapshot().At.String(), f.corpusHash)
}

func sourceLinkKinds() []driver.IncomingLinkKind {
	return []driver.IncomingLinkKind{driver.IncomingLinkParentChild, driver.IncomingLinkBlocks, driver.IncomingLinkConditionalBlocks, driver.IncomingLinkWaitsFor}
}

// Both optional domains validate coverage through this actual committed head,
// including empty probes. A Parent-only declaration cannot satisfy this check.
func (f *fixture) verifySourceCoverage(t *testing.T, phase string) {
	t.Helper()
	ctx := context.Background()
	meta, err := f.store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	r, err := f.store.Store.Read(ctx, driver.View{Incarnation: meta.Incarnation, At: &meta.Head})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	snapshot := r.Snapshot()
	if snapshot.At == nil || snapshot.At.String() != meta.Head.String() || snapshot.Incarnation != meta.Incarnation {
		t.Fatal("source coverage reader is not bound to the actual head")
	}
	children, ok := r.(driver.HierarchyChildrenExcludingReader)
	if !ok {
		t.Fatal("declared SQL reader lacks canonical child capabilities")
	}
	links, ok := r.(driver.InverseSourceLinksReader)
	if !ok {
		t.Fatal("declared SQL reader lacks inverse source-link capability")
	}
	if rows, more, err := children.HierarchyChildren(ctx, nil, 0); err != nil || more || len(rows) != 0 {
		t.Fatalf("%s parent coverage unavailable at head %s: %v", phase, meta.Head, err)
	}
	if rows, more, err := links.InverseSourceLinks(ctx, nil, nil, nil, 0); err != nil || more || len(rows) != 0 {
		t.Fatalf("%s link coverage unavailable at head %s: %v", phase, meta.Head, err)
	}
	rows, more, err := children.HierarchyChildren(ctx, []model.Ref{beadRef(1)}, 1)
	want := min(f.n-1, 1)
	if err != nil || more || len(rows) != want {
		t.Fatalf("%s canonical child probe: rows=%d more=%v err=%v", phase, len(rows), more, err)
	}
	if want == 1 {
		actual, err := r.Get(ctx, []model.Ref{beadRef(2)})
		if err != nil || len(actual) != 1 || rows[0].Ref.ID != beadRef(2).ID || rows[0].Kind != actual[0].Kind || rows[0].Revision.String() != actual[0].Revision.String() || !bytes.Equal(rows[0].Body, actual[0].Body) {
			t.Fatalf("%s indexed child differs from its actual source: %v", phase, err)
		}
	}
	if rows, more, err := children.HierarchyChildrenExcluding(ctx, []model.Ref{beadRef(1)}, []model.Ref{beadRef(2)}, 1); err != nil || more || len(rows) != 0 {
		t.Fatalf("%s canonical child exclusion: %v", phase, err)
	}
	if rows, more, err := links.InverseSourceLinks(ctx, []model.Ref{beadRef(1)}, sourceLinkKinds(), nil, 1); err != nil || more || len(rows) != 0 {
		t.Fatalf("%s unexpected inverse links: %v", phase, err)
	}
	// Observe actual persisted annotations at the same head, including the
	// first batch before composition can perform any startup maintenance.
	probe := beadRef(1)
	actual, err := r.Get(ctx, []model.Ref{probe})
	if err != nil || len(actual) != 1 {
		t.Fatalf("%s selection source image: %v", phase, err)
	}
	projectionKeys, err := pipeline.SourceSelectionRows(probe)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range projectionKeys {
		states, err := r.Observe(ctx, []driver.LockKey{{Kind: driver.LockDerived, Derived: &key}})
		if err != nil || len(states) != 1 || !states[0].Present || states[0].Derived == nil {
			t.Fatalf("%s missing persisted %s source projection: %v", phase, key.Table, err)
		}
		row := states[0].Derived
		if row.Index == nil || row.Index.Validate(key.Table, *row) != nil || row.Index.Source.Key.Ref == nil ||
			!driver.SelectionSameRef(*row.Index.Source.Key.Ref, probe) || row.Index.Source.Revision.String() != actual[0].Revision.String() ||
			row.Index.Source.Digest != driver.SelectionDigest(actual[0].Body) {
			t.Fatalf("%s stale %s source projection at head %s", phase, key.Table, meta.Head)
		}
	}
	t.Logf("CP7_SOURCE_COVERAGE phase=%s head=%s parent=true links=true exact_child_image=true persisted_selection_rows=3", phase, meta.Head)
}

// Patch source bytes through the same production adapter as initial seeding.
// Canonical Parent and closure rows stay unchanged; Links are republished from
// the exact body, including the small accounting control's temporary relation.
func (f *fixture) patchSource(t *testing.T, phase string, ref model.Ref, encoded []byte, changedPaths []string) {
	t.Helper()
	ctx := context.Background()
	meta, err := f.store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := pipeline.PrepareSourceRecord(driver.Record{Ref: ref, Kind: "bead", Body: encoded})
	if err != nil {
		t.Fatal(err)
	}
	// A body-only rewrite must refresh the source-bound closure annotations
	// without changing any established closure Key/Value or graph membership.
	prefix, err := pipeline.AncestorPrefix(ref.ID)
	if err != nil {
		t.Fatal(err)
	}
	r := f.reader(t, nil)
	closure, err := r.DerivedRows(ctx, driver.DerivedAncestorClosure, prefix)
	r.Close()
	if err != nil || len(closure) == 0 {
		t.Fatalf("%s source closure unavailable: %v", phase, err)
	}
	keys := plan.LockKeys()
	for _, row := range closure {
		key := driver.DerivedRowKey{Table: driver.DerivedAncestorClosure, Key: row.Key}
		keys = append(keys, driver.LockKey{Kind: driver.LockDerived, Derived: &key})
	}
	sort.Slice(keys, func(i, j int) bool {
		a, _ := driver.EncodeLockKey(keys[i])
		b, _ := driver.EncodeLockKey(keys[j])
		return a < b
	})
	locks := make([]driver.LockRequest, len(keys))
	for i, key := range keys {
		locks[i] = driver.LockRequest{Key: key, Mode: driver.LockUpdate}
	}
	tx, err := f.store.Store.Write(ctx, f.lease)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Close()
	if _, ok := tx.(driver.GraphSourceWriter); !ok {
		t.Fatal("declared SQL transaction lacks complete typed graph publication")
	}
	if _, ok := tx.(driver.SelectionIndexWriter); !ok {
		t.Fatal("declared SQL transaction lacks typed selection publication")
	}
	states, err := tx.Lock(ctx, locks)
	if err != nil {
		t.Fatal(err)
	}
	var previous model.Revision
	for _, state := range states {
		if state.Key.Kind == driver.LockRecord {
			previous = state.Revision
		}
	}
	if _, err := tx.Head(ctx); err != nil {
		t.Fatal(err)
	}
	if err := plan.Publish(ctx, tx); err != nil {
		t.Fatal(err)
	}
	if err := pipeline.PutAncestorProjections(ctx, tx, closure, closure); err != nil {
		t.Fatal(err)
	}
	if err := tx.BumpSentinels(ctx, plan.Sentinels()); err != nil {
		t.Fatal(err)
	}
	committed, err := tx.Commit(ctx, driver.ChangesetHeader{Incarnation: meta.Incarnation, Kind: model.ChangesetWrite, Principal: model.PrincipalRef{Kind: "system", ID: "storage-fixture"}, LockKeys: keys, Changes: []model.ChangeRecord{{Ref: ref, Op: model.OpPatch, FromRevision: previous, ChangedPaths: append([]string(nil), changedPaths...)}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Close(); err != nil {
		t.Fatal(err)
	}
	r = f.reader(t, nil)
	rows, err := r.Get(ctx, []model.Ref{ref})
	r.Close()
	if err != nil || len(rows) != 1 || rows[0].Ref.ID != ref.ID || rows[0].Kind != "bead" || rows[0].Revision.String() != committed.Revision.String() || !bytes.Equal(rows[0].Body, encoded) {
		t.Fatalf("%s did not publish its exact source bytes at the commit revision: %v", phase, err)
	}
	f.verifySourceCoverage(t, phase)
}

func (f *fixture) user(divisor int) model.PrincipalRef {
	p := model.PrincipalRef{Kind: "user", ID: "user:reader"}
	if divisor > 1 {
		refs := []model.Ref{}
		for n := divisor; n <= f.n; n += divisor {
			refs = append(refs, beadRef(n))
		}
		p.Caveats = map[string]any{"refs": refs}
	}
	return p
}

func (f *fixture) agent(divisor int, team bool) model.PrincipalRef {
	sponsor := "user:direct"
	if team {
		sponsor = "user:team"
	}
	p := model.PrincipalRef{Kind: "agent", ID: "agent:worker", Sponsor: &model.PrincipalRef{Kind: "user", ID: sponsor}, Caveats: f.user(divisor).Caveats}
	return p
}

func (f *fixture) resetClaim(t *testing.T, ref model.Ref) {
	t.Helper()
	var n int
	if _, err := fmt.Sscanf(ref.ID, "00000000-0000-4000-8000-%012d", &n); err != nil || n < 1 || n > f.n {
		t.Fatalf("claim identity outside corpus: %s", ref.ID)
	}
	// Main publishes a separate rank row atomically with each public claim.
	// Bind it to the actual claimed revision before the source-only reset;
	// retain its exact bytes so the later complete census proves conservation.
	r := f.reader(t, nil)
	claimed, err := r.Get(context.Background(), []model.Ref{ref})
	key, keyErr := pipeline.ReadySortKeyRowKey(ref.ID)
	rows, rankErr := r.DerivedRows(context.Background(), driver.DerivedSortKey, key)
	r.Close()
	if err != nil || keyErr != nil || rankErr != nil || len(claimed) != 1 || len(rows) != 1 || rows[0].Index != nil {
		t.Fatalf("claimed source/rank readback: %v %v %v", err, keyErr, rankErr)
	}
	id, rank, err := pipeline.DecodeReadySortKey(rows[0])
	seed := document(n)
	bucket, priority, known, orderErr := service.ReadySortKey(seed, rank.RankedAt)
	if err != nil || orderErr != nil || id != ref.ID || !known || rank.Revision.String() != claimed[0].Revision.String() || !rank.CreatedAt.Equal(*seed.CreatedAt) || rank.Bucket != bucket || rank.Rank != priority {
		t.Fatalf("claim rank is not bound to its source and clock: %+v %v %v", rank, err, orderErr)
	}
	if f.readyRows == nil {
		f.readyRows = map[string]driver.DerivedRow{}
	}
	f.readyRows[key] = rows[0].Clone()
	f.patchSource(t, "claim_reset", ref, body(t, n), []string{"/lease"})
	r = f.reader(t, nil)
	defer r.Close()
	restored, err := r.Get(context.Background(), []model.Ref{ref})
	if err != nil || len(restored) != 1 || !bytes.Equal(restored[0].Body, body(t, n)) {
		t.Fatalf("claim reset did not restore the exact original body: %v", err)
	}
	rows, err = r.DerivedRows(context.Background(), driver.DerivedSortKey, key)
	if err != nil || len(rows) != 1 || !reflect.DeepEqual(rows[0], f.readyRows[key]) {
		t.Fatalf("source reset changed the independent rank row: %v", err)
	}
}

func groupedRequest() viewops.RunRequest {
	return viewops.RunRequest{Project: projectName, Definition: &viewops.Definition{
		Filter: &viewops.Filter{Query: viewQuery},
		Fields: []string{"id", "type", "fields.title", "parent"}, GroupBy: []string{"type"}, Sort: []string{"id"},
	}}
}

func (f *fixture) checkPage(t *testing.T, page service.BeadPage, divisor int) {
	t.Helper()
	if err := validatePage(page, f.n, divisor); err != nil {
		t.Fatal(err)
	}
}

func validatePage(page service.BeadPage, n, divisor int) error {
	want := min(pageSize, n/divisor)
	if len(page.Items) != want || (page.Next != nil) != (n/divisor > pageSize) {
		return fmt.Errorf("page cardinality/cursor=%d/%v want=%d", len(page.Items), page.Next != nil, want)
	}
	for i, row := range page.Items {
		number := (i + 1) * divisor
		if row.ID != beadRef(number).ID {
			return fmt.Errorf("page order/membership mismatch at %d: %s", i, row.ID)
		}
		d := document(number)
		if d.Parent == nil {
			if row.Parent != "" || row.ParentHidden {
				return fmt.Errorf("root leaked a parent")
			}
		} else if divisor == 1 {
			if row.Parent != d.Parent.ID || row.ParentHidden {
				return fmt.Errorf("readable parent missing")
			}
		} else if row.Parent != "" || !row.ParentHidden {
			return fmt.Errorf("unreadable parent identity leaked or existence lost")
		}
	}
	return nil
}

func (f *fixture) checkGrouped(t *testing.T, result viewops.Result, divisor int) {
	t.Helper()
	if err := validateGrouped(result, f.n, divisor); err != nil {
		t.Fatal(err)
	}
}

func validateGrouped(result viewops.Result, n, divisor int) error {
	population := n / divisor
	if result.Count != nil {
		return fmt.Errorf("ordinary grouped result emitted countOnly output")
	}
	if population > viewops.OrdinaryCandidateCap {
		return fmt.Errorf("ordinary count bypassed candidate cap")
	}
	if len(result.Items) != 0 || len(result.Groups) != 1 || result.Groups[0].Key != "issue" || result.Groups[0].Count != population {
		return fmt.Errorf("grouped count differs from independent population %d", population)
	}
	if (result.NextCursor != nil) != (population > viewops.DefaultOutputBudget) {
		return fmt.Errorf("grouped cursor omitted/invented")
	}
	g := result.Groups[0]
	wantItems := min(population, viewops.DefaultOutputBudget)
	if result.NextCursor != nil && result.NextCursor.Index != uint64(wantItems) {
		return fmt.Errorf("grouped cursor index differs from emitted prefix")
	}
	if len(g.Items) != wantItems {
		return fmt.Errorf("grouped payload cardinality=%d want=%d", len(g.Items), wantItems)
	}
	for i, row := range g.Items {
		number := (i + 1) * divisor
		doc := document(number)
		if row.ID != doc.ID || row.Type != doc.Type || row.Fields["title"] != doc.Fields["title"] {
			return fmt.Errorf("grouped payload differs from independent corpus at %d", i)
		}
		if doc.Parent == nil {
			if row.Parent != "" || row.ParentHidden {
				return fmt.Errorf("grouped root invented a parent")
			}
		} else if divisor == 1 {
			if row.Parent != doc.Parent.ID || row.ParentHidden {
				return fmt.Errorf("grouped readable parent missing")
			}
		} else if row.Parent != "" || !row.ParentHidden {
			return fmt.Errorf("grouped hidden parent identity leaked or existence lost")
		}
	}
	if population > wantItems {
		if g.Elided == nil || g.Elided.Count != population-wantItems || g.Elided.ContinueIndex != uint64(wantItems) {
			return fmt.Errorf("grouped count excludes elided authorized tail")
		}
	} else if g.Elided != nil {
		return fmt.Errorf("grouped count has invented elision")
	}
	return nil
}

func (f *fixture) checkClaim(t *testing.T, p model.PrincipalRef, out *port.WriteOutcome, meta model.WriteMeta, divisor int) {
	t.Helper()
	if out == nil || out.NullResult || out.Replayed || out.Commit.Revision.IsZero() || len(out.Commit.Results) != 1 {
		t.Fatal("full claim did not commit exactly one fresh result")
	}
	ref := out.Commit.Results[0].Ref
	var number int
	if _, err := fmt.Sscanf(ref.ID, "00000000-0000-4000-8000-%012d", &number); err != nil || number < 1 || number > f.n || number%divisor != 0 {
		t.Fatal("claim selected an unauthorized/noncorpus bead")
	}
	r := f.reader(t, nil)
	rows, err := r.Get(context.Background(), []model.Ref{ref})
	if err != nil || len(rows) != 1 {
		r.Close()
		t.Fatalf("committed claim readback: %v", err)
	}
	d, err := pipeline.DecodeDocument(rows[0].Body)
	if err != nil || d.Lease == nil || d.Lease.Principal.Kind != p.Kind || d.Lease.Principal.ID != p.ID || rows[0].Revision.String() != out.Commit.Revision.String() {
		r.Close()
		t.Fatal("claim did not publish the current caller's lease at its receipt revision")
	}
	principal, err := driver.PrincipalKey(p)
	if err != nil {
		r.Close()
		t.Fatal(err)
	}
	receipt, err := r.IdempotencyByKey(context.Background(), principal, "", "bead.claimNext", meta.IdempotencyKey)
	r.Close()
	if err != nil || receipt == nil || receipt.Commit.Revision.String() != out.Commit.Revision.String() || len(receipt.Changeset.Changes) != 1 || receipt.Changeset.Changes[0].Ref.ID != ref.ID {
		t.Fatalf("claim lacks exact one-row durable receipt: %v", err)
	}
	replayed, err := f.app.Service.ClaimNextWithOutcome(context.Background(), p, service.ClaimNextRequest{Project: projectName}, meta)
	if err != nil || replayed == nil || !replayed.Replayed || replayed.Commit.Revision.String() != out.Commit.Revision.String() || len(replayed.Commit.Results) != 1 || replayed.Commit.Results[0].Ref.ID != ref.ID {
		t.Fatalf("claim receipt replay drifted: %v", err)
	}
	f.verifySourceCoverage(t, "claim_commit")
}
