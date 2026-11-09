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
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
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
	}
	f.verifyCorpus(t)
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
	f.views, err = viewops.New(f.store.Store, f.lease, viewops.Config{OntologyRevision: model.Natural64(1),
		AuthorizeRead: func(ctx context.Context, p model.PrincipalRef, r model.ResourceRef, _ string) error {
			_, err := app.Service.Get(ctx, p, service.GetRequest{ID: r.Ref})
			return err
		},
		AuthorizeReadBatch: app.Service.AuthorizeReadBatch,
		ParentProjection: &viewops.ParentProjectionConfig{
			Project: func(ctx context.Context, p model.PrincipalRef, r driver.Reader, refs []model.Ref) (viewops.ParentProjectionAuthority, error) {
				a, err := app.Checker.AuthorizeParentProjections(ctx, p, r, refs)
				out := viewops.ParentProjectionAuthority{PolicyBinding: a.PolicyBinding, Parents: make(map[string]viewops.ParentDisclosure, len(a.Parents))}
				for id, value := range a.Parents {
					out.Parents[id] = viewops.ParentDisclosure{Exists: value.Exists, Ref: value.Ref}
				}
				return out, err
			},
			ValidateAuthority: app.Checker.ValidateReadAuthorityBinding,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
}

// Corpus installation is a storage fixture, not a competing product Writer.
// Use only existing declared records and the real core ancestor codec/table.
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
	bumps := []driver.Sentinel{}
	for n := first; n <= last; n++ {
		ref := beadRef(n)
		add(driver.LockKey{Kind: driver.LockRecord, Ref: &ref})
		bead, _ := driver.NewBeadSentinel(ref.ID)
		closure, _ := driver.NewClosureSentinel(ref.ID)
		for _, sentinel := range []driver.Sentinel{bead, closure} {
			add(driver.LockKey{Kind: driver.LockSentinel, Sentinel: sentinel})
			bumps = append(bumps, sentinel)
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
	if _, err := tx.Lock(ctx, locks); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Head(ctx); err != nil {
		t.Fatal(err)
	}
	changes := []model.ChangeRecord{}
	for n := first; n <= last; n++ {
		ref := beadRef(n)
		if err := tx.Put(ctx, driver.Record{Ref: ref, Kind: "bead", Body: body(t, n)}); err != nil {
			t.Fatal(err)
		}
		changes = append(changes, model.ChangeRecord{Ref: ref, Op: model.OpCreate, FromRevision: model.Natural64(0)})
	}
	if err := tx.PutDerived(ctx, driver.DerivedAncestorClosure, nil, derived); err != nil {
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
		}
	}
	f.corpusHash = fmt.Sprintf("%x", h.Sum(nil))
	rows, err := r.DerivedRows(context.Background(), driver.DerivedAncestorClosure, "")
	if err != nil {
		t.Fatal(err)
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
		delete(expected, row.Key)
	}
	if len(expected) != 0 {
		t.Fatal("ancestor readback missed rows")
	}
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
	if _, err := conformance.Seed(context.Background(), f.store, f.lease, driver.ChangesetHeader{}, []conformance.FixtureChange{{Ref: ref, Kind: "bead", Op: model.OpPatch, Body: body(t, n), ChangedPaths: []string{"/lease"}}}); err != nil {
		t.Fatal(err)
	}
	r := f.reader(t, nil)
	defer r.Close()
	rows, err := r.Get(context.Background(), []model.Ref{ref})
	if err != nil || len(rows) != 1 || !bytes.Equal(rows[0].Body, body(t, n)) {
		t.Fatalf("claim reset did not restore the exact original body: %v", err)
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
}
