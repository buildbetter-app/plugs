package converterroundtrip

import (
	"archive/tar"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/internal/storage/postgres"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

// A positive decimal child with leading zeroes is admitted by the source ID
// contract. Keep that spelling while advancing its numeric child counter.
// Re-key every exact JSON identity/reference in a detached synthetic snapshot;
// the source counter remains 4. This fixture claims no verified source history.
func identityCounterCollisionFixture(t *testing.T) ([]byte, string) {
	t.Helper()
	collision := "demo-7." + strings.Repeat("0", 31) + "4"
	admitted := admittedFixtureBytes(t)
	data := rewriteData(t, func(members map[string][]byte) {
		r := tar.NewReader(bytes.NewReader(admitted))
		for {
			h, err := r.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			raw, err := io.ReadAll(r)
			if err != nil {
				t.Fatal(err)
			}
			members[h.Name] = bytes.ReplaceAll(raw, []byte(`"demo-7.4"`), []byte(`"`+collision+`"`))
		}
	})
	return data, collision
}

func TestImportedSnapshotRetriesRestoredChildIdentityAcrossDrivers(t *testing.T) {
	data, collision := identityCounterCollisionFixture(t)
	p, source := admittedImportProfile(t, data)
	if source.Manifest().History != "unavailable" || source.Manifest().Projects[0].LastID != 8 || source.Manifest().Projects[0].ChildCounters["demo-7"] != 4 {
		t.Fatal("synthetic snapshot changed its history/counter boundary")
	}
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			exerciseImportedIdentityWrites(t, engine, data, source, p, collision)
		})
	}
}

func TestFullSourceImportCreatesWithoutReusingSourceIdentityAcrossDrivers(t *testing.T) {
	// Use the unchanged complete-root public producer fixture. In particular,
	// do not rewrite a history image, range, event, or verification declaration.
	data, source := publicSourceFixture(t, true)
	p, _ := admittedImportProfile(t, data)
	p.HistoryMode = driver.RestoreHistoryModeFull
	var err error
	p, err = BindHistoryImport(context.Background(), bytes.NewReader(data), p, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			exerciseImportedIdentityWrites(t, engine, data, source, p, "")
		})
	}
}

func exerciseImportedIdentityWrites(t *testing.T, engine string, data []byte, source *dumpformat.Archive, p driver.RestoreImportProfile, collision string) {
	t.Helper()
	ctx := context.Background()
	// This fixture fails if its real PostgreSQL cannot start; there is
	// no optional-driver skip or in-memory substitute for the SQL matrix.
	store, lease, reopen := openIdentityTarget(t, engine)
	empty := reverseState(t, store)
	if !empty.Meta.Head.IsZero() || len(empty.Records) != 0 {
		t.Fatal("identity restore requires a fresh target")
	}
	report, err := RestoreImportWithLease(ctx, bytes.NewReader(data), store, lease, p)
	if err != nil {
		t.Fatal(err)
	}
	if report.Restored.StoreID != p.SourceStoreID || report.Restored.Incarnation == empty.Meta.Incarnation {
		t.Fatalf("restore lost source identity or reused target incarnation: %+v", report.Restored)
	}
	if p.HistoryMode == driver.RestoreHistoryModeFull && (report.Restored.Head.String() != "8" || !report.Restored.HistoryFloor.IsZero()) {
		t.Fatalf("complete public history was flattened: %+v", report.Restored)
	}
	before := reverseState(t, store)
	beforeHead, err := before.Meta.Head.Uint64()
	if err != nil {
		t.Fatal(err)
	}
	assertImportedIdentityEvidence(t, store, source)
	prior := map[string]driver.Record{}
	for _, row := range before.Records {
		prior[row.Ref.ID] = row
	}
	parent := model.RefByID(source.Records("bead")[0].ID, model.Natural64(0))
	options := []pipeline.Option{
		pipeline.WithOntology(service.NewOntology(nil)),
		pipeline.WithIDGenerator(func() (string, error) {
			return "", fmt.Errorf("restored project allocator was bypassed")
		}),
	}
	if collision != "" {
		if _, exists := prior[collision]; !exists || parent.ID != "demo-7" {
			t.Fatal("collision control is not an actual restored child identity")
		}
	}
	seedCounter := uint64(0)
	for _, project := range source.Manifest().Projects {
		if project.Prefix == "demo" {
			seedCounter = project.ChildCounters[parent.ID]
		}
	}
	freshChild := fmt.Sprintf("%s.%d", parent.ID, seedCounter+1)
	secondChild := fmt.Sprintf("%s.%d", parent.ID, seedCounter+2)
	writer := pipeline.New(store, lease, options...)
	created := 0
	assertPreserved := func() {
		t.Helper()
		after := reverseState(t, store)
		if after.Meta.StoreID != before.Meta.StoreID || after.Meta.Incarnation != before.Meta.Incarnation || after.Meta.HistoryFloor != before.Meta.HistoryFloor || after.Meta.Head != model.Natural64(beforeHead+uint64(created)) || len(after.Records) != len(before.Records)+created {
			t.Fatalf("creation changed restore identity/floor or published extra rows/revisions: %+v created=%d rows=%d", after.Meta, created, len(after.Records))
		}
		remaining := len(prior)
		for _, row := range after.Records {
			if original, exists := prior[row.Ref.ID]; exists {
				if !reflect.DeepEqual(row, original) {
					t.Fatalf("creation rewrote restored record %q", row.Ref.ID)
				}
				remaining--
			}
		}
		if remaining != 0 {
			t.Fatal("creation removed restored records")
		}
	}
	// Exercise every declared prefix, including the zero-row source project in
	// the snapshot. Root IDs use the production adaptive hash allocator.
	for _, project := range source.Manifest().Projects {
		commit, err := writer.Apply(ctx, p.Admission.Actor, model.ChangeSet{Ops: []model.Op{{
			Kind: model.OpCreate, Create: &model.CreateOp{Type: "issue", Project: project.Prefix, Fields: map[string]any{"title": "created after import"}},
		}}}, model.WriteMeta{Method: "tx.apply", Project: project.Prefix, IdempotencyKey: hashBytes([]byte("identity-root-" + project.Prefix))})
		if err != nil || len(commit.Results) != 1 {
			t.Fatalf("public root create for %s: %+v %v", project.Prefix, commit, err)
		}
		id := commit.Results[0].Ref.ID
		suffix, ok := strings.CutPrefix(id, project.Prefix+"-")
		if !ok || len(suffix) < 3 || len(suffix) > 8 || strings.Trim(suffix, "0123456789abcdefghijklmnopqrstuvwxyz") != "" {
			t.Fatalf("restored prefix did not use the live adaptive hash profile: %q", id)
		}
		if _, reused := prior[id]; reused {
			t.Fatalf("public root create reused restored ID %q", id)
		}
		created++
		assertPreserved()
	}
	changes := model.ChangeSet{Ops: []model.Op{{Kind: model.OpCreate, Create: &model.CreateOp{
		Type: "issue", Project: "demo", Parent: &parent, Fields: map[string]any{"title": "child of restored source bead"},
	}}}}
	meta := model.WriteMeta{Method: "tx.apply", Project: "demo", IdempotencyKey: hashBytes([]byte("identity-child"))}
	commit, err := writer.Apply(ctx, p.Admission.Actor, changes, meta)
	if err != nil || len(commit.Results) != 1 {
		t.Fatalf("public child create under restored parent: %+v %v", commit, err)
	}
	child := commit.Results[0].Ref.ID
	if child != freshChild {
		t.Fatalf("restored-parent counter did not advance: got=%q want=%q", child, freshChild)
	}
	if _, reused := prior[child]; reused {
		t.Fatalf("restored child identity reused: %q", child)
	}
	created++
	assertPreserved()
	beforeReplay := reverseState(t, store)
	foundChild := false
	for _, row := range beforeReplay.Records {
		if row.Ref.ID == child {
			doc, err := pipeline.DecodeDocument(row.Body)
			if err != nil || doc.ID != child || doc.Project != "demo" || doc.Parent == nil || doc.Parent.ID != parent.ID {
				t.Fatalf("created child did not retain its restored-parent reference: %+v %v", doc, err)
			}
			foundChild = true
		}
	}
	if !foundChild {
		t.Fatal("public child create did not persist its returned identity")
	}
	replay, err := writer.ApplyWithOutcome(ctx, p.Admission.Actor, changes, meta)
	if err != nil || !replay.Replayed || !reflect.DeepEqual(replay.Commit, commit) || !reflect.DeepEqual(beforeReplay, reverseState(t, store)) {
		t.Fatalf("child replay consumed another identity or changed restored state: %+v %v", replay, err)
	}
	store, lease = reopen()
	if !reflect.DeepEqual(beforeReplay, reverseState(t, store)) {
		t.Fatal("reopen changed restored state or the first public child")
	}
	writer = pipeline.New(store, lease, options...)
	replay, err = writer.ApplyWithOutcome(ctx, p.Admission.Actor, changes, meta)
	if err != nil || !replay.Replayed || !reflect.DeepEqual(replay.Commit, commit) || !reflect.DeepEqual(beforeReplay, reverseState(t, store)) {
		t.Fatalf("reopened writer did not retain child replay: %+v %v", replay, err)
	}
	second, err := writer.Apply(ctx, p.Admission.Actor, changes, model.WriteMeta{Method: "tx.apply", Project: "demo", IdempotencyKey: hashBytes([]byte("identity-child-after-reopen"))})
	if err != nil || len(second.Results) != 1 {
		t.Fatalf("public second child after reopen: %+v %v", second, err)
	}
	next := second.Results[0].Ref.ID
	if next != secondChild || next == child {
		t.Fatalf("second child reused or lost its restored-parent counter: got=%q want=%q", next, secondChild)
	}
	if _, reused := prior[next]; reused {
		t.Fatalf("reopened writer reused a restored child: %q", next)
	}
	created++
	assertPreserved()
	after := reverseState(t, store)
	retained := make(map[string]driver.Record, len(after.Records))
	for _, row := range after.Records {
		retained[row.Ref.ID] = row
	}
	for _, row := range beforeReplay.Records {
		if !reflect.DeepEqual(retained[row.Ref.ID], row) {
			t.Fatalf("second child creation changed pre-reopen record %q", row.Ref.ID)
		}
	}
	doc, err := pipeline.DecodeDocument(retained[next].Body)
	if err != nil || doc.ID != next || doc.Project != "demo" || doc.Parent == nil || *doc.Parent != parent {
		t.Fatalf("second child did not retain restored-parent reference: %+v %v", doc, err)
	}
	assertImportedIdentityEvidence(t, store, source)
	lifecycle := "persistent-close-reopen"
	if engine == "memory" {
		lifecycle = "same-store-lease-writer-refresh"
	}
	t.Logf("IDENTITY_CONTINUITY engine=%s lifecycle=%s mode=%s archive=%s restored_head=%s final_head=%s prefixes=%d creates=%d source_collision=%t original_records=%d history=%s", engine, lifecycle, p.HistoryMode, source.SHA256(), before.Meta.Head, after.Meta.Head, len(source.Manifest().Projects), created, collision != "", len(prior), source.Manifest().History)
}

// Persistent engines really close and reopen their file/database. Memory has
// no durable reopen contract, so it retains the store and refreshes the lease.
func openIdentityTarget(t *testing.T, engine string) (driver.Store, driver.ActiveLease, func() (driver.Store, driver.ActiveLease)) {
	t.Helper()
	ctx := context.Background()
	var open func() (driver.Store, error)
	switch engine {
	case "memory":
		open = func() (driver.Store, error) { return memory.New(), nil }
	case "sqlite":
		path := filepath.Join(t.TempDir(), "identity.db")
		open = func() (driver.Store, error) { return (sqlite.Driver{}).Open(ctx, driver.Config{DSN: path}) }
	case "postgres":
		dsn := identityPostgresDSN(t)
		open = func() (driver.Store, error) { return postgres.Open(ctx, driver.Config{DSN: dsn}) }
	default:
		t.Fatalf("unknown identity driver %q", engine)
	}
	store, err := open()
	if err != nil {
		t.Fatal(err)
	}
	var lease driver.ActiveLease
	t.Cleanup(func() {
		if lease != nil {
			if err := lease.Release(ctx); err != nil {
				t.Errorf("release identity lease: %v", err)
			}
		}
		if store != nil {
			if err := store.Close(); err != nil {
				t.Errorf("close identity store: %v", err)
			}
		}
	})
	acquire := func() {
		t.Helper()
		lease, err = store.Lease(ctx, "converter-identity-"+engine, driver.LeaseTTL)
		if err != nil {
			t.Fatal(err)
		}
	}
	acquire()
	return store, lease, func() (driver.Store, driver.ActiveLease) {
		t.Helper()
		if err := lease.Release(ctx); err != nil {
			t.Fatal(err)
		}
		lease = nil
		if engine != "memory" {
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			store = nil
			store, err = open()
			if err != nil {
				t.Fatal(err)
			}
		}
		acquire()
		return store, lease
	}
}

func identityPostgresDSN(t *testing.T) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	docker := func(ctx context.Context, args ...string) ([]byte, error) {
		cmd := exec.CommandContext(ctx, "docker", append([]string{"--host=unix:///var/run/docker.sock"}, args...)...)
		for _, entry := range os.Environ() {
			if !strings.HasPrefix(entry, "DOCKER_HOST=") && !strings.HasPrefix(entry, "DOCKER_CONTEXT=") && !strings.HasPrefix(entry, "DOCKER_TLS=") && !strings.HasPrefix(entry, "DOCKER_TLS_VERIFY=") && !strings.HasPrefix(entry, "DOCKER_CERT_PATH=") {
				cmd.Env = append(cmd.Env, entry)
			}
		}
		return cmd.CombinedOutput()
	}
	name := "plug-swarm-i6-identity-" + hashBytes([]byte(t.TempDir()))[:16]
	const image = "postgres@sha256:2d2b8998d31037bf721cfdf764d76ba74171b4fab3431b7f72c27c56ddbdf9e3"
	if out, err := docker(ctx, "run", "-d", "--rm", "--name", name, "-e", "POSTGRES_HOST_AUTH_METHOD=trust", "-e", "POSTGRES_DB=plug_identity", "-p", "127.0.0.1::5432", image); err != nil {
		t.Fatalf("real PostgreSQL start failed: %v %s", err, out)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		if out, err := docker(cleanupCtx, "rm", "-f", name); err != nil {
			t.Errorf("owned PostgreSQL cleanup failed: %v %s", err, out)
		}
	})
	address, err := docker(ctx, "port", name, "5432/tcp")
	if err != nil {
		t.Fatal(err)
	}
	addr := strings.TrimSpace(string(address))
	if host, _, err := net.SplitHostPort(addr); err != nil || host != "127.0.0.1" {
		t.Fatal("test PostgreSQL must bind loopback")
	}
	dsn := "postgres://postgres@" + addr + "/plug_identity?sslmode=disable"
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for {
		if err := db.PingContext(ctx); err == nil {
			return dsn
		}
		if ctx.Err() != nil {
			t.Fatal("real PostgreSQL did not become ready")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func assertImportedIdentityEvidence(t *testing.T, store driver.Store, source *dumpformat.Archive) {
	t.Helper()
	ctx := context.Background()
	state := reverseState(t, store)
	r, err := store.Read(ctx, driver.View{Incarnation: state.Meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	rows := map[string]driver.Record{}
	for _, row := range state.Records {
		rows[row.Ref.ID] = row
	}
	for _, bead := range source.Records("bead") {
		row, exists := rows[bead.ID]
		doc, err := pipeline.DecodeDocument(row.Body)
		if !exists || err != nil || row.Kind != "bead" || doc.ID != bead.ID || doc.Project != bead.Project {
			t.Fatalf("source ID/prefix was not retained: %q %v", bead.ID, err)
		}
		wantLinks := []pipeline.Link{}
		for _, edge := range source.Records("edge") {
			if edge.Body["from"] == bead.ID {
				if edge.Body["type"] != "blocks" {
					t.Fatal("identity fixture requires a supported block dependency")
				}
				wantLinks = append(wantLinks, pipeline.Link{Kind: "blocks", To: model.RefByID(edge.Body["to"].(string), model.Natural64(0))})
			}
		}
		if !reflect.DeepEqual(append([]pipeline.Link{}, doc.Links...), wantLinks) {
			t.Fatalf("source dependency references changed for %q: %+v", bead.ID, doc.Links)
		}
	}
	for _, project := range source.Manifest().Projects {
		actual, err := r.(driver.AllocatorReader).LoadAllocator(ctx, project.Prefix)
		wantAllocator, seedErr := importedAllocator(source, project)
		var saved importProjectMetadata
		if err != nil || seedErr != nil || !allocatorContinues(wantAllocator, actual) || json.Unmarshal(rows[importProjectRecordID(project.Prefix)].Body, &saved) != nil {
			t.Fatalf("restored project allocator/evidence missing: %s %v", project.Prefix, err)
		}
		want, err := json.Marshal(project)
		if err != nil || saved.Project != project.Prefix || !bytes.Equal(saved.Metadata["plug.restore.sourceAllocator"], want) || !reflect.DeepEqual(saved.TargetAllocator, wantAllocator) {
			t.Fatalf("source prefix/counters or admitted target profile changed: %s", project.Prefix)
		}
	}
	for _, archived := range source.Records("beads-archive") {
		row, exists := rows["source/beads-archive/"+archived.ID]
		var body map[string]json.RawMessage
		if !exists || row.Kind != "beads-archive" || json.Unmarshal(row.Body, &body) != nil || string(body["read_only"]) != "true" || string(body["redactable"]) != "true" {
			t.Fatalf("source archive disposition changed: %s", archived.ID)
		}
		want, err := json.Marshal(archived.Source.Fields)
		if err != nil || !bytes.Equal(body["source"], want) {
			t.Fatalf("source archive provenance changed: %s", archived.ID)
		}
	}
	// Exact envelope retention also binds every manifest disposition, source
	// counter and dependency member; no regenerated or verified-history claim.
	envelope, err := r.Blob(ctx, driver.Hash(source.SHA256()))
	if err != nil || !bytes.Equal(envelope, source.TarBytes()) {
		t.Fatalf("source envelope/dispositions were not retained exactly: %v", err)
	}
}
