package b09native

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/integration/converterroundtrip"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/internal/storage/postgres"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

const b09PostgresImage = "postgres@sha256:2d2b8998d31037bf721cfdf764d76ba74171b4fab3431b7f72c27c56ddbdf9e3"

var b09NativeID = regexp.MustCompile(`^b09-[0-9a-f]{32}$`)

type b09ReplayStage struct {
	Name             string                               `json:"name"`
	Meta             driver.Meta                          `json:"meta"`
	BeadIDs          []string                             `json:"bead_ids"`
	VersionRevisions map[string][]string                  `json:"version_revisions"`
	ChangesetRevs    []string                             `json:"changeset_revisions"`
	Allocator        driver.AllocatorSnapshot             `json:"allocator"`
	AllocatorSHA256  string                               `json:"allocator_sha256"`
	SQL              *b09SQLCensus                        `json:"sql,omitempty"`
	Records          map[string]driver.Record             `json:"records"`
	Versions         map[string][]driver.Version          `json:"versions"`
	Changesets       []model.Changeset                    `json:"changesets"`
	Receipts         map[string]*driver.IdempotencyRecord `json:"receipts"`
	LeaseEpoch       uint64                               `json:"lease_epoch"`
}

type b09SQLCensus struct {
	Engine   string                 `json:"engine"`
	Tables   map[string]b09SQLTable `json:"tables"`
	SHA256   string                 `json:"sha256"`
	Commands []string               `json:"commands"`
}

type b09SQLTable struct {
	Columns []string `json:"columns"`
	Rows    []string `json:"rows"`
}

type b09ReplayReceipt struct {
	Engine             string           `json:"engine"`
	SourceStoreID      driver.StoreID   `json:"source_store_id"`
	ArchiveSHA256      string           `json:"archive_sha256"`
	DerivedSHA256      string           `json:"derived_sha256"`
	SourceIDs          []string         `json:"source_ids"`
	NewIDs             []string         `json:"new_ids"`
	RestoreHead        string           `json:"restore_head"`
	CloseRevision      string           `json:"close_revision"`
	CompactionRevision string           `json:"compaction_revision"`
	RevertRevision     string           `json:"revert_revision"`
	Stages             []b09ReplayStage `json:"stages"`
	Operations         []string         `json:"operations"`
}

type b09ReplayTarget struct {
	t           *testing.T
	engine      string
	path        string
	dsn         string
	container   string
	store       driver.Store
	lease       driver.ActiveLease
	now         time.Time
	postgresDSN string
	output      string
	receipts    map[string]string
}

func (r *recorder) replayNative(source captureReceipt) {
	r.t.Helper()
	ctx := context.Background()
	data, err := os.ReadFile(source.ArchivePath)
	if err != nil {
		r.t.Fatalf("same-run native archive read: %v", err)
	}
	archive, err := dumpformat.Read(bytes.NewReader(data))
	if err != nil {
		r.t.Fatalf("same-run native archive validation: %v", err)
	}
	if archive.SHA256() != source.ArchiveSHA256 || archive.Manifest().History != "available-unverified" {
		r.t.Fatalf("same-run native archive binding changed: sha=%s history=%s", archive.SHA256(), archive.Manifest().History)
	}
	inspection, err := converterroundtrip.InspectImport(ctx, bytes.NewReader(data))
	if err != nil {
		r.t.Fatalf("same-run InspectImport refused the admitted archive: %v", err)
	}
	if inspection.ArchiveSHA256 != source.ArchiveSHA256 {
		r.t.Fatalf("InspectImport archive digest=%s want=%s", inspection.ArchiveSHA256, source.ArchiveSHA256)
	}
	sourceIDs := append([]string(nil), source.BeadIDs...)
	sort.Strings(sourceIDs)
	if len(sourceIDs) != 3 {
		r.t.Fatalf("same-run native source bead count=%d want=3", len(sourceIDs))
	}
	for _, id := range sourceIDs {
		if !strings.HasPrefix(id, "b09-") || len(id) != len("b09-")+3 {
			r.t.Fatalf("same-run native source ID is not the admitted three-character hash shape: %q", id)
		}
	}
	if source.SourceConfigBytes != "export:\n  auto: false\n" || source.SourceConfigSHA256 != hashBytes([]byte(source.SourceConfigBytes)) {
		r.t.Fatalf("same-run YAML object source bytes were not retained exactly: %+v", source)
	}

	results := make([]b09ReplayReceipt, 0, 3)
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		results = append(results, runB09Replay(r.t, engine, data, inspection, archive, sourceIDs, filepath.Join(r.root, "paired", engine)))
	}
	r.writeJSON(filepath.Join(r.root, "paired-replay.json"), map[string]any{
		"schema":               "plug-s1-b09-native-paired-replay/1",
		"source_capture":       source.Label,
		"archive_path":         source.ArchivePath,
		"archive_sha256":       source.ArchiveSHA256,
		"archive_size":         len(data),
		"derived_sha256":       inspection.DerivedSHA256,
		"source_store_id":      inspection.SourceStoreID,
		"source_config_path":   source.SourceConfigPath,
		"source_config_bytes":  source.SourceConfigBytes,
		"source_config_sha256": source.SourceConfigSHA256,
		"history_mode":         string(driver.RestoreHistoryModeSnapshot),
		"engines":              results,
	})
}

func runB09Replay(t *testing.T, engine string, data []byte, inspection converterroundtrip.ImportInspection, archive *dumpformat.Archive, sourceIDs []string, output string) b09ReplayReceipt {
	t.Helper()
	target := openB09ReplayTarget(t, engine)
	target.output = output
	if err := os.MkdirAll(output, 0700); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	profile := driver.RestoreImportProfile{
		Admission: driver.RestoreImportAdmission{
			Mode: "authenticated", Actor: model.PrincipalRef{Kind: "user", ID: "b09-native-review"}, Authority: "restore-admin", AdmissionDigest: strings.Repeat("c", 64),
		},
		SourceStoreID: inspection.SourceStoreID, Method: "offline.restore.import", IdempotencyKey: hashBytes([]byte("b09-native-restore-" + engine)),
		ArchiveDigest: inspection.ArchiveSHA256, DerivedDigest: inspection.DerivedSHA256, HistoryMode: driver.RestoreHistoryModeSnapshot,
	}
	if profile.SourceStoreID != driver.StoreID(archive.Manifest().StoreID) {
		t.Fatalf("%s source store identity changed between inspection and archive", engine)
	}
	report, err := converterroundtrip.RestoreImportWithLease(ctx, bytes.NewReader(data), target.store, target.lease, profile)
	if err != nil {
		t.Fatalf("%s same-run RestoreImport: %v", engine, err)
	}
	if report.Restored.StoreID != profile.SourceStoreID || report.Restored.Head.IsZero() || report.Restored.HistoryFloor.IsZero() {
		t.Fatalf("%s restore metadata lost source identity/floor: %+v", engine, report.Restored)
	}
	target.receipts[profile.Method] = profile.IdempotencyKey
	writer, domain := target.bind()
	_ = writer
	initial := target.stage("restore", sourceIDs, nil)
	assertB09Source(t, archive, initial)
	if !reflect.DeepEqual(initial.BeadIDs, sourceIDs) {
		t.Fatalf("%s restored bead IDs differ from same-run source: got=%v want=%v", engine, initial.BeadIDs, sourceIDs)
	}
	if len(initial.ChangesetRevs) != 0 {
		t.Fatalf("%s snapshot restore exposed fabricated source history: %+v", engine, initial.ChangesetRevs)
	}
	if len(initial.Allocator.Inventory.Projects) != 1 || !reflect.DeepEqual(initial.Allocator.Inventory.Projects[0], driver.NewRandomAllocator("b09")) {
		t.Fatalf("%s restore did not install the admitted live random allocator: %+v", engine, initial.Allocator.Inventory.Projects)
	}

	// A real close and a provider-backed compaction append ordinary Plug history.
	if err := target.reopen(); err != nil {
		t.Fatal(err)
	}
	firstReopen := target.stage("reopen-before-create", sourceIDs, nil)
	assertB09Reopen(t, initial, firstReopen)
	_, domain = target.bind()
	firstNew := createB09Continuation(t, domain, engine, "first")
	target.receipts["bead.create/first"] = b09Meta(engine, "create-first").IdempotencyKey
	postCreate := target.stage("create-before-reopen", sourceIDs, []string{firstNew})
	target.assertMutation(firstReopen, postCreate, firstNew, "create-first")
	closeRef := model.RefByID(sourceIDs[0], model.Natural64(0))
	closeCommit, err := domain.Close(ctx, b09Principal, service.CloseRequest{ID: closeRef, Reason: "B09 native paired close"}, b09Meta(engine, "close"))
	if err != nil {
		t.Fatalf("%s close source bead: %v", engine, err)
	}
	if closeCommit.Revision.IsZero() {
		t.Fatalf("%s close returned no revision", engine)
	}
	target.receipts["bead.close"] = b09Meta(engine, "close").IdempotencyKey
	closed := target.stage("close", sourceIDs, []string{firstNew})
	target.assertMutation(postCreate, closed, sourceIDs[0], "close")
	if len(closed.VersionRevisions[sourceIDs[0]]) < 2 {
		t.Fatalf("%s close did not append source history: %+v", engine, closed.VersionRevisions)
	}
	target.now = target.now.Add(3 * 24 * time.Hour)
	compact, domain := target.bind()
	_ = compact
	outcome, err := domain.CompactWithOutcome(ctx, b09Principal, service.CompactionRequest{}, b09Meta(engine, "compact"))
	if err != nil {
		t.Fatalf("%s compaction: %v", engine, err)
	}
	assertB09ResultPair(t, "compaction", sourceIDs[0], outcome.Commit)
	target.receipts["admin.compact"] = b09Meta(engine, "compact").IdempotencyKey
	compacted := target.stage("compaction", sourceIDs, []string{firstNew})
	target.assertMutation(closed, compacted, sourceIDs[0], "compact")
	if len(compacted.VersionRevisions[sourceIDs[0]]) < 3 {
		t.Fatalf("%s compaction did not retain ordinary version history: %+v", engine, compacted.VersionRevisions[sourceIDs[0]])
	}
	latest := latestB09Version(t, target, closeRef)
	latestDoc, err := pipeline.DecodeDocument(latest.Body)
	if err != nil || latestDoc.Fields["description"] != "native compacted summary" || len(latestDoc.Metadata["plug.compaction"]) == 0 {
		t.Fatalf("%s compacted source does not retain summary/state: %v %+v", engine, err, latestDoc)
	}
	compactRevision := outcome.Commit.Revision
	meta, err := target.store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	revertReq := service.CompactionRevertRequest{Ref: closeRef, BaseRevision: compactRevision, Expect: []model.Precondition{}}
	digest, err := service.CompactionRevertDigest(revertReq, b09Meta(engine, "revert"), meta.Incarnation)
	if err != nil {
		t.Fatal(err)
	}
	revertReq.HumanPresence = map[string]any{"mode": "test-presence", "digest": digest}
	_, domain = target.bind()
	reverted, err := domain.RevertCompactionWithOutcome(ctx, b09Principal, revertReq, b09Meta(engine, "revert"))
	if err != nil {
		t.Fatalf("%s compaction history restore: %v", engine, err)
	}
	assertB09ResultPair(t, "history restore", sourceIDs[0], reverted.Commit)
	target.receipts["history.revert"] = b09Meta(engine, "revert").IdempotencyKey
	revertedImage := target.stage("revert", sourceIDs, []string{firstNew})
	target.assertMutation(compacted, revertedImage, sourceIDs[0], "revert")
	if len(revertedImage.VersionRevisions[sourceIDs[0]]) < 4 {
		t.Fatalf("%s history restore did not append a reversal version: %+v", engine, revertedImage.VersionRevisions[sourceIDs[0]])
	}
	revertedDoc, err := pipeline.DecodeDocument(latestB09Version(t, target, closeRef).Body)
	if err != nil || revertedDoc.Fields["description"] != "B09 accepted native recorder" || len(revertedDoc.Metadata["plug.compaction"]) != 0 {
		t.Fatalf("%s history restore did not recover original text: %v %+v", engine, err, revertedDoc)
	}

	if err := target.reopen(); err != nil {
		t.Fatal(err)
	}
	secondReopen := target.stage("reopen-after-history", sourceIDs, []string{firstNew})
	assertB09Reopen(t, revertedImage, secondReopen)
	_, domain = target.bind()
	secondNew := createB09Continuation(t, domain, engine, "after-reopen")
	target.receipts["bead.create/after-reopen"] = b09Meta(engine, "create-after-reopen").IdempotencyKey
	final := target.stage("create-after-reopen", sourceIDs, []string{firstNew, secondNew})
	target.assertMutation(secondReopen, final, secondNew, "create-after-reopen")
	if firstNew == secondNew || !b09NativeID.MatchString(firstNew) || !b09NativeID.MatchString(secondNew) {
		t.Fatalf("%s live allocator did not mint independent different-shape IDs: %q %q", engine, firstNew, secondNew)
	}
	for _, id := range sourceIDs {
		if id == firstNew || id == secondNew {
			t.Fatalf("%s continuation collided with restored ID %s", engine, id)
		}
	}
	if len(final.ChangesetRevs) != 5 {
		t.Fatalf("%s history stream omitted paired writes: %+v", engine, final.ChangesetRevs)
	}
	return b09ReplayReceipt{
		Engine: engine, SourceStoreID: inspection.SourceStoreID, ArchiveSHA256: inspection.ArchiveSHA256, DerivedSHA256: inspection.DerivedSHA256,
		SourceIDs: sourceIDs, NewIDs: []string{firstNew, secondNew}, RestoreHead: initial.Meta.Head.String(), CloseRevision: closeCommit.Revision.String(),
		CompactionRevision: compactRevision.String(), RevertRevision: reverted.Commit.Revision.String(), Stages: []b09ReplayStage{initial, firstReopen, postCreate, closed, compacted, revertedImage, secondReopen, final},
		Operations: []string{"converterroundtrip.InspectImport", "converterroundtrip.RestoreImportWithLease(history=snapshot)", "service.Close", "service.CompactWithOutcome", "service.RevertCompactionWithOutcome", "persistent close/reopen", "service.Create"},
	}
}

var b09Principal = model.PrincipalRef{Kind: "user", ID: "b09-native-review"}

func b09Meta(engine, stage string) model.WriteMeta {
	return model.WriteMeta{Project: "b09", Agent: "b09-native-review", AgentLabel: "b09-native-review", IdempotencyKey: hashBytes([]byte("b09-native-" + engine + "-" + stage))}
}

func (r *b09ReplayTarget) bind() (*pipeline.Writer, *service.Service) {
	r.t.Helper()
	ontology := service.NewOntology(nil)
	writer := pipeline.New(r.store, r.lease, pipeline.WithOntology(ontology), pipeline.WithClock(func() time.Time { return r.now }))
	config := service.CompactionConfig{
		Summary: service.CompactionSummaryFunc(func(_ context.Context, input service.CompactionInput) (string, error) {
			if input.Ref.ID == "" || input.Tier != 1 || len(input.Text["description"]) == 0 {
				return "", fmt.Errorf("unexpected native compaction input: %+v", input)
			}
			return "native compacted summary", nil
		}),
		ThresholdDays: map[int]int{1: 1, 2: 2},
		VerifyPresence: func(_ context.Context, method, digest string, proof map[string]any) error {
			if method != "history.revert" || proof["mode"] != "test-presence" || proof["digest"] != digest {
				return errors.New("B09 test presence digest mismatch")
			}
			return nil
		},
	}
	domain := service.New(r.store, r.lease, writer, service.WithOntology(ontology), service.WithClock(func() time.Time { return r.now }), service.WithCompaction(config))
	return writer, domain
}

func createB09Continuation(t *testing.T, domain *service.Service, engine, label string) string {
	t.Helper()
	commit, err := domain.Create(context.Background(), b09Principal, service.CreateRequest{Type: "issue", Fields: map[string]any{"title": "Plug native continuation " + label, "description": "continuation after accepted native restore"}}, b09Meta(engine, "create-"+label))
	if err != nil || len(commit.Results) != 1 {
		t.Fatalf("%s native continuation %s: %+v %v", engine, label, commit, err)
	}
	id := commit.Results[0].Ref.ID
	if !b09NativeID.MatchString(id) {
		t.Fatalf("%s continuation ID has wrong live allocator shape: %q", engine, id)
	}
	return id
}

func latestB09Version(t *testing.T, target *b09ReplayTarget, ref model.Ref) driver.Version {
	t.Helper()
	meta, err := target.store.Meta(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	r, err := target.store.Read(context.Background(), driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	page, err := r.Versions(context.Background(), ref, driver.PageReq{Limit: 1000})
	if err != nil || page.NextCursor != nil || len(page.Items) == 0 {
		t.Fatalf("latest B09 history read: %v %+v", err, page)
	}
	return page.Items[len(page.Items)-1]
}

func (r *b09ReplayTarget) stage(name string, sourceIDs, extraIDs []string) b09ReplayStage {
	r.t.Helper()
	ctx := context.Background()
	meta, err := r.store.Meta(ctx)
	if err != nil {
		r.t.Fatal(err)
	}
	reader, err := r.store.Read(ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		r.t.Fatal(err)
	}
	defer reader.Close()
	page, err := reader.Query(ctx, driver.Query{Limit: 1000})
	if err != nil || page.NextCursor != nil {
		r.t.Fatalf("%s bead census: %v %+v", name, err, page)
	}
	ids := make([]string, 0, len(page.Items))
	records := map[string]driver.Record{}
	fullVersions := map[string][]driver.Version{}
	for _, row := range page.Items {
		key := b09RecordKey(r.t, row.Ref)
		records[key] = row
		v, err := reader.Versions(ctx, row.Ref, driver.PageReq{Limit: 1000})
		if err != nil || v.NextCursor != nil {
			r.t.Fatalf("%s complete versions %s: %v", name, key, err)
		}
		fullVersions[key] = v.Items
		if row.Kind == "bead" {
			ids = append(ids, row.Ref.ID)
		}
	}
	sort.Strings(ids)
	versions := map[string][]string{}
	tracked := append(append([]string{}, sourceIDs...), extraIDs...)
	for _, id := range tracked {
		v, err := reader.Versions(ctx, model.RefByID(id, model.Natural64(0)), driver.PageReq{Limit: 1000})
		if err != nil || v.NextCursor != nil {
			r.t.Fatalf("%s versions for %s: %v %+v", name, id, err, v)
		}
		for _, item := range v.Items {
			versions[id] = append(versions[id], item.Revision.String())
		}
	}
	var cursor *driver.Cursor
	if !meta.HistoryFloor.IsZero() {
		cursor = &driver.Cursor{Incarnation: meta.Incarnation, Revision: meta.HistoryFloor, Method: "changesets"}
	}
	changesets, err := reader.Changesets(ctx, cursor, 1000)
	if err != nil {
		r.t.Fatalf("%s changeset census: %v", name, err)
	}
	changeRevs := make([]string, 0, len(changesets))
	for _, change := range changesets {
		changeRevs = append(changeRevs, change.Revision.String())
	}
	allocator, ok := reader.(driver.AllocatorSnapshotReader)
	if !ok {
		r.t.Fatalf("%s allocator snapshot capability missing", name)
	}
	allocation, err := allocator.AllocatorSnapshot(ctx)
	if err != nil {
		r.t.Fatalf("%s allocator snapshot: %v", name, err)
	}
	encoded, err := driver.EncodeAllocatorSnapshot(allocation)
	if err != nil {
		r.t.Fatal(err)
	}
	receipts := map[string]*driver.IdempotencyRecord{}
	for name, key := range r.receipts {
		method := strings.Split(name, "/")[0]
		receipt, err := reader.IdempotencyByKey(ctx, "user:b09-native-review", "", method, key)
		if err != nil || receipt == nil {
			r.t.Fatalf("%s receipt %s missing: %v", name, method, err)
		}
		receipts[name] = receipt
	}
	stage := b09ReplayStage{Name: name, Meta: meta, BeadIDs: ids, VersionRevisions: versions, ChangesetRevs: changeRevs, Allocator: allocation, AllocatorSHA256: hashBytes(encoded), Records: records, Versions: fullVersions, Changesets: changesets, Receipts: receipts, LeaseEpoch: r.lease.Epoch()}
	if r.engine != "memory" {
		stage.SQL = r.sqlCensus()
	}
	(&recorder{t: r.t}).writeJSON(filepath.Join(r.output, name+".json"), stage)
	return stage
}

func openB09ReplayTarget(t *testing.T, engine string) *b09ReplayTarget {
	t.Helper()
	r := &b09ReplayTarget{t: t, engine: engine, now: time.Date(2026, 11, 5, 1, 0, 0, 0, time.UTC), receipts: map[string]string{}}
	ctx := context.Background()
	var err error
	switch engine {
	case "memory":
		r.store = memory.New()
	case "sqlite":
		r.path = filepath.Join(t.TempDir(), "b09-native.db")
		r.store, err = (sqlite.Driver{}).Open(ctx, driver.Config{DSN: r.path})
	case "postgres":
		r.postgresDSN = startB09Postgres(t, r)
		r.store, err = postgres.Open(ctx, driver.Config{DSN: r.postgresDSN})
	default:
		t.Fatalf("unknown B09 replay engine %q", engine)
	}
	if err != nil {
		t.Fatalf("%s open: %v", engine, err)
	}
	r.lease, err = r.store.Lease(ctx, "b09-native-replay-"+engine, driver.LeaseTTL)
	if err != nil {
		_ = r.store.Close()
		t.Fatalf("%s lease: %v", engine, err)
	}
	t.Cleanup(func() {
		if r.lease != nil {
			if err := r.lease.Release(context.Background()); err != nil {
				t.Errorf("%s cleanup lease: %v", engine, err)
			}
		}
		if r.store != nil {
			if err := r.store.Close(); err != nil {
				t.Errorf("%s cleanup store: %v", engine, err)
			}
		}
		t.Logf("B09 cleanup lease/store completed for %s", engine)
	})
	return r
}

func (r *b09ReplayTarget) reopen() error {
	ctx := context.Background()
	if r.lease != nil {
		if err := r.lease.Release(ctx); err != nil {
			return err
		}
		r.lease = nil
	}
	if r.engine != "memory" {
		if err := r.store.Close(); err != nil {
			return err
		}
		var err error
		switch r.engine {
		case "sqlite":
			r.store, err = (sqlite.Driver{}).Open(ctx, driver.Config{DSN: r.path})
		case "postgres":
			r.store, err = postgres.Open(ctx, driver.Config{DSN: r.postgresDSN})
		}
		if err != nil {
			return err
		}
	}
	lease, err := r.store.Lease(ctx, "b09-native-replay-"+r.engine, driver.LeaseTTL)
	if err != nil {
		return err
	}
	r.lease = lease
	return nil
}

func (r *b09ReplayTarget) sqlCensus() *b09SQLCensus {
	r.t.Helper()
	driverName := "sqlite"
	schema := ""
	dsn := r.path
	listTables := "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
	if r.engine == "postgres" {
		driverName, schema, dsn = "pgx", "plug_postgres.", r.postgresDSN
		listTables = "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname='plug_postgres' ORDER BY tablename"
	}
	db, err := sql.Open(driverName, dsn)
	if err != nil {
		r.t.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			r.t.Errorf("SQL census close: %v", err)
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	census := &b09SQLCensus{Engine: r.engine, Tables: map[string]b09SQLTable{}}
	list, err := db.QueryContext(ctx, listTables)
	if err != nil {
		r.t.Fatal(err)
	}
	tables := []string{}
	for list.Next() {
		var table string
		if err := list.Scan(&table); err != nil {
			r.t.Fatal(err)
		}
		if _, ok := b09SQLPrimary[table]; !ok {
			r.t.Fatalf("unreviewed physical SQL table %s", table)
		}
		tables = append(tables, table)
	}
	if err := list.Err(); err != nil {
		r.t.Fatal(err)
	}
	if err := list.Close(); err != nil {
		r.t.Fatal(err)
	}
	census.Commands = append(census.Commands, listTables)
	for _, table := range tables {
		query := "SELECT * FROM " + schema + table
		census.Commands = append(census.Commands, query)
		rows, err := db.QueryContext(ctx, query)
		if err != nil {
			r.t.Fatalf("%s physical SQL census %s: %v", r.engine, table, err)
		}
		columns, err := rows.Columns()
		if err != nil {
			_ = rows.Close()
			r.t.Fatal(err)
		}
		image := b09SQLTable{Columns: append([]string(nil), columns...)}
		for rows.Next() {
			values := make([]any, len(columns))
			pointers := make([]any, len(columns))
			for i := range values {
				pointers[i] = &values[i]
			}
			if err := rows.Scan(pointers...); err != nil {
				_ = rows.Close()
				r.t.Fatal(err)
			}
			encoded := make([]string, len(values))
			for i, value := range values {
				encoded[i] = b09SQLValue(value)
			}
			row, err := json.Marshal(encoded)
			if err != nil {
				_ = rows.Close()
				r.t.Fatal(err)
			}
			image.Rows = append(image.Rows, string(row))
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			r.t.Fatal(err)
		}
		if err := rows.Close(); err != nil {
			r.t.Fatal(err)
		}
		sort.Strings(image.Rows)
		census.Tables[table] = image
	}
	data, err := json.Marshal(census.Tables)
	if err != nil {
		r.t.Fatal(err)
	}
	census.SHA256 = hashBytes(data)
	return census
}

func startB09Postgres(t *testing.T, target *b09ReplayTarget) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	name := "plug-swarm-b09-native-" + hashBytes([]byte(t.Name()))[:12]
	docker := func(ctx context.Context, args ...string) ([]byte, error) {
		cmd := exec.CommandContext(ctx, "docker", append([]string{"--host=unix:///var/run/docker.sock"}, args...)...)
		for _, value := range os.Environ() {
			if !strings.HasPrefix(value, "DOCKER_HOST=") && !strings.HasPrefix(value, "DOCKER_CONTEXT=") && !strings.HasPrefix(value, "DOCKER_TLS=") && !strings.HasPrefix(value, "DOCKER_TLS_VERIFY=") && !strings.HasPrefix(value, "DOCKER_CERT_PATH=") {
				cmd.Env = append(cmd.Env, value)
			}
		}
		return cmd.CombinedOutput()
	}
	if out, err := docker(ctx, "run", "-d", "--rm", "--name", name, "-e", "POSTGRES_HOST_AUTH_METHOD=trust", "-e", "POSTGRES_DB=plug_b09_native", "-p", "127.0.0.1::5432", b09PostgresImage); err != nil {
		t.Fatalf("real PostgreSQL start failed: %v %s", err, out)
	}
	target.container = name
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		if out, err := docker(cleanupCtx, "rm", "-f", name); err != nil {
			t.Errorf("PostgreSQL cleanup: %v %s", err, out)
		} else {
			t.Log("B09 cleanup PostgreSQL container removed")
		}
	})
	out, err := docker(ctx, "port", name, "5432/tcp")
	if err != nil {
		t.Fatal(err)
	}
	address := strings.TrimSpace(string(out))
	host, _, err := net.SplitHostPort(address)
	if err != nil || host != "127.0.0.1" {
		t.Fatalf("PostgreSQL must bind loopback: %q", address)
	}
	dsn := "postgres://postgres@" + address + "/plug_b09_native?sslmode=disable"
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
		time.Sleep(30 * time.Millisecond)
	}
}
