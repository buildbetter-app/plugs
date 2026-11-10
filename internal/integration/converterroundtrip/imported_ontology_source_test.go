package converterroundtrip

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/ontologyops"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// These detached regression inputs prove importer mechanics. They are not
// native recordings or evidence of an installed authorization provider.
func importedOntologyBytes(t *testing.T, custom string) []byte {
	t.Helper()
	archive, err := dumpformat.Read(bytes.NewReader(admittedFixtureBytes(t)))
	if err != nil {
		t.Fatal(err)
	}
	return rewriteData(t, func(members map[string][]byte) {
		clear(members)
		for name := range archive.Manifest().Files {
			members[name], _ = archive.Member(name)
		}
		members["manifest.json"], _ = archive.Member("manifest.json")
		config := archive.Records("config")
		if custom != "" {
			row := dumpformat.Record{Kind: "config", ID: "config/status.custom", Project: "",
				Body:   map[string]any{"key": "config/status.custom", "classification": "public", "value": custom},
				Source: dumpformat.Provenance{Table: "config", Fields: map[string]any{"key": "status.custom", "value": custom}},
			}
			config = append(config, row)
		}
		// Keep every original typed config row and insert the declaration in
		// the reader's canonical (project, ID) order, before metadata rows.
		sort.Slice(config, func(i, j int) bool {
			return config[i].Project+"\x00"+config[i].ID < config[j].Project+"\x00"+config[j].ID
		})
		members["records/config.ndjson"] = nil
		for _, row := range config {
			members["records/config.ndjson"] = append(members["records/config.ndjson"], encode(t, row)...)
		}
		changeManifest(t, members, func(manifest map[string]any) {
			manifest["verification"].(map[string]any)["ontology"] = dumpformat.StrictUnionOntologyVerification
			// Strict union carries both declaration families even when their
			// independent source census is empty. Keep counts and reasons intact.
			for _, value := range manifest["sources"].([]any) {
				row := value.(map[string]any)
				if row["family"] == "custom_statuses" || row["family"] == "custom_types" {
					row["action"] = "carry"
				}
			}
			// admittedFixtureBytes adds a second, deliberately empty allocator
			// to test multi-project import. This detached strict-union control
			// instead declares exactly its one actual project. Refuse rather
			// than silently discarding any populated or unexpected project.
			projects := archive.Manifest().Projects
			if len(projects) != 2 || projects[0].Prefix != "demo" || projects[1].Prefix != "empty" || projects[1].LastID != 0 || len(projects[1].ChildCounters) != 0 {
				t.Fatal("authored ontology control requires demo plus an empty allocator")
			}
			for _, kind := range archive.Manifest().Kinds {
				for _, row := range archive.Records(kind.Name) {
					if row.Project != "" && row.Project != "demo" {
						t.Fatal("authored ontology control contains another project")
					}
				}
			}
			manifest["projects"] = manifest["projects"].([]any)[:1]
			if custom == "" {
				return
			}
			for _, value := range manifest["sources"].([]any) {
				row := value.(map[string]any)
				if row["family"] == "config" {
					// Read the independent source census rather than a hardcoded
					// count from this particular archived fixture.
					count := 1
					for _, kind := range archive.Manifest().Kinds {
						for _, original := range archive.Records(kind.Name) {
							if original.Source.Table == "config" {
								count++
							}
						}
					}
					row["rows"] = count
				}
			}
			for _, value := range manifest["kind_registry"].([]any) {
				row := value.(map[string]any)
				if row["name"] == "config" {
					row["count"] = len(archive.Records("config")) + 1
				}
			}
		})
	})
}

func requireImportedOntology(t *testing.T, store driver.Store, archive *dumpformat.Archive, profile driver.RestoreImportProfile, at model.Revision) driver.Record {
	t.Helper()
	ctx := context.Background()
	meta, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	r, err := store.Read(ctx, driver.View{Incarnation: meta.Incarnation, At: &at})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	ref := ImportedOntologySourceRef()
	lineageRef := model.RefByID("restore/ontology-source", model.Natural64(0))
	rows, err := r.Get(ctx, []model.Ref{ref, lineageRef})
	if err != nil || len(rows) != 2 {
		t.Fatalf("actual imported source/lineage absent: %v", err)
	}
	byID := map[string]driver.Record{}
	for _, row := range rows {
		byID[row.Ref.ID] = row
	}
	row, lineage := byID[ref.ID], byID[lineageRef.ID]
	if row.Ref != ref || row.Kind != "ontology" || row.Revision.Validate() != nil || row.Revision.IsZero() || lineage.Ref != lineageRef || lineage.Kind != "instance_config" || lineage.Revision != row.Revision {
		t.Fatalf("uncommitted/incorrect protected source identity: source=%+v lineage=%+v", row, lineage)
	}
	migration, ok := archive.ImportMigration()
	if !ok || len(migration.Declarations()) != 4 {
		t.Fatal("independent archive lacks the four authored native declarations")
	}
	// The source contract is canonical JSON. Document() initializes optional
	// empty maps while omitempty omits them on the wire; compare the complete
	// independently derived bytes instead of Go's nil-versus-empty map identity.
	expectedSource, err := json.Marshal(struct {
		Base ontology.Document `json:"base"`
	}{Base: migration.Document()})
	if err != nil || !bytes.Equal(row.Body, expectedSource) {
		t.Fatalf("stored source bytes differ from the complete validated union: %v", err)
	}
	source, err := service.DecodeOntologySource(row.Body)
	if err != nil || len(source.Overlays) != 0 || len(source.Plugins) != 0 {
		t.Fatalf("stored source differs from validated union: %v", err)
	}
	compiled, problems := ontology.Compile(source.Base, source.Overlays, source.Plugins)
	if len(problems) != 0 {
		t.Fatal(problems)
	}
	expectedCompiled, problems := ontology.Compile(migration.Document(), nil, nil)
	if len(problems) != 0 || compiled.Digest() != expectedCompiled.Digest() {
		t.Fatalf("stored source semantics differ from the independently validated union: %v", problems)
	}
	for name, category := range map[string]ontology.Category{"queued": ontology.Unstarted, "working": ontology.Started, "shelved": ontology.Backlog, "finished": ontology.Completed} {
		if source.Base.Statuses[name].Category != category {
			t.Fatalf("custom source status %s changed", name)
		}
	}
	var evidence struct {
		Source         model.Ref                      `json:"source"`
		BodySHA256     string                         `json:"body_sha256"`
		CompiledDigest string                         `json:"compiled_digest"`
		SourceStore    string                         `json:"source_store"`
		ArchiveSHA256  string                         `json:"archive_sha256"`
		DerivedSHA256  string                         `json:"derived_sha256"`
		Profile        string                         `json:"profile"`
		Declarations   []dumpformat.ConfigDeclaration `json:"declarations"`
		Members        map[string]dumpformat.File     `json:"members"`
	}
	if err := json.Unmarshal(lineage.Body, &evidence); err != nil || evidence.Source != ref || evidence.BodySHA256 != hashBytes(row.Body) || evidence.CompiledDigest != compiled.Digest() || evidence.SourceStore != archive.Manifest().StoreID || evidence.ArchiveSHA256 != archive.SHA256() || evidence.DerivedSHA256 != profile.DerivedDigest || evidence.Profile != "beads-compat-union-head/1" || !reflect.DeepEqual(evidence.Declarations, migration.Declarations()) || !reflect.DeepEqual(evidence.Members, archive.Manifest().Files) {
		t.Fatalf("source lineage differs from independently admitted archive/profile: %+v %v", evidence, err)
	}
	principal, err := driver.PrincipalKey(profile.Admission.Actor)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := r.IdempotencyByKey(ctx, principal, profile.AgentLabel(), profile.Method, profile.IdempotencyKey)
	if err != nil || receipt == nil || receipt.ResponsePending || receipt.Changeset.Kind != model.ChangesetRestore || !reflect.DeepEqual(receipt.Changeset.Principal, profile.Admission.Actor) {
		t.Fatalf("source lacks actual admitted restore receipt: %v", err)
	}
	for _, actual := range rows {
		matches := 0
		for _, result := range receipt.Commit.Results {
			if result.Ref == actual.Ref {
				matches++
				if result.Revision != actual.Revision || !bytes.Equal(result.Record, actual.Body) {
					t.Fatal("restore receipt/source revision or bytes differ")
				}
			}
		}
		if matches != 1 {
			t.Fatal("source/lineage not in the same admitted transaction receipt")
		}
		versions, err := r.Versions(ctx, actual.Ref, driver.PageReq{Limit: 10})
		if err != nil || versions.NextCursor != nil || len(versions.Items) != 1 || versions.Items[0].Revision != actual.Revision {
			t.Fatalf("protected source version absent or fabricated: %+v %v", versions, err)
		}
	}
	return row
}

func TestImportedOntologySourceIdentityLineageHistoryAndReopen(t *testing.T) {
	const custom = "queued:active,working:wip,shelved:frozen,finished:done"
	data := importedOntologyBytes(t, custom)
	profile, archive := admittedImportProfile(t, data)
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			ctx := context.Background()
			store, lease, reopen := openIdentityTarget(t, engine)
			report, err := RestoreImportWithLease(ctx, bytes.NewReader(data), store, lease, profile)
			if err != nil {
				t.Fatal(err)
			}
			at := report.Restored.Head
			original := requireImportedOntology(t, store, archive, profile, at)
			before := savedLabelImport(t, store, archive, profile, "unused-ontology-read-key")
			denied := errors.New("current ontology access revoked")
			allow, checked := true, 0
			makeService := func() *ontologyops.Service {
				t.Helper()
				svc, err := ontologyops.New(store, lease, ontologyops.Config{
					Source: service.StaticOntologySource(ImportedOntologySourceRef()),
					AuthorizeRead: func(_ context.Context, by model.PrincipalRef, resource model.ResourceRef, _ string) error {
						if resource.Kind == "ontology" {
							checked++
							if resource.Ref != original.Ref || !reflect.DeepEqual(by, profile.Admission.Actor) {
								t.Fatal("source authorization identity changed")
							}
							if !allow {
								return denied
							}
						}
						return nil
					},
				})
				if err != nil {
					t.Fatal(err)
				}
				return svc
			}
			svc := makeService()
			result, err := svc.Get(ctx, profile.Admission.Actor, "demo")
			migration, _ := archive.ImportMigration()
			if err != nil || result.Document == nil || !reflect.DeepEqual(*result.Document, migration.Document()) || result.Revision != original.Revision || result.Drifted || len(result.Problems) != 0 || checked == 0 {
				t.Fatalf("real protected-source consumer did not read admitted union: %+v %v", result, err)
			}
			if !reflect.DeepEqual(before, savedLabelImport(t, store, archive, profile, "unused-ontology-read-key")) {
				t.Fatal("ontology source reads changed imported state")
			}
			store, lease = reopen()
			if got := requireImportedOntology(t, store, archive, profile, at); !reflect.DeepEqual(got, original) || !reflect.DeepEqual(before, savedLabelImport(t, store, archive, profile, "unused-ontology-read-key")) {
				t.Fatal("reopen changed source body/revision/lineage/import receipt")
			}
			// Advance actual durable history through an ordinary Writer. The
			// source's historical bytes remain pinned after the live head moves.
			writer := pipeline.New(store, lease, pipeline.WithOntology(service.NewOntology(nil)))
			commit, err := writer.Apply(ctx, profile.Admission.Actor, model.ChangeSet{Ops: []model.Op{{Kind: model.OpPatch, Patch: &model.PatchOp{Target: model.RefByID("demo-7", model.Natural64(0)), Fields: map[string]any{"description": "ordinary edit after authentic source import"}}}}}, model.WriteMeta{Method: "tx.apply", Project: "demo", IdempotencyKey: strings.Repeat("e", 32)})
			if err != nil || commit.Revision == at {
				t.Fatalf("actual history advance failed: %+v %v", commit, err)
			}
			store, lease = reopen()
			if got := requireImportedOntology(t, store, archive, profile, at); !reflect.DeepEqual(got, original) {
				t.Fatal("historical protected source bytes changed after write and reopen")
			}
			svc = makeService()
			diff, err := svc.Diff(ctx, profile.Admission.Actor, "demo", at, commit.Revision)
			if err != nil || len(diff.Patch) != 0 {
				t.Fatalf("pinned source history changed: %+v %v", diff, err)
			}
			allow = false
			if _, err := svc.Get(ctx, profile.Admission.Actor, "demo"); !errors.Is(err, denied) {
				t.Fatalf("current source authorization not enforced: %v", err)
			}
			if _, err := svc.Diff(ctx, profile.Admission.Actor, "demo", at, commit.Revision); !errors.Is(err, denied) {
				t.Fatalf("historical source read bypassed current authorization: %v", err)
			}
			t.Logf("IMPORTED_ONTOLOGY_SOURCE engine=%s ref=%s revision=%s body_sha256=%s archive=%s current_head=%s native_recording=false", engine, original.Ref.ID, original.Revision, hashBytes(original.Body), archive.SHA256(), commit.Revision)
		})
	}
}

func TestImportedOntologySourceAbsentWithoutCustomDeclarations(t *testing.T) {
	data := importedOntologyBytes(t, "")
	profile, archive := admittedImportProfile(t, data)
	migration, ok := archive.ImportMigration()
	if !ok || len(migration.Declarations()) != 0 {
		t.Fatal("default-only strict regression input changed")
	}
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			store, lease, cleanup := openReverseTarget(t, engine)
			defer cleanup()
			report, err := RestoreImportWithLease(context.Background(), bytes.NewReader(data), store, lease, profile)
			if err != nil {
				t.Fatal(err)
			}
			r, err := store.Read(context.Background(), driver.View{Incarnation: report.Restored.Incarnation})
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			rows, err := r.Get(context.Background(), []model.Ref{ImportedOntologySourceRef(), model.RefByID("restore/ontology-source", model.Natural64(0))})
			if err != nil || len(rows) != 0 {
				t.Fatalf("default/derived-only source was synthesized: %+v %v", rows, err)
			}
		})
	}
}

type ontologySourceFaultTx struct {
	driver.WriteTx
	driver.RestoreImportTx
	driver.AllocatorContinuation
	hit *bool
	err error
}

func (tx *ontologySourceFaultTx) Put(ctx context.Context, row driver.Record) error {
	if row.Ref.ID == "restore/ontology-source" {
		*tx.hit = true
		return tx.err
	}
	return tx.WriteTx.Put(ctx, row)
}

func TestImportedOntologySourceAndLineageAreAtomic(t *testing.T) {
	data := importedOntologyBytes(t, "queued:active,working:wip,shelved:frozen,finished:done")
	profile, _ := admittedImportProfile(t, data)
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			store, lease, reopen := openIdentityTarget(t, engine)
			before := reverseState(t, store)
			failure, hit := errors.New("injected source-lineage staging refusal"), false
			wrapped := &observedTarget{Store: store, wrap: func(tx driver.WriteTx) driver.WriteTx {
				return &ontologySourceFaultTx{WriteTx: tx, RestoreImportTx: tx.(driver.RestoreImportTx), AllocatorContinuation: tx.(driver.AllocatorContinuation), hit: &hit, err: failure}
			}}
			if _, err := RestoreImportWithLease(context.Background(), bytes.NewReader(data), wrapped, lease, profile); !errors.Is(err, failure) || !hit {
				t.Fatalf("source-lineage atomicity fault did not reach real staging: hit=%v err=%v", hit, err)
			}
			if !reflect.DeepEqual(before, reverseState(t, store)) {
				t.Fatal("refused source lineage left committed source/beads or changed target identity")
			}
			store, lease = reopen()
			if !reflect.DeepEqual(before, reverseState(t, store)) {
				t.Fatal("reopen exposed a partial imported source transaction")
			}
			if _, err := RestoreImportWithLease(context.Background(), bytes.NewReader(data), store, lease, profile); err != nil {
				t.Fatalf("fresh import after rollback failed: %v", err)
			}
		})
	}
}
