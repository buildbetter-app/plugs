package converterroundtrip

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/reverseconverter"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// admittedFixtureBytes keeps the checked-in canonical fixture intact and
// produces a detached, canonical in-memory archive with the same shape as the
// server exporter: no verified history, a hash source declaration, and a
// block dependency. This is test data only; it is never written back.
func admittedFixtureBytes(t *testing.T) []byte {
	t.Helper()
	return rewriteData(t, func(members map[string][]byte) {
		var manifest map[string]any
		decode(t, members["manifest.json"], &manifest)
		manifest["history"] = "unavailable"
		projects := manifest["projects"].([]any)
		project := projects[0].(map[string]any)
		project["id_mode"] = "hash"
		project["hash_length"] = 1
		project["last_id"] = 8
		project["child_counters"] = map[string]any{"demo-7": 4}
		for _, raw := range manifest["sources"].([]any) {
			source := raw.(map[string]any)
			if source["family"] == "issue_versions" {
				source["rows"] = 0
			}
		}

		archiveLines := bytes.Split(bytes.TrimSuffix(members["records/beads-archive.ndjson"], []byte{'\n'}), []byte{'\n'})
		kept := archiveLines[:0]
		for _, line := range archiveLines {
			var row map[string]any
			decode(t, line, &row)
			source := row["source"].(map[string]any)
			if source["table"] == "issue_versions" {
				continue
			}
			kept = append(kept, line)
		}
		members["records/beads-archive.ndjson"] = append(bytes.Join(kept, []byte{'\n'}), '\n')
		for _, raw := range manifest["kind_registry"].([]any) {
			kind := raw.(map[string]any)
			if kind["name"] == "beads-archive" {
				kind["count"] = len(kept)
			}
		}

		var edge map[string]any
		decode(t, bytes.TrimSuffix(members["records/edge.ndjson"], []byte{'\n'}), &edge)
		edge["body"].(map[string]any)["type"] = "blocks"
		edge["source"].(map[string]any)["fields"].(map[string]any)["type"] = "blocks"
		members["records/edge.ndjson"] = encode(t, edge)
		members["manifest.json"] = encode(t, manifest)
	})
}

func admittedImportProfile(t *testing.T, archiveBytes []byte) (driver.RestoreImportProfile, *dumpformat.Archive) {
	t.Helper()
	archive, err := dumpformat.Read(bytes.NewReader(archiveBytes))
	if err != nil {
		t.Fatal(err)
	}
	profile := driver.RestoreImportProfile{
		Admission: driver.RestoreImportAdmission{
			Mode: "authenticated", Actor: model.PrincipalRef{Kind: "user", ID: "restore-admin"},
			Scope: "", Authority: "restore-admin", AdmissionDigest: strings.Repeat("a", 64),
		},
		SourceStoreID: driver.StoreID(archive.Manifest().StoreID), Method: "offline.restore.import",
		IdempotencyKey: strings.Repeat("b", 32), ArchiveDigest: archive.SHA256(), DerivedDigest: strings.Repeat("c", 64),
	}
	restoreTime := time.Date(2026, 10, 30, 19, 0, 0, 0, time.UTC)
	plan, err := buildPlanWithProfile(archive, &profile, restoreTime)
	if err != nil {
		t.Fatal(err)
	}
	profile.DerivedDigest = plan.derivedDigest
	return profile, archive
}

func TestAdmittedImportRestoresCommentAndServesWorkload(t *testing.T) {
	data := admittedFixtureBytes(t)
	profile, archive := admittedImportProfile(t, data)
	inspection, err := InspectImport(context.Background(), bytes.NewReader(data))
	if err != nil || inspection.SourceStoreID != profile.SourceStoreID || inspection.ArchiveSHA256 != profile.ArchiveDigest || inspection.DerivedSHA256 != profile.DerivedDigest {
		t.Fatalf("import inspection=%+v err=%v profile=%+v", inspection, err, profile)
	}
	for _, kind := range []string{"memory", "sqlite"} {
		t.Run(kind, func(t *testing.T) {
			store, lease, cleanup := openTarget(t, kind)
			defer cleanup()
			ctx := context.Background()
			before, err := store.Meta(ctx)
			if err != nil {
				t.Fatal(err)
			}
			reader, err := store.Read(ctx, driver.View{Incarnation: before.Incarnation})
			if err != nil {
				t.Fatal(err)
			}
			page, err := reader.Query(ctx, driver.Query{Limit: 10})
			_ = reader.Close()
			if err != nil || len(page.Items) != 0 || !before.Head.IsZero() {
				t.Fatalf("pre-call census meta=%+v rows=%d err=%v", before, len(page.Items), err)
			}

			report, err := RestoreImportWithLease(ctx, bytes.NewReader(data), store, lease, profile)
			if err != nil {
				t.Fatalf("import restore: %v", err)
			}
			if report.Archive.SHA256() != archive.SHA256() || report.Restored.Head.String() != "1" || report.IdempotencyKey != profile.IdempotencyKey {
				t.Fatalf("restore report=%+v", report)
			}
			recovered, err := RecoverImport(ctx, store, profile, Verification{ArchiveSHA256: profile.ArchiveDigest, DerivedSHA256: profile.DerivedDigest})
			if err != nil || !recovered.Recovered || recovered.Restored.Head.String() != "1" {
				t.Fatalf("import recovery=%+v err=%v", recovered, err)
			}
			altered := profile
			altered.Admission.Actor.ID = "restore-admin-altered"
			if _, err = RecoverImport(ctx, store, altered, Verification{ArchiveSHA256: profile.ArchiveDigest, DerivedSHA256: profile.DerivedDigest}); !errors.Is(err, driver.ErrConflict) {
				t.Fatalf("altered import profile accepted: %v", err)
			}
			after, err := store.Meta(ctx)
			if err != nil || after.Head.String() != "1" {
				t.Fatalf("post-call meta=%+v err=%v", after, err)
			}
			reader, err = store.Read(ctx, driver.View{Incarnation: after.Incarnation})
			if err != nil {
				t.Fatal(err)
			}
			rows, err := reader.Query(ctx, driver.Query{Filter: map[string]any{"project": "demo"}, Limit: 100})
			_ = reader.Close()
			beadRows := 0
			for _, row := range rows.Items {
				if row.Kind == "bead" {
					beadRows++
				}
			}
			if err != nil || beadRows != 3 {
				t.Fatalf("restored bead rows=%d total=%d err=%v", beadRows, len(rows.Items), err)
			}
			var imported pipeline.Comment
			foundComment := false
			for _, row := range rows.Items {
				if row.Kind != "bead" {
					continue
				}
				doc, decodeErr := pipeline.DecodeDocument(row.Body)
				if decodeErr != nil {
					t.Fatal(decodeErr)
				}
				if len(doc.Comments) == 1 {
					imported, foundComment = doc.Comments[0], true
				}
			}
			if !foundComment || imported.Origin != "import" || !reflect.DeepEqual(imported.Principal, profile.Admission.Actor) || imported.SourceAuthor != "fixture-commenter" || imported.SourceTime == nil || imported.Time.IsZero() {
				t.Fatalf("imported comment=%+v found=%v", imported, foundComment)
			}

			ontology := service.NewOntology(nil)
			writer := pipeline.New(store, lease, pipeline.WithOntology(ontology))
			svc := service.New(store, lease, writer, service.WithOntology(ontology))
			claimMeta := model.WriteMeta{Project: "demo", Agent: "restore-workload", IdempotencyKey: strings.Repeat("d", 32)}
			claimed, err := svc.ClaimNextWithOutcome(ctx, profile.Admission.Actor, service.ClaimNextRequest{Project: "demo"}, claimMeta)
			if err != nil || claimed == nil || len(claimed.Commit.Results) != 1 || claimed.Commit.Results[0].Ref.ID != "demo-7" {
				t.Fatalf("claimNext=%+v err=%v", claimed, err)
			}
			applyMeta := model.WriteMeta{Method: "tx.apply", Project: "demo", Agent: "restore-workload", IdempotencyKey: strings.Repeat("e", 32)}
			changes := model.ChangeSet{Ops: []model.Op{{Kind: model.OpPatch, Patch: &model.PatchOp{Target: model.RefByID("demo-7", model.Natural64(0)), Fields: map[string]any{"description": "updated after import"}}}}}
			applied, err := writer.Apply(ctx, profile.Admission.Actor, changes, applyMeta)
			if err != nil || len(applied.Results) != 1 {
				t.Fatalf("tx.apply=%+v err=%v", applied, err)
			}
			replayed, err := writer.Apply(ctx, profile.Admission.Actor, changes, applyMeta)
			if err != nil || !reflect.DeepEqual(applied, replayed) {
				t.Fatalf("tx.apply replay applied=%+v replay=%+v err=%v", applied, replayed, err)
			}
			if _, err := reverseconverter.ReadBytes(data); err != nil {
				t.Fatalf("reverse read: %v", err)
			} else if _, err := reverseconverter.ConvertArchive(data); err != nil {
				t.Fatalf("reverse convert: %v", err)
			}
		})
	}
}

func TestAdmittedImportRejectsUntrustedProfileBeforeWrite(t *testing.T) {
	data := admittedFixtureBytes(t)
	profile, _ := admittedImportProfile(t, data)
	profile.Admission.Actor = model.PrincipalRef{Kind: "system", ID: "plug-converter"}
	store, lease, cleanup := openTarget(t, "memory")
	defer cleanup()
	_, err := RestoreImportWithLease(context.Background(), bytes.NewReader(data), store, lease, profile)
	if err == nil || !errors.Is(err, driver.ErrConflict) && !strings.Contains(err.Error(), "import admission") {
		t.Fatalf("forged profile error=%v", err)
	}
	meta, metaErr := store.Meta(context.Background())
	if metaErr != nil || !meta.Head.IsZero() {
		t.Fatalf("forged profile mutated target meta=%+v err=%v", meta, metaErr)
	}
}

// Keep json imported in this file's test-only fixture helpers even when the
// compiler inlines the common decode helper differently across Bazel modes.
var _ = json.Valid
