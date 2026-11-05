package converterroundtrip

// This opt-in collector produces public fixture evidence for the rollback
// author. It does not run bd, qualify a release, or change the reverse seam.
import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/reverseconverter"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

func TestRollbackRehearsalFixtureEvidence(t *testing.T) {
	if os.Getenv("PLUG_ROLLBACK_FIXTURE_EVIDENCE") != "1" {
		t.Skip("explicit disposable rollback fixture collection only")
	}
	out := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR")
	if out == "" {
		t.Fatal("Bazel evidence directory required")
	}
	for _, ephemeral := range []bool{false, true} {
		name := "active-temporary"
		if ephemeral {
			name = "ephemeral"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			write := func(name string, value any) {
				t.Helper()
				data, err := json.MarshalIndent(value, "", "  ")
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(out, name), append(data, '\n'), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			if err := os.MkdirAll("/tmp/plug-swarm/work", 0o700); err != nil {
				t.Fatal(err)
			}
			root, err := os.MkdirTemp("/tmp/plug-swarm/work", "rollback-reverse-"+name+"-")
			if err != nil {
				t.Fatal(err)
			}
			var store driver.Store
			var lease driver.ActiveLease
			defer func() {
				var failures []string
				if lease != nil {
					if err := lease.Release(ctx); err != nil {
						failures = append(failures, err.Error())
					}
				}
				if store != nil {
					if err := store.Close(); err != nil {
						failures = append(failures, err.Error())
					}
				}
				if err := os.RemoveAll(root); err != nil {
					failures = append(failures, err.Error())
				}
				_, stat := os.Lstat(root)
				write(name+"-cleanup.json", map[string]any{"root": root, "absent": errors.Is(stat, os.ErrNotExist), "failures": failures})
				if len(failures) != 0 || !errors.Is(stat, os.ErrNotExist) {
					t.Error("disposable fixture cleanup failed", failures, stat)
				}
			}()
			dsn := filepath.Join(root, "plug.sqlite")
			store, err = (sqlite.Driver{}).Open(ctx, driver.Config{DSN: dsn})
			if err != nil {
				t.Fatal(err)
			}
			lease, err = store.Lease(ctx, "rollback-fixture", driver.LeaseTTL)
			if err != nil {
				t.Fatal(err)
			}
			data := sourceReverseFixture(t, ephemeral)
			profile := sourceReverseProfile(t, data, store, lease, ephemeral)
			restored, err := RestoreImportWithLease(ctx, bytes.NewReader(data), store, lease, profile)
			if err != nil {
				t.Fatal(err)
			}
			_, server := sourcePublicService(t, store, lease, profile, restored, &sourcePublicPolicy{})
			edit := model.ChangeSet{Ops: []model.Op{
				{Kind: model.OpPatch, Patch: &model.PatchOp{Target: model.RefByID("demo-9", model.Natural64(0)), Fields: map[string]any{"title": "rollback current active", "labels": []string{"rollback-active"}}}},
				{Kind: model.OpPatch, Patch: &model.PatchOp{Target: model.RefByID("demo-8", model.Natural64(0)), Fields: map[string]any{"title": "rollback current durable"}}},
			}}
			sourcePublicCall(t, server, profile.Admission.Actor, "tx.apply", 9201, map[string]any{"changeSet": edit}, "")
			archive, err := dumpformat.Read(bytes.NewReader(data))
			if err != nil {
				t.Fatal(err)
			}
			kinds, tables := map[string]int{}, map[string]int{}
			for _, kind := range archive.Manifest().Kinds {
				kinds[kind.Name] = len(archive.Records(kind.Name))
				for _, row := range archive.Records(kind.Name) {
					tables[row.Source.Table]++
				}
			}
			before := savedLabelImport(t, store, restored.Archive, profile, "")
			reverse, err := ReverseRestored(ctx, store)
			write(name+"-reverse-outcome.json", map[string]any{"error": rollbackError(err), "bytes": len(reverse.Result.JSONL), "snapshot": reverse.Snapshot, "report": reverse.Result.Report})
			if err != nil {
				t.Fatal("default reverse", err)
			}
			if !reflect.DeepEqual(before, savedLabelImport(t, store, restored.Archive, profile, "")) {
				t.Fatal("reverse changed the source store")
			}
			if err := os.WriteFile(filepath.Join(out, name+"-reverse.jsonl"), reverse.Result.JSONL, 0o600); err != nil {
				t.Fatal(err)
			}
			write(name+"-reverse-report.json", reverse.Result.Report)
			r, err := store.Read(ctx, driver.View{Incarnation: restored.Restored.Incarnation})
			if err != nil {
				t.Fatal(err)
			}
			page, err := r.Query(ctx, driver.Query{Limit: 1000})
			if err != nil || page.NextCursor != nil {
				t.Fatal("incomplete actual record census", err)
			}
			commits, err := r.Changesets(ctx, nil, 1000)
			if err != nil || len(commits) == 1000 {
				t.Fatal("incomplete actual changeset census", err)
			}
			if err := r.Close(); err != nil {
				t.Fatal(err)
			}
			coreKinds, principals := map[string]int{}, map[string]bool{}
			for _, row := range page.Items {
				coreKinds[row.Kind]++
			}
			postCommits, postEvents := 0, 0
			for _, commit := range commits {
				principal, err := driver.PrincipalKey(commit.Principal)
				if err != nil {
					t.Fatal(err)
				}
				principals[principal] = true
				if commit.Kind != model.ChangesetRestore {
					postCommits++
					postEvents += len(commit.Events)
				}
			}
			db, err := sql.Open("sqlite", dsn)
			if err != nil {
				t.Fatal(err)
			}
			var blobs, blobBytes, sessions int
			if err := db.QueryRowContext(ctx, "SELECT COUNT(*), COALESCE(SUM(length(data)),0) FROM blobs").Scan(&blobs, &blobBytes); err != nil {
				t.Fatal(err)
			}
			if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM core_sessions").Scan(&sessions); err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			write(name+"-census.json", map[string]any{"fixture_archive_sha256": archive.SHA256(), "store_root": root, "source_kind_records": kinds, "source_table_records": tables, "core_kind_records": coreKinds, "post_switch_core_changesets": postCommits, "post_switch_core_events": postEvents, "authenticated_changeset_principals": len(principals), "principal_sessions": sessions, "blobs": blobs, "blob_bytes": blobBytes, "ephemeral_items": map[bool]int{true: 1, false: 0}[ephemeral], "active_temporary_items": 1, "omitted_ephemeral_rows": len(reverse.Result.Report.OmittedEphemeralRows), "unsupported_fields_reported": len(reverse.Result.Report.UnsupportedFields), "archived_source_field_instances": len(reverse.Result.Report.ArchivedSourceFields)})
			write(name+"-core-changesets.json", commits)
			write(name+"-core-records.json", page.Items)
			claim := sourcePublicCall(t, server, profile.Admission.Actor, "bead.claim", 9202, map[string]any{"id": model.RefByID("demo-9", model.Natural64(0))}, "")
			write(name+"-claim.json", claim)
			claimed, claimErr := ReverseRestored(ctx, store)
			all, allErr := ReverseRestoredWithOptions(ctx, store, reverseconverter.Options{All: true})
			write(name+"-claimed-reverse.json", map[string]any{"default_error": rollbackError(claimErr), "default_bytes": len(claimed.Result.JSONL), "all_error": rollbackError(allErr), "all_bytes": len(all.Result.JSONL), "claimed_items": 1})
			if ephemeral {
				if claimErr != nil || !bytes.Equal(claimed.Result.JSONL, reverse.Result.JSONL) {
					t.Fatal("default ephemeral exclusion changed after claim", claimErr)
				}
			} else if claimErr == nil || len(claimed.Result.JSONL) != 0 {
				t.Fatal("included active claim was not refused")
			}
			if allErr == nil || len(all.Result.JSONL) != 0 {
				t.Fatal("explicit All claim was not refused")
			}
		})
	}
}

func rollbackError(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
