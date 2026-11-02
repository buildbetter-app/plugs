package s1b17b20

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/api/wire"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestB19NativeHeadSnapshotSurvivesSQLiteReopen(t *testing.T) {
	sourcePath := filepath.Join(t.TempDir(), "source.db")
	source := openLocal(t, sourcePath)
	id := seed(t, source)
	sourceMeta := metadata(t, source)
	_, original, archive := dump(t, source, "head")
	if archive.Manifest().NativeHead != "2" || archive.Manifest().RecordCount != 1 || archive.Manifest().VersionCount != 1 {
		t.Fatalf("head archive does not describe its source: %+v", archive.Manifest())
	}
	source.close(t)
	source = openLocal(t, sourcePath)
	path, reopened, _ := dump(t, source, "head")
	if !bytes.Equal(original, reopened) {
		t.Fatal("the same native snapshot changed across SQLite close/reopen")
	}
	assertState(t, source, id)
	source.close(t)

	targetPath := filepath.Join(t.TempDir(), "target.db")
	target := openLocal(t, targetPath)
	before := metadata(t, target)
	if err := restoreArchive(target, path, "head"); err != nil {
		t.Fatalf("native admin restore over byte RPC: %v", err)
	}
	after := metadata(t, target)
	if after.StoreID != sourceMeta.StoreID || after.Incarnation == before.Incarnation || after.Incarnation == sourceMeta.Incarnation || after.Head.String() != "1" || after.HistoryFloor.String() != "1" {
		t.Fatalf("head-only restore identity/floor=%+v; source=%+v; old target=%+v", after, sourceMeta, before)
	}
	assertState(t, target, id)
	target.close(t)
	target = openLocal(t, targetPath)
	if !reflect.DeepEqual(metadata(t, target), after) {
		t.Fatal("native restored identity/head/floor changed after SQLite reopen")
	}
	assertState(t, target, id)
	t.Log("native admin.dump/admin.restore over the in-process byte transport preserved the recorded four-field head projection and new incarnation/floor across two real SQLite reopen boundaries")
}

func TestB19RestoredHistoryRefusesDiscardedUnknownAndOldIncarnation(t *testing.T) {
	source := openLocal(t, filepath.Join(t.TempDir(), "source.db"))
	id := seed(t, source)
	sourceMeta := metadata(t, source)
	path, _, _ := dump(t, source, "head")
	source.close(t)
	targetPath := filepath.Join(t.TempDir(), "target.db")
	target := openLocal(t, targetPath)
	oldTarget := metadata(t, target)
	if err := restoreArchive(target, path, "head"); err != nil {
		t.Fatal(err)
	}
	after := metadata(t, target)
	target.close(t)
	target = openLocal(t, targetPath)

	var version struct {
		Revision model.Natural
		Body     struct {
			Fields state
		}
	}
	call(t, target, "history.get", map[string]any{"ref": id, "asOf": map[string]any{"revision": 1, "incarnation": after.Incarnation}}, &version)
	if version.Revision.String() != "1" || version.Body.Fields != adoptedState(t) {
		t.Fatalf("restored base differs from the recorded head state: %+v", version)
	}
	for _, test := range []struct {
		name, incarnation, code string
		revision                uint64
		ref                     model.Ref
		detail                  string
	}{
		{"discarded-prefix", after.Incarnation, "cursor_expired", 0, id, "floor"},
		{"unknown-address", after.Incarnation, "not_found", 1, model.RefByID("pl-ffffffffffffffffffffffffffffffff", model.Natural64(0)), ""},
		{"future-address", after.Incarnation, "not_found", 2, id, "requestedRevision"},
		{"old-source-incarnation", sourceMeta.Incarnation, "incarnation_changed", 1, id, "fromIncarnation"},
		{"old-target-incarnation", oldTarget.Incarnation, "incarnation_changed", 1, id, "fromIncarnation"},
	} {
		t.Run(test.name, func(t *testing.T) {
			result, err := target.local.Client.Call(context.Background(), "history.get", map[string]any{"ref": test.ref, "asOf": map[string]any{"revision": test.revision, "incarnation": test.incarnation}}, "")
			var failure *wire.Error
			if !errors.As(err, &failure) || failure.Data.Code != test.code || len(result.JSON) != 0 {
				t.Fatalf("historical address must refuse without a substituted state: result=%s error=%v", result.JSON, err)
			}
			data, marshalErr := json.Marshal(failure)
			if marshalErr != nil {
				t.Fatal(marshalErr)
			}
			retain(t, "refusal.json", data)
			if test.detail != "" {
				if _, ok := failure.Data.Details[test.detail]; !ok {
					t.Fatalf("typed refusal omitted %s: %+v", test.detail, failure.Data)
				}
			}
			switch test.code {
			case "cursor_expired":
				if fmt.Sprint(failure.Data.Details["floor"]) != "1" {
					t.Fatalf("refusal returned the wrong retained floor: %+v", failure.Data.Details)
				}
			case "not_found":
				if test.detail != "" && (fmt.Sprint(failure.Data.Details["requestedRevision"]) != "2" || fmt.Sprint(failure.Data.Details["headRevision"]) != "1") {
					t.Fatalf("future refusal returned the wrong bounds: %+v", failure.Data.Details)
				}
			case "incarnation_changed":
				if failure.Data.Details["fromIncarnation"] != test.incarnation || failure.Data.Details["toIncarnation"] != after.Incarnation {
					t.Fatalf("refusal returned the wrong incarnation pair: %+v", failure.Data.Details)
				}
			}
			if !reflect.DeepEqual(metadata(t, target), after) {
				t.Fatal("historical refusal changed the restored store")
			}
		})
	}
	t.Log("history.get distinguishes the restore-discarded prefix, unknown/future addresses and old incarnations; this is native floor behavior, not production redaction or retention-reason parity")
}

func TestB19NativeFullHistoryRestoreRefusesWithoutMutation(t *testing.T) {
	source := openLocal(t, filepath.Join(t.TempDir(), "source.db"))
	seed(t, source)
	path, _, archive := dump(t, source, "full")
	if archive.Manifest().VersionCount != 2 || archive.Manifest().ChangesetCount != 2 {
		t.Fatalf("full dump did not retain both native writes: %+v", archive.Manifest())
	}
	source.close(t)
	targetPath := filepath.Join(t.TempDir(), "target.db")
	target := openLocal(t, targetPath)
	before := metadata(t, target)
	err := restoreArchive(target, path, "full")
	var failure *wire.Error
	if !errors.As(err, &failure) || failure.Data.Code != "restore_violation" || !strings.Contains(failure.Data.Hint, "full-history restore is unavailable") {
		t.Fatalf("native full history must be explicitly refused: %v", err)
	}
	if !reflect.DeepEqual(metadata(t, target), before) {
		t.Fatal("full-history refusal changed target metadata")
	}
	target.close(t)
	target = openLocal(t, targetPath)
	if !reflect.DeepEqual(metadata(t, target), before) {
		t.Fatal("full-history refusal persisted a target mutation")
	}
	reader, err := target.store.Read(context.Background(), driver.View{Incarnation: before.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	page, err := reader.Query(context.Background(), driver.Query{Limit: 10})
	if err != nil || len(page.Items) != 0 {
		t.Fatalf("full-history refusal persisted records: %+v %v", page, err)
	}
	t.Log("a genuine full native archive was refused by admin.restore without flattening source versions or mutating SQLite")
}

func TestB19NativeRestoreRefusesPopulatedSQLite(t *testing.T) {
	source := openLocal(t, filepath.Join(t.TempDir(), "source.db"))
	seed(t, source)
	path, _, _ := dump(t, source, "head")
	source.close(t)
	targetPath := filepath.Join(t.TempDir(), "target.db")
	target := openLocal(t, targetPath)
	var created model.Commit
	call(t, target, "bead.create", map[string]any{"type": "issue", "fields": map[string]any{"title": "Already occupied"}}, &created)
	if len(created.Results) != 1 {
		t.Fatal("occupied target setup did not commit one record")
	}
	id := created.Results[0].Ref
	var original map[string]any
	call(t, target, "bead.get", map[string]any{"id": id}, &original)
	before := metadata(t, target)
	err := restoreArchive(target, path, "head")
	var failure *wire.Error
	if !errors.As(err, &failure) || failure.Data.Code != "restore_requires_empty" {
		t.Fatalf("populated SQLite target was not refused: %v", err)
	}
	target.close(t)
	target = openLocal(t, targetPath)
	var current map[string]any
	call(t, target, "bead.get", map[string]any{"id": id}, &current)
	if !reflect.DeepEqual(original, current) || !reflect.DeepEqual(metadata(t, target), before) {
		t.Fatal("refused native restore changed the occupied target across reopen")
	}
	t.Log("P44 populated-target refusal preserved the pre-existing record and store metadata across SQLite close/reopen")
}
