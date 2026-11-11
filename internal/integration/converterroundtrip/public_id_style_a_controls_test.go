package converterroundtrip

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// These finite rejection controls are synthetic and earn no fresh-case credit.
func TestPublicIDStyleAImportedRowsRemainExact(t *testing.T) {
	initial := []driver.Record{{Ref: model.RefByID("native/current/opaque", model.Natural64(0)), Kind: "beads-archive", Revision: model.Natural64(4), Body: []byte(`{"n":9007199254740993,"null":null}`)}}
	if err := publicIDPreserved(initial, initial); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"missing", "rounded", "kind", "revision", "branch"} {
		t.Run(name, func(t *testing.T) {
			actual := append([]driver.Record{}, initial...)
			switch name {
			case "missing":
				actual = nil
			case "rounded":
				actual[0].Body = []byte(`{"n":9007199254740992,"null":null}`)
			case "kind":
				actual[0].Kind = "bead"
			case "revision":
				actual[0].Revision = model.Natural64(5)
			case "branch":
				actual[0].Ref = model.RefByID("native/current/other", model.Natural64(0))
			}
			if publicIDPreserved(initial, actual) == nil {
				t.Fatal("changed imported row was accepted")
			}
		})
	}
}

func TestPublicIDStyleANativeCreateClockAndWriteCensus(t *testing.T) {
	t.Setenv("PLUG_ID_STYLE_A_FIELD_PROFILE", filepath.Join(os.Getenv("TEST_SRCDIR"), os.Getenv("TEST_WORKSPACE"), "tools/roundtrip/testdata/fresh-native/continuation-fields.json"))
	query, err := publicIDJournalQuery()
	if err != nil {
		t.Fatal(err)
	}
	when := time.Date(2026, 11, 1, 12, 0, 0, 123456789, time.UTC)
	op := publicIDOperation{Kind: "root", Clock: when, NativeActor: "user:control", ExpectedID: "demo-control", Window: publicIDWindow{Started: when.Add(-time.Second), Ended: when.Add(time.Second)}}
	row := []string{"1", "2026-11-01 12:00:00", "create", op.ExpectedID, "{}", "", "", op.NativeActor}
	journal := map[string]any{"query": query, "arguments": []any{op.ExpectedID}, "columns": []string{"seq", "ts", "op", "issue_id", "issue_json", "dep_json", "comment_json", "actor"}, "database_types": []string{"BIGINT", "DATETIME", "VARCHAR", "VARCHAR", "LONGTEXT", "LONGTEXT", "LONGTEXT", "VARCHAR"}, "rows": [][]string{row}}
	op.NativeCreateJournal, _ = json.Marshal(journal)
	response := map[string]any{"updated_at": when.Add(time.Microsecond)}
	op.NativeResponse, _ = json.Marshal(response)
	if gap, err := publicIDNativeCreateClock(op); err != nil || gap != time.Microsecond {
		t.Fatalf("actual two-read gap control: %v %v", gap, err)
	}
	for _, name := range []string{"before-created", "outside-window", "missing-journal", "extra-write", "wrong-item", "filtered-query", "wrong-actor"} {
		t.Run(name, func(t *testing.T) {
			changed := op
			switch name {
			case "before-created":
				changed.NativeResponse, _ = json.Marshal(map[string]any{"updated_at": when.Add(-time.Nanosecond)})
			case "outside-window":
				changed.NativeResponse, _ = json.Marshal(map[string]any{"updated_at": op.Window.Ended.Add(time.Nanosecond)})
			case "missing-journal":
				changed.NativeCreateJournal = nil
			case "extra-write", "filtered-query", "wrong-actor":
				copy := map[string]any{}
				for k, v := range journal {
					copy[k] = v
				}
				if name == "extra-write" {
					more := append([]string{}, row...)
					more[0], more[2] = "2", "update"
					copy["rows"] = [][]string{row, more}
				} else if name == "filtered-query" {
					copy["query"] = query + " LIMIT 1"
				} else {
					other := append([]string{}, row...)
					other[7] = "user:another"
					copy["rows"] = [][]string{other}
				}
				changed.NativeCreateJournal, _ = json.Marshal(copy)
			case "wrong-item":
				changed.ExpectedID = "demo-other"
			}
			if _, err := publicIDNativeCreateClock(changed); err == nil {
				t.Fatal("invalid create clock/write census accepted")
			}
		})
	}
}

func TestPublicIDStyleANativeFieldsFailClosed(t *testing.T) {
	profile := filepath.Join(os.Getenv("TEST_SRCDIR"), os.Getenv("TEST_WORKSPACE"), "tools/roundtrip/testdata/fresh-native/continuation-fields.json")
	t.Setenv("PLUG_ID_STYLE_A_FIELD_PROFILE", profile)
	if err := publicIDNativeCreateFields(map[string]any{"storage_class": "versioned", "notes": ""}); err != nil {
		t.Fatal(err)
	}
	for _, values := range []map[string]any{{"unmapped": true}, {"notes": "actual non-default text"}, {"storage_class": "unversioned"}, {"closed_at": "2026-11-01T00:00:00Z"}} {
		if err := publicIDNativeCreateFields(values); err == nil {
			t.Fatal("unmapped/non-default native field omitted")
		}
	}
	t.Setenv("PLUG_ID_STYLE_A_FIELD_PROFILE", filepath.Join(t.TempDir(), "missing.json"))
	if publicIDNativeCreateFields(map[string]any{}) == nil {
		t.Fatal("missing immutable field profile admitted")
	}
}

func TestPublicIDStyleAActualPerWriteEventRequired(t *testing.T) {
	ctx := context.Background()
	store, lease, _ := openIdentityTarget(t, "memory")
	when := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	actor := model.PrincipalRef{Kind: "user", ID: "event-control"}
	writer := pipeline.New(store, lease, pipeline.WithClock(func() time.Time { return when }), pipeline.WithIDGenerator(func() (string, error) { return "event-control-root", nil }), pipeline.WithOntology(service.NewOntology(nil)), pipeline.WithOntologyAudit(service.NewOntologyAudit(nil)))
	meta := publicIDMeta("event-control", 0, "local")
	commit, err := writer.Apply(ctx, actor, model.ChangeSet{Ops: []model.Op{{Kind: model.OpCreate, Create: &model.CreateOp{Type: "issue", Project: "local", Fields: map[string]any{"title": "Event control"}}}}}, meta)
	if err != nil {
		t.Fatal(err)
	}
	op := publicIDOperation{Kind: "root", Actor: actor, Clock: when, ExpectedID: commit.Results[0].Ref.ID}
	change, err := publicIDEvent(ctx, store, op, meta)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"missing", "legacy", "wrong-kind", "wrong-actor", "wrong-item", "wrong-key"} {
		t.Run(name, func(t *testing.T) {
			copy := change
			copy.Events = model.CloneEventDescriptors(change.Events)
			candidate := &copy
			switch name {
			case "missing":
				candidate = nil
			case "legacy":
				copy.Events = nil
			case "wrong-kind":
				copy.Events[0].Type = model.EventBeadCommented
			case "wrong-actor":
				copy.Principal.ID = "another"
			case "wrong-item":
				copy.Events[0].Resource.Ref = model.RefByID("another-root", model.Natural64(0))
			case "wrong-key":
				copy.IdempotencyKey = "another"
			}
			if publicIDCheckEvent(candidate, op, meta) == nil {
				t.Fatal("missing or misattributed per-write event accepted")
			}
		})
	}
}
