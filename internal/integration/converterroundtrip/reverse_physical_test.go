package converterroundtrip

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/reverseconverter"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestReverseExactIntegersReadBackThroughThreeDriversAndNativeRefusal(t *testing.T) {
	ctx := context.Background()
	wide := json.Number("9007199254740993")
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			data := admittedFixtureBytesWithFields(t, map[string]any{"timeout_ns": wide, "agent_state": "opaque", "payload": map[string]any{"wide": wide, "nested": []any{json.Number("-9007199254740993"), nil}}})
			p, archive := admittedImportProfile(t, data)
			store, lease, cleanup := openReverseTarget(t, engine)
			defer cleanup()
			if _, err := RestoreImportWithLease(ctx, bytes.NewReader(data), store, lease, p); err != nil {
				t.Fatal(err)
			}
			before := savedLabelImport(t, store, archive, p, "")
			reader, err := store.Read(ctx, driver.View{Incarnation: before.Meta.Incarnation})
			if err != nil {
				t.Fatal(err)
			}
			wantRows := 0
			for _, source := range archive.Records("bead") {
				if source.Source.Fields["timeout_ns"] != wide {
					continue
				}
				wantRows++
				rows, err := reader.Get(ctx, []model.Ref{model.RefByID(source.ID, model.Natural64(0))})
				if err != nil || len(rows) != 1 {
					t.Fatal("missing actual driver row", err)
				}
				doc, err := pipeline.DecodeDocument(rows[0].Body)
				var actual any
				if err != nil || decodeReverseJSON(doc.Metadata["beads.timeout_ns"], &actual) != nil || actual != wide {
					t.Fatalf("driver persisted metadata number changed: %v %v", actual, err)
				}
			}
			if err := reader.Close(); err != nil || wantRows == 0 {
				t.Fatal("driver readback incomplete", err)
			}
			serialized, err := reverseAllRestored(ctx, store)
			if err != nil {
				t.Fatal(err)
			}
			decoder := json.NewDecoder(bytes.NewReader(serialized.Result.JSONL))
			decoder.UseNumber()
			seen := 0
			for {
				var line map[string]any
				if err := decoder.Decode(&line); err == io.EOF {
					break
				} else if err != nil {
					t.Fatal(err)
				}
				metadata, _ := line["metadata"].(map[string]any)
				if value, exists := metadata["beads.timeout_ns"]; exists && value == wide {
					payload, _ := metadata["beads.payload"].(map[string]any)
					if payload["wide"] != wide || !reflect.DeepEqual(payload["nested"], []any{json.Number("-9007199254740993"), nil}) {
						t.Fatal("nested exact metadata changed", payload)
					}
					seen++
				}
			}
			if seen != wantRows {
				t.Fatalf("bare metadata count=%d want=%d", seen, wantRows)
			}
			refused, err := ReverseRestoredWithOptions(ctx, store, reverseconverter.Options{All: true, NativeImport: true})
			var fields *reverseconverter.PhysicalFieldRefusal
			if !errors.As(err, &fields) || len(refused.Result.JSONL) != 0 || refused.Result.Digest != "" {
				t.Fatalf("native route accepted populated cells: %v", err)
			}
			counts := map[string]int{}
			for _, field := range fields.Fields {
				counts[field.Field] += field.Rows
			}
			if counts["timeout_ns"] != wantRows || counts["payload"] != wantRows || counts["agent_state"] != wantRows {
				t.Fatalf("typed populated field census=%v want rows=%d", counts, wantRows)
			}
			if !reflect.DeepEqual(before, savedLabelImport(t, store, archive, p, "")) {
				t.Fatal("reverse/readback/refusal changed SQL records, versions, receipts, blobs or projections")
			}
		})
	}
}

func TestActualPublicSQLCellsRefuseNativeReverseAcrossThreeDrivers(t *testing.T) {
	ctx := context.Background()
	for _, complete := range []bool{true, false} {
		for _, engine := range []string{"memory", "sqlite", "postgres"} {
			name := "partial/" + engine
			if complete {
				name = "complete/" + engine
			}
			t.Run(name, func(t *testing.T) {
				data, archive := publicSourceFixture(t, complete)
				p, _ := admittedImportProfile(t, data)
				p.HistoryMode = driver.RestoreHistoryModeSnapshot
				store, lease, cleanup := openReverseTarget(t, engine)
				defer cleanup()
				if _, err := RestoreImportWithLease(ctx, bytes.NewReader(data), store, lease, p); err != nil {
					t.Fatal(err)
				}
				before := savedLabelImport(t, store, archive, p, "")
				result, err := ReverseRestoredWithOptions(ctx, store, reverseconverter.Options{All: true, NativeImport: true})
				var refused *reverseconverter.PhysicalFieldRefusal
				if !errors.As(err, &refused) || len(result.Result.JSONL) != 0 || result.Result.Digest != "" {
					t.Fatalf("actual populated SQL cells reached native import: %v", err)
				}
				counts := map[string]int{}
				for _, field := range refused.Fields {
					counts[field.Field] += field.Rows
				}
				wantRows := len(archive.Records("bead"))
				if counts["timeout_ns"] != wantRows || counts["payload"] != wantRows {
					t.Fatalf("actual SQL field census=%v want rows=%d", counts, wantRows)
				}
				for _, name := range []string{"agent_state", "hook_bead", "role_bead", "role_type"} {
					want := wantRows
					if complete {
						want = 0 // Original complete capture has SQL NULL defaults.
					}
					if counts[name] != want {
						t.Fatalf("actual field %s rows=%d want=%d", name, counts[name], want)
					}
				}
				if !reflect.DeepEqual(before, savedLabelImport(t, store, archive, p, "")) {
					t.Fatal("actual public-source refusal changed destination")
				}
			})
		}
	}
}
