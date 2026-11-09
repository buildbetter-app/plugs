package converterroundtrip

import (
	"bytes"
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestImportedSourceMemoryIsReadableWithoutLosingProvenance(t *testing.T) {
	for _, joint := range []bool{false, true} {
		name, data := "legacy", admittedFixtureBytes(t)
		if joint {
			name, data = "source-planes", jointSourceFixture(t)
		}
		for _, engine := range []string{"memory", "sqlite", "postgres"} {
			t.Run(name+"/"+engine, func(t *testing.T) {
				ctx := context.Background()
				store, lease, _ := openIdentityTarget(t, engine)
				var profile driver.RestoreImportProfile
				archive, err := dumpformat.Read(bytes.NewReader(data))
				if err != nil {
					t.Fatal(err)
				}
				if joint {
					profile = jointProfile(t, data, store, lease)
				} else {
					profile, _ = admittedImportProfile(t, data)
				}
				// This public fixture explicitly opts into snapshot policy. It is
				// a memory projection regression, not strict-history evidence.
				report, err := RestoreImportWithLease(ctx, bytes.NewReader(data), store, lease, profile)
				if err != nil {
					t.Fatal(err)
				}
				catalog := service.NewOntology(nil)
				domain := service.New(store, lease, pipeline.New(store, lease, pipeline.WithOntology(catalog)), service.WithOntology(catalog))
				actor := profile.Admission.Actor
				page, err := domain.MemoryList(ctx, actor, service.MemoryListRequest{})
				if err != nil || len(page.Items) != len(archive.Records("memory")) || len(page.Items) == 0 {
					t.Fatalf("imported memory list: %+v %v", page, err)
				}
				reader, err := store.Read(ctx, driver.View{Incarnation: report.Restored.Incarnation})
				if err != nil {
					t.Fatal(err)
				}
				defer reader.Close()
				retained, err := reader.Blob(ctx, driver.Hash(archive.SHA256()))
				if err != nil || !bytes.Equal(retained, data) {
					t.Fatal("memory projection changed retained raw archive", err)
				}
				for _, source := range archive.Records("memory") {
					ref := model.RefByID(sourcePlaneMirrorID(source, joint), model.Natural64(0))
					got, err := domain.MemoryGet(ctx, actor, service.MemoryGetRequest{ID: ref})
					if err != nil || got.ID != ref.ID || got.Key != source.Body["key"] || got.Content != source.Body["value"] {
						t.Fatalf("imported memory get: %+v %v", got, err)
					}
					rows, err := reader.Get(ctx, []model.Ref{ref})
					if err != nil || len(rows) != 1 {
						t.Fatal("memory mirror missing", err)
					}
					var body map[string]any
					decode(t, rows[0].Body, &body)
					if joint {
						if !sameReverseJSON(body["source"], source.Source.Fields) || !sameReverseJSON(body["value"], source.Body["value"]) || body["classification"] != source.Body["classification"] {
							t.Fatal("canonical projection changed raw memory value or source")
						}
					} else {
						metadata := map[string]any{
							"plug.restore.source_id": source.ID, "plug.restore.source_table": source.Source.Table,
							"plug.restore.source": source.Source.Fields, "plug.restore.archive_sha256": archive.SHA256(),
						}
						want := map[string]any{"id": ref.ID, "key": source.Body["key"], "content": source.Body["value"], "metadata": metadata}
						if !sameReverseJSON(body, want) || !sameReverseJSON(got.Metadata, metadata) {
							t.Fatal("public memory document changed exact source value or provenance")
						}
					}
				}
				brief, err := domain.Brief(ctx, actor, service.BriefRequest{})
				if err != nil || !reflect.DeepEqual(brief.Memories, page.Items) {
					t.Fatalf("brief lost imported memory: %+v %v", brief.Memories, err)
				}
				meta, err := store.Meta(ctx)
				if err != nil || !reflect.DeepEqual(meta, report.Restored) {
					t.Fatal("memory reads changed restored state", err)
				}
			})
		}
	}
}

func TestSourceMemoryJSONProjectionPreservesNumbersAndSecretReferences(t *testing.T) {
	for _, value := range []any{json.Number("9007199254740993"), map[string]any{"$secret": "config/kv.memory.token"}, nil} {
		source := dumpformat.Record{Kind: "memory", ID: "fixture", Body: dumpformat.Row{"key": "fixture", "value": value, "classification": "public"}}
		body, err := sourceMirrorBody(source, false)
		if err != nil {
			t.Fatal(err)
		}
		var decoded any
		decode(t, []byte(body["content"].(string)), &decoded)
		if !sameReverseJSON(decoded, value) || !sameReverseJSON(body["value"], value) {
			t.Fatal("source memory JSON changed")
		}
	}
}
