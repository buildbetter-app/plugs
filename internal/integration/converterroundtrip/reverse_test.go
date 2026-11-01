package converterroundtrip

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestLiveReverseReadsPostRestoreEditsAndOpaqueValues(t *testing.T) {
	ctx := context.Background()
	opaque := map[string]any{
		"agent_state": "state\x00", "await_id": "await\x00", "await_type": "human",
		"timeout_ns": json.Number("9007199254740993"),
		"payload":    map[string]any{"number": json.Number("9007199254740993"), "null": nil},
		"waiters":    []any{"other", nil},
	}
	for _, kind := range []string{"memory", "sqlite"} {
		t.Run(kind, func(t *testing.T) {
			data := admittedFixtureBytesWithFields(t, opaque)
			profile, _ := admittedImportProfile(t, data)
			store, lease, cleanup := openTarget(t, kind)
			defer cleanup()
			if _, err := RestoreImportWithLease(ctx, bytes.NewReader(data), store, lease, profile); err != nil {
				t.Fatal(err)
			}
			before, err := ReverseRestored(ctx, store)
			if err != nil {
				t.Fatal(err)
			}
			ontology := service.NewOntology(nil)
			writer := pipeline.New(store, lease, pipeline.WithOntology(ontology))
			apply := func(description, key string) {
				t.Helper()
				_, err := writer.Apply(ctx, profile.Admission.Actor, model.ChangeSet{Ops: []model.Op{{
					Kind: model.OpPatch, Patch: &model.PatchOp{Target: model.RefByID("demo-7", model.Natural64(0)), Fields: map[string]any{"description": description}},
				}}}, model.WriteMeta{Method: "tx.apply", Project: "demo", Agent: "live-reverse", IdempotencyKey: strings.Repeat(key, 32)})
				if err != nil {
					t.Fatal(err)
				}
			}
			apply("live edit", "f")
			state := reverseState(t, store)
			after, err := ReverseRestored(ctx, store)
			if err != nil || after.Snapshot.At == nil || after.Snapshot.At.String() != state.Meta.Head.String() || after.Result.Digest == before.Result.Digest {
				t.Fatalf("live reverse revision/digest failed: %v", err)
			}
			assertReverseDescription(t, after.Result.JSONL, "live edit")
			decoder := json.NewDecoder(bytes.NewReader(after.Result.JSONL))
			for {
				var row struct {
					ID       string                     `json:"id"`
					Metadata map[string]json.RawMessage `json:"metadata"`
				}
				if err := decoder.Decode(&row); err == io.EOF {
					break
				} else if err != nil {
					t.Fatal(err)
				}
				if row.Metadata["beads.agent_state"] == nil {
					continue
				}
				for field, want := range opaque {
					var got any
					if err := decodeReverseJSON(row.Metadata["beads."+field], &got); err != nil || !sameReverseJSON(got, want) {
						t.Fatalf("opaque field %s changed: %v", field, err)
					}
				}
			}
			if !reflect.DeepEqual(state, reverseState(t, store)) {
				t.Fatal("reverse changed the store")
			}
			// The wrapper writes after opening the reader. Export must remain
			// entirely on its captured snapshot, including all source blobs.
			bound := reverseReadHook{Store: store, afterRead: func() { apply("concurrent edit", "a") }}
			captured, err := ReverseRestored(ctx, bound)
			if err != nil || captured.Result.Digest != after.Result.Digest {
				t.Fatalf("reverse mixed snapshots: %v", err)
			}
			assertReverseDescription(t, captured.Result.JSONL, "live edit")
		})
	}
}

func assertReverseDescription(t *testing.T, data []byte, want string) {
	t.Helper()
	d := json.NewDecoder(bytes.NewReader(data))
	for {
		var row struct {
			ID          string `json:"id"`
			Description string `json:"description"`
		}
		if err := d.Decode(&row); err != nil {
			t.Fatalf("missing live bead: %v", err)
		}
		if row.ID == "demo-7" {
			if row.Description != want {
				t.Fatal("reverse exported stale source state")
			}
			return
		}
	}
}

func TestLiveReverseRefusesUnrepresentableDeltasWithoutPartialOutput(t *testing.T) {
	ctx := context.Background()
	for _, kind := range []string{"memory", "sqlite"} {
		t.Run(kind, func(t *testing.T) {
			data := admittedFixtureBytes(t)
			profile, _ := admittedImportProfile(t, data)
			store, lease, cleanup := openTarget(t, kind)
			defer cleanup()
			if _, err := ReverseRestored(ctx, store); err == nil {
				t.Fatal("empty store was treated as a restored source")
			}
			if _, err := RestoreImportWithLease(ctx, bytes.NewReader(data), store, lease, profile); err != nil {
				t.Fatal(err)
			}
			state := reverseState(t, store)
			for _, mode := range []string{"extra", "missing", "comment", "opaque", "provenance", "claim"} {
				t.Run(mode, func(t *testing.T) {
					got, err := ReverseRestored(ctx, reverseReadHook{Store: store, mode: mode})
					var refused ReverseRefusal
					if !errors.As(err, &refused) || len(got.Result.JSONL) != 0 || got.Result.Digest != "" {
						t.Fatalf("unrepresentable image produced partial output: %v", err)
					}
				})
			}
			if !reflect.DeepEqual(state, reverseState(t, store)) {
				t.Fatal("refused reverse changed the store")
			}
		})
	}
}

func reverseState(t *testing.T, store driver.Store) struct {
	Meta    driver.Meta
	Records []driver.Record
} {
	t.Helper()
	ctx := context.Background()
	meta, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	r, err := store.Read(ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	rows, err := r.Query(ctx, driver.Query{Limit: 1000})
	if err != nil || rows.NextCursor != nil {
		t.Fatalf("test census incomplete: %v", err)
	}
	return struct {
		Meta    driver.Meta
		Records []driver.Record
	}{meta, rows.Items}
}

type reverseReadHook struct {
	driver.Store
	afterRead func()
	mode      string
}

func (s reverseReadHook) Read(ctx context.Context, view driver.View) (driver.Reader, error) {
	r, err := s.Store.Read(ctx, view)
	if err != nil {
		return nil, err
	}
	if s.afterRead != nil {
		s.afterRead()
	}
	return reverseQueryHook{Reader: r, mode: s.mode}, nil
}

type reverseQueryHook struct {
	driver.Reader
	mode string
}

func (r reverseQueryHook) Query(ctx context.Context, q driver.Query) (driver.Page[driver.Record], error) {
	page, err := r.Reader.Query(ctx, q)
	if err != nil {
		return page, err
	}
	for i, row := range page.Items {
		if row.Ref.ID != "demo-7" {
			continue
		}
		if r.mode == "missing" {
			page.Items = append(page.Items[:i], page.Items[i+1:]...)
			break
		}
		doc, err := pipeline.DecodeDocument(row.Body)
		if err != nil {
			return page, err
		}
		switch r.mode {
		case "extra":
			page.Items = append(page.Items, driver.Record{Ref: model.RefByID("unmapped", model.Natural64(0)), Kind: "memory", Body: []byte(`{}`)})
		case "comment":
			doc.Comments = append(doc.Comments, pipeline.Comment{})
		case "opaque":
			delete(doc.Metadata, "beads.await_id")
		case "provenance":
			doc.Metadata["beads.source_fields"] = json.RawMessage(`{}`)
		case "claim":
			doc.Lease = &pipeline.ClaimLease{}
		}
		page.Items[i].Body, err = json.Marshal(doc)
		if err != nil {
			return page, err
		}
		break
	}
	return page, nil
}
