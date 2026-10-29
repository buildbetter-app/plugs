package pluginhost

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/pluginapi"
	sdk "github.com/buildbetter-app/plugs/sdk/go"
	"github.com/buildbetter-app/plugs/sdk/go/conformance"
)

// These tests exercise the production writer and memory driver. The ontology
// hook accepts the fixture's fields; ontology routing is outside this host.
type acceptingOntology struct{}

func (acceptingOntology) Normalize(context.Context, port.ProposedChange, port.SnapshotReader) ([]port.Patch, error) {
	return nil, nil
}
func (acceptingOntology) Validate(context.Context, port.ProposedChange, port.SnapshotReader) error {
	return nil
}

func nativeWriter(t *testing.T, h *Host) (*pipeline.Writer, *memory.Store) {
	t.Helper()
	store := memory.New()
	t.Cleanup(func() { _ = store.Close() })
	lease, err := store.Lease(context.Background(), "native-conformance", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lease.Release(context.Background()) })
	return pipeline.New(store, lease, pipeline.WithOntology(acceptingOntology{}),
		pipeline.WithMutators(h.Mutators()...), pipeline.WithValidators(h.Validators()...),
		pipeline.WithPostSubscribers(h.Posts()...),
		pipeline.WithIDGenerator(func() (string, error) { return "pl-native", nil })), store
}

func nativeCreate(title string) model.ChangeSet {
	return model.ChangeSet{Ops: []model.Op{{Kind: model.OpCreate, Create: &model.CreateOp{
		Type: "issue", Fields: map[string]any{"title": title, "private": "PRIVATE-SENTINEL"},
	}}}}
}

var nativePrincipal = model.PrincipalRef{Kind: "user", ID: "native-test"}
var nativeMeta = model.WriteMeta{Method: "tx.apply", IdempotencyKey: "00000000000000000000000000000001", Project: "test"}

func nativeReader(t *testing.T, store *memory.Store) driver.Reader {
	t.Helper()
	meta, err := store.Meta(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	r, err := store.Read(context.Background(), driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func requireUnpublished(t *testing.T, store *memory.Store) {
	t.Helper()
	ctx := context.Background()
	r := nativeReader(t, store)
	if r.Snapshot().At.String() != "0" {
		t.Fatalf("refused callback advanced head: %s", r.Snapshot().At)
	}
	page, err := r.Query(ctx, driver.Query{})
	if err != nil || len(page.Items) != 0 {
		t.Fatalf("refused callback published records: %+v, %v", page, err)
	}
	log, err := r.Changesets(ctx, nil, 10)
	if err != nil || len(log) != 0 {
		t.Fatalf("refused callback published changesets: %+v, %v", log, err)
	}
	versions, err := r.Versions(ctx, model.RefByID("pl-native", ""), driver.PageReq{})
	if err != nil || len(versions.Items) != 0 {
		t.Fatalf("refused callback published versions: %+v, %v", versions, err)
	}
}

func TestPipelineNativeCommitAndReplay(t *testing.T) {
	for _, failPost := range []bool{false, true} {
		t.Run(fmt.Sprintf("postFailure=%t", failPost), func(t *testing.T) {
			var mutations, posts atomic.Int32
			var postChecked atomic.Bool
			native := conformance.Native(conformance.Options{
				Probe: func(ctx context.Context, _ pluginapi.Host, c pluginapi.ProposedChange, r pluginapi.SnapshotReader) error {
					mutations.Add(1)
					page, err := r.Query(ctx, pluginapi.Query{})
					if err != nil {
						return err
					}
					if page.AsOf.String() != c.BaseRevision.String() || len(page.Items) != 0 {
						return fmt.Errorf("pre read was not pinned before creation: %+v", page)
					}
					return nil
				},
				After: func(ctx context.Context, _ pluginapi.Host, c pluginapi.CommittedChange, r pluginapi.SnapshotReader) error {
					posts.Add(1)
					if c.Revision.String() != "1" || len(c.Changes) != 1 || c.Changes[0].SetBy["fields.normalized"] != "normalize" {
						return fmt.Errorf("post change/provenance differs: %+v", c)
					}
					records, err := r.Get(ctx, []pluginapi.Ref{c.Changes[0].Resource.Ref})
					if err != nil {
						return err
					}
					if len(records) != 1 || records[0].Revision.String() != c.Revision.String() {
						return fmt.Errorf("post read missed committed revision: %+v", records)
					}
					var body struct{ Fields map[string]any }
					if err := json.Unmarshal(records[0].Body, &body); err != nil {
						return err
					}
					if body.Fields["normalized"] != "HELLO" || len(body.Fields) != 1 {
						return fmt.Errorf("post reader has wrong value or widened field grant: %s", records[0].Body)
					}
					postChecked.Store(true)
					if failPost {
						return errors.New("fixture delivery failed")
					}
					return nil
				},
			})
			h, err := New(Config{Natives: []sdk.Native{native}, Bindings: fixtureBindings()})
			if err != nil {
				t.Fatal(err)
			}
			w, store := nativeWriter(t, h)
			cs := nativeCreate("hello")
			before, _ := json.Marshal(cs)
			commit, err := w.Apply(context.Background(), nativePrincipal, cs, nativeMeta)
			if err != nil {
				t.Fatal(err)
			}
			if commit.Revision.String() != "1" || len(commit.Results) != 1 {
				t.Fatalf("commit: %+v", commit)
			}
			if !postChecked.Load() {
				t.Fatalf("native post could not observe committed data: %+v", commit.Warnings)
			}
			if failPost {
				if len(commit.Warnings) != 1 || commit.Warnings[0].Code != "post_subscriber_failed" {
					t.Fatalf("post failure did not preserve committed response: %+v", commit)
				}
			} else if len(commit.Warnings) != 0 {
				t.Fatalf("native post could not observe committed data: %+v", commit.Warnings)
			}
			result := commit.Results[0]
			body, err := pipeline.DecodeDocument(result.Record)
			if err != nil || body.Fields["normalized"] != "HELLO" || body.Fields["title"] != "hello" || body.Fields["private"] != "PRIVATE-SENTINEL" {
				t.Fatalf("native patch was not applied atomically: %+v, %v", body, err)
			}
			if result.SetBy["fields.normalized"] != "normalize" || result.SetBy["fields.title"] != "request" {
				t.Fatalf("provenance: %+v", result.SetBy)
			}
			r := nativeReader(t, store)
			records, err := r.Get(context.Background(), []model.Ref{result.Ref})
			if err != nil || len(records) != 1 || string(records[0].Body) != string(result.Record) {
				t.Fatalf("stored record differs from response: %+v, %v", records, err)
			}
			log, err := r.Changesets(context.Background(), nil, 10)
			if err != nil || len(log) != 1 || !reflect.DeepEqual(log[0].Mutators, []string{"normalize"}) {
				t.Fatalf("changeset did not record native mutator: %+v, %v", log, err)
			}
			again, err := w.Apply(context.Background(), nativePrincipal, cs, nativeMeta)
			firstWire, firstErr := json.Marshal(commit)
			replayWire, replayErr := json.Marshal(again)
			if err != nil || firstErr != nil || replayErr != nil || string(firstWire) != string(replayWire) || mutations.Load() != 1 || posts.Load() != 1 {
				t.Fatalf("replay reran native callbacks or changed response: %+v, %v; calls=%d/%d", again, err, mutations.Load(), posts.Load())
			}
			versions, err := nativeReader(t, store).Versions(context.Background(), result.Ref, driver.PageReq{})
			if err != nil || len(versions.Items) != 1 || nativeReader(t, store).Snapshot().At.String() != "1" {
				t.Fatalf("replay published another revision: %+v, %v", versions, err)
			}
			after, _ := json.Marshal(cs)
			if string(before) != string(after) {
				t.Fatal("native pipeline changed caller input")
			}
		})
	}
}

func TestPipelineRefusesUndeclaredNativePatch(t *testing.T) {
	h, err := New(Config{Natives: []sdk.Native{conformance.Native(conformance.Options{PatchPath: "fields.private"})}, Bindings: fixtureBindings()})
	if err != nil {
		t.Fatal(err)
	}
	w, store := nativeWriter(t, h)
	_, err = w.Apply(context.Background(), nativePrincipal, nativeCreate("hello"), nativeMeta)
	var failure *model.Error
	if !errors.As(err, &failure) || failure.Data.Code != "subscriber_unavailable" {
		t.Fatalf("undeclared field accepted: %v", err)
	}
	requireUnpublished(t, store)
	if _, err := h.Invoke(context.Background(), "x.fixture.undeclared", json.RawMessage(`{"id":"pl-native"}`)); code(err) != ErrMethod {
		t.Fatalf("undeclared method accepted: %v", err)
	}
}

func TestPipelineNativeValidatorRefusalPublishesNothing(t *testing.T) {
	h, err := New(Config{Natives: []sdk.Native{conformance.Native(conformance.Options{})}, Bindings: fixtureBindings()})
	if err != nil {
		t.Fatal(err)
	}
	w, store := nativeWriter(t, h)
	_, err = w.Apply(context.Background(), nativePrincipal, nativeCreate(""), nativeMeta)
	var failure *model.Error
	if !errors.As(err, &failure) || failure.Data.Code != "ontology_guard_failed" {
		t.Fatalf("native validator did not refuse empty normalized title: %v", err)
	}
	requireUnpublished(t, store)
}

func TestPipelineNativeWildcardDataflow(t *testing.T) {
	producer := orderNative("zulu", "fields.source", "fields.title")
	consumer := orderNative("alpha", "fields.result", "fields.*")
	for _, item := range []struct {
		native    *sdk.Native
		read, own string
	}{{&producer, "title", "source"}, {&consumer, "source", "result"}} {
		read, own := item.read, item.own
		item.native.New = func(pluginapi.Host) (sdk.Registration, error) {
			return sdk.Registration{Mutators: map[string]pluginapi.Mutator{"mutate": sdk.MutateFunc(func(_ context.Context, c pluginapi.ProposedChange, _ pluginapi.SnapshotReader) ([]pluginapi.Patch, error) {
				var body struct{ Fields map[string]string }
				if err := json.Unmarshal(c.Changes[0].After, &body); err != nil {
					return nil, err
				}
				if body.Fields[read] == "" {
					return nil, fmt.Errorf("upstream %s not populated", read)
				}
				value, _ := json.Marshal(strings.ToUpper(body.Fields[read]))
				return []pluginapi.Patch{{Resource: c.Changes[0].Resource, Paths: map[string]json.RawMessage{"fields." + own: value}}}, nil
			})}}, nil
		}
	}
	h, err := New(Config{Natives: []sdk.Native{consumer, producer}, Bindings: []Binding{
		orderBinding("alpha", "fields.result", "fields.*"), orderBinding("zulu", "fields.source", "fields.title"),
	}})
	if err != nil {
		t.Fatal(err)
	}
	w, store := nativeWriter(t, h)
	commit, err := w.Apply(context.Background(), nativePrincipal, nativeCreate("hello"), nativeMeta)
	if err != nil {
		t.Fatal(err)
	}
	body, err := pipeline.DecodeDocument(commit.Results[0].Record)
	if err != nil || body.Fields["source"] != "HELLO" || body.Fields["result"] != "HELLO" {
		t.Fatalf("wildcard dataflow lost upstream patch: %+v, %v", body, err)
	}
	log, err := nativeReader(t, store).Changesets(context.Background(), nil, 10)
	if err != nil || len(log) != 1 || !reflect.DeepEqual(log[0].Mutators, []string{"zulu", "alpha"}) {
		t.Fatalf("pipeline reordered wildcard reader before owner: %+v, %v", log, err)
	}
}
