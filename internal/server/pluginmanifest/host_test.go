package pluginmanifest_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/server/pluginhost"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/pluginapi"
	sdk "github.com/buildbetter-app/plugs/sdk/go"
)

func bindings() []pluginhost.Binding {
	return []pluginhost.Binding{
		{ID: "normalize-title", Alias: "normalize", Instance: "default", Subscriber: "title", Phase: pluginapi.PhaseMutate, Reads: []string{"fields.title"}, Owns: []string{"fields.normalized"}, MaxPointReads: 2, MaxQueries: 1},
		{ID: "normalize-check", Alias: "normalize", Instance: "default", Subscriber: "check", Phase: pluginapi.PhaseValidate, Reads: []string{"fields.normalized"}, MaxPointReads: 1, MaxQueries: 1},
	}
}

func TestSourceManifestNativeHostPipelineCommitsOnlyDeclaredField(t *testing.T) {
	for _, undeclared := range []bool{false, true} {
		t.Run(fmt.Sprintf("undeclaredPatch=%t", undeclared), func(t *testing.T) {
			ctx := context.Background()
			bundle := load(t, checkout(t))
			implementations := natives(t)
			constructor := implementations[0].New
			var probed atomic.Bool
			implementations[0].New = func(host pluginapi.Host) (sdk.Registration, error) {
				registration, err := constructor(host)
				if err != nil {
					return registration, err
				}
				original := registration.Mutators["title"]
				registration.Mutators["title"] = sdk.MutateFunc(func(ctx context.Context, c pluginapi.ProposedChange, reader pluginapi.SnapshotReader) ([]pluginapi.Patch, error) {
					data, err := json.Marshal(c)
					if err != nil || strings.Contains(string(data), "PRIVATE-SENTINEL") {
						return nil, fmt.Errorf("undeclared proposal fields leaked: %v", err)
					}
					for i := 0; i < 2; i++ {
						if _, err := reader.Get(ctx, []pluginapi.Ref{pluginapi.RefByID("pl-absent", "")}); err != nil {
							return nil, err
						}
					}
					if _, err := reader.Get(ctx, []pluginapi.Ref{pluginapi.RefByID("pl-absent", "")}); !hasHostCode(err, pluginhost.ErrBudget) {
						return nil, fmt.Errorf("point read budget escaped: %v", err)
					}
					page, err := reader.Query(ctx, pluginapi.Query{})
					if err != nil || page.AsOf.String() != c.BaseRevision.String() {
						return nil, fmt.Errorf("reader was not pinned: %v", err)
					}
					if _, err := reader.Query(ctx, pluginapi.Query{}); !hasHostCode(err, pluginhost.ErrBudget) {
						return nil, fmt.Errorf("query budget escaped: %v", err)
					}
					if _, err := reader.Query(ctx, pluginapi.Query{Filter: map[string]any{"fields.description": "x"}}); !hasHostCode(err, pluginhost.ErrRead) {
						return nil, fmt.Errorf("read field grant escaped: %v", err)
					}
					if err := host.Call(ctx, "tx.apply", map[string]any{}, nil); !hasHostCode(err, pluginhost.ErrHotWrite) {
						return nil, fmt.Errorf("hot write escaped: %v", err)
					}
					if _, err := host.HTTP().Get("https://denied.example"); !hasHostCode(err, pluginhost.ErrHotEgress) {
						return nil, fmt.Errorf("hot egress escaped: %v", err)
					}
					patches, err := original.Mutate(ctx, c, reader)
					if undeclared && err == nil {
						patches[0].Paths = map[string]json.RawMessage{"fields.description": json.RawMessage(`"unexpected"`)}
					}
					probed.Store(true)
					return patches, err
				})
				return registration, nil
			}
			descriptors, err := bundle.Natives(ctx, implementations)
			if err != nil {
				t.Fatal(err)
			}
			host, err := pluginhost.New(pluginhost.Config{Natives: descriptors, Bindings: bindings(), Dispatcher: &dispatch{}})
			if err != nil {
				t.Fatal(err)
			}
			store := memory.New()
			t.Cleanup(func() { _ = store.Close() })
			lease, err := store.Lease(ctx, "manifest-fixture", driver.LeaseTTL)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = lease.Release(context.Background()) })
			doc := ontology.DefaultDocument()
			doc.Types["issue"].Fields["normalized"] = ontology.Field{Type: "string", Owner: "normalize-title"}
			snapshot, problems := ontology.Compile(doc, nil, ontology.PluginCatalog{"normalize-title": true})
			if len(problems) != 0 {
				t.Fatal(problems)
			}
			hook := service.NewOntology(func(string) ontology.Effective { return snapshot.Effective("") })
			writer := pipeline.New(store, lease, pipeline.WithOntology(hook), pipeline.WithMutators(host.Mutators()...), pipeline.WithValidators(host.Validators()...), pipeline.WithIDGenerator(func() (string, error) { return "pl-manifest", nil }))
			cs := model.ChangeSet{Ops: []model.Op{{Kind: model.OpCreate, Create: &model.CreateOp{Type: "issue", Fields: map[string]any{"title": "hello", "description": "PRIVATE-SENTINEL"}}}}}
			commit, err := writer.Apply(ctx, model.PrincipalRef{Kind: "user", ID: "fixture"}, cs, model.WriteMeta{Method: "tx.apply", IdempotencyKey: "00000000000000000000000000000001"})
			if !probed.Load() {
				t.Fatal("actual manifest-backed native callback did not run")
			}
			metadata, metaErr := store.Meta(ctx)
			if metaErr != nil {
				t.Fatal(metaErr)
			}
			if undeclared {
				var failure *model.Error
				if !errors.As(err, &failure) || failure.Data.Code != "subscriber_unavailable" || metadata.Head.String() != "0" {
					t.Fatalf("undeclared native patch published: %v head=%s", err, metadata.Head.String())
				}
				return
			}
			if err != nil || commit.Revision.String() != "1" || len(commit.Results) != 1 {
				t.Fatalf("manifest native did not commit: %+v %v", commit, err)
			}
			reader, err := store.Read(ctx, driver.View{Incarnation: metadata.Incarnation})
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Close()
			records, err := reader.Get(ctx, []model.Ref{commit.Results[0].Ref})
			if err != nil || len(records) != 1 {
				t.Fatalf("committed record missing: %v", err)
			}
			body, err := pipeline.DecodeDocument(records[0].Body)
			if err != nil || body.Fields["normalized"] != "HELLO" || body.Fields["description"] != "PRIVATE-SENTINEL" || commit.Results[0].SetBy["fields.normalized"] != "normalize-title" {
				t.Fatalf("native field/provenance or input isolation failed: %+v %v", body, err)
			}
			t.Logf("source %s -> actual native host -> writer -> stored revision %s; budgets/field masking/hot restrictions executed", bundle.Commit(), commit.Revision.String())
		})
	}
}

func hasHostCode(err error, code string) bool {
	var failure *pluginhost.Error
	return errors.As(err, &failure) && failure.Code == code
}

func TestHostBindingsCannotWidenLoadedSourceGrants(t *testing.T) {
	bundle := load(t, checkout(t))
	for _, name := range []string{"read", "own", "points", "queries"} {
		t.Run(name, func(t *testing.T) {
			descriptors, err := bundle.Natives(context.Background(), natives(t))
			if err != nil {
				t.Fatal(err)
			}
			bound := bindings()
			switch name {
			case "read":
				bound[0].Reads = []string{"fields.description"}
			case "own":
				bound[0].Owns = []string{"fields.description"}
			case "points":
				bound[0].MaxPointReads = 3
			case "queries":
				bound[0].MaxQueries = 2
			}
			_, err = pluginhost.New(pluginhost.Config{Natives: descriptors, Bindings: bound})
			hostCode(t, err, pluginhost.ErrManifest)
		})
	}
}

type dispatch struct{ principals []pluginapi.PrincipalRef }

func (*dispatch) Effect(method string) (pluginhost.Effect, bool) {
	switch method {
	case "bead.get", "bead.ready":
		return pluginhost.Read, true
	case "tx.apply":
		return pluginhost.Write, true
	}
	return "", false
}
func (d *dispatch) Call(_ context.Context, principal pluginapi.PrincipalRef, _ string, _ json.RawMessage) (json.RawMessage, error) {
	d.principals = append(d.principals, principal)
	return json.RawMessage(`{"allowed":true}`), nil
}

func TestLoadedNativeRuntimeCapabilitiesAndInstanceIsolation(t *testing.T) {
	ctx := context.Background()
	bundle := load(t, checkout(t))
	newHost := func(instance string, dispatcher *dispatch) *pluginhost.Host {
		implementations := natives(t)
		implementations[1].Instance = instance
		descriptors, err := bundle.Natives(ctx, implementations)
		if err != nil {
			t.Fatal(err)
		}
		host, err := pluginhost.New(pluginhost.Config{Natives: descriptors, Dispatcher: dispatcher})
		if err != nil {
			t.Fatal(err)
		}
		return host
	}
	dispatcher := &dispatch{}
	a, b := newHost("default", dispatcher), newHost("worker", &dispatch{})
	for _, item := range []struct {
		host *pluginhost.Host
		want string
	}{{a, `{"count":1}`}, {b, `{"count":1}`}, {a, `{"count":2}`}} {
		result, err := item.host.Invoke(ctx, "x.echo.access", json.RawMessage(`{"id":"pl-a","action":"state"}`))
		if err != nil || string(result) != item.want {
			t.Fatalf("instance state crossed namespaces: %s %v", result, err)
		}
	}
	result, err := a.Invoke(ctx, "x.echo.echo", json.RawMessage(`{"id":"pl-a","value":9007199254740993}`))
	if err != nil || string(result) != `{"id":"pl-a","value":9007199254740993}` {
		t.Fatalf("actual extension method failed: %s %v", result, err)
	}
	result, err = a.Invoke(ctx, "x.echo.access", json.RawMessage(`{"id":"pl-a","action":"call","method":"bead.get"}`))
	if err != nil || string(result) != `{"allowed":true}` || len(dispatcher.principals) != 1 || dispatcher.principals[0].ID != "app.example.echo/default" || dispatcher.principals[0].Kind != "plugin" {
		t.Fatalf("declared API grant or principal changed: %s %v %+v", result, err, dispatcher.principals)
	}
	_, err = a.Invoke(ctx, "x.echo.access", json.RawMessage(`{"id":"pl-a","action":"call","method":"bead.ready"}`))
	hostCode(t, err, pluginhost.ErrMethod)
	// A declared-method refusal disposes the callback guest. Exercise the
	// remaining capability boundaries on fresh guests rather than reusing the
	// disposed member for the same principal.
	a = newHost("default", dispatcher)
	_, err = a.Invoke(ctx, "x.echo.access", json.RawMessage(`{"id":"pl-a","action":"egress","url":"https://denied.example"}`))
	hostCode(t, err, pluginhost.ErrEgress)
	a = newHost("default", dispatcher)
	_, err = a.Invoke(ctx, "x.echo.undeclared", json.RawMessage(`{"id":"pl-a"}`))
	hostCode(t, err, pluginhost.ErrMethod)
	if len(dispatcher.principals) != 1 {
		t.Fatal("undeclared API call dispatched")
	}
}
