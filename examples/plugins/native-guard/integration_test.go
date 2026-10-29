package nativeguard_test

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	guard "github.com/buildbetter-app/plugs/examples/plugins/native-guard"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/server/pluginhost"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/pluginapi"
	sdk "github.com/buildbetter-app/plugs/sdk/go"
)

//go:embed ontology.json
var ontologyJSON []byte

// schemaHook is test composition glue for the real ontology compiler and
// evaluator. It contains no schema/ownership rules of its own. This example
// supports ordinary create/update/delete, without lifecycle or graph guards.
type schemaHook struct{ effective ontology.Effective }

func exampleSchema(t *testing.T, installed bool) schemaHook {
	t.Helper()
	overlay, err := ontology.Decode(ontologyJSON)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, problems := ontology.Compile(ontology.DefaultDocument(), map[string]ontology.Document{"example": overlay}, ontology.PluginCatalog{guard.Owner: installed})
	if len(problems) != 0 {
		t.Fatal(problems)
	}
	return schemaHook{snapshot.Effective("example")}
}

func beadState(raw json.RawMessage) (*ontology.BeadState, error) {
	if len(raw) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil, nil
	}
	doc, err := pipeline.DecodeDocument(raw)
	if err != nil {
		return nil, err
	}
	status, _ := doc.Fields["status"].(string)
	delete(doc.Fields, "status")
	return &ontology.BeadState{ID: doc.ID, Project: doc.Project, Type: doc.Type, Status: status, Fields: doc.Fields}, nil
}

func schemaError(violations []ontology.Violation) error {
	if len(violations) == 0 {
		return nil
	}
	return &model.Error{Message: violations[0].Message, Data: model.ErrorData{Code: violations[0].Code}}
}

func (s schemaHook) Normalize(ctx context.Context, c port.ProposedChange, _ port.SnapshotReader) ([]port.Patch, error) {
	patches := []port.Patch{}
	seen := map[model.ResourceRef]bool{}
	for _, change := range c.Changes {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		before, err := beadState(change.Before)
		if err != nil {
			return nil, err
		}
		after, err := beadState(change.After)
		if err != nil {
			return nil, err
		}
		if before != nil || after == nil || seen[change.Resource] {
			continue
		}
		seen[change.Resource] = true
		defaults, violations := ontology.Defaults(s.effective, *after)
		if err := schemaError(violations); err != nil {
			return nil, err
		}
		paths := map[string]json.RawMessage{}
		if defaults.Status != after.Status {
			paths["fields.status"], _ = json.Marshal(defaults.Status)
		}
		for name, value := range defaults.Fields {
			if old, exists := after.Fields[name]; !exists || !reflect.DeepEqual(old, value) {
				paths["fields."+name], _ = json.Marshal(value)
			}
		}
		patches = append(patches, port.Patch{Resource: change.Resource, Paths: paths})
	}
	return patches, nil
}

func (s schemaHook) Validate(ctx context.Context, c port.ProposedChange, _ port.SnapshotReader) error {
	for _, change := range c.Changes {
		if err := ctx.Err(); err != nil {
			return err
		}
		before, err := beadState(change.Before)
		if err != nil {
			return err
		}
		after, err := beadState(change.After)
		if err != nil {
			return err
		}
		if err := schemaError(ontology.Validate(s.effective, before, after, c.Meta.Method, ontology.GuardFacts{SetBy: change.SetBy})); err != nil {
			return err
		}
	}
	return nil
}

func bindings() []pluginhost.Binding {
	return []pluginhost.Binding{
		{ID: guard.Owner, Alias: guard.Alias, Instance: "default", Subscriber: "enrich", Phase: pluginapi.PhaseMutate,
			Reads: []string{"fields.title"}, Owns: []string{"fields." + guard.NormalizedField, "fields." + guard.WordsField}, MaxPointReads: 1, MaxQueries: 1, Timeout: 250 * time.Millisecond},
		{ID: "nativeguard-title", Alias: guard.Alias, Instance: "default", Subscriber: "title", Phase: pluginapi.PhaseValidate,
			Reads: []string{"fields." + guard.NormalizedField}, MaxPointReads: 1, MaxQueries: 1, Timeout: 500 * time.Millisecond},
		{ID: "nativeguard-observe", Alias: guard.Alias, Instance: "default", Subscriber: "observe", Phase: pluginapi.PhasePost,
			Reads: []string{"fields." + guard.NormalizedField, "fields." + guard.WordsField}, MaxPointReads: guard.MaxResources, MaxQueries: 1, Timeout: 500 * time.Millisecond},
	}
}

type counts struct{ mutate, validate, post atomic.Int32 }

func (c *counts) check(t *testing.T, mutate, validate, post int32) {
	t.Helper()
	if got := [3]int32{c.mutate.Load(), c.validate.Load(), c.post.Load()}; got != [3]int32{mutate, validate, post} {
		t.Fatalf("callback counts mutate/validate/post = %v; want %v", got, [3]int32{mutate, validate, post})
	}
	t.Logf("callbacks mutate=%d validate=%d post=%d", mutate, validate, post)
}

type fixture struct {
	host   *pluginhost.Host
	store  *memory.Store
	lease  driver.ActiveLease
	schema schemaHook
	calls  *counts
}

// decorate instruments or deliberately corrupts only this example's returned
// registration. The production plugin has no test/probe/patch-path options.
func setup(t *testing.T, observe guard.Observer, decorate func(*sdk.Registration), configure func([]pluginhost.Binding)) *fixture {
	t.Helper()
	native, err := guard.Native("default", observe)
	if err != nil {
		t.Fatal(err)
	}
	calls := &counts{}
	constructor := native.New
	native.New = func(host pluginapi.Host) (sdk.Registration, error) {
		registration, err := constructor(host)
		if err != nil {
			return registration, err
		}
		mutator, validator, post := registration.Mutators["enrich"], registration.Validators["title"], registration.Posts["observe"]
		registration.Mutators["enrich"] = sdk.MutateFunc(func(ctx context.Context, c pluginapi.ProposedChange, r pluginapi.SnapshotReader) ([]pluginapi.Patch, error) {
			calls.mutate.Add(1)
			return mutator.Mutate(ctx, c, r)
		})
		registration.Validators["title"] = sdk.ValidateFunc(func(ctx context.Context, c pluginapi.ProposedChange, r pluginapi.SnapshotReader) (pluginapi.Verdict, error) {
			calls.validate.Add(1)
			return validator.Validate(ctx, c, r)
		})
		registration.Posts["observe"] = sdk.AfterFunc(func(ctx context.Context, c pluginapi.CommittedChange, r pluginapi.SnapshotReader) error {
			calls.post.Add(1)
			return post.After(ctx, c, r)
		})
		if decorate != nil {
			decorate(&registration)
		}
		return registration, nil
	}
	grants := bindings()
	if configure != nil {
		configure(grants)
	}
	host, err := pluginhost.New(pluginhost.Config{Natives: []sdk.Native{native}, Bindings: grants})
	if err != nil {
		t.Fatal(err)
	}
	store := memory.New()
	t.Cleanup(func() { _ = store.Close() })
	lease, err := store.Lease(context.Background(), "nativeguard-example", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lease.Release(context.Background()) })
	return &fixture{host: host, store: store, lease: lease, schema: exampleSchema(t, true), calls: calls}
}

func (f *fixture) writer() *pipeline.Writer {
	var ids atomic.Int32
	return pipeline.New(f.store, f.lease, pipeline.WithOntology(f.schema),
		pipeline.WithMutators(f.host.Mutators()...), pipeline.WithValidators(f.host.Validators()...), pipeline.WithPostSubscribers(f.host.Posts()...),
		pipeline.WithIDGenerator(func() (string, error) { return fmt.Sprintf("pl-guard-%d", ids.Add(1)), nil }))
}

var principal = model.PrincipalRef{Kind: "user", ID: "synthetic-user"}

func meta(key int) model.WriteMeta {
	return model.WriteMeta{Method: "tx.apply", Project: "example", IdempotencyKey: fmt.Sprintf("%032x", key)}
}

func create(title string) model.ChangeSet {
	return model.ChangeSet{Ops: []model.Op{{Kind: model.OpCreate, Create: &model.CreateOp{Type: "issue", Fields: map[string]any{"title": title, "description": "synthetic private description"}}}}}
}

func update(ref model.Ref, title string) model.ChangeSet {
	return model.ChangeSet{Ops: []model.Op{{Kind: model.OpPatch, Patch: &model.PatchOp{Target: ref, Fields: map[string]any{"title": title}}}}}
}

func apply(t *testing.T, w *pipeline.Writer, cs model.ChangeSet, key int) model.Commit {
	t.Helper()
	c, err := w.Apply(context.Background(), principal, cs, meta(key))
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func wire(t *testing.T, value any) string {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func read(t *testing.T, f *fixture) driver.Reader {
	t.Helper()
	metadata, err := f.store.Meta(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	r, err := f.store.Read(context.Background(), driver.View{Incarnation: metadata.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Close() })
	return r
}

func graphSize(t *testing.T, f *fixture, head string, records, versions, changesets int) {
	t.Helper()
	r := read(t, f)
	page, err := r.Query(context.Background(), driver.Query{})
	if err != nil {
		t.Fatal(err)
	}
	log, err := r.Changesets(context.Background(), nil, 100)
	if err != nil {
		t.Fatal(err)
	}
	v, err := r.Versions(context.Background(), model.RefByID("pl-guard-1", ""), driver.PageReq{})
	if err != nil {
		t.Fatal(err)
	}
	if r.Snapshot().At.String() != head || len(page.Items) != records || len(v.Items) != versions || len(log) != changesets {
		t.Fatalf("head/records/versions/log = %s/%d/%d/%d; want %s/%d/%d/%d", r.Snapshot().At, len(page.Items), len(v.Items), len(log), head, records, versions, changesets)
	}
}

func requireCode(t *testing.T, err error, code model.ErrorCode) {
	t.Helper()
	var failure *model.Error
	if !errors.As(err, &failure) || failure.Data.Code != code {
		t.Fatalf("error = %v; want %s", err, code)
	}
}

func TestCreateUpdateNoopAndDurableReplay(t *testing.T) {
	observed := []guard.Observation{}
	f := setup(t, func(_ context.Context, observation guard.Observation) error {
		observed = append(observed, observation)
		return nil
	}, nil, nil)
	w := f.writer()
	cs := create("  Ship\t the\n example  ")
	before := wire(t, cs)
	first := apply(t, w, cs, 1)
	if before != wire(t, cs) {
		t.Fatal("caller input changed")
	}
	if first.Revision.String() != "1" || len(first.Warnings) != 0 || len(first.Results) != 1 {
		t.Fatalf("create = %+v", first)
	}
	result := first.Results[0]
	doc, err := pipeline.DecodeDocument(result.Record)
	if err != nil || doc.Fields["title"] != "  Ship\t the\n example  " || doc.Fields[guard.NormalizedField] != "Ship the example" || doc.Fields[guard.WordsField] != json.Number("3") {
		t.Fatalf("derived fields = %+v; %v", doc, err)
	}
	if result.SetBy["fields."+guard.NormalizedField] != guard.Owner || result.SetBy["fields."+guard.WordsField] != guard.Owner || result.SetBy["fields.title"] != "request" {
		t.Fatalf("provenance = %+v", result.SetBy)
	}
	second := apply(t, w, update(result.Ref, "  Review\u2003it  "), 2)
	if second.Revision.String() != "2" || len(second.Warnings) != 0 || len(observed) != 2 {
		t.Fatalf("update = %+v; post calls = %d", second, len(observed))
	}
	item := observed[1].Items[0]
	if observed[1].Revision.String() != "2" || item.Revision.String() != "2" || item.Title != "Review it" || item.Words != 2 || item.Resource.Ref.ID != result.Ref.ID {
		t.Fatalf("committed observation = %+v", observed[1])
	}
	stored, err := read(t, f).Get(context.Background(), []model.Ref{result.Ref})
	if err != nil || len(stored) != 1 || string(stored[0].Body) != string(second.Results[0].Record) {
		t.Fatalf("stored result = %+v; %v", stored, err)
	}
	log, err := read(t, f).Changesets(context.Background(), nil, 100)
	if err != nil || len(log) != 2 || !reflect.DeepEqual(log[1].Mutators, []string{guard.Owner}) {
		t.Fatalf("persisted provenance = %+v; %v", log, err)
	}
	receipt, err := read(t, f).IdempotencyByKey(context.Background(), "user:synthetic-user", "", "tx.apply", meta(2).IdempotencyKey)
	if err != nil || receipt == nil || len(receipt.Commit.Results) != 1 || receipt.Commit.Results[0].SetBy["fields."+guard.WordsField] != guard.Owner {
		t.Fatalf("durable field provenance = %+v; %v", receipt, err)
	}
	noopCS := update(result.Ref, "  Review\u2003it  ")
	noop := apply(t, w, noopCS, 3)
	if noop.Revision.String() != "2" || noop.Results[0].Revision.String() != "2" || string(noop.Results[0].Record) != string(second.Results[0].Record) || len(noop.Warnings) != 0 {
		t.Fatalf("same-value result = %+v", noop)
	}
	f.calls.check(t, 3, 3, 2) // a new no-op runs pre admission, suppressing post
	graphSize(t, f, "2", 1, 2, 2)
	// A new Writer has no in-memory receipt cache. Both graph and no-op replay
	// must read the durable memory-driver receipts before running subscribers.
	fresh := f.writer()
	for _, replay := range []struct {
		cs   model.ChangeSet
		key  int
		want model.Commit
	}{{cs, 1, first}, {noopCS, 3, noop}} {
		if got := apply(t, fresh, replay.cs, replay.key); wire(t, got) != wire(t, replay.want) {
			t.Fatalf("replay differs: %+v", got)
		}
	}
	f.calls.check(t, 3, 3, 2)
	graphSize(t, f, "2", 1, 2, 2)
	t.Logf("provenance normalized/words=%s; committed observer revisions=1,2; no-op/replay retained head=2", guard.Owner)
}

func TestValidatorRefusalRollsBack(t *testing.T) {
	for _, title := range []string{" \t\u2003\n ", strings.Repeat("界", 81)} {
		t.Run(fmt.Sprintf("runes=%d", len([]rune(title))), func(t *testing.T) {
			f := setup(t, nil, nil, nil)
			w := f.writer()
			_, err := w.Apply(context.Background(), principal, create(title), meta(1))
			requireCode(t, err, "ontology_guard_failed")
			f.calls.check(t, 1, 1, 0)
			graphSize(t, f, "0", 0, 0, 0)
			first := apply(t, w, create("Valid title"), 2)
			_, err = w.Apply(context.Background(), principal, update(first.Results[0].Ref, title), meta(3))
			requireCode(t, err, "ontology_guard_failed")
			f.calls.check(t, 3, 3, 1)
			// The refused first create consumed a synthetic ID, so inspect the
			// successful record directly rather than assuming a particular ID.
			r := read(t, f)
			records, getErr := r.Get(context.Background(), []model.Ref{first.Results[0].Ref})
			versions, versionErr := r.Versions(context.Background(), first.Results[0].Ref, driver.PageReq{})
			log, logErr := r.Changesets(context.Background(), nil, 100)
			if getErr != nil || versionErr != nil || logErr != nil || len(records) != 1 || string(records[0].Body) != string(first.Results[0].Record) || len(versions.Items) != 1 || len(log) != 1 || r.Snapshot().At.String() != "1" {
				t.Fatal("validator refusal published partial state", getErr, versionErr, logErr)
			}
		})
	}
}

func TestPostWarningIsCommittedAndDurablyReplayed(t *testing.T) {
	var observed atomic.Bool
	f := setup(t, func(_ context.Context, observation guard.Observation) error {
		if observation.Revision.String() == "1" && len(observation.Items) == 1 && observation.Items[0].Title == "Committed title" && observation.Items[0].Revision.String() == "1" {
			observed.Store(true)
		}
		return errors.New("synthetic observer failure")
	}, nil, nil)
	cs := create(" Committed\t title ")
	commit := apply(t, f.writer(), cs, 1)
	if !observed.Load() || commit.Revision.String() != "1" || len(commit.Warnings) != 1 || commit.Warnings[0].Code != "post_subscriber_failed" {
		t.Fatalf("post did not observe committed state or lost warning: %+v", commit)
	}
	if replay := apply(t, f.writer(), cs, 1); wire(t, replay) != wire(t, commit) {
		t.Fatalf("durable warning replay differs: %+v", replay)
	}
	f.calls.check(t, 1, 1, 1)
	graphSize(t, f, "1", 1, 1, 1)
	receipt, err := read(t, f).IdempotencyByKey(context.Background(), "user:synthetic-user", "", "tx.apply", meta(1).IdempotencyKey)
	if err != nil || receipt == nil || receipt.ResponsePending || wire(t, receipt.Commit) != wire(t, commit) {
		t.Fatalf("response was not durably finalized: %+v; %v", receipt, err)
	}
}

func TestPendingPostReceiptCannotReplayAsSuccess(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{}, 1)
	t.Cleanup(func() {
		select {
		case release <- struct{}{}:
		default:
		}
	})
	f := setup(t, func(ctx context.Context, _ guard.Observation) error {
		close(entered)
		select {
		case <-release:
			return errors.New("synthetic pending observer failure")
		case <-ctx.Done():
			return ctx.Err()
		}
	}, nil, nil)
	cs := create("Pending response")
	type outcome struct {
		commit model.Commit
		err    error
	}
	first := make(chan outcome, 1)
	go func() {
		c, err := f.writer().Apply(context.Background(), principal, cs, meta(1))
		first <- outcome{c, err}
	}()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("committed post callback did not start")
	}
	receipt, err := read(t, f).IdempotencyByKey(context.Background(), "user:synthetic-user", "", "tx.apply", meta(1).IdempotencyKey)
	if err != nil || receipt == nil || !receipt.ResponsePending {
		t.Fatalf("in-flight response was not durably pending: %+v; %v", receipt, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	if _, err := f.writer().Apply(ctx, principal, cs, meta(1)); err == nil {
		t.Fatal("unfinalized pending receipt replayed as success")
	}
	f.calls.check(t, 1, 1, 1)
	graphSize(t, f, "1", 1, 1, 1)
	release <- struct{}{}
	var committed model.Commit
	select {
	case result := <-first:
		if result.err != nil || len(result.commit.Warnings) != 1 || result.commit.Warnings[0].Code != "post_subscriber_failed" {
			t.Fatalf("known post outcome was not finalized: %+v; %v", result.commit, result.err)
		}
		committed = result.commit
	case <-time.After(2 * time.Second):
		t.Fatal("original writer did not finalize its response")
	}
	if replay := apply(t, f.writer(), cs, 1); wire(t, replay) != wire(t, committed) {
		t.Fatal("final replay lost the original post warning")
	}
	f.calls.check(t, 1, 1, 1)
}

func TestRealSchemaEnforcesFieldOwnership(t *testing.T) {
	f := setup(t, nil, nil, nil)
	schema, violations := f.schema.effective.Schema("issue", "")
	if len(violations) != 0 {
		t.Fatal(violations)
	}
	properties := schema["properties"].(map[string]any)["fields"].(map[string]any)["properties"].(map[string]any)
	for _, field := range []string{guard.NormalizedField, guard.WordsField} {
		property := properties[field].(map[string]any)
		if property["readOnly"] != true || property["x-plug-owner"] != guard.Owner {
			t.Fatalf("schema lacks field owner: %+v", property)
		}
	}
	// Removing the actual mutator must not turn a caller-supplied derived value
	// into a valid request. The real ontology evaluator checks provenance.
	w := pipeline.New(f.store, f.lease, pipeline.WithOntology(f.schema), pipeline.WithIDGenerator(func() (string, error) { return "pl-guard-1", nil }))
	cs := create("Forged derived fields")
	cs.Ops[0].Create.Fields[guard.NormalizedField] = "Forged derived fields"
	cs.Ops[0].Create.Fields[guard.WordsField] = 3
	_, err := w.Apply(context.Background(), principal, cs, meta(1))
	requireCode(t, err, "field_owned")
	graphSize(t, f, "0", 0, 0, 0)
	first := apply(t, f.writer(), create("Installed owner"), 2)
	degraded := pipeline.New(f.store, f.lease, pipeline.WithOntology(exampleSchema(t, false)))
	_, err = degraded.Apply(context.Background(), principal, update(first.Results[0].Ref, "Unavailable owner"), meta(3))
	requireCode(t, err, "subscriber_unavailable")
	graphSize(t, f, "1", 1, 1, 1)
}

func TestHostRefusesOutOfScopeAndOverlappingPatches(t *testing.T) {
	for _, mode := range []string{"unowned-field", "overlap", "changed-target", "narrowed-ownership"} {
		t.Run(mode, func(t *testing.T) {
			decorate := func(registration *sdk.Registration) {
				original := registration.Mutators["enrich"]
				registration.Mutators["enrich"] = sdk.MutateFunc(func(ctx context.Context, c pluginapi.ProposedChange, r pluginapi.SnapshotReader) ([]pluginapi.Patch, error) {
					patches, err := original.Mutate(ctx, c, r)
					if err != nil {
						return nil, err
					}
					switch mode {
					case "unowned-field":
						patches[0].Paths["fields.description"] = json.RawMessage(`"outside the grant"`)
					case "overlap":
						patches = append(patches, patches[0])
					case "changed-target":
						// A callback's private input edit cannot redefine the original
						// write set used by the repaired host's output validation.
						c.Changes[0].Resource.Ref = pluginapi.RefByID("pl-unproposed", "")
						patches[0].Resource = c.Changes[0].Resource
					}
					return patches, nil
				})
			}
			configure := func(grants []pluginhost.Binding) {
				if mode == "narrowed-ownership" {
					grants[0].Owns = []string{"fields." + guard.NormalizedField}
				}
			}
			f := setup(t, nil, decorate, configure)
			_, err := f.writer().Apply(context.Background(), principal, create("Scoped title"), meta(1))
			requireCode(t, err, "subscriber_unavailable")
			f.calls.check(t, 1, 0, 0)
			graphSize(t, f, "0", 0, 0, 0)
		})
	}
}

func TestCallerCallbackAndReturnedCopiesAreIsolated(t *testing.T) {
	var retained []pluginapi.Patch
	var captured pluginapi.ProposedChange
	var checkedMask atomic.Bool
	f := setup(t, func(_ context.Context, observation guard.Observation) error {
		// The observer owns this DTO and may retain or change it.
		observation.Items[0].Title = "observer changed its copy"
		return nil
	}, func(registration *sdk.Registration) {
		original := registration.Mutators["enrich"]
		registration.Mutators["enrich"] = sdk.MutateFunc(func(ctx context.Context, c pluginapi.ProposedChange, r pluginapi.SnapshotReader) ([]pluginapi.Patch, error) {
			if bytes.Contains(c.Changes[0].After, []byte("synthetic private description")) || bytes.Contains(c.Changes[0].Op.Payload, []byte("synthetic private description")) {
				return nil, errors.New("mutator grant exposed description")
			}
			patches, err := original.Mutate(ctx, c, r)
			retained, captured = patches, c
			// Full callback input isolation includes nested principal values,
			// resources, operation payload bytes, images and provenance maps.
			c.Principal.Caveats["nested"].(map[string]any)["label"] = "callback copy"
			c.Changes[0].Resource.Ref = pluginapi.RefByID("pl-callback-copy", "")
			c.Changes[0].After[0] = '['
			c.Changes[0].Op.Payload[0] = '['
			c.Changes[0].SetBy["fields.title"] = "callback copy"
			return patches, err
		})
		post := registration.Posts["observe"]
		registration.Posts["observe"] = sdk.AfterFunc(func(ctx context.Context, c pluginapi.CommittedChange, r pluginapi.SnapshotReader) error {
			if bytes.Contains(c.Changes[0].After, []byte("synthetic private description")) || bytes.Contains(c.Changes[0].After, []byte(`"title"`)) {
				return errors.New("post grant exposed request fields")
			}
			records, err := r.Get(ctx, []pluginapi.Ref{c.Changes[0].Resource.Ref})
			if err != nil || len(records) != 1 {
				return fmt.Errorf("post read: %v", err)
			}
			var body struct{ Fields map[string]any }
			if err := json.Unmarshal(records[0].Body, &body); err != nil || len(body.Fields) != 2 {
				return errors.New("post read was not restricted to its two fields")
			}
			records[0].Body[0] = '[' // the next real post read must be detached
			checkedMask.Store(true)
			return post.After(ctx, c, r)
		})
	}, nil)
	cs := create("  Copy\tboundary  ")
	by := model.PrincipalRef{Kind: "user", ID: principal.ID, Caveats: map[string]any{"nested": map[string]any{"label": "caller"}}}
	beforeCS, beforePrincipal := wire(t, cs), wire(t, by)
	commit, err := f.writer().Apply(context.Background(), by, cs, meta(1))
	if err != nil || len(commit.Warnings) != 0 || !checkedMask.Load() || wire(t, cs) != beforeCS || wire(t, by) != beforePrincipal {
		t.Fatalf("copy isolation failed: %+v; %v", commit, err)
	}
	stable := wire(t, commit)
	retained[0].Paths["fields."+guard.NormalizedField][0] = '['
	retained[0].Paths["fields.description"] = json.RawMessage(`"retained output edit"`)
	captured.Changes[0].SetBy["fields.title"] = "retained input edit"
	commit.Results[0].Record[0] = '['
	commit.Results[0].SetBy["fields.title"] = "returned result edit"
	replay, err := f.writer().Apply(context.Background(), by, cs, meta(1))
	if err != nil || wire(t, replay) != stable {
		t.Fatalf("durable receipt aliased callback/returned values: %+v; %v", replay, err)
	}
	stored, err := read(t, f).Get(context.Background(), []model.Ref{replay.Results[0].Ref})
	if err != nil || len(stored) != 1 || string(stored[0].Body) != string(replay.Results[0].Record) {
		t.Fatalf("stored body aliased returned values: %+v; %v", stored, err)
	}
	f.calls.check(t, 1, 1, 1)
}

func TestAtomicBatchUsesOnePatchPerResource(t *testing.T) {
	f := setup(t, nil, nil, nil)
	cs := create("Intermediate title")
	cs.Ops[0].Create.Key = "new"
	cs.Ops = append(cs.Ops, model.Op{Kind: model.OpPatch, Patch: &model.PatchOp{Target: model.RefByKey("new"), Fields: map[string]any{"title": "  Final\t title "}}})
	commit := apply(t, f.writer(), cs, 1)
	if len(commit.Results) != 2 || len(commit.Warnings) != 0 {
		t.Fatalf("atomic batch = %+v", commit)
	}
	doc, err := pipeline.DecodeDocument(commit.Results[1].Record)
	if err != nil || doc.Fields[guard.NormalizedField] != "Final title" || doc.Fields[guard.WordsField] != json.Number("2") || string(commit.Results[0].Record) != string(commit.Results[1].Record) {
		t.Fatalf("atomic post-image = %+v; %v", doc, err)
	}
	f.calls.check(t, 1, 1, 1)
	graphSize(t, f, "1", 1, 1, 1)
}

func TestUnicodeBoundaryAndResourceCap(t *testing.T) {
	f := setup(t, nil, nil, nil)
	commit := apply(t, f.writer(), create(strings.Repeat("界", guard.MaxTitleRunes)), 1)
	if len(commit.Warnings) != 0 {
		t.Fatal(commit.Warnings)
	}
	f.calls.check(t, 1, 1, 1)
	for _, count := range []int{guard.MaxResources, guard.MaxResources + 1} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			var items atomic.Int32
			f := setup(t, func(_ context.Context, observation guard.Observation) error {
				items.Store(int32(len(observation.Items)))
				return nil
			}, nil, nil)
			cs := model.ChangeSet{}
			for i := 0; i < count; i++ {
				cs.Ops = append(cs.Ops, create(fmt.Sprintf("Title %d", i)).Ops[0])
			}
			commit, err := f.writer().Apply(context.Background(), principal, cs, meta(1))
			if count > guard.MaxResources {
				requireCode(t, err, "subscriber_unavailable")
				graphSize(t, f, "0", 0, 0, 0)
				f.calls.check(t, 1, 0, 0)
			} else if err != nil || len(commit.Warnings) != 0 || items.Load() != int32(count) {
				t.Fatalf("read-budget boundary = %+v; observed=%d; %v", commit, items.Load(), err)
			}
		})
	}
}

func TestManifestGrantsAndUnsupportedScopes(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func([]pluginhost.Binding)
	}{
		{"read-widening", func(b []pluginhost.Binding) { b[1].Reads = append(b[1].Reads, "fields.description") }},
		{"owner-widening", func(b []pluginhost.Binding) { b[0].Owns = append(b[0].Owns, "fields.description") }},
		{"read-budget-widening", func(b []pluginhost.Binding) { b[2].MaxPointReads = guard.MaxResources + 1 }},
		{"project-routing", func(b []pluginhost.Binding) { b[0].Project = "example" }},
		{"type-routing", func(b []pluginhost.Binding) { b[0].Type = "issue" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			native, err := guard.Native("default", nil)
			if err != nil {
				t.Fatal(err)
			}
			grants := bindings()
			tc.edit(grants)
			_, err = pluginhost.New(pluginhost.Config{Natives: []sdk.Native{native}, Bindings: grants})
			var failure *pluginhost.Error
			if !errors.As(err, &failure) || failure.Code != pluginhost.ErrManifest {
				t.Fatalf("host accepted unsupported/widened grants: %v", err)
			}
		})
	}
	if _, err := guard.Native("bad/instance", nil); err == nil {
		t.Fatal("invalid instance accepted")
	}
	f := setup(t, nil, nil, nil)
	first := apply(t, f.writer(), create("Operation scope"), 1)
	cs := model.ChangeSet{Ops: []model.Op{{Kind: model.OpComment, Comment: &model.CommentOp{Target: first.Results[0].Ref, Body: "unsupported scope"}}}}
	_, err := f.writer().Apply(context.Background(), principal, cs, meta(2))
	requireCode(t, err, "subscriber_unavailable")
	graphSize(t, f, "1", 1, 1, 1)
	if _, err := f.host.Invoke(context.Background(), "x.nativeguard.install", json.RawMessage(`{}`)); err == nil {
		t.Fatal("undeclared extension method accepted")
	}
}

func TestCancellationDoesNotPublish(t *testing.T) {
	for _, duringCallback := range []bool{false, true} {
		t.Run(fmt.Sprint(duringCallback), func(t *testing.T) {
			entered, exited := make(chan struct{}), make(chan struct{})
			var decorate func(*sdk.Registration)
			if duringCallback {
				decorate = func(registration *sdk.Registration) {
					original := registration.Mutators["enrich"]
					registration.Mutators["enrich"] = sdk.MutateFunc(func(ctx context.Context, c pluginapi.ProposedChange, r pluginapi.SnapshotReader) ([]pluginapi.Patch, error) {
						if _, err := original.Mutate(ctx, c, r); err != nil {
							return nil, err
						}
						close(entered)
						<-ctx.Done()
						defer close(exited)
						return original.Mutate(ctx, c, r)
					})
				}
			}
			f := setup(t, nil, decorate, nil)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if !duringCallback {
				cancel()
			}
			done := make(chan error, 1)
			go func() {
				_, err := f.writer().Apply(ctx, principal, create("Canceled write"), meta(1))
				done <- err
			}()
			if duringCallback {
				select {
				case <-entered:
					cancel()
				case <-time.After(2 * time.Second):
					t.Fatal("callback did not start")
				}
			}
			select {
			case err := <-done:
				if err == nil {
					t.Fatal("canceled write succeeded")
				}
			case <-time.After(2 * time.Second):
				t.Fatal("canceled write did not return")
			}
			if duringCallback {
				select {
				case <-exited:
				case <-time.After(2 * time.Second):
					t.Fatal("native callback ignored cancellation")
				}
				f.calls.check(t, 2, 0, 0)
			} else {
				f.calls.check(t, 0, 0, 0)
			}
			graphSize(t, f, "0", 0, 0, 0)
		})
	}
}
