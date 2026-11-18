package pipeline

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"reflect"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func link(from, to model.Ref, kind string) model.Op {
	return model.Op{Kind: model.OpLink, Link: &model.LinkOp{From: from, To: to, LinkKind: kind}}
}

func TestConditionalLinksAreAtomicGuardedAndDoNotWriteEndpoints(t *testing.T) {
	h := setup(t)
	w := h.writer()
	seed := apply(t, w, model.ChangeSet{Ops: []model.Op{create(""), create("")}}, meta(1))
	dependent, blocker := seed.Results[0].Ref, seed.Results[1].Ref
	conditional := link(dependent, blocker, "conditional-blocks")
	linked := apply(t, w, model.ChangeSet{Ops: []model.Op{conditional}}, meta(2))
	doc, err := DecodeDocument(linked.Results[0].Record)
	if err != nil || len(doc.Links) != 1 || doc.Links[0].Kind != "conditional-blocks" || doc.Links[0].To.ID != blocker.ID {
		t.Fatalf("dependent links=%+v err=%v", doc.Links, err)
	}
	versions, err := h.read(t).Versions(context.Background(), blocker, driver.PageReq{})
	if err != nil || len(versions.Items) != 1 {
		t.Fatalf("read endpoint gained a version: %+v err=%v", versions, err)
	}
	for i, kind := range []string{"blocks", "conditional-blocks"} {
		_, err := w.Apply(context.Background(), by, model.ChangeSet{Ops: []model.Op{patch(blocker, map[string]any{"title": "must roll back"}), link(blocker, dependent, kind)}}, meta(i+3))
		code(t, err, "ontology_guard_failed")
	}
	// Unknown but well-formed dependency kinds remain import-compatible; an
	// empty kind is the malformed wire case for this refusal assertion.
	_, err = w.Apply(context.Background(), by, model.ChangeSet{Ops: []model.Op{link(dependent, blocker, "")}}, meta(5))
	code(t, err, "invalid_params")
	_, err = w.Apply(context.Background(), by, model.ChangeSet{Ops: []model.Op{{Kind: model.OpDelete, Delete: &model.DeleteOp{Target: blocker}}}}, meta(6))
	code(t, err, "ontology_guard_failed")
	// A duplicate edge and an absent unlink have the same guarded no-op semantics
	// as other unchanged images; they create no versions or changesets.
	duplicate := apply(t, w, model.ChangeSet{Ops: []model.Op{conditional}}, meta(7))
	if duplicate.Revision.String() != "2" {
		t.Fatalf("duplicate edge minted revision: %+v", duplicate)
	}
	conditional.Kind = model.OpUnlink
	unlinked := apply(t, w, model.ChangeSet{Ops: []model.Op{conditional}}, meta(8))
	if unlinked.Revision.String() != "3" {
		t.Fatalf("unlink=%+v", unlinked)
	}
	absent := apply(t, w, model.ChangeSet{Ops: []model.Op{conditional}}, meta(9))
	if absent.Revision.String() != "3" {
		t.Fatalf("absent unlink minted revision: %+v", absent)
	}
	records, err := h.read(t).Get(context.Background(), []model.Ref{blocker})
	if err != nil || len(records) != 1 {
		t.Fatal(records, err)
	}
	unchanged, err := DecodeDocument(records[0].Body)
	if err != nil || unchanged.Fields["title"] != "initial" {
		t.Fatalf("cycle refusal published partial field: %+v err=%v", unchanged, err)
	}
	deleted := apply(t, w, model.ChangeSet{Ops: []model.Op{{Kind: model.OpDelete, Delete: &model.DeleteOp{Target: blocker}}}}, meta(10))
	if deleted.Revision.String() != "4" || len(deleted.Results[0].Record) != 0 {
		t.Fatalf("delete=%+v", deleted)
	}
	records, err = h.read(t).Get(context.Background(), []model.Ref{blocker})
	if err != nil || len(records) != 0 {
		t.Fatalf("delete did not remove record: %+v err=%v", records, err)
	}
	log, err := h.read(t).Changesets(context.Background(), nil, 10)
	if err != nil || len(log) != 4 || log[3].Changes[0].Op != model.OpDelete {
		t.Fatalf("log=%+v err=%v", log, err)
	}
}

func TestDependencyEndpointIdentityRefusesRetypeAndWrongKindUnlink(t *testing.T) {
	h := setup(t)
	w := h.writer()
	seed := apply(t, w, model.ChangeSet{Ops: []model.Op{create("source"), create("target")}}, meta(21))
	source, target := seed.Results[0].Ref, seed.Results[1].Ref
	apply(t, w, model.ChangeSet{Ops: []model.Op{link(source, target, "blocks")}}, meta(22))
	// Association annotations may coexist with a workflow edge. Use a second
	// workflow kind to exercise the typed endpoint conflict contract.
	retype := link(source, target, "waits-for")
	_, err := w.Apply(context.Background(), by, model.ChangeSet{Ops: []model.Op{retype}}, meta(23))
	code(t, err, "ontology_guard_failed")
	wrongKind := retype
	wrongKind.Kind = model.OpUnlink
	_, err = w.Apply(context.Background(), by, model.ChangeSet{Ops: []model.Op{wrongKind}}, meta(24))
	code(t, err, "not_found")
	doc, err := DecodeDocument(apply(t, w, model.ChangeSet{Ops: []model.Op{patch(source, map[string]any{"title": "read"})}}, meta(25)).Results[0].Record)
	if err != nil || len(doc.Links) != 1 || doc.Links[0].Kind != "blocks" || doc.Links[0].To.ID != target.ID {
		t.Fatalf("endpoint identity changed after typed refusal: %+v err=%v", doc.Links, err)
	}
}

func TestUnresolvedExternalEdgeIsRetainedButDeletedLocalTargetIsRefused(t *testing.T) {
	h := setup(t)
	w := h.writer()
	seed := apply(t, w, model.ChangeSet{Ops: []model.Op{create("source"), create("local-target")}}, meta(31))
	source, localTarget := seed.Results[0].Ref, seed.Results[1].Ref
	external := model.RefByID("foreign-prefix-404", model.Natural64(0))
	externalOp := link(source, external, "blocks")
	externalOp.Link.TargetKind = model.LinkTargetExternal
	linked := apply(t, w, model.ChangeSet{Ops: []model.Op{externalOp}}, meta(32))
	doc, err := DecodeDocument(linked.Results[0].Record)
	if err != nil || len(doc.Links) != 1 || doc.Links[0].To.ID != external.ID || doc.Links[0].Kind != "blocks" || doc.Links[0].TargetKind != model.LinkTargetExternal {
		t.Fatalf("unresolved external edge was not retained: links=%+v err=%v", doc.Links, err)
	}
	// A source edge to a local target cannot be hidden by deleting the target
	// in the same final graph. The refusal is atomic and leaves both records.
	apply(t, w, model.ChangeSet{Ops: []model.Op{link(source, localTarget, "relates-to")}}, meta(33))
	before := metadataGraph(t, h)
	_, err = w.Apply(context.Background(), by, model.ChangeSet{Ops: []model.Op{{Kind: model.OpDelete, Delete: &model.DeleteOp{Target: localTarget}}}}, meta(34))
	code(t, err, "ontology_guard_failed")
	if !reflect.DeepEqual(before, metadataGraph(t, h)) {
		t.Fatal("deleting a local edge target changed durable state")
	}
}

func TestTxApplyRefusesWorkflowPlaneEdgeBeforeTargetLoad(t *testing.T) {
	h := setup(t)
	w := h.writer()
	seed := apply(t, w, model.ChangeSet{Ops: []model.Op{create("source")}}, meta(35))
	source := seed.Results[0].Ref
	workflow := model.RefByID("workflow:item/0123456789abcdef0123456789abcdef", model.Natural64(0))
	for i, pair := range [][2]model.Ref{{source, workflow}, {workflow, source}} {
		before := metadataGraph(t, h)
		_, err := w.Apply(context.Background(), by, model.ChangeSet{Ops: []model.Op{link(pair[0], pair[1], "blocks")}}, meta(36+i))
		code(t, err, "invalid_params")
		if !reflect.DeepEqual(before, metadataGraph(t, h)) {
			t.Fatalf("cross-plane edge refusal published state for pair %d", i)
		}
	}
}

func TestCommentsMetadataAndNullFieldsPreserveAtomicImages(t *testing.T) {
	h := setup(t)
	w := h.writer()
	seed := apply(t, w, model.ChangeSet{Ops: []model.Op{create("")}}, meta(1))
	ref := seed.Results[0].Ref
	body := "\n**exact comment body**\n"
	comment := model.Op{Kind: model.OpComment, Comment: &model.CommentOp{Target: ref, Body: body}}
	metadata := model.Op{Kind: model.OpSetMeta, SetMeta: &model.SetMetaOp{Target: ref, Key: "plugin.literal.dot/~key", Value: json.RawMessage(`{"answer":42}`)}}
	cs := model.ChangeSet{Ops: []model.Op{comment, metadata, patch(ref, map[string]any{"title": nil})}}
	result := apply(t, w, cs, meta(2))
	doc, err := DecodeDocument(result.Results[0].Record)
	if err != nil || len(doc.Comments) != 1 || doc.Comments[0].Body != body || !reflect.DeepEqual(doc.Comments[0].Principal, by) || !doc.Comments[0].Time.Equal(h.now) {
		t.Fatalf("comment=%+v err=%v", doc.Comments, err)
	}
	if _, exists := doc.Fields["title"]; exists || doc.ID != ref.ID || doc.Type != "issue" || doc.Project != "test" || doc.Fields["status"] != "todo" {
		t.Fatalf("field clearing changed anchors: %+v", doc)
	}
	for _, opResult := range result.Results {
		if !reflect.DeepEqual(opResult.Record, result.Results[0].Record) {
			t.Fatal("batch results did not share final image")
		}
	}
	log, err := h.read(t).Changesets(context.Background(), nil, 10)
	want := []string{"/comments", "/fields/title", "/metadata/plugin.literal.dot~1~0key"}
	if err != nil || len(log) != 2 || len(log[1].Changes) != 1 || !reflect.DeepEqual(log[1].Changes[0].ChangedPaths, want) {
		t.Fatalf("JSON Pointer paths=%+v want=%v err=%v", log, want, err)
	}
	next := apply(t, w, model.ChangeSet{Ops: []model.Op{comment}}, meta(3))
	doc, err = DecodeDocument(next.Results[0].Record)
	if err != nil || !doc.Comments[1].Time.Equal(h.now.Add(time.Second)) {
		t.Fatalf("same-clock thread stamps=%+v err=%v", doc.Comments, err)
	}
	metadata.SetMeta.Value = json.RawMessage(`null`)
	cleared := apply(t, w, model.ChangeSet{Ops: []model.Op{metadata}}, meta(4))
	doc, err = DecodeDocument(cleared.Results[0].Record)
	if err != nil || len(doc.Metadata) != 0 {
		t.Fatalf("metadata clear=%+v err=%v", doc.Metadata, err)
	}
}

func metadataOp(ref model.Ref, key, value string) model.Op {
	return model.Op{Kind: model.OpSetMeta, SetMeta: &model.SetMetaOp{Target: ref, Key: key, Value: json.RawMessage(value)}}
}

// Digests are computed from independently specified expected bytes, never the
// production metadata canonicalizer.
func metadataDigest(canonical string) string {
	sum := sha256.Sum256([]byte(canonical))
	return hex.EncodeToString(sum[:])
}

type metadataState struct {
	Meta         driver.Meta
	Records      []driver.Record
	Versions     map[string][]driver.Version
	Log          []model.Changeset // includes the complete durable event/outbox plan
	Observations []driver.LockState
	Allocator    *driver.OfflineAllocator
}

func metadataGraph(t *testing.T, h *harness) metadataState {
	t.Helper()
	ctx := context.Background()
	state := metadataState{Versions: map[string][]driver.Version{}}
	var err error
	state.Meta, err = h.store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	r := h.read(t)
	defer r.Close()
	page, err := r.Query(ctx, driver.Query{Limit: 1000})
	if err != nil || page.NextCursor != nil {
		t.Fatalf("incomplete metadata graph: %+v %v", page, err)
	}
	state.Records = page.Items
	var after *driver.Cursor
	if !state.Meta.HistoryFloor.IsZero() {
		// Offline allocator initialization starts a new lifetime. Its
		// discarded prefix remains unavailable; compare all readable history.
		after = &driver.Cursor{Incarnation: state.Meta.Incarnation, Revision: state.Meta.HistoryFloor, Method: "changesets"}
	}
	state.Log, err = r.Changesets(ctx, after, 1000)
	if err != nil {
		t.Fatal(err)
	}
	ready, err := driver.NewReadySentinel("test", "issue")
	if err != nil {
		t.Fatal(err)
	}
	allocator, err := driver.AllocatorLock("test")
	if err != nil {
		t.Fatal(err)
	}
	keys := []driver.LockKey{driver.GraphBlockingSentinelLock, driver.OntologySentinelLock, driver.PluginsSentinelLock, {Kind: driver.LockSentinel, Sentinel: ready}, allocator}
	for _, record := range state.Records {
		versions, err := r.Versions(ctx, record.Ref, driver.PageReq{Limit: 1000})
		if err != nil || versions.NextCursor != nil {
			t.Fatalf("incomplete metadata versions: %+v %v", versions, err)
		}
		state.Versions[record.Ref.ID] = versions.Items
		ref := record.Ref
		bead, err := driver.NewBeadSentinel(ref.ID)
		if err != nil {
			t.Fatal(err)
		}
		keys = append(keys, driver.LockKey{Kind: driver.LockRecord, Ref: &ref}, driver.LockKey{Kind: driver.LockSentinel, Sentinel: bead})
		for _, table := range []driver.DerivedTable{driver.DerivedReadyState, driver.DerivedAncestorClosure, driver.DerivedCutClosure, driver.DerivedAttribute, driver.DerivedSortKey, driver.DerivedReferenceIndex} {
			keys = append(keys, driver.LockKey{Kind: driver.LockDerived, Derived: &driver.DerivedRowKey{Table: table, Key: ref.ID}})
		}
	}
	sort.Slice(keys, func(i, j int) bool {
		a, _ := driver.EncodeLockKey(keys[i])
		b, _ := driver.EncodeLockKey(keys[j])
		return a < b
	})
	state.Observations, err = r.Observe(ctx, keys)
	if err != nil {
		t.Fatal(err)
	}
	value, err := r.(driver.AllocatorReader).LoadAllocator(ctx, "test")
	if err == nil {
		state.Allocator = &value
	} else if !errors.Is(err, driver.ErrNotFound) {
		t.Fatal(err)
	}
	return state
}

func metadataReceipt(t *testing.T, h *harness, m model.WriteMeta) *driver.IdempotencyRecord {
	t.Helper()
	r := h.read(t)
	defer r.Close()
	principal, err := driver.PrincipalKey(by)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := r.IdempotencyByKey(context.Background(), principal, "", m.Method, m.IdempotencyKey)
	if err != nil {
		t.Fatal(err)
	}
	return receipt
}

func TestMetadataFieldHashCASUsesCanonicalCommittedValue(t *testing.T) {
	for _, tc := range []struct{ name, stored, canonical string }{
		{"object", ` {"b":2,"a":1} `, `{"a":1,"b":2}`},
		{"numeric", `1.0`, `1.0`},
		{"nested", `{"z":[{"b":2.00,"a":1e0},"1",true,null,[3,2]],"a":-1.2500}`, `{"a":-1.2500,"z":[{"a":1e0,"b":2.00},"1",true,null,[3,2]]}`},
		{"large", `9007199254740993123456789.12345678901234567890`, `9007199254740993123456789.12345678901234567890`},
		{"integerZeros", `100.00`, `100.00`},
		{"negativeZero", `-0.00e999999999999999999999999999`, `-0.00e999999999999999999999999999`},
		{"fractionExponent", `1.25e-2`, `1.25e-2`},
		{"largeExponent", `10e999999999999999999999999999`, `10e999999999999999999999999999`},
		{"tinyExponent", `10e-1000000000000000000000000001`, `10e-1000000000000000000000000001`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := setup(t)
			w := h.writer()
			ref := apply(t, w, model.ChangeSet{Ops: []model.Op{create("")}}, meta(1)).Results[0].Ref
			const key = "plugin.literal.dot/~key"
			apply(t, w, model.ChangeSet{Ops: []model.Op{metadataOp(ref, key, tc.stored), metadataOp(ref, "sibling", `{"kept":true}`)}}, meta(2))
			// The expectation must survive a disjoint committed record revision.
			apply(t, w, model.ChangeSet{Ops: []model.Op{patch(ref, map[string]any{"description": "disjoint"})}}, meta(3))
			before := metadataGraph(t, h)
			hash := metadataDigest(tc.canonical)
			if tc.name == "object" && hash != "43258cff783fe7036d8a43033f830adfc60ec037382473548ac742b888292777" || tc.name == "numeric" && hash != "d0ff5974b6aa52cf562bea5921840c032a860a91a3512f7fe8f768f6bbe005f6" {
				t.Fatal("binding digest vector drifted")
			}
			cs := model.ChangeSet{Ops: []model.Op{patch(ref, map[string]any{"title": "atomic"}), metadataOp(ref, key, `{"next":true}`)}, Expect: []model.Precondition{model.PreconditionByHash(ref, "metadata."+key, hash)}}
			input, _ := json.Marshal(cs)
			result := apply(t, w, cs, meta(4))
			doc, err := DecodeDocument(result.Results[0].Record)
			if err != nil || string(doc.Metadata[key]) != `{"next":true}` || string(doc.Metadata["sibling"]) != `{"kept":true}` || doc.Fields["description"] != "disjoint" || doc.Fields["title"] != "atomic" {
				t.Fatalf("metadata swap/siblings=%+v %v", doc, err)
			}
			after := metadataGraph(t, h)
			if len(after.Log) != len(before.Log)+1 || len(after.Versions[ref.ID]) != len(before.Versions[ref.ID])+1 || len(after.Log[len(after.Log)-1].Changes) != 1 || !reflect.DeepEqual(result.Results[0].Record, result.Results[1].Record) {
				t.Fatal("atomic metadata swap did not produce exactly one version/changeset")
			}
			unchangedInput, _ := json.Marshal(cs)
			if string(input) != string(unchangedInput) {
				t.Fatal("CAS mutated caller input")
			}
			oldDoc, err := DecodeDocument(before.Records[0].Body)
			if err != nil || !bytes.Equal(oldDoc.Metadata[key], bytes.TrimSpace([]byte(tc.stored))) {
				t.Fatalf("comparison normalized the stored pre-image: %+v %v", oldDoc, err)
			}
		})
	}
}

func TestMetadataFieldHashRefusalsPublishNothing(t *testing.T) {
	h := setup(t)
	seedAllocator(t, h.store, h.lease, driver.OfflineAllocator{Prefix: "test", Mode: driver.AllocatorSequential, LastID: 8})
	r := h.read(t)
	_, err := r.Changesets(context.Background(), nil, 1000)
	_ = r.Close()
	var truncated driver.ErrTruncated
	if !errors.As(err, &truncated) || truncated.Floor.String() != "1" {
		t.Fatalf("restored allocator history must reject the discarded prefix: %v", err)
	}
	w := h.writer()
	ref := apply(t, w, model.ChangeSet{Ops: []model.Op{create("")}}, meta(1)).Results[0].Ref
	apply(t, w, model.ChangeSet{Ops: []model.Op{metadataOp(ref, "compare", `{"b":2,"a":1}`), metadataOp(ref, "sibling", `true`), patch(ref, map[string]any{"compare": "field"})}}, meta(2))
	var posts atomic.Int32
	w = h.writer(WithPostSubscribers(PostBinding{ID: "metadata-post", Subscriber: postFunc(func(context.Context, port.CommittedChange, port.SnapshotReader) error { posts.Add(1); return nil })}))
	before := metadataGraph(t, h)
	for i, expect := range []model.Precondition{
		model.PreconditionByHash(ref, "metadata.compare", metadataDigest(`null`)), // caught the former wrong-map false acceptance
		model.PreconditionByHash(ref, "metadata.compare", metadataDigest(`{"a":1,"b":3}`)),
		model.PreconditionByHash(ref, "metadata.compare", metadataDigest(`"post-image"`)),
		model.PreconditionByHash(ref, "metadata.missing", metadataDigest(`null`)),
		model.PreconditionByHash(ref, "metadata.title", metadataDigest(`"initial"`)),
		model.PreconditionByHash(ref, "metadata.compare", metadataDigest(`"field"`)),
		model.PreconditionByHash(ref, "fields.compare", metadataDigest(`{"a":1,"b":2}`)),
	} {
		m := meta(100 + i)
		cs := model.ChangeSet{Ops: []model.Op{patch(ref, map[string]any{"title": "must not publish"}), create("staged"), metadataOp(ref, "compare", `"post-image"`)}, Expect: []model.Precondition{expect}}
		_, err := w.Apply(context.Background(), by, cs, m)
		code(t, err, "precondition_failed")
		var typed *model.Error
		if !errors.As(err, &typed) || typed.Data.Retryable {
			t.Fatal("metadata mismatch must be a typed nonretryable refusal")
		}
		if !reflect.DeepEqual(before, metadataGraph(t, h)) || metadataReceipt(t, h, m) != nil || posts.Load() != 0 {
			t.Fatalf("refused metadata batch published graph/history/events/predicates/allocator/receipt/post: %d", i)
		}
	}
	// Both legacy field selectors keep their byte contract and namespace.
	for i, selector := range []string{"compare", "fields.compare"} {
		apply(t, w, model.ChangeSet{Ops: []model.Op{patch(ref, map[string]any{})}, Expect: []model.Precondition{model.PreconditionByHash(ref, selector, metadataDigest(`"field"`))}}, meta(200+i))
	}
	if !reflect.DeepEqual(before, metadataGraph(t, h)) || posts.Load() != 0 {
		t.Fatal("matching legacy field no-op changed the graph")
	}
	if got, err := FieldHash(json.Number("1.0")); err != nil || got != metadataDigest(`1.0`) {
		t.Fatal("exported FieldHash bytes changed")
	}
}

func TestMetadataTypesArraysAndLargeNumbersRemainDistinct(t *testing.T) {
	for _, tc := range []struct{ stored, other string }{
		{`[1,2]`, `[2,1]`}, {`"1"`, `1`}, {`true`, `"true"`},
		{`9007199254740993`, `9007199254740992`},
		{`0.12345678901234567890123456789`, `0.12345678901234567890123456788`},
		{`{"n":[9007199254740993,1]}`, `{"n":[9007199254740992,1]}`},
	} {
		h := setup(t)
		ref := apply(t, h.writer(), model.ChangeSet{Ops: []model.Op{create("")}}, meta(1)).Results[0].Ref
		apply(t, h.writer(), model.ChangeSet{Ops: []model.Op{metadataOp(ref, "compare", tc.stored)}}, meta(2))
		before := metadataGraph(t, h)
		_, err := h.writer().Apply(context.Background(), by, model.ChangeSet{Ops: []model.Op{metadataOp(ref, "compare", `false`)}, Expect: []model.Precondition{model.PreconditionByHash(ref, "metadata.compare", metadataDigest(tc.other))}}, meta(3))
		code(t, err, "precondition_failed")
		if !reflect.DeepEqual(before, metadataGraph(t, h)) || metadataReceipt(t, h, meta(3)) != nil {
			t.Fatal("a different JSON type/order/precise number was accepted")
		}
	}
}

func TestMetadataNoopReceiptReplayAndStoredNull(t *testing.T) {
	h := setup(t)
	ref := model.Ref{ID: "pl-archive-null"}
	// A valid storage pre-image can contain null data even though setMeta(null)
	// is the decided deletion operation. This does not qualify archive restore.
	body := []byte(`{"id":"pl-archive-null","type":"issue","project":"test","fields":{"title":"initial","status":"todo"},"metadata":{"nullable":null,"sibling":{"kept":true}}}`)
	if _, err := conformance.Seed(context.Background(), h.store, h.lease, driver.ChangesetHeader{}, []conformance.FixtureChange{{Ref: ref, Kind: "bead", Op: model.OpCreate, Body: body}}); err != nil {
		t.Fatal(err)
	}
	var posts atomic.Int32
	w := h.writer(WithPostSubscribers(PostBinding{ID: "metadata-post", Subscriber: postFunc(func(context.Context, port.CommittedChange, port.SnapshotReader) error { posts.Add(1); return nil })}))
	before := metadataGraph(t, h)
	cs := model.ChangeSet{Ops: []model.Op{metadataOp(ref, "sibling", `{"kept":true}`)}, Expect: []model.Precondition{model.PreconditionByHash(ref, "metadata.nullable", metadataDigest(`null`))}}
	out, err := w.ApplyWithOutcome(context.Background(), by, cs, meta(1))
	if err != nil || out.Replayed || out.Commit.Revision.String() != before.Meta.Head.String() {
		t.Fatalf("valid metadata no-op=%+v %v", out, err)
	}
	receipt := metadataReceipt(t, h, meta(1))
	if receipt == nil || receipt.ResponsePending || receipt.Changeset.Kind != "" || !reflect.DeepEqual(receipt.Commit, out.Commit) {
		t.Fatalf("missing final graph-free no-op receipt: %+v", receipt)
	}
	again, err := h.writer().ApplyWithOutcome(context.Background(), by, cs, meta(1))
	if err != nil || !again.Replayed || !reflect.DeepEqual(again.Commit, out.Commit) || !reflect.DeepEqual(before, metadataGraph(t, h)) || posts.Load() != 0 {
		t.Fatalf("metadata no-op replay mutated graph/post: %+v %v", again, err)
	}
	apply(t, w, model.ChangeSet{Ops: []model.Op{metadataOp(ref, "nullable", `null`)}, Expect: cs.Expect}, meta(2))
	rows, err := h.read(t).Get(context.Background(), []model.Ref{ref})
	if err != nil || len(rows) != 1 {
		t.Fatal(rows, err)
	}
	doc, err := DecodeDocument(rows[0].Body)
	_, present := doc.Metadata["nullable"]
	if err != nil || present || string(doc.Metadata["sibling"]) != `{"kept":true}` || posts.Load() != 1 {
		t.Fatalf("null deletion/sibling semantics changed: %+v %v", doc, err)
	}
	for _, raw := range []string{``, `{"x":`, `1 2`, `null {}`, `[NaN]`} {
		_, err := expectationHash(Document{Metadata: map[string]json.RawMessage{"bad": json.RawMessage(raw)}}, "metadata.bad")
		code(t, err, "precondition_failed")
	}
}
