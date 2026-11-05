package s1b09b12

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

// Only the allocator/edge slice is under test. This fixed ontology supplies
// open/closed slots for the real ordered OpClose path; it does not certify B08
// policy guards, service authorization, or B12 clock/compaction behavior.
type acceptedOntology struct{ fixtureOntology }

func (acceptedOntology) CloseSlots(string, string) (string, string, bool) {
	return "open", "closed", true
}
func (acceptedOntology) ValidateOrdered(context.Context, []port.Change, port.ProposedChange, port.SnapshotReader) error {
	return nil
}

type acceptedState struct {
	Meta      driver.Meta
	Records   []driver.Record
	Versions  map[string][]driver.Version
	Allocator *driver.OfflineAllocator
}

type acceptedReplay struct {
	t        *testing.T
	o        opened
	project  string
	sequence int
	seen     map[string]bool
}

func newAcceptedReplay(t *testing.T, engine, project string) *acceptedReplay {
	f := &acceptedReplay{t: t, o: open(t, engine), project: project, sequence: 800, seen: map[string]bool{}}
	t.Cleanup(func() { f.o.close(t) })
	return f
}

func (f *acceptedReplay) writer(ids ...string) *pipeline.Writer {
	return writerWithOptions(f.o, ids, pipeline.WithOntology(acceptedOntology{}))
}

func (f *acceptedReplay) state() acceptedState {
	f.t.Helper()
	ctx := context.Background()
	meta, err := f.o.store.Meta(ctx)
	if err != nil {
		f.t.Fatal(err)
	}
	r, err := f.o.store.Read(ctx, driver.View{Incarnation: meta.Incarnation, At: &meta.Head})
	if err != nil {
		f.t.Fatal(err)
	}
	defer r.Close()
	page, err := r.Query(ctx, driver.Query{Limit: 1000})
	if err != nil || page.NextCursor != nil {
		f.t.Fatalf("read complete selected store: %v", err)
	}
	sort.Slice(page.Items, func(i, j int) bool { return page.Items[i].Ref.ID < page.Items[j].Ref.ID })
	state := acceptedState{Meta: meta, Records: page.Items, Versions: map[string][]driver.Version{}}
	for id := range f.seen {
		versions, err := r.Versions(ctx, model.RefByID(id, model.Natural64(0)), driver.PageReq{Limit: 1000})
		if err != nil || versions.NextCursor != nil {
			f.t.Fatalf("read %s history: %v", id, err)
		}
		state.Versions[id] = versions.Items
	}
	value, err := r.(driver.AllocatorReader).LoadAllocator(ctx, f.project)
	if err == nil {
		state.Allocator = &value
	} else if !errors.Is(err, driver.ErrNotFound) {
		f.t.Fatal(err)
	}
	return state
}

func (f *acceptedReplay) record(label string, ops []model.Op, commit model.Commit, err error) {
	f.t.Helper()
	entry := struct {
		PacketSHA256, Label string
		Ops                 []model.Op
		Commit              *model.Commit
		Error               string
		State               acceptedState
	}{PacketSHA256: acceptedPacketSHA256, Label: label, Ops: ops, State: f.state()}
	if err == nil && commit.Results != nil && commit.Warnings != nil {
		entry.Commit = &commit
	}
	if err != nil {
		entry.Error = err.Error()
	}
	data, encodeErr := json.Marshal(entry)
	if encodeErr != nil {
		f.t.Fatal(encodeErr)
	}
	f.t.Logf("accepted-reference-replay %s", data)
}

func (f *acceptedReplay) apply(w *pipeline.Writer, label string, ops ...model.Op) (model.Commit, error) {
	f.t.Helper()
	f.sequence++
	m := meta(f.sequence)
	m.Project = f.project
	commit, err := w.Apply(context.Background(), recorderPrincipal, model.ChangeSet{Ops: ops}, m)
	if err == nil {
		for _, result := range commit.Results {
			if result.Ref.ID != "" {
				f.seen[result.Ref.ID] = true
			}
		}
	}
	f.record(label, ops, commit, err)
	return commit, err
}

func (f *acceptedReplay) mustApply(w *pipeline.Writer, label string, ops ...model.Op) model.Commit {
	f.t.Helper()
	commit, err := f.apply(w, label, ops...)
	if err != nil {
		f.t.Fatalf("%s: %v", label, err)
	}
	return commit
}

func persisted(t *testing.T, state acceptedState, id string) pipeline.Document {
	t.Helper()
	for _, record := range state.Records {
		if record.Ref.ID == id {
			doc, err := pipeline.DecodeDocument(record.Body)
			if err != nil {
				t.Fatal(err)
			}
			return doc
		}
	}
	t.Fatalf("missing persisted endpoint %s", id)
	return pipeline.Document{}
}

func capturedIssue(t *testing.T, run acceptedRun, label string) sourceIssue {
	t.Helper()
	for _, stream := range run.Streams {
		if stream.Label == label {
			raw, err := base64.StdEncoding.Strict().DecodeString(stream.StdoutBase64)
			if err != nil {
				t.Fatal(err)
			}
			var row sourceIssue
			if err := json.Unmarshal(raw, &row); err != nil {
				t.Fatal(err)
			}
			return row
		}
	}
	t.Fatalf("missing source input %s", label)
	return sourceIssue{}
}

func capturedCreate(row sourceIssue, parent *model.Ref) model.Op {
	return model.Op{Kind: model.OpCreate, Create: &model.CreateOp{Type: row.Type,
		Fields: map[string]any{"title": row.Title, "description": row.Description, "status": row.Status}, Parent: parent}}
}

func importedDocument(row sourceIssue, project string) pipeline.Document {
	return pipeline.Document{ID: row.ID, Type: row.Type, Project: project,
		Fields: map[string]any{"title": row.Title, "description": row.Description, "status": row.Status}}
}

func replayAcceptedHash(t *testing.T, engine string, run acceptedRun) map[string]any {
	f := newAcceptedReplay(t, engine, "b09")
	seedAllocator(t, f.o, driver.NewHashAllocator("b09", 3, 8, 0.25))
	w := f.writer()
	firstInput, secondInput := capturedIssue(t, run, "hash-first"), capturedIssue(t, run, "hash-second")
	first := f.mustApply(w, "hash-first", capturedCreate(firstInput, nil)).Results[0].Ref
	second := f.mustApply(w, "hash-second", capturedCreate(secondInput, nil)).Results[0].Ref
	before := f.state()
	// tx.apply has no user-supplied ID. Lower the source explicit-ID collision
	// through the existing trusted generator (no restored allocator/project).
	// The actual writer must refuse every occupied candidate, never overwrite.
	attempts := 0
	collisionWriter := pipeline.New(f.o.store, f.o.lease, pipeline.WithOntology(acceptedOntology{}),
		pipeline.WithIDGenerator(func() (string, error) { attempts++; return first.ID, nil }))
	f.sequence++
	m := meta(f.sequence)
	m.Project = ""
	cs := model.ChangeSet{Ops: []model.Op{capturedCreate(firstInput, nil)}}
	commit, err := collisionWriter.Apply(context.Background(), recorderPrincipal, cs, m)
	f.record("hash-collision-refusal:trusted-generator", cs.Ops, commit, err)
	requireCode(t, err, "write_conflict")
	after := f.state()
	if attempts != 4 || !reflect.DeepEqual(before, after) {
		t.Fatalf("occupied candidate mutated state or retry bound: attempts=%d", attempts)
	}
	a, b := persisted(t, after, first.ID), persisted(t, after, second.ID)
	pattern := regexp.MustCompile(`^b09-[a-z0-9]{3,8}$`)
	return map[string]any{
		"createdIDsDistinct": a.ID != b.ID,
		"createdIDPattern":   pattern.MatchString(a.ID) && pattern.MatchString(b.ID),
		"sameSemanticFields": a.Type == b.Type && reflect.DeepEqual(a.Fields, b.Fields),
		"collisionRefused":   err != nil && reflect.DeepEqual(before, after),
		"rowsAfterCollision": float64(len(after.Records)),
	}
}

func (f *acceptedReplay) reopenSQLite() {
	f.t.Helper()
	if f.o.path == "" {
		return
	}
	before, path := f.state(), f.o.path
	f.o.close(f.t)
	store, err := (sqlite.Driver{}).Open(context.Background(), driver.Config{DSN: path})
	if err != nil {
		f.t.Fatal(err)
	}
	lease, err := store.Lease(context.Background(), "s1-accepted-reopen", driver.LeaseTTL)
	if err != nil {
		_ = store.Close()
		f.t.Fatal(err)
	}
	f.o = opened{store: store, lease: lease, path: path}
	after := f.state()
	if !reflect.DeepEqual(before, after) {
		f.t.Fatal("SQLite reopen lost records/history/allocator continuation")
	}
	f.record("sqlite-reopen:before-next-mint", nil, model.Commit{}, nil)
}

func replayAcceptedChildren(t *testing.T, engine string, run acceptedRun) map[string]any {
	f := newAcceptedReplay(t, engine, "b10")
	parentInput, childInput := capturedIssue(t, run, "create-b10-parent"), capturedIssue(t, run, "create-b10-parent.8")
	// Source explicit IDs map to the existing driver restore seam. Subsequent
	// allocation, delete and ordered close operations use the product writer.
	seedAllocator(t, f.o, driver.OfflineAllocator{Prefix: "b10", Mode: driver.AllocatorSequential, Children: map[string]uint64{parentInput.ID: 1}})
	seedDocuments(t, f.o, importedDocument(parentInput, "b10"), importedDocument(childInput, "b10"))
	f.seen[parentInput.ID], f.seen[childInput.ID] = true, true
	f.record("import-explicit-parent-and-child:restore-seam", nil, model.Commit{}, nil)
	parent := model.RefByID(parentInput.ID, model.Natural64(0))
	ids := []string{persisted(t, f.state(), childInput.ID).ID}
	w := f.writer()
	mint := func(n int) model.Ref {
		label := fmt.Sprintf("child-%d", n)
		input := capturedIssue(t, run, label)
		ref := f.mustApply(w, label, capturedCreate(input, &parent)).Results[0].Ref
		ids = append(ids, persisted(t, f.state(), ref.ID).ID)
		return ref
	}
	mint(9)
	ten := mint(10)
	f.mustApply(w, "delete-10", model.Op{Kind: model.OpDelete, Delete: &model.DeleteOp{Target: ten}})
	eleven := mint(11)
	f.mustApply(w, "close-11", model.Op{Kind: model.OpClose, Close: &model.CloseOp{Target: eleven, Reason: "done"}})
	if persisted(t, f.state(), eleven.ID).Fields["status"] != "closed" {
		t.Fatal("ordered close did not persist closed status")
	}
	mint(12)
	deletes := make([]model.Op, 0, 4)
	for _, id := range []string{childInput.ID, ids[1], ids[3], ids[4]} {
		deletes = append(deletes, model.Op{Kind: model.OpDelete, Delete: &model.DeleteOp{Target: model.RefByID(id, model.Natural64(0))}})
	}
	f.mustApply(w, "delete-live-children", deletes...)
	before := f.state()
	if before.Allocator == nil || before.Allocator.Children[parent.ID] != 12 || len(before.Records) != 1 {
		t.Fatal("last-child deletion lost high-water or left children")
	}
	f.reopenSQLite()
	w = f.writer()
	mint(13)
	final := f.state()
	if final.Allocator == nil || final.Allocator.Children[parent.ID] != 13 {
		t.Fatal("final high-water did not persist")
	}
	children := []string{}
	for _, record := range final.Records {
		doc, err := pipeline.DecodeDocument(record.Body)
		if err != nil {
			t.Fatal(err)
		}
		if doc.Parent != nil && doc.Parent.ID == parent.ID {
			children = append(children, doc.ID)
		}
	}
	return childProjection(ids, children)
}

func hasPlugEdge(doc pipeline.Document, kind string) bool {
	return len(doc.Links) == 1 && doc.Links[0].To.ID == "b11-target" && doc.Links[0].Kind == kind
}

func replayAcceptedDependency(t *testing.T, engine string, run acceptedRun) map[string]any {
	f := newAcceptedReplay(t, engine, "b11")
	sourceInput, targetInput := capturedIssue(t, run, "create-b11-source"), capturedIssue(t, run, "create-b11-target")
	w := f.writer(sourceInput.ID, targetInput.ID)
	source := f.mustApply(w, "create-b11-source", capturedCreate(sourceInput, nil)).Results[0].Ref
	target := f.mustApply(w, "create-b11-target", capturedCreate(targetInput, nil)).Results[0].Ref
	link := func(kind model.OpKind, edge string) model.Op {
		return model.Op{Kind: kind, Link: &model.LinkOp{Kind: kind, From: source, To: target, LinkKind: edge}}
	}
	f.mustApply(w, "add-blocks", link(model.OpLink, "blocks"))
	beforeReadd := f.state()
	f.mustApply(w, "readd-blocks", link(model.OpLink, "blocks"))
	before := f.state()
	if !reflect.DeepEqual(beforeReadd, before) {
		t.Fatal("same-type re-add wrote records/history")
	}
	_, err := f.apply(w, "retype-refusal:blocks-to-relates-to", link(model.OpLink, "relates-to"))
	after := f.state()
	// This is the exact new source mismatch regression. On the old candidate,
	// actual persisted links/head/history show a write and the test stays RED.
	requireCode(t, err, "ontology_guard_failed")
	if !reflect.DeepEqual(before, after) {
		t.Fatal("retype refusal changed durable state/history/allocator")
	}
	beforeDoc, afterDoc := persisted(t, before, source.ID), persisted(t, after, source.ID)
	f.mustApply(w, "remove-blocks", link(model.OpUnlink, "blocks"))
	f.mustApply(w, "add-relates", link(model.OpLink, "relates-to"))
	readded := persisted(t, f.state(), source.ID)
	f.mustApply(w, "remove-relates", link(model.OpUnlink, "relates-to"))
	f.reopenSQLite()
	final := f.state()
	finalSource, finalTarget := persisted(t, final, source.ID), persisted(t, final, target.ID)
	return map[string]any{
		"sameTypeReaddAccepted":           reflect.DeepEqual(beforeReadd, before),
		"typedRetypeRefused":              err != nil,
		"retypeNoWrite":                   hasPlugEdge(beforeDoc, "blocks") && hasPlugEdge(afterDoc, "blocks") && reflect.DeepEqual(before, after),
		"unlinkThenDifferentTypeAccepted": hasPlugEdge(readded, "relates-to"),
		"unlinkConservesEndpoints":        len(finalSource.Links) == 0 && finalSource.Fields["title"] == sourceInput.Title && finalTarget.Fields["title"] == targetInput.Title && len(final.Records) == 2,
	}
}

func TestAcceptedReferenceReplayAcrossStores(t *testing.T) {
	for _, pair := range acceptedPairs(t) {
		t.Run(pair.Card, func(t *testing.T) {
			engines := []string{"memory", "sqlite"}
			if pair.Card == "B11" {
				engines = append(engines, "postgres")
			}
			for _, engine := range engines {
				t.Run(engine, func(t *testing.T) {
					for i, run := range []acceptedRun{pair.First, pair.Repeat} {
						t.Run([]string{"first", "repeat"}[i], func(t *testing.T) {
							want, err := sourceProjection(run)
							if err != nil {
								t.Fatal(err)
							}
							var got map[string]any
							switch pair.Card {
							case "B09":
								got = replayAcceptedHash(t, engine, run)
							case "B10":
								got = replayAcceptedChildren(t, engine, run)
							case "B11":
								got = replayAcceptedDependency(t, engine, run)
							}
							if !reflect.DeepEqual(got, want) {
								t.Fatalf("actual %s projection=%v raw-source projection=%v", engine, got, want)
							}
							t.Logf("selected comparison PASS packet=%s sourceWorkspace=%s card=%s store=%s projection=%s rawStreamsEqual=false B12=NOT_EXECUTED", acceptedPacketSHA256, run.WorkspaceID, pair.Card, engine, referenceDigest(got))
						})
					}
				})
			}
		})
	}
}

// Additional canonical product regressions, not additional reference captures.
// Refused batches leave records, histories, counters and receipts untouched;
// an ordered unlink/link in one ChangeSet is the explicit type-change path.
func TestB11AtomicEndpointTypeReplacementAcrossStores(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := newAcceptedReplay(t, engine, "b11")
			w := f.writer("b11-source", "b11-target", "b11-other")
			seed := f.mustApply(w, "product-regression:create-endpoints",
				create(map[string]any{"title": "source"}, nil, ""),
				create(map[string]any{"title": "target"}, nil, ""),
				create(map[string]any{"title": "other"}, nil, ""))
			source, target, other := seed.Results[0].Ref, seed.Results[1].Ref, seed.Results[2].Ref
			link := func(op model.OpKind, kind string) model.Op {
				return model.Op{Kind: op, Link: &model.LinkOp{Kind: op, From: source, To: target, LinkKind: kind}}
			}
			f.mustApply(w, "product-regression:association-first", link(model.OpLink, "relates-to"))
			before := f.state()
			_, err := f.apply(w, "product-regression:association-to-blocks-refused",
				model.Op{Kind: model.OpPatch, Patch: &model.PatchOp{Target: other, Fields: map[string]any{"title": "must roll back"}}},
				link(model.OpLink, "blocks"))
			requireCode(t, err, "ontology_guard_failed")
			if !reflect.DeepEqual(before, f.state()) {
				t.Fatal("retype refusal published an earlier batch patch")
			}
			f.assertNoReceipt()
			_, err = f.apply(w, "product-regression:wrong-kind-unlink-refused", link(model.OpUnlink, "blocks"))
			requireCode(t, err, "not_found")
			if !reflect.DeepEqual(before, f.state()) {
				t.Fatal("wrong-kind unlink changed durable state")
			}
			f.assertNoReceipt()
			replacement := f.mustApply(w, "product-regression:atomic-unlink-and-retype",
				link(model.OpUnlink, "relates-to"), link(model.OpLink, "blocks"))
			after := f.state()
			beforeRevision, beforeErr := before.Meta.Head.Uint64()
			afterRevision, afterErr := replacement.Revision.Uint64()
			if beforeErr != nil || afterErr != nil || afterRevision != beforeRevision+1 || len(after.Versions[source.ID]) != len(before.Versions[source.ID])+1 ||
				!hasPlugEdge(persisted(t, after, source.ID), "blocks") || !reflect.DeepEqual(before.Versions[target.ID], after.Versions[target.ID]) {
				t.Fatal("explicit type replacement did not publish one source version while conserving its endpoint")
			}
			// Association on another endpoint remains legal and nonblocking.
			f.mustApply(w, "product-regression:separate-endpoint-association", model.Op{Kind: model.OpLink,
				Link: &model.LinkOp{From: source, To: other, LinkKind: "relates-to"}})
			if len(persisted(t, f.state(), source.ID).Links) != 2 {
				t.Fatal("independent endpoint association was lost")
			}
			f.reopenSQLite()
		})
	}
}

func (f *acceptedReplay) assertNoReceipt() {
	f.t.Helper()
	ctx := context.Background()
	state := f.state()
	r, err := f.o.store.Read(ctx, driver.View{Incarnation: state.Meta.Incarnation, At: &state.Meta.Head})
	if err != nil {
		f.t.Fatal(err)
	}
	defer r.Close()
	principal, err := driver.PrincipalKey(recorderPrincipal)
	if err != nil {
		f.t.Fatal(err)
	}
	receipt, err := r.IdempotencyByKey(ctx, principal, "", "tx.apply", meta(f.sequence).IdempotencyKey)
	if err != nil || receipt != nil {
		f.t.Fatalf("refused operation published an idempotency receipt: %+v %v", receipt, err)
	}
}
