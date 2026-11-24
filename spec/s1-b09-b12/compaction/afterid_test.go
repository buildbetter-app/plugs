package compaction_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// P47: admin.compact's optional exclusive afterId cursor (Nikhil chose (a),
// 2026-11-24). Selection order is closed time, then record ID; the cursor
// resumes strictly after the named closed record's position, refusals name the
// selected record and write nothing, and afterId is part of the operation.

func p47Engines() []string { return []string{"memory", "sqlite", "postgres"} }

// p47Fixture builds a, b, c closed at the same time and d closed earlier, so
// the order is d (earlier close despite the larger ID), then a, b, c by ID.
func p47Fixture(t *testing.T, engine string, summary func(service.CompactionInput) (string, error)) (*fixture, *atomic.Int32, map[string]map[string]any) {
	t.Helper()
	calls := &atomic.Int32{}
	config := service.CompactionConfig{Summary: service.CompactionSummaryFunc(func(_ context.Context, in service.CompactionInput) (string, error) {
		calls.Add(1)
		return summary(in)
	})}
	f := newFixture(t, engine, service.WithCompaction(config))
	text := strings.Repeat("P47 permanent source text. ", 30)
	refs := map[string]map[string]any{}
	for _, name := range []string{"a", "b", "c", "d"} {
		refs[name] = f.create("p47-create-"+name, text+name)
	}
	f.call("bead.close", "p47-close-d", map[string]any{"id": refs["d"]}, "")
	f.clock = f.clock.Add(time.Hour)
	for _, name := range []string{"a", "b", "c"} {
		f.call("bead.close", "p47-close-"+name, map[string]any{"id": refs[name]}, "")
	}
	open := f.create("p47-create-open", text+"open")
	refs["open"] = open
	f.clock = f.clock.Add(31 * 24 * time.Hour)
	return f, calls, refs
}

func p47Small(service.CompactionInput) (string, error) { return "P47 summary.", nil }

func p47ID(ref map[string]any) string { return ref["id"].(string) }

// p47Refusal asserts a refusal with the given code that writes nothing and
// returns its error data details.
func p47Refusal(t *testing.T, f *fixture, key, code string, params map[string]any) map[string]any {
	t.Helper()
	details, _ := p47RefusalData(t, f, key, code, params)["details"].(map[string]any)
	return details
}

// p47CursorRefused pins the cause, not only the code, of a cursor refusal.
func p47CursorRefused(t *testing.T, f *fixture, key, cursor string) {
	t.Helper()
	data := p47RefusalData(t, f, key, "invalid_params", map[string]any{"dryRun": false, "afterId": cursor})
	if hint := fmt.Sprint(data["hint"]); !strings.Contains(hint, "afterId must name a closed record in this project") {
		t.Fatalf("%s refused for another reason: %q", key, hint)
	}
}

func p47RefusalData(t *testing.T, f *fixture, key, code string, params map[string]any) map[string]any {
	t.Helper()
	before := f.meta()
	response := f.call("admin.compact", key, params, code)
	f.assertSameHead(before)
	f.assertNoReceipt("admin.compact", key)
	return response["error"].(map[string]any)["data"].(map[string]any)
}

func TestP47AfterIDEqualClosedAtOrdersByID(t *testing.T) {
	for _, engine := range p47Engines() {
		t.Run(engine, func(t *testing.T) {
			f, calls, refs := p47Fixture(t, engine, p47Small)
			before := f.meta()
			cursor := map[string]any{"dryRun": true}
			for i, want := range []string{"d", "a", "b", "c"} {
				dry := f.call("admin.compact", fmt.Sprintf("p47-order-%d", i), cursor, "")
				assertCommitTarget(t, dry, refs[want])
				if dry["wouldCommit"] != true {
					t.Fatalf("dry run %d did not preview: %v", i, dry)
				}
				cursor = map[string]any{"dryRun": true, "afterId": p47ID(refs[want])}
			}
			p47Refusal(t, f, "p47-order-end", "compaction_unavailable", cursor)
			f.assertSameHead(before)
			if calls.Load() != 0 {
				t.Fatalf("dry runs called the provider %d times", calls.Load())
			}
			t.Logf("P47_ORDER engine=%s order=d,a,b,c equal_closed_at=a,b,c by_id=true", engine)
		})
	}
}

func TestP47AfterIDPastEndRefusesWithoutWrite(t *testing.T) {
	for _, engine := range p47Engines() {
		t.Run(engine, func(t *testing.T) {
			f, calls, refs := p47Fixture(t, engine, p47Small)
			// The last record in the order is still eligible, but the cursor
			// is exclusive: nothing lies strictly after it.
			p47Refusal(t, f, "p47-past-last", "compaction_unavailable", map[string]any{"dryRun": false, "afterId": p47ID(refs["c"])})
			if calls.Load() != 0 {
				t.Fatal("past-end refusal called the provider")
			}
			for _, name := range []string{"d", "a", "b"} {
				f.call("admin.compact", "p47-drain-"+name, map[string]any{"dryRun": false}, "")
			}
			f.call("admin.compact", "p47-drain-c", map[string]any{"dryRun": false, "afterId": p47ID(refs["b"])}, "")
			details := p47Refusal(t, f, "p47-past-end", "compaction_unavailable", map[string]any{"dryRun": false, "afterId": p47ID(refs["c"])})
			if details["id"] != nil {
				t.Fatalf("a refusal with no selected record named %v", details["id"])
			}
			if calls.Load() != 4 {
				t.Fatalf("provider calls=%d want 4", calls.Load())
			}
		})
	}
}

func TestP47AfterIDIdempotencyKeyMismatchRefused(t *testing.T) {
	for _, engine := range p47Engines() {
		t.Run(engine, func(t *testing.T) {
			f, calls, refs := p47Fixture(t, engine, p47Small)
			params := map[string]any{"dryRun": false, "afterId": p47ID(refs["a"])}
			first := f.call("admin.compact", "p47-key", params, "")
			assertCommitTarget(t, first, refs["b"])
			stable := f.meta()
			again := f.call("admin.compact", "p47-key", params, "")
			if !reflect.DeepEqual(first, again) {
				t.Fatalf("same key and afterId did not replay: %v %v", first, again)
			}
			assertReplayed(t, f)
			f.assertSameHead(stable)
			for label, changed := range map[string]map[string]any{
				"other-cursor": {"dryRun": false, "afterId": p47ID(refs["b"])},
				"no-cursor":    {"dryRun": false},
			} {
				f.call("admin.compact", "p47-key", changed, "idempotency_key_reused")
				f.assertSameHead(stable)
				t.Logf("P47_KEY engine=%s changed=%s refused=idempotency_key_reused", engine, label)
			}
			// A committed call without a cursor also refuses a later cursor.
			f.call("admin.compact", "p47-key-plain", map[string]any{"dryRun": false}, "")
			stable = f.meta()
			f.call("admin.compact", "p47-key-plain", map[string]any{"dryRun": false, "afterId": p47ID(refs["d"])}, "idempotency_key_reused")
			f.assertSameHead(stable)
			if calls.Load() != 2 {
				t.Fatalf("provider calls=%d want 2 (replays and refusals never summarize)", calls.Load())
			}
		})
	}
}

func TestP47RefusalNamesRecordAndWritesNothing(t *testing.T) {
	for _, engine := range p47Engines() {
		t.Run(engine, func(t *testing.T) {
			var ids map[string]string
			f, calls, refs := p47Fixture(t, engine, func(in service.CompactionInput) (string, error) {
				switch in.Ref.ID {
				case ids["a"]:
					return in.Text["description"] + " longer", nil
				case ids["b"]:
					return "", fmt.Errorf("synthetic provider failure")
				}
				return "P47 summary.", nil
			})
			ids = map[string]string{"a": p47ID(refs["a"]), "b": p47ID(refs["b"])}
			f.call("admin.compact", "p47-name-d", map[string]any{"dryRun": false}, "")
			versionsA, versionsB := f.versions(refs["a"]), f.versions(refs["b"])
			notSmallerData := p47RefusalData(t, f, "p47-name-a", "compaction_unavailable", map[string]any{"dryRun": false})
			notSmaller, _ := notSmallerData["details"].(map[string]any)
			if notSmaller["id"] != ids["a"] || fmt.Sprint(notSmaller["tier"]) != "1" {
				t.Fatalf("not-smaller refusal named %v", notSmaller)
			}
			// Repeating the same call selects the same record, so the refusal is
			// final and its hint names the cursor to pass instead.
			if notSmallerData["retryable"] != false || !strings.Contains(fmt.Sprint(notSmallerData["hint"]), "afterId="+ids["a"]) {
				t.Fatalf("not-smaller refusal retryable/hint: %v", notSmallerData)
			}
			providerData := p47RefusalData(t, f, "p47-name-b", "compaction_unavailable", map[string]any{"dryRun": false, "afterId": ids["a"]})
			providerFailed, _ := providerData["details"].(map[string]any)
			if providerFailed["id"] != ids["b"] {
				t.Fatalf("provider-failure refusal named %v", providerFailed)
			}
			// A provider failure may be transient: it keeps the code's default.
			if providerData["retryable"] != true {
				t.Fatalf("provider failure lost its default retryable flag: %v", providerData)
			}
			if !reflect.DeepEqual(versionsA, f.versions(refs["a"])) || !reflect.DeepEqual(versionsB, f.versions(refs["b"])) {
				t.Fatal("a refusal wrote a version")
			}
			dry := f.call("admin.compact", "p47-name-dry", map[string]any{"dryRun": true, "afterId": ids["b"]}, "")
			assertCommitTarget(t, dry, refs["c"])
			f.call("admin.compact", "p47-name-c", map[string]any{"dryRun": false, "afterId": ids["b"]}, "")
			if calls.Load() != 4 {
				t.Fatalf("provider calls=%d want 4", calls.Load())
			}
			t.Logf("P47_REFUSAL engine=%s not_smaller=%v retryable=false provider_error=%v retryable=true writes=0", engine, notSmaller["id"], providerFailed["id"])
		})
	}
}

func TestP47AfterIDCursorResolution(t *testing.T) {
	for _, engine := range p47Engines() {
		t.Run(engine, func(t *testing.T) {
			f, calls, refs := p47Fixture(t, engine, p47Small)
			for label, cursor := range map[string]any{
				"unknown": "p47-no-such-record", "open": p47ID(refs["open"]),
				"empty": "", "padded": " " + p47ID(refs["a"]), "not-a-string": 7,
			} {
				p47Refusal(t, f, "p47-cursor-"+label, "invalid_params", map[string]any{"dryRun": false, "afterId": cursor})
			}
			// A compacted cursor is no longer eligible but keeps its position.
			f.call("admin.compact", "p47-cursor-compact-d", map[string]any{"dryRun": false}, "")
			f.call("admin.compact", "p47-cursor-compact-a", map[string]any{"dryRun": false}, "")
			dry := f.call("admin.compact", "p47-cursor-ineligible", map[string]any{"dryRun": true, "afterId": p47ID(refs["a"])}, "")
			assertCommitTarget(t, dry, refs["b"])
			if calls.Load() != 2 {
				t.Fatalf("provider calls=%d want 2", calls.Load())
			}
		})
	}
}

// Records are created and closed in reverse ID order at the same time, so only
// the ID tie-break (not creation or close order) explains the selection order.
func TestP47AfterIDTieBreakIsByIDNotCreationOrder(t *testing.T) {
	for _, engine := range p47Engines() {
		t.Run(engine, func(t *testing.T) {
			var calls atomic.Int32
			f := newFixture(t, engine, service.WithCompaction(service.CompactionConfig{Summary: service.CompactionSummaryFunc(func(context.Context, service.CompactionInput) (string, error) {
				calls.Add(1)
				return "P47 summary.", nil
			})}))
			text := strings.Repeat("P47 tie-break source. ", 30)
			refs := []map[string]any{}
			for _, next := range []int{10, 5, 0} {
				f.ids = next
				refs = append(refs, f.create(fmt.Sprintf("p47-tie-create-%d", next), text))
			}
			for i, ref := range refs {
				f.call("bead.close", fmt.Sprintf("p47-tie-close-%d", i), map[string]any{"id": ref}, "")
			}
			created := []string{p47ID(refs[0]), p47ID(refs[1]), p47ID(refs[2])}
			byID := append([]string(nil), created...)
			sort.Strings(byID)
			if reflect.DeepEqual(created, byID) {
				t.Fatalf("fixture did not create in reverse ID order: %v", created)
			}
			f.clock = f.clock.Add(31 * 24 * time.Hour)
			params := map[string]any{"dryRun": true}
			for i, want := range byID {
				dry := f.call("admin.compact", fmt.Sprintf("p47-tie-%d", i), params, "")
				if got := dry["results"].([]any)[0].(map[string]any)["ref"].(map[string]any)["id"]; got != want {
					t.Fatalf("selection %d=%v want %s (created %v)", i, got, want, created)
				}
				params = map[string]any{"dryRun": true, "afterId": want}
			}
			p47Refusal(t, f, "p47-tie-end", "compaction_unavailable", params)
			if calls.Load() != 0 {
				t.Fatal("dry runs called the provider")
			}
			t.Logf("P47_TIE engine=%s created=%v selected=%v", engine, created, byID)
		})
	}
}

// A cursor from another project or naming a deleted record has no position in
// this selection and is refused. A cursor excluded by beforeRevision is still a
// closed record of this project, so it keeps its position.
func TestP47AfterIDCursorScopeDeletedAndRevisionBoundary(t *testing.T) {
	for _, engine := range p47Engines() {
		t.Run(engine, func(t *testing.T) {
			f, calls, refs := p47Fixture(t, engine, p47Small)
			other := f.call("bead.create", "p47-other-create", map[string]any{"type": "issue", "fields": map[string]any{"title": "Other project", "description": strings.Repeat("Other project source. ", 30)}, "_meta": map[string]any{"project": "p47-other"}}, "")
			otherRef := other["results"].([]any)[0].(map[string]any)["ref"].(map[string]any)
			f.call("bead.close", "p47-other-close", map[string]any{"id": otherRef, "_meta": map[string]any{"project": "p47-other"}}, "")
			p47CursorRefused(t, f, "p47-cursor-other-project", p47ID(otherRef))

			gone := f.create("p47-deleted-create", strings.Repeat("Deleted source. ", 30))
			f.call("bead.close", "p47-deleted-close", map[string]any{"id": gone}, "")
			deleteRef := model.RefByID(p47ID(gone), model.Natural64(0))
			if _, err := f.writer.ApplyWithOutcome(context.Background(), anonymous, model.ChangeSet{Ops: []model.Op{{Kind: model.OpDelete, Delete: &model.DeleteOp{Kind: model.OpDelete, Target: deleteRef}}}}, writeMeta("bead.delete", "p47-delete")); err != nil {
				t.Fatalf("delete cursor record: %v", err)
			}
			p47CursorRefused(t, f, "p47-cursor-deleted", p47ID(gone))

			// The boundary is the head before d is edited: d stays first in the
			// order (earliest close) but its current revision is excluded.
			boundary := revisionAfter(t, f.meta().Head)
			f.call("bead.update", "p47-edit-d", map[string]any{"id": refs["d"], "fields": map[string]any{"title": "Edited after close"}}, "")
			for _, step := range []struct {
				label  string
				params map[string]any
				want   string
			}{
				{"no-cursor", map[string]any{"dryRun": true, "beforeRevision": boundary}, "a"},
				{"excluded-cursor", map[string]any{"dryRun": true, "beforeRevision": boundary, "afterId": p47ID(refs["d"])}, "a"},
				{"after-a", map[string]any{"dryRun": true, "beforeRevision": boundary, "afterId": p47ID(refs["a"])}, "b"},
				{"after-b", map[string]any{"dryRun": true, "beforeRevision": boundary, "afterId": p47ID(refs["b"])}, "c"},
				{"without-boundary", map[string]any{"dryRun": true}, "d"},
			} {
				dry := f.call("admin.compact", "p47-boundary-"+step.label, step.params, "")
				assertCommitTarget(t, dry, refs[step.want])
			}
			p47Refusal(t, f, "p47-boundary-end", "compaction_unavailable", map[string]any{"dryRun": true, "beforeRevision": boundary, "afterId": p47ID(refs["c"])})
			if calls.Load() != 0 {
				t.Fatal("cursor checks called the provider")
			}
		})
	}
}

// A failure at the write step, after a record was selected, still names it.
func TestP47WriteStepFailureNamesSelectedRecord(t *testing.T) {
	for _, engine := range p47Engines() {
		for _, mode := range []string{"put-failure", "commit-failure"} {
			t.Run(engine+"/"+mode, func(t *testing.T) {
				f, _, refs := p47Fixture(t, engine, p47Small)
				f.store = &observedStore{Store: f.store, failPut: mode == "put-failure", failCommit: mode == "commit-failure"}
				f.bind()
				versions := f.versions(refs["d"])
				data := p47RefusalData(t, f, "p47-write-"+mode, "internal_error", map[string]any{"dryRun": false})
				details, _ := data["details"].(map[string]any)
				if details["id"] != p47ID(refs["d"]) || fmt.Sprint(details["tier"]) != "1" {
					t.Fatalf("%s did not name the selected record: %v", mode, data)
				}
				if !reflect.DeepEqual(versions, f.versions(refs["d"])) {
					t.Fatal("a failed write published a version")
				}
			})
		}
	}
}

// p47PreAfterIDState is the plug-compaction/1 state exactly as the build
// before P47 decodes it (DisallowUnknownFields).
type p47PreAfterIDState struct {
	Format         string          `json:"format"`
	Tier           int             `json:"tier"`
	SourceRevision model.Revision  `json:"sourceRevision"`
	BeforeRevision *model.Revision `json:"beforeRevision,omitempty"`
	Time           time.Time       `json:"time"`
	OriginalSize   int             `json:"originalSize"`
}

// Rollback safety: afterId joins only the request hash, so a record compacted
// with a cursor carries the same plug-compaction/1 metadata shape that the
// previous build reads, with no extra metadata key.
func TestP47AfterIDRecordReadableByPreP47Reader(t *testing.T) {
	for _, engine := range p47Engines() {
		t.Run(engine, func(t *testing.T) {
			f, _, refs := p47Fixture(t, engine, p47Small)
			f.call("admin.compact", "p47-rollback-plain", map[string]any{"dryRun": false}, "")
			cursor := f.call("admin.compact", "p47-rollback-cursor", map[string]any{"dryRun": false, "afterId": p47ID(refs["d"])}, "")
			assertCommitTarget(t, cursor, refs["a"])
			shapes := map[string][]string{}
			for name, ref := range map[string]map[string]any{"plain": refs["d"], "cursor": refs["a"]} {
				doc := p47CurrentDocument(t, f, ref)
				keys := []string{}
				for key := range doc.Metadata {
					keys = append(keys, key)
				}
				sort.Strings(keys)
				shapes[name] = keys
				raw := doc.Metadata["plug.compaction"]
				if bytes.Contains(raw, []byte("afterId")) {
					t.Fatalf("%s compaction stored the cursor: %s", name, raw)
				}
				var old p47PreAfterIDState
				decoder := json.NewDecoder(bytes.NewReader(raw))
				decoder.DisallowUnknownFields()
				if err := decoder.Decode(&old); err != nil || old.Format != "plug-compaction/1" || old.Tier != 1 {
					t.Fatalf("%s compaction is not readable by the pre-P47 reader: %v %s", name, err, raw)
				}
			}
			if !reflect.DeepEqual(shapes["plain"], shapes["cursor"]) || !reflect.DeepEqual(shapes["cursor"], []string{"plug.compaction"}) {
				t.Fatalf("metadata keys differ: %v", shapes)
			}
			t.Logf("P47_ROLLBACK engine=%s format=plug-compaction/1 keys=%v pre_p47_strict_decode=ok", engine, shapes["cursor"])
		})
	}
}

// p47CurrentDocument reads the record's current stored body through the
// driver, as any later build would read it.
func p47CurrentDocument(t *testing.T, f *fixture, ref map[string]any) pipeline.Document {
	t.Helper()
	r, err := f.store.Read(context.Background(), driver.View{Incarnation: f.meta().Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	records, err := r.Get(context.Background(), []driver.Ref{model.RefByID(p47ID(ref), model.Natural64(0))})
	if err != nil || len(records) != 1 {
		t.Fatalf("read current record: %v %d", err, len(records))
	}
	doc, err := pipeline.DecodeDocument(records[0].Body)
	if err != nil {
		t.Fatal(err)
	}
	return doc
}
