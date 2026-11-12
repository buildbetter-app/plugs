package cardb20

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/client"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

const b20VocabularySubject = "b20-vocab-1"
const b20VocabularyOther = "b20-vocab-2"
const b20VocabularyComment = "B20 exact public comment\nsecond line"

// This is the complete closed native journal vocabulary at 9d6fff760, in
// declaration order. It is checked independently against the pinned source in
// the native target. A Plug type is a semantic mapping, never a renamed raw row.
type b20VocabularyKind struct {
	Native      string                 `json:"native"`
	Plug        model.DurableEventType `json:"plug"`
	Disposition string                 `json:"disposition"`
}

var b20VocabularyKinds = []b20VocabularyKind{
	{"create", model.EventBeadCreated, "MATCH_ADOPTED_FIELDS"},
	{"update", model.EventBeadUpdated, "MATCH_ADOPTED_FIELDS"},
	{"close", model.EventBeadClosed, "MATCH_ADOPTED_FIELDS"},
	{"delete", model.EventBeadDeleted, "MATCH_ADOPTED_FIELDS"},
	{"dep_add", model.EventBeadLinked, "MATCH_ADOPTED_FIELDS"},
	{"dep_remove", model.EventBeadUnlinked, "MATCH_ADOPTED_FIELDS"},
	{"comment", model.EventBeadCommented, "NATIVE_ENGINE_ONLY_PLUG_DURABLE_COMMENT"},
}

type b20VocabularyProjection struct {
	Sequence   string `json:"sequence"`
	NativeOp   string `json:"native_op"`
	ID         string `json:"id"`
	Title      string `json:"title"`
	Status     string `json:"status"`
	LinkKind   string `json:"link_kind,omitempty"`
	LinkTarget string `json:"link_target,omitempty"`
	Comment    string `json:"comment,omitempty"`
}

// Authored expectations are independent of native or Plug output. There are
// eight mutations because both dependency endpoints must first exist.
func b20VocabularyExpected() []b20VocabularyProjection {
	return []b20VocabularyProjection{
		{"1", "create", b20VocabularySubject, "vocabulary anchor", "open", "", "", ""},
		{"2", "create", b20VocabularyOther, "vocabulary target", "open", "", "", ""},
		{"3", "update", b20VocabularySubject, "vocabulary edited", "open", "", "", ""},
		{"4", "comment", b20VocabularySubject, "vocabulary edited", "open", "", "", b20VocabularyComment},
		{"5", "dep_add", b20VocabularySubject, "vocabulary edited", "open", "relates-to", b20VocabularyOther, ""},
		{"6", "dep_remove", b20VocabularySubject, "vocabulary edited", "open", "relates-to", b20VocabularyOther, ""},
		{"7", "close", b20VocabularySubject, "vocabulary edited", "closed", "", "", ""},
		{"8", "delete", b20VocabularySubject, "vocabulary edited", "closed", "", "", ""},
	}
}

func b20VocabularyObject(t *testing.T, value any) map[string]any {
	t.Helper()
	v, ok := value.(map[string]any)
	if !ok {
		t.Fatalf("expected object, got %T", value)
	}
	return v
}

func b20VocabularyPlugProjection(t *testing.T, events []model.Event) []b20VocabularyProjection {
	t.Helper()
	var out []b20VocabularyProjection
	for _, event := range events {
		d := event.Durable
		if d == nil {
			t.Fatal("vocabulary mutation produced a non-durable event")
		}
		op := ""
		for _, kind := range b20VocabularyKinds {
			if d.Type == kind.Plug {
				op = kind.Native
			}
		}
		if op == "" {
			t.Fatalf("unclassified actual Plug type %q", d.Type)
		}
		image := d.After
		if op == "delete" {
			if d.After != nil || d.Before == nil {
				t.Fatal("delete must retain before and omit after")
			}
			image = d.Before
		}
		fields := b20VocabularyObject(t, image["fields"])
		status, _ := fields["status"].(string)
		switch status {
		case "todo":
			status = "open"
		case "done":
			status = "closed"
		default:
			t.Fatalf("unadopted Plug status %q", status)
		}
		p := b20VocabularyProjection{Sequence: d.Revision.String(), NativeOp: op, ID: fmt.Sprint(image["id"]), Title: fmt.Sprint(fields["title"]), Status: status}
		if d.Resource != "bead/"+p.ID || d.Index.String() != "0" || d.Principal.ID != "cardb20-reader" || d.Derived {
			t.Fatalf("incorrect actual event envelope: %+v", d)
		}
		if op == "dep_add" || op == "dep_remove" {
			linkImage := d.After
			if op == "dep_remove" {
				linkImage = d.Before
			}
			links, ok := linkImage["links"].([]any)
			if !ok || len(links) != 1 {
				t.Fatal("dependency event lost the exact edge image")
			}
			link := b20VocabularyObject(t, links[0])
			p.LinkKind, _ = link["kind"].(string)
			p.LinkTarget, _ = b20VocabularyObject(t, link["to"])["id"].(string)
			if op == "dep_remove" {
				if links, _ := d.After["links"].([]any); len(links) != 0 {
					t.Fatal("unlinked edge remains in after image")
				}
			}
		}
		if op == "comment" {
			comments, ok := image["comments"].([]any)
			if !ok || len(comments) != 1 {
				t.Fatal("comment event lost exact stored payload")
			}
			comment := b20VocabularyObject(t, comments[0])
			p.Comment, _ = comment["body"].(string)
			if b20VocabularyObject(t, comment["principal"])["id"] != "cardb20-reader" {
				t.Fatal("comment lost trusted author")
			}
		}
		out = append(out, p)
	}
	return out
}

type b20VocabularyReadback struct {
	Head      string                     `json:"head"`
	Sets      []model.Changeset          `json:"changesets"`
	Records   map[string]json.RawMessage `json:"records"`
	Events    []model.Event              `json:"events"`
	PublicGet map[string]json.RawMessage `json:"public_get"`
}

func b20VocabularyStoreReadback(t *testing.T, r *runtime) b20VocabularyReadback {
	t.Helper()
	meta, sets := journal(t, r.store)
	reader, err := r.store.Read(r.ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := reader.Close(); err != nil {
			t.Error(err)
		}
	}()
	got, err := reader.Get(r.ctx, []driver.Ref{{ID: b20VocabularySubject}, {ID: b20VocabularyOther}})
	if err != nil {
		t.Fatal(err)
	}
	result := b20VocabularyReadback{Head: meta.Head.String(), Sets: sets, Records: map[string]json.RawMessage{}, Events: list(t, r, "", 100).Events, PublicGet: map[string]json.RawMessage{}}
	for _, record := range got {
		result.Records[record.Ref.ID] = append(json.RawMessage(nil), record.Body...)
	}
	return result
}

func b20ReplayVocabulary(t *testing.T, engine string) []b20VocabularyProjection {
	t.Helper()
	f := openStore(t, engine)
	r := newRuntime(t, f.store, false, b20VocabularySubject, b20VocabularyOther)
	var requests, responses []json.RawMessage
	requestNumber := 2000
	call := func(method string, params map[string]any, write bool, code string) json.RawMessage {
		transport := &callerTransport{r: r}
		c, err := client.New(transport, "cardb20", "b20-vocabulary-client")
		if err != nil {
			t.Fatal(err)
		}
		key := ""
		if write {
			requestNumber++
			key = fmt.Sprintf("%032x", requestNumber)
		}
		value, err := c.Call(r.ctx, method, params, key)
		requests = append(requests, transport.Requests...)
		responses = append(responses, transport.Responses...)
		if code != "" {
			var envelope response
			if len(transport.Responses) != 1 || json.Unmarshal(transport.Responses[0], &envelope) != nil || envelope.Error == nil || envelope.Error.Data.Code != code || err == nil {
				t.Fatalf("%s expected typed %s refusal: %v", method, code, err)
			}
			return nil
		}
		if err != nil {
			t.Fatalf("actual public %s: %v", method, err)
		}
		return value.JSON
	}
	ref := map[string]any{"id": b20VocabularySubject}
	edge := map[string]any{"from": ref, "to": map[string]any{"id": b20VocabularyOther}, "kind": "relates-to"}
	steps := []struct {
		method string
		params map[string]any
	}{
		{"bead.create", map[string]any{"type": "issue", "fields": map[string]any{"title": "vocabulary anchor"}}},
		{"bead.create", map[string]any{"type": "issue", "fields": map[string]any{"title": "vocabulary target"}}},
		{"bead.update", map[string]any{"id": ref, "fields": map[string]any{"title": "vocabulary edited"}}},
		{"bead.comment", map[string]any{"id": ref, "body": b20VocabularyComment}},
		{"bead.link", edge}, {"bead.unlink", edge},
		{"bead.close", map[string]any{"id": ref, "reason": "complete"}},
		{"tx.apply", map[string]any{"changeSet": map[string]any{"ops": []any{map[string]any{"kind": "delete", "target": ref}}}}},
	}
	want := b20VocabularyExpected()
	var checkpoints []b20VocabularyReadback
	var cursors []string
	for i, step := range steps {
		call(step.method, step.params, true, "")
		state := b20VocabularyStoreReadback(t, r)
		if state.Head != fmt.Sprint(i+1) || len(state.Sets) != i+1 || !reflect.DeepEqual(b20VocabularyPlugProjection(t, state.Events), want[:i+1]) {
			t.Fatalf("mutation %s did not commit its exact classified row at %d", step.method, i+1)
		}
		for j, set := range state.Sets {
			if set.Revision.String() != fmt.Sprint(j+1) || len(set.Changes) != 1 {
				t.Fatal("vocabulary commit order or mutation count differs")
			}
		}
		last := state.Events[len(state.Events)-1].Durable
		if i == 7 {
			if _, present := state.Records[b20VocabularySubject]; present {
				t.Fatal("deleted subject remains in current store")
			}
			prior, err := decodeNativeSQLDocument(checkpoints[i-1].Records[b20VocabularySubject])
			if err != nil {
				t.Fatal(err)
			}
			raw, err := json.Marshal(last.Before)
			if err != nil {
				t.Fatal(err)
			}
			before, err := decodeNativeSQLDocument(raw)
			if err != nil || !reflect.DeepEqual(prior, before) {
				t.Fatal("delete before-image differs from original committed predecessor")
			}
			call("bead.get", map[string]any{"id": ref}, false, "not_found")
		} else {
			stored, err := decodeNativeSQLDocument(state.Records[want[i].ID])
			if err != nil {
				t.Fatal(err)
			}
			raw, err := json.Marshal(last.After)
			if err != nil {
				t.Fatal(err)
			}
			image, err := decodeNativeSQLDocument(raw)
			if err != nil || !reflect.DeepEqual(stored, image) {
				t.Fatal("committed event after-image differs from exact store readback")
			}
			state.PublicGet[want[i].ID] = call("bead.get", map[string]any{"id": map[string]any{"id": want[i].ID}}, false, "")
		}
		for form, args := range [][]string{{"events", "--limit", "0", "--page-size", "1"}, {"events", "--limit", "all", "--page-size", "2"}, {"events", "all", "--page-size", "1"}, {"events", "export", "--page-size", "2"}} {
			events, wire, stdout := runCaller(t, r, args)
			b20CallerWire(t, wire, stdout, form == 3, "", []int{1, 2, 1, 2}[form])
			if !reflect.DeepEqual(events, state.Events) {
				t.Fatal("real CLI omitted/reordered a vocabulary event")
			}
			retain(t, fmt.Sprintf("vocabulary-%s-prefix%d-form%d.json", engine, i+1, form), map[string]any{"argv": args, "requests": wire.Requests, "responses": wire.Responses, "stdout": stdout, "events": events})
		}
		cursors = append(cursors, list(t, r, "", i+1).Cursor)
		checkpoints = append(checkpoints, state)
		if i == 4 {
			before := b20VocabularyStoreReadback(t, r)
			call("bead.update", map[string]any{"id": ref, "fields": map[string]any{"title": "vocabulary edited"}}, true, "")
			call("bead.comment", map[string]any{"id": ref, "body": "  "}, true, "invalid_params")
			after := b20VocabularyStoreReadback(t, r)
			if !reflect.DeepEqual(before, after) {
				t.Fatal("vocabulary no-op/typed refusal changed committed state or sequence")
			}
			retain(t, "vocabulary-"+engine+"-no-op-refusal.json", map[string]any{"before": before, "after": after, "refusal": "invalid_params", "classification": "ADMISSION_REFUSAL_NOT_PHYSICAL_ROLLBACK"})
		}
	}
	before := b20VocabularyStoreReadback(t, r)
	if engine != "memory" {
		if err := r.Close(); err != nil {
			t.Fatal(err)
		}
		store, err := f.open()
		if err != nil {
			t.Fatal(err)
		}
		r = newRuntime(t, store, false, "unused-vocabulary-id")
	}
	after := b20VocabularyStoreReadback(t, r)
	if !reflect.DeepEqual(before, after) {
		t.Fatal("physical SQL reopen changed exact vocabulary rows, journal or event payloads")
	}
	for i, cursor := range cursors {
		if got := list(t, r, cursor, 100).Events; !reflect.DeepEqual(got, before.Events[i+1:]) && !(len(got) == 0 && i == 7) {
			t.Fatalf("original vocabulary cursor%d did not resume after reopen", i+1)
		}
	}
	call("bead.update", map[string]any{"id": map[string]any{"id": b20VocabularyOther}, "fields": map[string]any{"title": "post-vocabulary next commit"}}, true, "")
	meta, sets := journal(t, r.store)
	if meta.Head.String() != "9" || len(sets) != 9 {
		t.Fatal("next post-reopen write did not retain gapless commit position9")
	}
	retain(t, "vocabulary-"+engine+"-complete.json", map[string]any{"kinds": b20VocabularyKinds, "checkpoints": checkpoints, "before_reopen": before, "after_reopen": after, "original_cursors": cursors, "same_process_signing_key": b20CursorKey, "physical_reopen": engine != "memory", "requests": requests, "responses": responses, "next_head": meta.Head, "next_sets": sets, "comment_difference": "native engine-only source row; Plug adopted durable bead.commented"})
	return b20VocabularyPlugProjection(t, before.Events)
}

func TestB20EveryNativeMutationKindHasPublicCommittedDisposition(t *testing.T) {
	for _, engine := range engines {
		t.Run(engine, func(t *testing.T) { b20ReplayVocabulary(t, engine) })
	}
}
