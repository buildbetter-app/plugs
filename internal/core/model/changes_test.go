package model

import (
	"encoding/json"
	"strings"
	"testing"
)

var operationCorpus = []string{
	`{"kind":"create","key":"new","type":"","fields":{"n":9007199254740993,"nested":{"anything":null}},"parent":{"id":""},"project":""}`,
	`{"kind":"patch","target":{"key":"new"},"fields":{}}`,
	`{"kind":"reparent","target":{"id":"a","branch":184467440737095516160},"parent":{"key":"new"},"project":"p"}`,
	`{"kind":"link","from":{"id":"a"},"to":{"id":"b"},"linkKind":""}`,
	`{"kind":"unlink","from":{"id":"a"},"to":{"id":"b"},"linkKind":"blocks"}`,
	`{"kind":"comment","target":{"key":""},"body":""}`,
	`{"kind":"attach","target":{"id":"a"},"attachment":{"kind":"blob","contentRef":"b","mediaType":"","summary":""}}`,
	`{"kind":"delete","target":{"id":"a"}}`,
	`{"kind":"setMeta","target":{"id":"a"},"key":"","value":null}`,
	`{"kind":"close","target":{"key":"new"},"reason":"completed","expect":[{"ref":{"id":"a"},"revision":0},{"ref":{"key":"new"},"field":"fields.title","hash":"abc"}]}`,
}

func TestAllOperationsAgainstOpenRPC(t *testing.T) {
	kinds := []OpKind{OpCreate, OpPatch, OpReparent, OpLink, OpUnlink, OpComment, OpAttach, OpDelete, OpSetMeta, OpClose}
	for i, raw := range operationCorpus {
		t.Run(string(kinds[i]), func(t *testing.T) {
			var cs ChangeSet
			roundtripComponent(t, "ChangeSet", `{"ops":[`+raw+`]}`, &cs)
			if len(cs.Ops) != 1 || cs.Ops[0].Kind != kinds[i] {
				t.Fatal("wrong discriminator")
			}
			encoded, err := json.Marshal(cs.Ops[0])
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(encoded), `"create":`) {
				t.Fatal("operation was wrapped")
			}
		})
	}
	var cs ChangeSet
	roundtripComponent(t, "ChangeSet", `{"ops":[`+strings.Join(operationCorpus, ",")+`],"expect":[{"ref":{"id":"a"},"revision":0},{"ref":{"key":"new"},"field":"","hash":""}]}`, &cs)
	if n := cs.Ops[0].Create.Fields["n"].(json.Number); n.String() != "9007199254740993" {
		t.Fatal("field number rounded")
	}
}

func TestCreatePreassignmentAgainstOpenRPC(t *testing.T) {
	for _, raw := range []string{
		`{"ops":[{"kind":"create","type":"issue","fields":{},"assignment":{"kind":"agent","id":"agent:worker.primary"}}]}`,
		`{"ops":[{"kind":"create","type":"issue","fields":{},"assignment":{"kind":"anonymous","id":"worker__primary"}}]}`,
		`{"ops":[{"kind":"create","type":"issue","fields":{},"assignment":{"kind":"pool","id":"pool-alpha"}}]}`,
	} {
		var cs ChangeSet
		roundtripComponent(t, "ChangeSet", raw, &cs)
	}
	for _, raw := range []string{
		`{"ops":[{"kind":"create","type":"issue","fields":{},"assignment":null}]}`,
		`{"ops":[{"kind":"create","type":"issue","fields":{},"assignment":{"kind":"pool","id":""}}]}`,
		`{"ops":[{"kind":"create","type":"issue","fields":{},"assignment":{"kind":"future","id":"pool-alpha"}}]}`,
		`{"ops":[{"kind":"create","type":"issue","fields":{},"assignment":{"kind":"pool","id":"pool-alpha","verified":true}}]}`,
	} {
		rejectComponent(t, "ChangeSet", raw, &ChangeSet{})
	}
}

func TestInvalidOperationUnions(t *testing.T) {
	cases := []string{
		`{"ops":[{"kind":"close"}]}`,
		`{"ops":[{"kind":"close","target":{"id":"a"},"force":true}]}`,
		`{"ops":[{"kind":"close","target":{"id":"a"},"status":"done"}]}`,
		`{"ops":[{"kind":"close","target":{"id":"a"},"expect":[{"ref":{"id":"a"},"revision":1,"hash":"x"}]}]}`,
		`{"ops":[{}]}`, `{"ops":[null]}`, `{"ops":[{"kind":"future"}]}`,
		`{"ops":[{"kind":"delete","target":{"id":"a"},"body":"extra"}]}`,
		`{"ops":[{"kind":"create","type":"task"}]}`,
		`{"ops":[{"kind":"create","type":"task","fields":null}]}`,
		`{"ops":[{"kind":"comment","target":{"id":"a"}}]}`,
		`{"ops":[{"kind":"comment","target":{"id":"a"},"body":null}]}`,
		`{"ops":[{"kind":"reparent","target":{"id":"a"},"parent":null}]}`,
		`{"ops":[{"kind":"setMeta","target":{"id":"a"},"key":"x"}]}`,
		`{"ops":[{"kind":"patch","target":{"id":"a"},"fields":[]}]}`,
		`{"ops":[{"kind":"attach","target":{"id":"a"},"attachment":{"kind":"future","mediaType":"x","summary":"x"}}]}`,
		`{"ops":[{"kind":"link","from":{"id":"a"},"to":{"id":"b"}}]}`,
		`{"Ops":[]}`, `{"ops":null}`, `{"ops":[],"unknown":true}`, `{"ops":[],"expect":null}`, `{}`,
	}
	for _, raw := range cases {
		t.Run(raw, func(t *testing.T) { rejectComponent(t, "ChangeSet", raw, &ChangeSet{}) })
	}
	target := RefByID("a", Natural64(0))
	bad := []Op{
		{}, {Kind: OpDelete}, {Kind: OpDelete, Delete: &DeleteOp{Target: target}, Patch: &PatchOp{Target: target, Fields: map[string]any{}}},
		{Kind: OpPatch, Delete: &DeleteOp{Target: target}},
		{Kind: OpDelete, Delete: &DeleteOp{Kind: OpPatch, Target: target}},
		{Kind: OpCreate, Create: &CreateOp{}},
		{Kind: OpSetMeta, SetMeta: &SetMetaOp{Target: target}},
	}
	for _, op := range bad {
		if _, err := json.Marshal(op); err == nil {
			t.Fatalf("marshaled invalid operation: %#v", op)
		}
	}
}

func TestPreconditionArms(t *testing.T) {
	for _, raw := range []string{
		`{"ref":{"id":""},"revision":0}`, `{"ref":{"key":""},"revision":184467440737095516160}`,
		`{"ref":{"id":"a"},"field":"","hash":""}`, `{"ref":{"key":"new"},"field":"title","hash":"sha256:x"}`,
	} {
		var p Precondition
		roundtripComponent(t, "Expect", raw, &p)
	}
	for _, raw := range []string{
		`{"ref":{"id":"a"}}`, `{"ref":{"id":"a"},"revision":null}`, `{"ref":{"id":"a"},"revision":-1}`,
		`{"ref":{"id":"a"},"revision":0,"field":"","hash":""}`, `{"ref":{"id":"a"},"revision":0,"field":""}`,
		`{"ref":{"id":"a"},"field":""}`, `{"ref":{"id":"a"},"hash":""}`, `{"ref":{"id":"a"},"field":null,"hash":""}`,
	} {
		rejectComponent(t, "Expect", raw, &Precondition{})
	}
	for _, p := range []Precondition{PreconditionByHash(RefByID("", Natural64(0)), "", ""), PreconditionByRevision(RefByKey(""), Natural64(0))} {
		encoded, err := json.Marshal(p)
		if err != nil {
			t.Fatal(err)
		}
		if err := checkComponent(t, "Expect", encoded); err != nil {
			t.Fatal(err)
		}
	}
}

func TestChangeSetLimit(t *testing.T) {
	raw := `{"ops":[` + strings.TrimSuffix(strings.Repeat(operationCorpus[7]+",", MaxChangeSetOps), ",") + `]}`
	var cs ChangeSet
	roundtripComponent(t, "ChangeSet", raw, &cs)
	cs.Ops = append(cs.Ops, cs.Ops[0])
	if err := cs.Validate(); err == nil {
		t.Fatal("1001 operations accepted")
	}
	if _, err := json.Marshal(cs); err == nil {
		t.Fatal("1001 operations marshaled")
	}
	raw = strings.TrimSuffix(raw, "]}") + "," + operationCorpus[7] + "]}"
	rejectComponent(t, "ChangeSet", raw, &ChangeSet{})
	roundtripComponent(t, "ChangeSet", `{"ops":[]}`, &ChangeSet{})
}

func TestEachOperationRequiredMembersAreStrict(t *testing.T) {
	components := []string{"CreateOp", "PatchOp", "ReparentOp", "LinkOp", "LinkOp", "CommentOp", "AttachOp", "DeleteOp", "SetMetaOp", "CloseOp"}
	schemas := componentSchemas(t)
	for i, raw := range operationCorpus {
		t.Run(string(components[i])+string(rune('0'+i)), func(t *testing.T) {
			base := parseJSON(t, []byte(raw)).(map[string]any)
			for _, name := range schemas[components[i]].(map[string]any)["required"].([]any) {
				field := name.(string)
				for _, omit := range []bool{true, false} {
					encoded, _ := json.Marshal(base)
					value := parseJSON(t, encoded).(map[string]any)
					if omit {
						delete(value, field)
					} else {
						value[field] = nil
					}
					// SetMeta's value is arbitrary JSON; an explicit null is valid.
					if !omit && field == "value" {
						continue
					}
					encoded, _ = json.Marshal(value)
					rejectComponent(t, "ChangeSet", `{"ops":[`+string(encoded)+`]}`, &ChangeSet{})
				}
			}
		})
	}
}

func TestFailedDecodeDoesNotPublishPartialUnion(t *testing.T) {
	original := Op{Kind: OpDelete, Delete: &DeleteOp{Target: RefByID("a", Natural64(0))}}
	op := original
	if err := json.Unmarshal([]byte(`{"kind":"comment","target":{"id":"b"}}`), &op); err == nil {
		t.Fatal("invalid decode passed")
	}
	if op.Kind != original.Kind || op.Delete != original.Delete || op.Comment != nil {
		t.Fatal("failed operation decode changed the value")
	}
	var event Event
	if err := json.Unmarshal([]byte(durableJSON("bead.created")), &event); err != nil {
		t.Fatal(err)
	}
	durable := event.Durable
	if err := json.Unmarshal([]byte(controlJSON("event.gap", `{}`, "")), &event); err == nil {
		t.Fatal("invalid event decode passed")
	}
	if event.Durable != durable || event.Control != nil || event.Category != EventDurable {
		t.Fatal("failed event decode changed the value")
	}
}
