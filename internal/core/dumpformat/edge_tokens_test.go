package dumpformat

import (
	"reflect"
	"strings"
	"testing"
)

func TestB07EdgeTokensSurviveHeadAndNestedHistoryDecode(t *testing.T) {
	const raw = `{"kind":"edge","id":"e","project":"p","body":{},"source":{"table":"dependencies","fields":{"id":"e","thread_id":"\\path\u03b1","metadata":null,"future_number":1.00e+02,"future_object":{ "wide":9007199254740993, "empty":"" }}}}`
	want := map[string]string{
		"beads.thread_id":     `"\\path\u03b1"`,
		"beads.metadata":      `null`,
		"beads.future_number": `1.00e+02`,
		"beads.future_object": `{ "wide":9007199254740993, "empty":"" }`,
	}
	var head Record
	if err := decodeJSON([]byte(raw), &head); err != nil {
		t.Fatal(err)
	}
	var history struct {
		Frames []struct {
			Records []Record `json:"records"`
		} `json:"frames"`
	}
	if err := decodeJSON([]byte(`{"frames":[{"records":[`+raw+`]}]}`), &history); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(head.NativeEdgeTokens, want) || !reflect.DeepEqual(history.Frames[0].Records[0], head) {
		t.Fatal("head/nested history lost exact opaque token bytes")
	}
	for _, invalid := range []string{
		strings.Replace(raw, `"project":"p"`, `"project":"p","project":"p"`, 1),
		strings.Replace(raw, `"project":"p"`, `"project":"p","NativeEdgeTokens":{}`, 1),
		strings.Replace(raw, `"thread_id":"\\path\u03b1"`, `"thread_id":"\ud800"`, 1),
	} {
		before := head
		if err := decodeJSON([]byte(invalid), &head); err == nil || !reflect.DeepEqual(before, head) {
			t.Fatal("invalid record admitted or changed previously decoded value")
		}
		if err := decodeJSON([]byte(`{"frames":[{"records":[`+invalid+`]}]}`), &history); err == nil {
			t.Fatal("nested record bypassed the strict grammar")
		}
	}
}
