package cli

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestParsePreservesPatchValuesAndLargeRevision(t *testing.T) {
	c, err := ParseCommand([]string{"update", "pl-1", "--fields", `{"description":null,"count":9007199254740993}`, "--title", "New title", "--expect", "18446744073709551616"})
	if err != nil {
		t.Fatal(err)
	}
	b, err := json.Marshal(c.Params)
	if err != nil {
		t.Fatal(err)
	}
	const want = `{"expect":[{"ref":{"id":"pl-1"},"revision":18446744073709551616}],"fields":{"count":9007199254740993,"description":null,"title":"New title"},"id":{"id":"pl-1"}}`
	if string(b) != want {
		t.Fatalf("got %s, want %s", b, want)
	}
}

func TestUnsupportedOrAmbiguousInputIsRefused(t *testing.T) {
	cases := [][]string{
		{"heartbeat", "pl-1"}, {"update", "pl-1"}, {"create", "--fields", "null"},
		{"create", "--fields", "{} {}"}, {"comment", "pl-1", "--body", " "},
		{"create", "--fields", `{"title":"first","title":"second"}`},
		{"ready", "--limit", "0"}, {"ready", "--query", "type=issue"},
		{"close", "pl-1", "--expect", "-1"}, {"history"}, {"get", "pl-1", "--as-of", "1e2"},
		{"claim", "pl-1", "extra"}, {"close", "pl-1", "--fields", "{}"},
		{"ontology", "apply", strings.Repeat("a", 64), "--proof", strings.Repeat("a", 64)},
	}
	for _, args := range cases {
		t.Run(args[0]+"/"+args[len(args)-1], func(t *testing.T) {
			if _, err := ParseCommand(args); err == nil {
				t.Fatalf("accepted %q", args)
			}
		})
	}
}

func TestHistoryUsesCanonicalResourceFilter(t *testing.T) {
	c, err := ParseCommand([]string{"history", "--as-of", "0002", "--id", "pl-1"})
	if err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(c.Params)
	if c.Method != "history.query" || string(b) != `{"asOf":{"revision":2},"filter":{"resources":["pl-1"]},"limit":1000}` {
		t.Fatalf("unexpected history: %+v %s", c, b)
	}
}

func TestOntologyCommandsUseCanonicalRPCShapes(t *testing.T) {
	plan, err := ParseCommand([]string{"ontology", "plan", "--document", `{"types":{}}`, "--expect", `[]`})
	if err != nil || plan.Method != "ontology.plan" || plan.Params["document"] == nil {
		t.Fatalf("plan = %+v, %v", plan, err)
	}
	apply, err := ParseCommand([]string{"ontology", "apply", "ab", "--expect", `[]`, "--proof", "digest"})
	if err == nil || apply.Method != "ontology.apply" {
		t.Fatalf("short apply digest accepted: %+v, %v", apply, err)
	}
	apply, err = ParseCommand([]string{"ontology", "apply", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "--expect", `[]`, "--proof", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"})
	if err != nil || apply.Params["humanPresence"] == nil || apply.Key != "" {
		t.Fatalf("apply = %+v, %v", apply, err)
	}
	schema, err := ParseCommand([]string{"ontology", "schema", "--project", "p", "--type", "issue", "--status", "todo"})
	if err != nil || schema.Params["project"] != "p" || schema.Params["type"] != "issue" {
		t.Fatalf("schema = %+v, %v", schema, err)
	}
	base, err := ParseCommand([]string{"ontology", "get", "--project", ""})
	if err != nil {
		t.Fatal(err)
	}
	if project, ok := base.Params["project"].(string); !ok || project != "" {
		t.Fatalf("explicit empty project was not preserved: %+v", base)
	}
}
