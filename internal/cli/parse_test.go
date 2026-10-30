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

func TestParseDependencyAndBatchWrites(t *testing.T) {
	link, err := ParseCommand([]string{"link", "pl-from", "pl-to", "--kind", "blocks", "--expect", "7", "--idempotency-key", "550e8400-e29b-41d4-a716-446655440000"})
	if err != nil {
		t.Fatal(err)
	}
	if link.Method != "bead.link" || link.Key == "" {
		t.Fatalf("link command=%+v", link)
	}
	wantLink := `{"expect":[{"ref":{"id":"pl-from"},"revision":7}],"from":{"id":"pl-from"},"kind":"blocks","to":{"id":"pl-to"}}`
	if got, _ := json.Marshal(link.Params); string(got) != wantLink {
		t.Fatalf("link params=%s want=%s", got, wantLink)
	}

	batch, err := ParseCommand([]string{"batch", `{"ops":[{"kind":"link","from":{"id":"pl-from"},"to":{"id":"pl-to"},"linkKind":"blocks"}]}`, "--idempotency-key", "550e8400-e29b-41d4-a716-446655440001"})
	if err != nil {
		t.Fatal(err)
	}
	if batch.Method != "tx.apply" || batch.Key == "" {
		t.Fatalf("batch command=%+v", batch)
	}
	wantBatch := `{"changeSet":{"ops":[{"from":{"id":"pl-from"},"kind":"link","linkKind":"blocks","to":{"id":"pl-to"}}]}}`
	if got, _ := json.Marshal(batch.Params); string(got) != wantBatch {
		t.Fatalf("batch params=%s want=%s", got, wantBatch)
	}
}

func TestParseDependencyAndBatchRejectInvalidInput(t *testing.T) {
	for _, args := range [][]string{
		{"link", "pl-from", "pl-to"},
		{"unlink", "pl-from", "pl-to", "--kind", " "},
		{"link", "pl-from", "pl-to", "--kind", "blocks", "extra"},
		{"batch"},
		{"batch", "--changeset", `{"ops":[]}`, "extra"},
		{"batch", "--changeset", "[]"},
	} {
		t.Run(args[0], func(t *testing.T) {
			if _, err := ParseCommand(args); err == nil {
				t.Fatalf("accepted %q", args)
			}
		})
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

func TestReadCommandParsersUseCanonicalRPCMethods(t *testing.T) {
	cases := []struct {
		args   []string
		method string
		check  func(map[string]any) bool
	}{
		{[]string{"events", "--event", "bead.updated", "--resource", "bead/pl-1", "--limit", "2"}, "event.list", func(p map[string]any) bool {
			f := p["filter"].(map[string]any)
			return p["limit"] == 2 && f["events"].([]string)[0] == "bead.updated" && f["resources"].([]string)[0] == "bead/pl-1"
		}},
		{[]string{"history", "get", "pl-1", "--as-of", "3"}, "history.get", func(p map[string]any) bool {
			return p["ref"].(map[string]any)["id"] == "pl-1" && p["asOf"].(map[string]any)["revision"].(json.Number) == "3"
		}},
		{[]string{"history", "diff", "pl-1", "--from", "1", "--to", "3"}, "history.diff", func(p map[string]any) bool {
			return p["fromRevision"].(json.Number) == "1" && p["toRevision"].(json.Number) == "3"
		}},
		{[]string{"health"}, "admin.health", func(p map[string]any) bool { return len(p) == 0 }},
	}
	for _, tc := range cases {
		t.Run(tc.method, func(t *testing.T) {
			command, err := ParseCommand(tc.args)
			if err != nil || command.Method != tc.method || !tc.check(command.Params) {
				t.Fatalf("command=%+v err=%v", command, err)
			}
		})
	}
}

func TestParseNamedLifecycleCommands(t *testing.T) {
	for _, name := range []string{"close", "reopen", "defer", "cancel", "fail"} {
		t.Run(name, func(t *testing.T) {
			command, err := ParseCommand([]string{name, "pl-1", "--reason", "evidence", "--expect", "7", "--idempotency-key", "00000000000000000000000000000001"})
			if err != nil {
				t.Fatal(err)
			}
			if command.Method != "bead."+name || command.Key == "" {
				t.Fatalf("command=%+v", command)
			}
			params, ok := command.Params["id"].(map[string]any)
			if !ok || params["id"] != "pl-1" || command.Params["reason"] != "evidence" {
				t.Fatalf("lifecycle parameters were not preserved: %+v", command.Params)
			}
			expect, ok := command.Params["expect"].([]any)
			if !ok || len(expect) != 1 || expect[0].(map[string]any)["revision"] != json.Number("7") {
				t.Fatalf("lifecycle CAS was not parsed: %+v", command.Params)
			}
		})
	}
}
