package cli

import (
	"encoding/json"
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
