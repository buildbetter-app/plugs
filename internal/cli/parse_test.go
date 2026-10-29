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
