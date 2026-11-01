package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func TestAgentCommandsUseFrozenMethodsAndRefuseUnknownFlags(t *testing.T) {
	for _, test := range []struct {
		args   []string
		method string
	}{
		{[]string{"brief", "--budget-tokens", "200"}, "agent.brief"},
		{[]string{"claim-next", "--type", "issue"}, "bead.claimNext"},
		{[]string{"memory", "list", "--limit", "1"}, "memory.list"},
		{[]string{"memory", "get", "memory-id"}, "memory.get"},
		{[]string{"memory", "save", "--key", "lesson", "--content", ""}, "memory.save"},
		{[]string{"memory", "delete", "memory-id"}, "memory.delete"},
	} {
		command, err := ParseCommand(test.args)
		if err != nil || command.Method != test.method {
			t.Fatalf("args=%v method=%s err=%v", test.args, command.Method, err)
		}
	}
	for _, args := range [][]string{
		{"brief", "--budget-tokens", "0"}, {"brief", "--unknown"},
		{"claim-next", "record-id"}, {"claim-next", "--expect", "1"},
		{"memory"}, {"memory", "unknown"}, {"memory", "get"},
		{"memory", "list", "--limit", "1001"},
		{"memory", "save", "--key", "lesson"},
		{"memory", "save", "--key", "lesson", "--content", "x", "--metadata", "[]"},
		{"memory", "delete", "memory-id", "--idempotency-key", "short"},
	} {
		if _, err := ParseCommand(args); err == nil {
			t.Fatalf("accepted unsupported agent command %v", args)
		}
	}
}

func TestAgentCallerSessionRetainsMemoriesAndAtomicClaimReplay(t *testing.T) {
	local, err := OpenLocal(context.Background(), LocalConfig{Project: "agent", Agent: "rewritten", Storage: StorageConfig{Driver: "memory"}})
	if err != nil {
		t.Fatal(err)
	}
	defer local.Close()
	var out, errOut bytes.Buffer
	s := &Session{Client: local.Client, Out: &out, Err: &errOut, JSON: true}
	run := func(args ...string) map[string]any {
		t.Helper()
		out.Reset()
		if err := s.Run(context.Background(), args); err != nil {
			t.Fatal(err)
		}
		var value map[string]any
		if err := json.Unmarshal(out.Bytes(), &value); err != nil {
			t.Fatal(err)
		}
		return value
	}
	run("memory", "save", "--key", "lesson", "--content", "remember this")
	brief := run("brief", "--budget-tokens", "500")
	raw, _ := json.Marshal(brief)
	if !bytes.Contains(raw, []byte("remember this")) {
		t.Fatalf("brief omitted saved memory: %s", raw)
	}
	run("create", "--title", "atomic claim")
	key := strings.Repeat("a", 32)
	first := run("claim-next", "--type", "issue", "--idempotency-key", key)
	id := s.LastID
	replay := run("claim-next", "--type", "issue", "--idempotency-key", key)
	a, _ := json.Marshal(first)
	b, _ := json.Marshal(replay)
	if id == "" || s.LastID != id || !bytes.Equal(a, b) {
		t.Fatalf("claim replay changed result: %s / %s", a, b)
	}
	ready := run("ready")
	if items, ok := ready["items"].([]any); !ok || len(items) != 0 {
		t.Fatalf("claim replay consumed unexpected work: %v", ready)
	}
}
