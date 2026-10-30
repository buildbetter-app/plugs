package webui_test

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/server/webui"
)

//go:embed presence_display_check.mjs
var presenceDisplayCheck []byte

type presenceDisplayFixture struct {
	Name       string           `json:"name"`
	Presence   []map[string]any `json:"presence"`
	Names      []string         `json:"names,omitempty"`
	Identities []string         `json:"identities,omitempty"`
	Labels     []string         `json:"labels,omitempty"`
	Refused    bool             `json:"refused,omitempty"`
	Refresh    string           `json:"refresh,omitempty"`
}

// These are explicit consumer contract fixtures, not a collab implementation
// or evidence of bound session.open normalization, policy, expiry, or E3.
func TestBrowserPresenceDisplayNameContractFixtures(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		t.Fatal("display-name browser fixtures require Node 24:", err)
	}
	chrome, err := exec.LookPath("google-chrome")
	if err != nil {
		t.Fatal("display-name browser fixtures require google-chrome:", err)
	}
	member := func(session, kind, id string, label any) map[string]any {
		entry := map[string]any{
			"sessionId": session, "principal": map[string]any{"kind": kind, "id": id},
			"focus": "bead/pl-focus", "workingOn": []string{"bead/pl-a", "bead/pl-b", "bead/pl-c", "bead/pl-d"},
		}
		if label != nil {
			entry["displayName"] = label
		}
		return entry
	}
	anonymous := func(label any) map[string]any { return member("fixture-session-1", "anonymous", "anonymous", label) }
	delegated := member("fixture-session-delegated", "agent", "api-key-1", "on behalf of forged-sponsor")
	delegated["onBehalfOf"] = "actual-sponsor"
	fixtures := []presenceDisplayFixture{
		{Name: "named", Presence: []map[string]any{anonymous("worker A")}, Names: []string{"anonymous"}, Identities: []string{"Anonymous principal"}, Labels: []string{"worker A"}},
		{Name: "absent", Presence: []map[string]any{anonymous(nil)}, Names: []string{"anonymous"}, Identities: []string{"Anonymous principal · client label unavailable"}, Labels: []string{""}},
		{Name: "identity", Presence: []map[string]any{member("fixture-session-key", "agent", "api-key-1", "user:owner")}, Names: []string{"agent:api-key-1"}, Identities: []string{"agent:api-key-1 principal"}, Labels: []string{"user:owner"}},
		{Name: "delegated", Presence: []map[string]any{delegated}, Names: []string{"on behalf of actual-sponsor"}, Identities: []string{"agent:api-key-1 principal"}, Labels: []string{"on behalf of forged-sponsor"}},
		{Name: "distinct", Presence: []map[string]any{member("fixture-session-a", "user", "same-user", "worker A"), member("fixture-session-b", "user", "same-user", "worker B")}, Names: []string{"user:same-user", "user:same-user"}, Identities: []string{"user:same-user principal", "user:same-user principal"}, Labels: []string{"worker A", "worker B"}},
		{Name: "same-label", Presence: []map[string]any{member("fixture-session-a", "user", "same-user", "worker"), member("fixture-session-b", "user", "same-user", "worker")}, Names: []string{"user:same-user", "user:same-user"}, Identities: []string{"user:same-user principal", "user:same-user principal"}, Labels: []string{"worker", "worker"}},
		{Name: "escaped", Presence: []map[string]any{anonymous(`<img src=x onerror="globalThis.labelExecuted=true"> & '`)}, Names: []string{"anonymous"}, Identities: []string{"Anonymous principal"}, Labels: []string{`<img src=x onerror="globalThis.labelExecuted=true"> & '`}},
		{Name: "ascii64", Presence: []map[string]any{anonymous(strings.Repeat("x", 64))}, Names: []string{"anonymous"}, Identities: []string{"Anonymous principal"}, Labels: []string{strings.Repeat("x", 64)}},
		{Name: "scalar64", Presence: []map[string]any{anonymous(strings.Repeat("\U0001f600", 64))}, Names: []string{"anonymous"}, Identities: []string{"Anonymous principal"}, Labels: []string{strings.Repeat("\U0001f600", 64)}},
		{Name: "combining64", Presence: []map[string]any{anonymous(strings.Repeat("e\u0301", 32))}, Names: []string{"anonymous"}, Identities: []string{"Anonymous principal"}, Labels: []string{strings.Repeat("e\u0301", 32)}},
		{Name: "internal-space", Presence: []map[string]any{anonymous("worker\u2003A")}, Names: []string{"anonymous"}, Identities: []string{"Anonymous principal"}, Labels: []string{"worker\u2003A"}},
		{Name: "empty-after-refresh", Presence: []map[string]any{anonymous("ephemeral")}, Names: []string{"anonymous"}, Identities: []string{"Anonymous principal"}, Labels: []string{"ephemeral"}, Refresh: "empty"},
		{Name: "refused-after-refresh", Presence: []map[string]any{anonymous("masked")}, Names: []string{"anonymous"}, Identities: []string{"Anonymous principal"}, Labels: []string{"masked"}, Refresh: "forbidden"},
	}
	invalid := map[string]any{
		"empty-output": "", "null-output": json.RawMessage(`null`), "number-output": 7,
		"control": "worker\nA", "format": "worker\u202eA", "format-bom": "\ufeffworker",
		"padded": "\u2003worker\u00a0", "unicode-nel": "\u0085worker\u0085",
		"scalar65": strings.Repeat("\U0001f600", 65), "combining65": strings.Repeat("e\u0301", 32) + "e",
		"surrogate": json.RawMessage(`"\ud800"`),
	}
	names := make([]string, 0, len(invalid))
	for name := range invalid {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		label := invalid[name]
		fixtures = append(fixtures, presenceDisplayFixture{Name: name, Presence: []map[string]any{anonymous(label)}, Refused: true})
	}
	compact := presenceDisplayFixture{Name: "compact"}
	for i := 0; i < 7; i++ {
		label := fmt.Sprintf("worker %d", i+1)
		compact.Presence = append(compact.Presence, member(fmt.Sprintf("fixture-session-%d", i+1), "user", "same-user", label))
		if i < 5 {
			compact.Names = append(compact.Names, "user:same-user")
			compact.Identities = append(compact.Identities, "user:same-user principal")
			compact.Labels = append(compact.Labels, label)
		}
	}
	fixtures = append(fixtures, compact, presenceDisplayFixture{Name: "empty", Presence: []map[string]any{}}, presenceDisplayFixture{Name: "missing-principal", Presence: []map[string]any{{"sessionId": "fixture-session-no-principal", "displayName": "worker"}}, Refused: true})

	mux := http.NewServeMux()
	for _, fixture := range fixtures {
		rpcPath := "/fixtures/" + fixture.Name + "/rpc"
		ui, err := webui.New(webui.Config{MountPath: "/fixtures/" + fixture.Name + "/ui/", RPCPath: rpcPath, Project: "web", MaxRows: 5})
		if err != nil {
			t.Fatal(err)
		}
		mux.Handle("/fixtures/"+fixture.Name+"/ui/", ui)
		var reads atomic.Int64
		mux.HandleFunc(rpcPath, func(w http.ResponseWriter, r *http.Request) {
			var request struct {
				ID     string         `json:"id"`
				Method string         `json:"method"`
				Params map[string]any `json:"params"`
			}
			if r.Method != http.MethodPost || r.Header.Get("Plug-RPC") != "1" || json.NewDecoder(r.Body).Decode(&request) != nil {
				t.Error("consumer fixture received an invalid read request")
				http.Error(w, "invalid fixture read", http.StatusBadRequest)
				return
			}
			response := map[string]any{"jsonrpc": "2.0", "id": request.ID}
			switch request.Method {
			case "server.hello":
				response["result"] = map[string]any{"authMode": "anonymous"}
			case "rpc.methods":
				response["result"] = map[string]any{"methods": []string{"presence.list"}}
			case "presence.list":
				presence := fixture.Presence
				if reads.Add(1) > 1 && fixture.Refresh != "" {
					presence = []map[string]any{}
				}
				response["result"] = map[string]any{"presence": presence}
				if reads.Load() > 1 && fixture.Refresh == "forbidden" {
					delete(response, "result")
					response["error"] = map[string]any{"code": -32000, "message": "fixture access revoked", "data": map[string]any{"code": "forbidden"}}
				}
			default:
				t.Error("consumer fixture received an unexpected method")
				http.Error(w, "unexpected fixture method", http.StatusBadRequest)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(response); err != nil {
				t.Error("consumer fixture response could not be encoded")
			}
		})
	}
	server := httptest.NewServer(mux)
	defer server.Close()
	dir := t.TempDir()
	script, cases := filepath.Join(dir, "presence-display-check.mjs"), filepath.Join(dir, "fixtures.json")
	data, err := json.Marshal(fixtures)
	if err != nil {
		t.Fatal(err)
	}
	for name, data := range map[string][]byte{script: presenceDisplayCheck, cases: data} {
		if err := os.WriteFile(name, data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, node, script, server.URL, chrome, cases)
	output, err := command.CombinedOutput()
	fmt.Print(string(output))
	if err != nil {
		t.Fatalf("display-name browser contract fixtures failed: %v; output above", err)
	}
}
