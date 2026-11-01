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
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/server/webui"
)

//go:embed epic_progress_check.mjs
var epicProgressCheck []byte

// Presentation checks deliberately do not certify hierarchy, authorization,
// ontology binding or the view.run producer. Those require the real RPC path.
func TestBrowserEpicProgressPresentation(t *testing.T) {
	mux := http.NewServeMux()
	ui, err := webui.New(webui.Config{Project: "web", MaxRows: 5})
	if err != nil {
		t.Fatal(err)
	}
	mux.Handle("/ui/", ui)
	mux.HandleFunc("/rpc/v1", func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			ID     string `json:"id"`
			Method string `json:"method"`
		}
		if json.NewDecoder(r.Body).Decode(&request) != nil {
			http.Error(w, "invalid presentation fixture request", http.StatusBadRequest)
			return
		}
		var result any
		switch request.Method {
		case "server.hello":
			result = map[string]any{"authMode": "anonymous"}
		case "rpc.methods":
			result = map[string]any{"methods": []string{}}
		default:
			t.Errorf("presentation check made an unexpected RPC: %s", request.Method)
			http.Error(w, "unexpected presentation RPC", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": request.ID, "result": result})
	})
	server := httptest.NewServer(mux)
	defer server.Close()
	runEpicProgressBrowser(t, server.URL, "presentation")
}

type epicWireCase struct {
	Name     string     `json:"name"`
	MaxRows  int        `json:"-"`
	Total    int        `json:"total"`
	Hidden   int        `json:"hidden"`
	Statuses [][]string `json:"statuses"`
	Code     string     `json:"code,omitempty"`
	Refresh  string     `json:"refresh,omitempty"`
	result   map[string]any
}

func epicWireDefinition() map[string]any {
	return map[string]any{"filter": map[string]any{"query": "parent=pl-parent"}, "fields": []any{}, "groupBy": []any{"epic-progress"}}
}

func epicWireResult() map[string]any {
	return map[string]any{
		"items": []any{}, "asOf": map[string]any{"revision": 17, "incarnation": "epic-contract"},
		"groups": []any{
			map[string]any{"key": "total", "count": 3, "items": []any{}, "elided": map[string]any{
				"count": 7, "reason": "authorization", "continueWith": map[string]any{
					"method": "view.run", "params": map[string]any{"definition": epicWireDefinition(), "_meta": map[string]any{"project": "web"}},
				},
			}},
			map[string]any{"key": "status:open", "count": 1, "items": []any{}},
			map[string]any{"key": "status:resolved", "count": 2, "items": []any{}},
		},
	}
}

// Explicit wire fixtures test the real UI reader, not the EP4 producer. The
// separate stored-graph test must certify that actual runtime boundary.
func TestBrowserEpicProgressWireContract(t *testing.T) {
	visible := func(name, refresh string) epicWireCase {
		return epicWireCase{Name: name, Total: 3, Hidden: 7, Statuses: [][]string{{"open", "1"}, {"resolved", "2"}}, Refresh: refresh, result: epicWireResult()}
	}
	cases := []epicWireCase{visible("visible", ""), visible("refresh-empty", "empty"), visible("refresh-forbidden", "forbidden"), visible("refresh-generation", "incarnation_changed"), visible("cancel", "slow")}
	for _, name := range []string{"empty", "hidden-only"} {
		result := epicWireResult()
		total := result["groups"].([]any)[0].(map[string]any)
		total["count"] = 0
		hidden := 7
		if name == "empty" {
			delete(total, "elided")
			hidden = 0
		}
		result["groups"] = []any{total}
		cases = append(cases, epicWireCase{Name: name, Hidden: hidden, Statuses: [][]string{}, result: result})
	}
	bad := func(name, code string, mutate func(map[string]any)) {
		result := epicWireResult()
		mutate(result)
		cases = append(cases, epicWireCase{Name: name, Code: code, result: result})
	}
	bad("payload", "invalid_response", func(r map[string]any) {
		r["items"] = []any{map[string]any{"id": "unreadable-child", "status": "unreadable-status"}}
	})
	bad("next-cursor", "invalid_response", func(r map[string]any) { r["nextCursor"] = "must-not-be-followed" })
	bad("missing-total", "invalid_response", func(r map[string]any) { r["groups"] = r["groups"].([]any)[1:] })
	bad("duplicate-total", "invalid_response", func(r map[string]any) { r["groups"] = append(r["groups"].([]any), r["groups"].([]any)[0]) })
	bad("status-payload", "invalid_response", func(r map[string]any) {
		r["groups"].([]any)[1].(map[string]any)["items"] = []any{map[string]any{"id": "unreadable-child"}}
	})
	bad("unknown-group", "invalid_response", func(r map[string]any) { r["groups"].([]any)[1].(map[string]any)["key"] = "type:issue" })
	bad("empty-status", "invalid_response", func(r map[string]any) { r["groups"].([]any)[1].(map[string]any)["key"] = "status:" })
	bad("negative", "invalid_response", func(r map[string]any) { r["groups"].([]any)[1].(map[string]any)["count"] = -1 })
	bad("inconsistent", "invalid_response", func(r map[string]any) { r["groups"].([]any)[0].(map[string]any)["count"] = 4 })
	bad("missing-generation", "invalid_response", func(r map[string]any) { delete(r["asOf"].(map[string]any), "incarnation") })
	bad("status-elision", "invalid_response", func(r map[string]any) { r["groups"].([]any)[1].(map[string]any)["elided"] = map[string]any{"count": 1} })
	elision := func(r map[string]any) map[string]any {
		return r["groups"].([]any)[0].(map[string]any)["elided"].(map[string]any)
	}
	bad("page-elision", "invalid_response", func(r map[string]any) { elision(r)["reason"] = "budget" })
	bad("unsafe-hidden", "invalid_response", func(r map[string]any) { elision(r)["count"] = uint64(9007199254740993) })
	bad("child-cap", "invalid_response", func(r map[string]any) { elision(r)["count"] = 20000 })
	bad("refresh-scope", "invalid_response", func(r map[string]any) {
		elision(r)["continueWith"].(map[string]any)["params"].(map[string]any)["_meta"] = map[string]any{"project": "unreadable-project"}
	})
	bad("refresh-cursor", "invalid_response", func(r map[string]any) {
		elision(r)["continueWith"].(map[string]any)["params"].(map[string]any)["cursor"] = "hidden-cursor"
	})
	bad("status-row-cap", "response_exceeds_budget", func(r map[string]any) {
		for i := 0; i < 4; i++ {
			r["groups"] = append(r["groups"].([]any), map[string]any{"key": "status:extra", "count": 0, "items": []any{}})
		}
	})
	// The producer's 100-group cap includes total. A UI configured for 100
	// status rows must still refuse 100 statuses plus total atomically.
	for _, statusGroups := range []int{99, 100} {
		groups := []any{map[string]any{"key": "total", "count": statusGroups, "items": []any{}}}
		statuses := make([][]string, 0, statusGroups)
		for i := 0; i < statusGroups; i++ {
			label := fmt.Sprintf("custom-%02d", i)
			groups = append(groups, map[string]any{"key": "status:" + label, "count": 1, "items": []any{}})
			statuses = append(statuses, []string{label, "1"})
		}
		fixture := epicWireCase{
			Name: fmt.Sprintf("groups-%d", statusGroups+1), MaxRows: 100,
			Total: statusGroups, Statuses: statuses,
			result: map[string]any{"items": []any{}, "asOf": map[string]any{"revision": 17, "incarnation": "epic-contract"}, "groups": groups},
		}
		if statusGroups == 100 {
			fixture.Code = "response_exceeds_budget"
		}
		cases = append(cases, fixture)
	}
	for _, code := range []string{"invalid_params", "scope_too_large", "history_gone"} {
		cases = append(cases, epicWireCase{Name: code, Code: code})
	}
	mux := http.NewServeMux()
	for _, fixture := range cases {
		maxRows := fixture.MaxRows
		if maxRows == 0 {
			maxRows = 5
		}
		mount, rpcPath := "/fixtures/"+fixture.Name+"/ui/", "/fixtures/"+fixture.Name+"/rpc/v1"
		ui, err := webui.New(webui.Config{MountPath: mount, RPCPath: rpcPath, Project: "web", MaxRows: maxRows})
		if err != nil {
			t.Fatal(err)
		}
		mux.Handle(mount, ui)
		if fixture.Name == "visible" {
			root, err := webui.New(webui.Config{RPCPath: rpcPath, Project: "web", MaxRows: maxRows})
			if err != nil {
				t.Fatal(err)
			}
			mux.Handle("/ui/", root)
		}
		var reads atomic.Int64
		mux.HandleFunc(rpcPath, func(w http.ResponseWriter, r *http.Request) {
			var request struct {
				ID     string         `json:"id"`
				Method string         `json:"method"`
				Params map[string]any `json:"params"`
			}
			if json.NewDecoder(r.Body).Decode(&request) != nil {
				http.Error(w, "invalid fixture read", http.StatusBadRequest)
				return
			}
			response := map[string]any{"jsonrpc": "2.0", "id": request.ID}
			switch request.Method {
			case "server.hello":
				response["result"] = map[string]any{"authMode": "anonymous"}
			case "rpc.methods":
				response["result"] = map[string]any{"methods": []string{"view.run"}}
			case "view.run":
				want := map[string]any{"definition": epicWireDefinition(), "_meta": map[string]any{"project": "web"}}
				// Normalize fixture integer/array representations exactly as JSON.
				encoded, _ := json.Marshal(want)
				_ = json.Unmarshal(encoded, &want)
				if !reflect.DeepEqual(request.Params, want) {
					t.Error("epic reader changed canonical parent/count-only/scope request")
				}
				index := reads.Add(1)
				code := ""
				response["result"] = fixture.result
				if fixture.result == nil {
					code = fixture.Code
				}
				if index > 1 {
					switch fixture.Refresh {
					case "slow":
						<-r.Context().Done()
						return
					case "empty":
						response["result"] = map[string]any{"items": []any{}, "asOf": map[string]any{"revision": 18, "incarnation": "epic-contract"}, "groups": []any{map[string]any{"key": "total", "count": 0, "items": []any{}}}}
					case "forbidden", "incarnation_changed":
						code = fixture.Refresh
					}
				}
				if code != "" {
					delete(response, "result")
					response["error"] = map[string]any{"code": -32000, "message": "unreadable-child unreadable-status", "data": map[string]any{"code": code, "hint": "unreadable-child unreadable-status"}}
				}
			default:
				t.Error("epic contract attempted an unadmitted RPC")
				http.Error(w, "unexpected fixture method", http.StatusBadRequest)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(response)
		})
	}
	mux.HandleFunc("/fixtures/cases.json", func(w http.ResponseWriter, _ *http.Request) { _ = json.NewEncoder(w).Encode(cases) })
	server := httptest.NewServer(mux)
	defer server.Close()
	runEpicProgressBrowser(t, server.URL, "contract")
}

func runEpicProgressBrowser(t *testing.T, base, mode string) {
	t.Helper()
	node, err := exec.LookPath("node")
	if err != nil {
		t.Fatal("epic progress checks require Node 24:", err)
	}
	chrome, err := exec.LookPath("google-chrome")
	if err != nil {
		t.Fatal("epic progress checks require google-chrome:", err)
	}
	dir := t.TempDir()
	script := filepath.Join(dir, "epic-progress-check.mjs")
	if err := os.WriteFile(script, epicProgressCheck, 0o600); err != nil {
		t.Fatal(err)
	}
	evidence := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR")
	if evidence == "" {
		evidence = dir
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, node, script, base, chrome, mode, evidence)
	output, err := command.CombinedOutput()
	t.Log(string(output))
	if err != nil {
		t.Fatalf("epic progress browser check failed: %v", err)
	}
}
