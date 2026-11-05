package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/api/wire"
)

// i8FixtureRPC drives the actual admitted Plug HTTP endpoint. It deliberately
// keeps project metadata on every project call and leaves server.hello bare.
func i8FixtureRPC(t *testing.T, endpoint, project, method string, params map[string]any, key string) sqliteRestartResponse {
	t.Helper()
	p := make(map[string]any, len(params)+1)
	for name, value := range params {
		p[name] = value
	}
	if method != "server.hello" {
		meta := map[string]any{"project": project, "agent": "i8-heartbeads-fixture"}
		if key != "" {
			meta["idempotencyKey"] = key
		}
		p["_meta"] = meta
	}
	body, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": p})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint+"/rpc/v1", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Plug-RPC", "1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s HTTP: %v", method, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("%s status=%d body=%s err=%v", method, resp.StatusCode, raw, err)
	}
	var out sqliteRestartResponse
	if err := wire.DecodeInto(raw, &out); err != nil || out.JSONRPC != "2.0" || out.ID != 1 {
		t.Fatalf("%s response=%s err=%v", method, raw, err)
	}
	return out
}

func i8CommitID(t *testing.T, method string, response sqliteRestartResponse) string {
	t.Helper()
	if response.Error != nil {
		t.Fatalf("%s refused: %+v", method, response.Error)
	}
	var value struct {
		Results []struct {
			Ref struct {
				ID string `json:"id"`
			} `json:"ref"`
		} `json:"results"`
	}
	if err := wire.DecodeInto(response.Result, &value); err != nil || len(value.Results) != 1 || value.Results[0].Ref.ID == "" {
		t.Fatalf("%s commit=%s err=%v", method, response.Result, err)
	}
	return value.Results[0].Ref.ID
}

func i8WriteJournal(t *testing.T, dir, project, endpoint, incarnation string, ids ...string) string {
	t.Helper()
	workspace := filepath.Join(dir, project)
	if err := os.MkdirAll(workspace, 0700); err != nil {
		t.Fatal(err)
	}
	calls := make([]map[string]any, 0, len(ids))
	for _, id := range ids {
		calls = append(calls, map[string]any{
			"method":   "bead.create",
			"attempts": []any{map[string]any{"params": map[string]any{"type": "issue"}}},
			"result":   map[string]any{"results": []any{map[string]any{"ref": map[string]any{"id": id}}}},
		})
	}
	journal := map[string]any{"schema": "plug-host-journal/1", "jobs": map[string]any{"i8": map[string]any{
		"scope": "actual-fixture", "calls": calls,
	}}}
	path := filepath.Join(workspace, ".plug-host-journal.json")
	data, err := json.MarshalIndent(journal, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestI8HeartbeadsActualSQLiteTwoProjectFixture(t *testing.T) {
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, ".plug"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, ".plug", "config.yaml"), []byte("driver: sqlite\ndsn: server.db\nlisten: 127.0.0.1:0\nproject: cody-ops\nui: false\n"), 0600); err != nil {
		t.Fatal(err)
	}
	server := startPlugServer(t, dir, false)
	t.Cleanup(func() { server.stop(t) })
	endpoint := server.endpoint

	hello := i8FixtureRPC(t, endpoint, "", "server.hello", map[string]any{}, "")
	if hello.Error != nil {
		t.Fatalf("bare server.hello refused: %+v", hello.Error)
	}
	var helloValue struct {
		Incarnation string `json:"incarnation"`
	}
	if err := wire.DecodeInto(hello.Result, &helloValue); err != nil || helloValue.Incarnation == "" {
		t.Fatalf("server.hello=%s err=%v", hello.Result, err)
	}
	incarnation := helloValue.Incarnation

	projects := []string{"cody-ops", "signals-v2"}
	roots := make(map[string]string, len(projects))
	children := make(map[string]string, len(projects))
	for _, project := range projects {
		rootResp := i8FixtureRPC(t, endpoint, project, "bead.create", map[string]any{
			"type": "issue", "fields": map[string]any{
				"title": "SHARED root " + project, "description": "before", "priority": 1,
				"external_ref": "SHARED", "labels": []string{"fixture", project},
			},
		}, fmt.Sprintf("%032d", len(roots)+1))
		roots[project] = i8CommitID(t, "bead.create", rootResp)
		childParams := map[string]any{
			"type": "issue", "fields": map[string]any{"title": "receipt new " + project, "priority": 2, "external_ref": "NEW"},
		}
		if project == "signals-v2" {
			childParams["parent"] = map[string]any{"id": roots[project]}
		}
		childResp := i8FixtureRPC(t, endpoint, project, "bead.create", childParams, fmt.Sprintf("%032d", len(children)+11))
		children[project] = i8CommitID(t, "bead.create", childResp)
		if project == "cody-ops" {
			link := i8FixtureRPC(t, endpoint, project, "bead.link", map[string]any{
				"from": map[string]any{"id": children[project]}, "to": map[string]any{"id": roots[project]}, "kind": "blocks",
			}, fmt.Sprintf("%032d", len(children)+21))
			if link.Error != nil {
				t.Fatalf("cody block link refused: %+v", link.Error)
			}
		}
	}

	// The lifecycle reason is a typed metadata slot in the same atomic tx as
	// CloseOp. Reopen/update/reclose proves amended fields and reason survive.
	closeWithReason := func(project, id, reason, key string) {
		value, _ := json.Marshal(map[string]any{"schema": "plug-host-close-reason/1", "reason": reason})
		response := i8FixtureRPC(t, endpoint, project, "tx.apply", map[string]any{"changeSet": map[string]any{"ops": []any{
			map[string]any{"kind": "close", "target": map[string]any{"id": id}, "reason": reason},
			map[string]any{"kind": "setMeta", "target": map[string]any{"id": id}, "key": "i8.host.close_reason", "value": json.RawMessage(value)},
		}}}, key)
		if response.Error != nil {
			t.Fatalf("typed close %s refused: %+v", project, response.Error)
		}
	}
	closeWithReason("cody-ops", roots["cody-ops"], "original", "0000000000000000000000000000000001")
	closed := i8FixtureRPC(t, endpoint, "cody-ops", "bead.get", map[string]any{"id": map[string]any{"id": roots["cody-ops"]}}, "")
	if closed.Error != nil {
		t.Fatalf("closed readback refused: %+v", closed.Error)
	}
	reopen := i8FixtureRPC(t, endpoint, "cody-ops", "bead.reopen", map[string]any{"id": map[string]any{"id": roots["cody-ops"]}}, "0000000000000000000000000000000002")
	if reopen.Error != nil {
		t.Fatalf("reopen refused: %+v", reopen.Error)
	}
	reopened := i8FixtureRPC(t, endpoint, "cody-ops", "bead.get", map[string]any{"id": map[string]any{"id": roots["cody-ops"]}}, "")
	if strings.Contains(string(reopened.Result), "plug-host-close-reason") {
		t.Fatalf("reopen projection retained typed close reason: %s", reopened.Result)
	}
	updated := i8FixtureRPC(t, endpoint, "cody-ops", "bead.update", map[string]any{"id": map[string]any{"id": roots["cody-ops"]}, "fields": map[string]any{"description": "after"}}, "0000000000000000000000000000000003")
	if updated.Error != nil {
		t.Fatalf("field update refused: %+v", updated.Error)
	}
	closeWithReason("cody-ops", roots["cody-ops"], "amended", "0000000000000000000000000000000004")
	final := i8FixtureRPC(t, endpoint, "cody-ops", "bead.get", map[string]any{"id": map[string]any{"id": roots["cody-ops"]}}, "")
	if final.Error != nil || !strings.Contains(string(final.Result), "after") || !strings.Contains(string(final.Result), "amended") {
		t.Fatalf("final amended readback missing field/reason: error=%+v result=%s", final.Error, final.Result)
	}

	for _, project := range projects {
		read := i8FixtureRPC(t, endpoint, project, "bead.get", map[string]any{"id": map[string]any{"id": roots[project]}}, "")
		if read.Error != nil {
			t.Fatalf("%s root readback refused: %+v", project, read.Error)
		}
		child := i8FixtureRPC(t, endpoint, project, "bead.get", map[string]any{"id": map[string]any{"id": children[project]}}, "")
		if child.Error != nil {
			t.Fatalf("%s child readback refused: %+v", project, child.Error)
		}
		ready := i8FixtureRPC(t, endpoint, project, "bead.ready", map[string]any{"limit": 10}, "")
		if ready.Error != nil {
			t.Fatalf("%s ready refused: %+v", project, ready.Error)
		}
	}

	badView := i8FixtureRPC(t, endpoint, "cody-ops", "view.run", map[string]any{"limit": 10}, "")
	if badView.Error == nil || badView.Error.Data.Code != "method_not_found" {
		t.Fatalf("expected pinned Plug to refuse unbound view.run, got %+v", badView)
	}
	wrongProject := i8FixtureRPC(t, endpoint, "signals-v2", "bead.get", map[string]any{"id": map[string]any{"id": roots["cody-ops"]}}, "")
	if wrongProject.Error == nil {
		t.Fatalf("cross-project canonical read unexpectedly succeeded: %s", wrongProject.Result)
	}

	journalOps := i8WriteJournal(t, dir, "cody-ops", endpoint, incarnation, roots["cody-ops"], children["cody-ops"])
	journalSig := i8WriteJournal(t, dir, "signals-v2", endpoint, incarnation, roots["signals-v2"], children["signals-v2"])
	binding := map[string]any{"schema": "plug-host-binding/1", "endpoint": endpoint, "incarnation": incarnation, "principal": map[string]any{"kind": "anonymous", "id": "anonymous"}, "agent": "i8-heartbeads-fixture", "projects": map[string]any{
		"cody-ops":   map[string]any{"workspace": filepath.Join(dir, "cody-ops"), "legacy_ids": map[string]any{"SHARED": map[string]any{"id": roots["cody-ops"]}}},
		"signals-v2": map[string]any{"workspace": filepath.Join(dir, "signals-v2"), "legacy_ids": map[string]any{"SHARED": map[string]any{"id": roots["signals-v2"]}}},
	}}
	bindingBytes, _ := json.MarshalIndent(binding, "", "  ")
	bindingPath := filepath.Join(dir, "host-binding.json")
	if err := os.WriteFile(bindingPath, bindingBytes, 0600); err != nil {
		t.Fatal(err)
	}

	server.stop(t)
	server = startPlugServer(t, dir, false)
	for _, project := range projects {
		read := i8FixtureRPC(t, server.endpoint, project, "bead.get", map[string]any{"id": map[string]any{"id": roots[project]}}, "")
		if read.Error != nil {
			t.Fatalf("%s durable restart readback refused: %+v", project, read.Error)
		}
	}
	t.Logf("I8_ACTUAL_FIXTURE_PASS endpoint=%s incarnation=%s projects=%v colliding_legacy=SHARED canonical=%v new_ids=%v typed_reason=amended field=after journals=[%s,%s] binding=%s", server.endpoint, incarnation, projects, roots, children, journalOps, journalSig, bindingPath)
	t.Logf("I8_ACTUAL_FIXTURE_BLOCK view.run is not bound by pinned Plug RPC registry; adapter contract requires view.run, so direct Heartbeads adapter qualification remains blocked until an authorized host view binding exists")
}
