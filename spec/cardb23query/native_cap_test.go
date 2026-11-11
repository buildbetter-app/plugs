package cardb20

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"reflect"
	"strings"
	"time"
)

// Extend this test's own seven-row native fixture through real public batches.
// No prior native runtime is borrowed, no SQL writes seed the corpus, and no
// 20,001-row archive is relabelled as a supported import.
func (n *b23Native) exactCap() {
	n.t.Helper()
	wantIDs := map[string]bool{}
	for _, id := range queryIDs {
		wantIDs[id] = true
	}
	for start := 0; start < 19993; start += 1000 {
		end := min(start+1000, 19993)
		items := make([]any, 0, end-start)
		for i := start; i < end; i++ {
			id := fmt.Sprintf("b23-bulk%05d", i)
			wantIDs[id] = true
			items = append(items, map[string]any{"kind": "create", "create": map[string]any{"id": id, "title": "ordinary B23 cap row", "issue_type": "task", "status": "open", "priority": 2}})
		}
		n.capCreate(fmt.Sprintf("cap-create-%05d", start), items)
	}
	if len(wantIDs) != 20000 {
		n.t.Fatal("independent cap expectation is not exactly 20,000 IDs")
	}
	n.count("cap-20000-full-count", "", 20000)
	raw := n.command("cap-20000-full-list", "--readonly", "--json", "list", "--all", "--limit", "0", "--max-rows", "20000")
	var rows []struct {
		ID, Title, Status string
	}
	if err := json.Unmarshal(raw, &rows); err != nil || len(rows) != 20000 {
		n.t.Fatalf("native exact-boundary list rows=%d: %v", len(rows), err)
	}
	seen := map[string]bool{}
	for _, row := range rows {
		if !wantIDs[row.ID] || seen[row.ID] {
			n.t.Fatal("native exact-boundary membership lost or repeated an ID", row.ID)
		}
		seen[row.ID] = true
		if strings.HasPrefix(row.ID, "b23-bulk") && (row.Title != "ordinary B23 cap row" || row.Status != "open") {
			n.t.Fatal("native committed bulk values changed", row)
		}
	}
	n.capCreate("cap-cross-boundary", []any{map[string]any{"kind": "create", "create": map[string]any{"id": "b23-bulk19993", "title": "ordinary B23 cap row", "issue_type": "task", "status": "open", "priority": 2}}})
	n.count("cap-20001-full-count", "", 20001)
	// Native's scalar count has no list cap. The unbounded list refuses with
	// its exact cap diagnostic and no partial stdout; these are distinct facts.
	before := n.state("cap-refusal-before")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, n.binary, "--readonly", "--json", "list", "--all", "--limit", "0", "--max-rows", "20000")
	cmd.Dir, cmd.Env = n.work, n.env
	var out, errOut bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errOut
	err := cmd.Run()
	n.save("cap-20001-refusal-command.json", map[string]any{"argv": cmd.Args, "cwd": cmd.Dir, "environment": cmd.Env, "stdout": out.String(), "stderr": errOut.String(), "error": fmt.Sprint(err), "exit_code": cmd.ProcessState.ExitCode()})
	if exit, ok := err.(*exec.ExitError); !ok || exit.ExitCode() != 2 || out.Len() != 0 || !strings.Contains(errOut.String(), "Error: too many rows: 20001 found, --max-rows=20000 exceeded.") {
		n.t.Fatalf("native cap refused incorrectly: error=%v stdout=%q stderr=%q", err, out.String(), errOut.String())
	}
	// A narrow full predicate remains admitted at the same physical corpus.
	filtered := n.command("cap-filtered-list", "--readonly", "--json", "list", "--all", "--title", "Needle", "--limit", "0", "--max-rows", "20000")
	var narrow []struct{ ID string }
	if err := json.Unmarshal(filtered, &narrow); err != nil || len(narrow) != 6 {
		n.t.Fatalf("native cap preceded its complete text predicate: rows=%d %v", len(narrow), err)
	}
	after := n.state("cap-refusal-after")
	if !reflect.DeepEqual(before, after) {
		n.t.Fatal("native cap refusal or filtered reads changed physical state")
	}
	n.save("cap-disposition.json", map[string]any{"native_durable_count": 20001, "accepted_list_boundary": 20000, "scalar_count": 20001, "list_cap": "typed too-many-rows exit2 with empty stdout", "state_unchanged": true, "native_source_import_claim": false, "inherited_Plug_ordinary_cap": "PR462 retained, no native transfer"})
}

func (n *b23Native) capCreate(label string, items []any) {
	n.t.Helper()
	body := n.request(label, "POST", "/v0/beads/issues:batchApply", map[string]any{"actor": "b23-recorder", "items": items}, 200)
	var results []struct{ Changed bool }
	if err := json.Unmarshal(body["items"], &results); err != nil || len(results) != len(items) {
		n.t.Fatalf("native public cap batch lost results: %d want=%d %v", len(results), len(items), err)
	}
	for _, result := range results {
		if !result.Changed {
			n.t.Fatal("native public cap batch did not create every row")
		}
	}
}
