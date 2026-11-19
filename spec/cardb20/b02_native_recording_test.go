package cardb20

import (
	"bytes"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/publicnative"
)

// This explicit manual target executes the canonical public CLI. Ordinary
// source/whole tests do not call it and cannot receive native runtime credit.
func TestB02CanonicalNativeBlocking(t *testing.T) {
	t.Setenv("PATH", "/tmp/plug-swarm/bin/worker-safe:"+os.Getenv("PATH"))
	root := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR")
	checkout := os.Getenv("PLUG_SOURCE_MODULE_ROOT")
	request := os.Getenv("PLUG_B02_REQUEST_SHA")
	if !filepath.IsAbs(root) || !filepath.IsAbs(checkout) || len(request) != 40 {
		t.Fatal("declared output, runner checkout and finite B02 request SHA required")
	}
	r := &nativeRecorder{t: t, root: root}
	actual := strings.TrimSpace(string(r.buildCommand(checkout, "b02-candidate-head", "/usr/bin/git", []string{"rev-parse", "HEAD"})))
	if actual != request {
		t.Fatal("B02 request differs from actual candidate checkout")
	}
	tuple := r.buildCommand(checkout, "b02-candidate-tuple", "/usr/bin/git", []string{"show", "-s", "--format=%H%n%T%n%P", "HEAD"})
	rawCommit := r.buildCommand(checkout, "b02-candidate-commit", "/usr/bin/git", []string{"cat-file", "commit", "HEAD"})
	// A shallow checkout may display an empty %P despite a real parent header.
	// Bind the tree and ordered parents to the retained immutable commit bytes.
	rawIdentity := sha1.Sum(append([]byte(fmt.Sprintf("commit %d\x00", len(rawCommit))), rawCommit...))
	if hex.EncodeToString(rawIdentity[:]) != request {
		t.Fatal("retained raw commit does not hash to the requested Git object")
	}
	tree := ""
	parents := []string{}
	for _, header := range strings.Split(strings.SplitN(string(rawCommit), "\n\n", 2)[0], "\n") {
		key, value, _ := strings.Cut(header, " ")
		if key != "tree" && key != "parent" {
			continue
		}
		decoded, err := hex.DecodeString(value)
		if err != nil || len(decoded) != 20 || strings.ToLower(value) != value {
			t.Fatal("invalid raw Git tree or parent identity")
		}
		if key == "tree" {
			if tree != "" {
				t.Fatal("duplicate raw Git tree header")
			}
			tree = value
		} else {
			parents = append(parents, value)
		}
	}
	displayed := strings.Split(string(tuple), "\n")
	if tree == "" || len(parents) == 0 || len(displayed) < 2 || displayed[0] != request || displayed[1] != tree {
		t.Fatal("candidate display, retained commit tree and ordered parents are not bound")
	}
	input, err := os.ReadFile(filepath.Join(checkout, "spec/s1-b09-native/native_inputs.json"))
	if err != nil || hashBytes(input) != "a7ddd9047eb94e54cdaa138b1adf756c5088a50bc01605ee2c6eafbfb9d0d9c2" {
		t.Fatal("immutable canonical source protocol changed")
	}
	var protocol struct {
		ExporterLabel string `json:"exporter_label"`
	}
	if err := json.Unmarshal(input, &protocol); err != nil || protocol.ExporterLabel == "" {
		t.Fatal("canonical build target absent")
	}
	r.target = protocol.ExporterLabel
	for _, key := range []string{"GITHUB_RUN_ID", "GITHUB_RUN_ATTEMPT", "GITHUB_SHA", "GITHUB_WORKFLOW_REF"} {
		if os.Getenv(key) == "" {
			t.Fatalf("missing original remote binding %s", key)
		}
	}
	manifest, err := os.ReadFile(filepath.Join(checkout, "tools/lexicon/source_manifest.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "b02-candidate-source-manifest.txt"), manifest, 0600); err != nil {
		t.Fatal(err)
	}
	r.writeJSON(filepath.Join(root, "b02-request-run-binding.json"), map[string]any{"candidate": actual, "tree": tree, "ordered_parents": parents, "displayed_tuple": string(tuple), "raw_commit_sha256": hashBytes(rawCommit), "source_manifest_sha256": hashBytes(manifest), "request_sha": request, "run": os.Getenv("GITHUB_RUN_ID"), "attempt": os.Getenv("GITHUB_RUN_ATTEMPT"), "workflow_sha": os.Getenv("GITHUB_SHA"), "workflow_ref": os.Getenv("GITHUB_WORKFLOW_REF"), "target": os.Getenv("TEST_TARGET"), "native_source": pinnedSource, "immutable_protocol_sha256": hashBytes(input), "qualification": "PUBLIC_OPERATIONAL_RECORDING_ONLY", "official_or_trusted_producer_admission": "NOT_DELIVERED", "positive_memory_mixed_plane": "CAPABILITY_ABSENT"})
	binary := r.buildCanonicalSource(pinnedSource) // Unchanged B20/B09 recipe, no competing builder.
	for _, plan := range b02GraphPlans() {
		t.Run(plan.Name, func(t *testing.T) {
			dir := filepath.Join(root, "native-"+plan.Name)
			if err := os.Mkdir(dir, 0700); err != nil {
				t.Fatal(err)
			}
			observed := b02RecordGraph(t, binary, plan, dir)
			for _, engine := range engines {
				if engine == "memory" && strings.HasPrefix(plan.Name, "mixed-") {
					continue
				} // Named refusal is in the ordinary public target.
				t.Run("counterpart-"+engine, func(t *testing.T) {
					public := b02ReplayPublic(t, engine, plan, filepath.Join(root, "counterpart-"+engine+"-"+plan.Name))
					b02Save(t, filepath.Join(root, "comparison-"+engine+"-"+plan.Name+".json"), map[string]any{"plan": plan, "native_original_observations": observed, "public_original_observations": public, "comparison": "independent exact expectations; adopted differences explicit in each phase", "native_closed_rows": "retained", "plug_completed_ephemeral": "removed", "official_admission": "NOT_DELIVERED"})
				})
			}
		})
	}
}

func b02NativeID(role string) string { return "b02-" + role }
func b02Quote(value string) string   { return "'" + strings.ReplaceAll(value, "'", "''") + "'" }
func b02RecordGraph(t *testing.T, binary string, plan b02GraphPlan, dir string) []map[string]any {
	r := &nativeRecorder{t: t, root: dir}
	server := publicnative.StartDolt(t, dir)
	workRoot := "/tmp/plug-swarm/work"
	if err := os.MkdirAll(workRoot, 0700); err != nil {
		t.Fatal(err)
	}
	workspace, err := os.MkdirTemp(workRoot, "b02-canonical-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(workspace); err != nil {
			t.Error(err)
		}
	})
	home, project := filepath.Join(workspace, "home"), filepath.Join(workspace, "project")
	for _, path := range []string{home, project, filepath.Join(workspace, "tmp")} {
		if err := os.Mkdir(path, 0700); err != nil {
			t.Fatal(err)
		}
	}
	base := []string{"PATH=/tmp/plug-swarm/bin/worker-safe:" + filepath.Dir(server.DoltBinary) + ":" + os.Getenv("PATH"), "HOME=" + home, "USER=b02-public", "TZ=UTC", "LC_ALL=C", "NO_COLOR=1", "CI=true", "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_AUTHOR_NAME=B02 Public", "GIT_AUTHOR_EMAIL=b02@example.invalid", "GIT_COMMITTER_NAME=B02 Public", "GIT_COMMITTER_EMAIL=b02@example.invalid", "TMPDIR=" + filepath.Join(workspace, "tmp"), "BEADS_ACTOR=b02-recorder", "BEADS_NO_DAEMON=1"}
	w := nativeWorkspace{dir: project, env: server.ChildEnv(base)}
	r.command(w, "git-init", "/usr/bin/git", []string{"-c", "core.hooksPath=/dev/null", "init", "--quiet", "--template="}, false)
	version := r.command(w, "version", binary, []string{"version", "--json"}, false)
	var identity struct{ Commit, Build string }
	if json.Unmarshal(version, &identity) != nil || identity.Commit != pinnedSource.Commit || identity.Build != "b20-public-fixture-"+pinnedSource.Commit {
		t.Fatalf("actual canonical executable identity mismatch: %s", version)
	}
	database := "b02_" + strings.ReplaceAll(plan.Name, "-", "_")
	r.command(w, "init", binary, []string{"init", "--server", "--external", "--server-host", "127.0.0.1", "--server-port", strconv.Itoa(server.Port), "--database", database, "--prefix", "b02", "--skip-hooks", "--skip-agents", "--non-interactive", "--quiet"}, false)
	physical := server.RequireDatabaseDir(t, database)
	b02Save(t, filepath.Join(dir, "native-workspace.json"), map[string]any{"physical_database": physical, "server_pid": server.PID, "data_root": server.DataRoot, "port": server.Port, "plan": plan, "binary_version": json.RawMessage(version)})
	statuses := map[string]string{}
	for _, node := range plan.Nodes {
		args := []string{"create", "--id", b02NativeID(node.Role), "--title", "B02 " + node.Role, "--type", "task", "--force", "--priority", "1", "--status", "open", "--json"}
		if node.Ephemeral {
			args = append(args, "--ephemeral")
		}
		if node.Spawner != "" {
			args = append(args, "--waits-for", b02NativeID(node.Spawner), "--waits-for-gate", node.Gate)
		}
		r.command(w, "create-"+node.Role, binary, args, false)
		statuses[node.Role] = "open"
	}
	for _, node := range plan.Nodes {
		if node.Parent != "" {
			r.command(w, "parent-"+node.Role, binary, []string{"update", b02NativeID(node.Role), "--parent", b02NativeID(node.Parent), "--json"}, false)
		}
	}
	for i, edge := range plan.Edges {
		r.command(w, fmt.Sprintf("edge-%d", i), binary, []string{"dep", "add", b02NativeID(edge.Dependent), b02NativeID(edge.Blocker), "--type", edge.Kind, "--json"}, false)
	}
	if plan.Custom {
		r.command(w, "custom-status-catalog", binary, []string{"config", "set", "status.custom", "b02active:active,b02done:done,b02frozen:frozen"}, false)
		r.command(w, "custom-status-catalog-readback", binary, []string{"config", "get", "status.custom", "--json"}, false)
	}
	var observations []map[string]any
	for _, step := range plan.Steps {
		if !t.Run("native-"+step.Name, func(t *testing.T) {
			parent := r.t
			r.t = t
			defer func() { r.t = parent }()
			var args []string
			switch step.Action {
			case "":
			case "close", "reopen", "force-close":
				verb := step.Action
				if verb == "force-close" {
					verb = "close"
				}
				args = []string{verb, b02NativeID(step.Role), "--reason", "B02 " + step.Name, "--json"}
				if step.Action == "force-close" {
					args = append(args, "--force")
				}
				if verb == "close" {
					statuses[step.Role] = "closed"
				} else {
					statuses[step.Role] = "open"
				}
			case "status":
				args = []string{"update", b02NativeID(step.Role), "--status", step.Value, "--json"}
				statuses[step.Role] = step.Value
			case "unlink":
				args = []string{"dep", "remove", b02NativeID(step.Role), b02NativeID(step.Target), "--json"}
			case "also":
				query := "UPDATE dependencies SET metadata='{" + `"gate":"all-children","also_blocks":true` + "}' WHERE issue_id=" + b02Quote(b02NativeID(step.Role)) + " AND type='waits-for'"
				raw := r.command(w, step.Name+"-SQL-STORED-METADATA-ONLY", binary, []string{"sql", query, "--json"}, false)
				var changed struct {
					Rows json.Number `json:"rows_affected"`
				}
				if json.Unmarshal(raw, &changed) != nil || changed.Rows.String() != "1" {
					t.Fatalf("stored metadata seam did not update one row: %s", raw)
				}
				b02Save(t, filepath.Join(dir, step.Name+"-boundary.json"), map[string]any{"query": query, "native_public_writer": "NOT_PROVEN", "scope": "actual existing stored waits-for metadata evaluator only"})
			default:
				t.Fatal("unknown independent native action")
			}
			if len(args) > 0 {
				r.command(w, step.Name+"-action", binary, args, false)
			}
			observations = append(observations, b02ObserveNative(t, r, w, binary, plan, step, statuses))
		}) {
			t.Fatalf("native %s failed; following phases NOT_RUN", step.Name)
		}
	}
	b02Save(t, filepath.Join(dir, "native-observations.json"), observations)
	if err := server.Stop(); err != nil {
		t.Fatal(err)
	}
	return observations
}

func b02NativeRows(t *testing.T, r *nativeRecorder, w nativeWorkspace, binary, label, query string) []map[string]any {
	var rows []map[string]any
	raw := r.command(w, label, binary, []string{"sql", query, "--json"}, false)
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&rows); err != nil {
		t.Fatalf("exact native SQL row array: %v", err)
	}
	// Sort a comparison representation only; original numeric/NULL/JSON bytes
	// remain untouched in the retained command stdout and its SHA256 receipt.
	sort.Slice(rows, func(i, j int) bool { return bytes.Compare(b02Bytes(t, rows[i]), b02Bytes(t, rows[j])) < 0 })
	return rows
}

func b02ObserveNative(t *testing.T, r *nativeRecorder, w nativeWorkspace, binary string, plan b02GraphPlan, step b02GraphStep, statuses map[string]string) map[string]any {
	t.Helper()
	label := step.Name
	roles := map[string]string{}
	for _, node := range plan.Nodes {
		roles[b02NativeID(node.Role)] = node.Role
	}
	shown := map[string]json.RawMessage{}
	for _, node := range plan.Nodes {
		raw := r.command(w, label+"-show-"+node.Role, binary, []string{"show", b02NativeID(node.Role), "--json"}, false)
		var rows []struct {
			ID, Title, Status string
			Type              string `json:"issue_type"`
			Parent            *string
		}
		if err := json.Unmarshal(raw, &rows); err != nil || len(rows) != 1 || rows[0].ID != b02NativeID(node.Role) || rows[0].Title != "B02 "+node.Role || rows[0].Status != statuses[node.Role] || rows[0].Type != "task" {
			t.Fatalf("native original identity/status %s: %s %v", node.Role, raw, err)
		}
		parent := ""
		if rows[0].Parent != nil {
			parent = *rows[0].Parent
		}
		wantParent := ""
		if node.Parent != "" {
			wantParent = b02NativeID(node.Parent)
		}
		if parent != wantParent {
			t.Fatalf("native %s parent=%s want=%s", node.Role, parent, wantParent)
		}
		shown[node.Role] = json.RawMessage(raw)
	}
	var ready []struct{ ID string }
	rawReady := r.command(w, label+"-ready-inclusive", binary, []string{"ready", "--json", "--sort", "priority", "--limit", "0", "--include-ephemeral"}, false)
	if err := json.Unmarshal(rawReady, &ready); err != nil {
		t.Fatal(err)
	}
	actualReady := []string{}
	seen := map[string]bool{}
	for _, row := range ready {
		role := roles[row.ID]
		if role == "" || seen[role] {
			t.Fatal("native ready unknown/duplicate identity")
		}
		seen[role] = true
		actualReady = append(actualReady, role)
	}
	if !reflect.DeepEqual(b02Sorted(actualReady), b02Sorted(step.NativeReady)) {
		t.Fatalf("canonical native %s/%s ready=%v expected=%v; original preserved", plan.Name, label, actualReady, step.NativeReady)
	}
	actualBlocked := map[string][]string{}
	bindReason := func(id string, reasons []string, count *int, into map[string][]string) {
		role := roles[id]
		if role == "" || into[role] != nil || count == nil || *count != len(reasons) || len(reasons) == 0 || statuses[role] == "closed" || statuses[role] == "pinned" {
			t.Fatal("native blocked subject/reason count invalid")
		}
		seen := map[string]bool{}
		by := []string{}
		for _, id := range reasons {
			reason := roles[id]
			if reason == "" || seen[reason] {
				t.Fatal("native blocked foreign/duplicate reason")
			}
			seen[reason] = true
			by = append(by, reason)
		}
		into[role] = b02Sorted(by)
	}
	var blocked []struct {
		ID    string
		By    []string `json:"blocked_by"`
		Count *int     `json:"blocked_by_count"`
	}
	rawBlocked := r.command(w, label+"-blocked", binary, []string{"blocked", "--json"}, false)
	if err := json.Unmarshal(rawBlocked, &blocked); err != nil {
		t.Fatal(err)
	}
	for _, row := range blocked {
		bindReason(row.ID, row.By, row.Count, actualBlocked)
	}
	if !reflect.DeepEqual(actualBlocked, step.NativeBlocked) {
		t.Fatalf("canonical native %s/%s returned reasons=%v expected=%v; original preserved", plan.Name, label, actualBlocked, step.NativeBlocked)
	}
	var explain struct {
		Ready   []struct{ ID, Reason string }
		Blocked []struct {
			ID    string
			By    []struct{ ID string } `json:"blocked_by"`
			Count *int                  `json:"blocked_by_count"`
		}
		Summary struct {
			Ready   *int `json:"total_ready"`
			Blocked *int `json:"total_blocked"`
		}
	}
	rawExplain := r.command(w, label+"-explain-default-plane", binary, []string{"ready", "--json", "--explain", "--limit", "0"}, false)
	if err := json.Unmarshal(rawExplain, &explain); err != nil || explain.Summary.Ready == nil || explain.Summary.Blocked == nil || *explain.Summary.Ready != len(explain.Ready) || *explain.Summary.Blocked != len(explain.Blocked) {
		t.Fatalf("native explanation array/count binding: %v", err)
	}
	explainBlocked := map[string][]string{}
	for _, row := range explain.Blocked {
		ids := []string{}
		for _, by := range row.By {
			ids = append(ids, by.ID)
		}
		bindReason(row.ID, ids, row.Count, explainBlocked)
	}
	if !reflect.DeepEqual(explainBlocked, step.NativeBlocked) {
		t.Fatalf("native explain reasons=%v expected=%v", explainBlocked, step.NativeBlocked)
	}
	// The canonical explain ready branch is default-plane only; keep its raw
	// answer, not a fabricated replacement for the explicit inclusive query.
	for _, row := range explain.Ready {
		if roles[row.ID] == "" || row.Reason == "" {
			t.Fatal("native explain ready lacks bound identity/reason")
		}
	}
	images := map[string][]map[string]any{}
	for _, table := range []string{"issues", "wisps", "dependencies", "wisp_dependencies"} {
		images[table] = b02NativeRows(t, r, w, binary, label+"-physical-"+table, "SELECT * FROM `"+table+"`")
		if label == "initial" {
			b02NativeRows(t, r, w, binary, "schema-"+table, "SHOW CREATE TABLE `"+table+"`")
		}
	}
	owners := map[string]string{}
	for _, node := range plan.Nodes {
		for _, table := range []string{"issues", "wisps"} {
			for _, row := range images[table] {
				if row["id"] != b02NativeID(node.Role) {
					continue
				}
				if owners[node.Role] != "" || row["status"] != statuses[node.Role] || row["updated_at"] == nil || row["is_blocked"] == nil {
					t.Fatal("actual native owner row/status/projection invalid")
				}
				owners[node.Role] = table
			}
		}
		want := "issues"
		if node.Ephemeral {
			want = "wisps"
		}
		if owners[node.Role] != want {
			t.Fatalf("real native %s owned by %q want %q", node.Role, owners[node.Role], want)
		}
	}
	if len(images["issues"])+len(images["wisps"]) != len(plan.Nodes) {
		t.Fatal("native physical census contains unplanned or missing rows")
	}
	edges := b02AllEdges(plan)
	for _, prior := range plan.Steps {
		if prior.Action == "unlink" {
			next := []b02GraphEdge{}
			for _, edge := range edges {
				if edge.Dependent != prior.Role || edge.Blocker != prior.Target || edge.Kind != "blocks" {
					next = append(next, edge)
				}
			}
			edges = next
		}
		if prior.Name == step.Name {
			break
		}
	}
	for _, node := range plan.Nodes {
		if node.Parent != "" {
			edges = append(edges, b02GraphEdge{node.Role, node.Parent, "parent-child"})
		}
	}
	actualEdges := []string{}
	for _, table := range []string{"dependencies", "wisp_dependencies"} {
		for _, row := range images[table] {
			source, ok := row["issue_id"].(string)
			if !ok || roles[source] == "" {
				t.Fatal("native edge has unknown source")
			}
			target := ""
			targetPlane := ""
			for _, column := range []string{"depends_on_issue_id", "depends_on_wisp_id"} {
				if value, ok := row[column].(string); ok && value != "" {
					if target != "" {
						t.Fatal("native edge has multiple target arms")
					}
					target = value
					targetPlane = column
				}
			}
			if roles[target] == "" {
				t.Fatal("native edge target is not a declared physical row")
			}
			if (table == "wisp_dependencies") != (owners[roles[source]] == "wisps") || (targetPlane == "depends_on_wisp_id") != (owners[roles[target]] == "wisps") {
				t.Fatal("actual native edge arms do not bind both physical endpoint planes")
			}
			actualEdges = append(actualEdges, roles[source]+"/"+fmt.Sprint(row["type"])+"/"+roles[target])
		}
	}
	wantEdges := []string{}
	for _, edge := range edges {
		wantEdges = append(wantEdges, edge.Dependent+"/"+edge.Kind+"/"+edge.Blocker)
	}
	if !reflect.DeepEqual(b02Sorted(actualEdges), b02Sorted(wantEdges)) {
		t.Fatalf("native physical adjacency=%v expected=%v", actualEdges, wantEdges)
	}
	result := map[string]any{"plan": plan.Name, "phase": label, "native_shown_originals": shown, "native_ready_original": json.RawMessage(rawReady), "native_blocked_original": json.RawMessage(rawBlocked), "native_explain_original": json.RawMessage(rawExplain), "ready_roles": b02Sorted(actualReady), "returned_direct_or_parent_fallback_reasons": actualBlocked, "physical_owners": owners, "physical_images": images, "adopted_difference": step.AdoptedDifference}
	b02Save(t, filepath.Join(r.root, label+"-native-observed.json"), result)
	t.Logf("B02_CANONICAL_NATIVE plan=%s phase=%s ready=%v reasons=%v owners=%v source=%s", plan.Name, label, b02Sorted(actualReady), actualBlocked, owners, pinnedSource.Commit)
	return result
}
