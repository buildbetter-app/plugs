package cardb20

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/publicnative"
)

// This independent native oracle names its original source IDs and raw report
// order explicitly. Plug's public workflow allocation returns different typed
// addresses; the final comparison binds roles by the invocation labels, never
// by sorting, stripping prefixes, or rewriting either original report.
func b06RecordMixedNative(r *b06NativeRecorder, binary string, server *publicnative.Server) [][]string {
	w := r.workspace(binary, server, "b06_mixed_current")
	for _, id := range []string{"b06-x-b", "b06-x-a", "b06-x-c", "b06-wait", "b06-schedule", "b06-child", "b06-grandchild"} {
		r.create(w, binary, id, false)
	}
	ids := []string{"b06-z-cycle-z", "b06-z-pure-u", "b06-z-pure-v", "b06-z-wait-z", "b06-z-cycle-y", "b06-z-schedule-z", "b06-z-parent-z"}
	for _, id := range ids {
		r.create(w, binary, id, true)
	}
	edges := []b06Edge{
		{"b06-x-a", ids[0], "blocks"}, {ids[0], "b06-x-b", "tracks"}, {"b06-x-b", "b06-x-a", "tracks"},
		{ids[1], ids[2], "tracks"}, {ids[2], ids[1], "tracks"},
		{"b06-wait", ids[3], "waits-for"}, {ids[3], "b06-wait", "blocks"},
		{"b06-x-c", ids[4], "blocks"}, {ids[4], "b06-x-c", "tracks"},
		{"b06-schedule", ids[5], "blocks"},
		{"b06-child", ids[6], "parent-child"}, {"b06-grandchild", "b06-child", "parent-child"},
	}
	for _, edge := range edges {
		r.edge(w, binary, edge, false)
	}
	r.requireEdges(w, binary, "mixed-ordinary", edges)
	r.refuse(w, binary, b06Edge{ids[0], ids[0], "tracks"}, "itself")
	r.refuse(w, binary, b06Edge{ids[5], "b06-schedule", "blocks"}, "cycle")
	r.refuse(w, binary, b06Edge{ids[6], "b06-grandchild", "parent-child"}, "cycle")
	r.refuseHierarchy(w, binary, b06Edge{"b06-grandchild", ids[6], "blocks"}, "b06-grandchild cannot be blocked by its ancestor "+ids[6]+": "+ids[6]+" cannot close until its descendants finish, so the gate would never clear")
	r.refuseHierarchy(w, binary, b06Edge{ids[6], "b06-grandchild", "blocks"}, ids[6]+" cannot be blocked by its descendant b06-grandchild: waiting on your own subtree is a close gate, not a blocks edge — use a waits-for gate over the children (bd gate)")
	want := [][]string{{"b06-x-a", ids[0], "b06-x-b"}, {"b06-x-c", ids[4]}}
	r.requirePaths(r.cycles(w, binary, "mixed-default", false), [][]string{})
	r.requirePaths(r.cycles(w, binary, "mixed-tracks", true), want)
	// Keep actual endpoint multiplicity and the exact typed ephemeral target
	// column from schema metadata. Only this disposable diagnostic index is removed.
	var target string
	for _, column := range w.targetColumns {
		if column != "depends_on_issue_id" && column != "depends_on_external" {
			target = column
		}
	}
	if target == "" {
		r.t.Fatal("missing actual native ephemeral-target column")
	}
	b06MixedRelaxParallel(r, w, binary, target)
	for i, kind := range []string{"blocks", "tracks"} {
		query := fmt.Sprintf("INSERT INTO dependencies (id,issue_id,%s,type,created_by) VALUES ('b06-mixed-diagnostic-%d','b06-x-a','%s','%s','b06-public-diagnostic')", target, i, ids[0], kind)
		r.command(w, fmt.Sprintf("mixed-parallel-insert-%d", i), binary, []string{"sql", query, "--json"}, false)
		edges = append(edges, b06Edge{"b06-x-a", ids[0], kind})
	}
	r.requireEdges(w, binary, "mixed-parallel", edges)
	r.requirePaths(r.cycles(w, binary, "mixed-parallel-default", false), [][]string{})
	r.requirePaths(r.cycles(w, binary, "mixed-parallel-tracks", true), want)
	before := r.image(w, binary, "mixed-limited-before")
	text := r.command(w, "mixed-limited-text", binary, []string{"dep", "cycles", "--include-tracks", "--limit", "1"}, false)
	if !strings.Contains(string(text), "Found 2 dependency cycles:") || !strings.Contains(string(text), "... and 1 more not shown.") || !strings.Contains(string(text), "b06-x-a") || strings.Contains(string(text), "b06-x-c") {
		r.t.Fatalf("native mixed count/truncation changed: %s", text)
	}
	if !reflect.DeepEqual(before, r.image(w, binary, "mixed-limited-after")) {
		r.t.Fatal("native mixed limited read changed original source rows")
	}
	r.writeJSON(filepath.Join(r.root, "mixed-native-expectations.json"), map[string]any{"raw_expected_paths": want, "actual_raw_paths": b06NativePaths(r.cycles(w, binary, "mixed-final-raw", true)), "original_ids_in_declaration_order": ids, "expected_raw_edges": edges, "meaning": "both native planes, blocked/nonready vertices, scheduling versus tracks/waits-for, self/hierarchy refusal, rotation, parallel collapse and limit count", "official_admission": "UNSPENT"})
	return want
}

func TestB06MixedPlaneCanonicalNative(t *testing.T) {
	root := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR")
	checkout := os.Getenv("PLUG_SOURCE_MODULE_ROOT")
	if !filepath.IsAbs(root) || !filepath.IsAbs(checkout) {
		t.Fatal("absolute evidence directory and candidate checkout required")
	}
	r := &nativeRecorder{t: t, root: root}
	for _, key := range []string{"PLUG_B06_REQUEST_SHA", "GITHUB_RUN_ID", "GITHUB_RUN_ATTEMPT", "GITHUB_SHA", "GITHUB_WORKFLOW_REF"} {
		if os.Getenv(key) == "" {
			t.Fatal("missing exact request/run binding " + key)
		}
	}
	head := strings.TrimSpace(string(r.buildCommand(checkout, "mixed-candidate-head", "/usr/bin/git", []string{"rev-parse", "HEAD"})))
	if head != os.Getenv("PLUG_B06_REQUEST_SHA") {
		t.Fatal("request/checkout mismatch")
	}
	protocol, err := os.ReadFile(filepath.Join(checkout, "spec/s1-b09-native/native_inputs.json"))
	if err != nil || hashBytes(protocol) != "a7ddd9047eb94e54cdaa138b1adf756c5088a50bc01605ee2c6eafbfb9d0d9c2" {
		t.Fatal("existing native build protocol mismatch")
	}
	var input struct {
		ExporterLabel string `json:"exporter_label"`
	}
	if json.Unmarshal(protocol, &input) != nil || input.ExporterLabel == "" {
		t.Fatal("missing existing native executable label")
	}
	r.target = input.ExporterLabel
	// Execute the public engine controls independently of native diagnostic
	// preparation; the native comparison below still requires both originals.
	plugResults := map[string][][]string{}
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run("paired_"+engine, func(t *testing.T) {
			plugResults[engine] = b06RunMixedPlug(t, engine)
		})
	}
	binary := r.buildCanonicalSource(pinnedSource)
	var twins [][][]string
	for _, twin := range []string{"a", "b"} {
		t.Run("independent_"+twin, func(t *testing.T) {
			dir := filepath.Join(root, "mixed-native-"+twin)
			if err := os.Mkdir(dir, 0700); err != nil {
				t.Fatal(err)
			}
			nr := &b06NativeRecorder{nativeRecorder: &nativeRecorder{t: t, root: dir, target: r.target}}
			twins = append(twins, b06RecordMixedNative(nr, binary, publicnative.StartDolt(t, dir)))
		})
	}
	if len(twins) != 2 || !reflect.DeepEqual(twins[0], twins[1]) {
		t.Fatal("independent native mixed originals disagree")
	}
	// Each engine still checks exact returned workflow addresses and raw order.
	// Role correspondence is explicit; source IDs remain in native originals.
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run("compare_"+engine, func(t *testing.T) {
			plug := plugResults[engine]
			if len(plug) != 2 || len(plug[0]) != 3 || len(plug[1]) != 2 || plug[0][0] != twins[0][0][0] || plug[0][2] != twins[0][0][2] || plug[1][0] != twins[0][1][0] {
				t.Fatal("native/Plug mixed role order or durable IDs changed")
			}
			b06Retain(t, "mixed-native-pair.json", map[string]any{"native_raw_paths": twins[0], "plug_raw_paths": plug, "typed_role_binding": map[string]string{twins[0][0][1]: plug[0][1], twins[0][1][1]: plug[1][1]}, "binding_origin": "public native create IDs and public Plug workflow invoke label/order/returned IDs", "originals": "neither sorted nor renamed", "native_history": "UNQUALIFIED", "full_card": "requires independent source and original focus/whole disposition"})
		})
	}
}
