package cardb20

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/publicnative"
)

type b06BlockingNativeInput struct {
	Schema               string     `json:"schema"`
	SourceCommit         string     `json:"source_commit"`
	DurableIDs           []string   `json:"durable_ids"`
	EphemeralIDs         []string   `json:"ephemeral_ids"`
	OrdinaryEdges        []b06Edge  `json:"ordinary_edges"`
	DiagnosticEdges      []b06Edge  `json:"diagnostic_edges"`
	ParallelEdges        []b06Edge  `json:"parallel_edges"`
	MissingID            string     `json:"missing_id"`
	ExpectedPaths        [][]string `json:"expected_paths"`
	ExpectedMissingCount int        `json:"expected_missing_count"`
	ExpectedCycleCount   int        `json:"expected_cycle_count"`
	Limit                int        `json:"limit"`
}

func b06ReadBlockingNativeInput(t *testing.T) b06BlockingNativeInput {
	t.Helper()
	const name = "spec/cardb20/testdata/b06-blocking-native.json"
	raw, err := os.ReadFile(filepath.Join(os.Getenv("PLUG_SOURCE_MODULE_ROOT"), name))
	if err != nil || hashBytes(raw) != "6944b755d7fda3f98086fbbff15a5c1d9e53db790b6d294419b5231a7cce1240" {
		t.Fatal("committed public native request/oracle authentication failed", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var input b06BlockingNativeInput
	if err := decoder.Decode(&input); err != nil {
		t.Fatal(err)
	}
	if decoder.Decode(new(any)) != io.EOF || input.Schema != "b06-public-diagnostic-request/1" || input.SourceCommit != pinnedSource.Commit || input.ExpectedCycleCount != 2 || input.ExpectedMissingCount != 1 || input.Limit != 1 {
		t.Fatal("wrong public native diagnostic request contract")
	}
	b06Retain(t, "blocking-native-input.json", map[string]any{"committed_path": name, "sha256": hashBytes(raw), "raw_request_oracle": json.RawMessage(raw), "scope": "public operational diagnostics; no import or native-history claim"})
	return input
}

func b06BlockingNativeCheck(r *b06NativeRecorder, w b06NativeWorkspace, binary, label string, input b06BlockingNativeInput, missing bool) [][]string {
	r.t.Helper()
	history := r.rows(w, binary, label+"-history-before", "SELECT commit_hash FROM dolt_log ORDER BY commit_hash")
	for _, tracks := range []bool{false, true} {
		cycles := r.cycles(w, binary, fmt.Sprintf("%s-tracks-%t", label, tracks), tracks)
		if !reflect.DeepEqual(b06NativePaths(cycles), input.ExpectedPaths) || len(cycles) != input.ExpectedCycleCount {
			r.t.Fatalf("native mixed blocking raw order/count changed: %+v", cycles)
		}
		for i, cycle := range cycles {
			wantPartial := missing && i == 0
			if cycle.Partial != wantPartial {
				r.t.Fatalf("native partial flag changed: %+v", cycle)
			}
			absent := 0
			for _, member := range cycle.Members {
				wantAbsent := missing && member.ID == input.MissingID
				isAbsent := len(member.Issue) == 0 || bytes.Equal(member.Issue, []byte("null"))
				if isAbsent != wantAbsent {
					r.t.Fatalf("native missing member hydration changed: %+v", member)
				}
				if isAbsent {
					absent++
				}
			}
			wantMissing := 0
			if wantPartial {
				wantMissing = input.ExpectedMissingCount
			}
			if absent != wantMissing {
				r.t.Fatalf("native absent count=%d want%d", absent, wantMissing)
			}
		}
	}
	before := r.image(w, binary, label+"-limited-before")
	text := r.command(w, label+"-limited-text", binary, []string{"dep", "cycles", "--limit", fmt.Sprint(input.Limit)}, false)
	if !bytes.Contains(text, []byte("Found 2 dependency cycles:")) || !bytes.Contains(text, []byte("... and 1 more not shown.")) || !bytes.Contains(text, []byte("b06-block-a")) || bytes.Contains(text, []byte("b06-block-c")) {
		r.t.Fatalf("native blocking count/truncation changed: %s", text)
	}
	if missing && (!bytes.Contains(text, []byte(input.MissingID)) || !bytes.Contains(text, []byte("no record in this database"))) {
		r.t.Fatalf("native missing member text is not honest: %s", text)
	}
	if !reflect.DeepEqual(before, r.image(w, binary, label+"-limited-after")) || !reflect.DeepEqual(history, r.rows(w, binary, label+"-history-after", "SELECT commit_hash FROM dolt_log ORDER BY commit_hash")) {
		r.t.Fatal("native blocking diagnostics changed graph/audit/history")
	}
	return b06NativePaths(r.cycles(w, binary, label+"-final-raw", false))
}

func b06RecordBlockingNative(r *b06NativeRecorder, binary string, server *publicnative.Server, input b06BlockingNativeInput) [][][]string {
	w := r.workspace(binary, server, "b06_blocking_missing")
	active := map[string]bool{}
	for _, id := range input.DurableIDs {
		r.create(w, binary, id, false)
	}
	for _, id := range input.EphemeralIDs {
		active[id] = true
		r.create(w, binary, id, true)
	}
	for _, edge := range input.OrdinaryEdges {
		r.edge(w, binary, edge, false)
	}
	// The canonical ordinary route must refuse the edges that close these
	// blocking cycles. Only explicit disposable SQL diagnostics install them.
	for _, edge := range input.DiagnosticEdges {
		r.refuse(w, binary, edge, "cycle")
	}
	for i, edge := range input.DiagnosticEdges {
		table := "dependencies"
		if active[edge.From] {
			table = w.ephemeralDependencies
		}
		query := fmt.Sprintf("INSERT INTO %s (id,issue_id,depends_on_issue_id,type,created_by) VALUES ('b06-blocking-close-%d','%s','%s','%s','b06-public-diagnostic')", table, i, edge.From, edge.To, edge.Kind)
		r.command(w, fmt.Sprintf("blocking-close-%d", i), binary, []string{"sql", query, "--json"}, false)
	}
	edges := append(append([]b06Edge{}, input.OrdinaryEdges...), input.DiagnosticEdges...)
	r.requireEdges(w, binary, "blocking-complete", edges)
	complete := b06BlockingNativeCheck(r, w, binary, "blocking-complete", input, false)
	var target string
	for _, column := range w.targetColumns {
		if column != "depends_on_issue_id" && column != "depends_on_external" {
			if target != "" {
				r.t.Fatal("ambiguous native ephemeral target arm")
			}
			target = column
		}
	}
	if target == "" {
		r.t.Fatal("missing actual ephemeral target arm")
	}
	b06MixedRelaxParallel(r, w, binary, target)
	for i, edge := range input.ParallelEdges {
		query := fmt.Sprintf("INSERT INTO dependencies (id,issue_id,%s,type,created_by) VALUES ('b06-blocking-parallel-%d','%s','%s','%s','b06-public-diagnostic')", target, i, edge.From, edge.To, edge.Kind)
		r.command(w, fmt.Sprintf("blocking-parallel-%d", i), binary, []string{"sql", query, "--json"}, false)
	}
	edges = append(edges, input.ParallelEdges...)
	r.requireEdges(w, binary, "blocking-parallel", edges)
	if !reflect.DeepEqual(complete, b06BlockingNativeCheck(r, w, binary, "blocking-parallel", input, false)) {
		r.t.Fatal("parallel native facts multiplied or reordered mixed blocking paths")
	}
	// Preserve actual independent dependency rows when removing hydration.
	constraint := regexp.MustCompile("CONSTRAINT `([^`]+)` FOREIGN KEY")
	for _, table := range []string{"dependencies", w.ephemeralDependencies} {
		rows := r.rows(w, binary, "blocking-missing-schema-"+table, "SHOW CREATE TABLE "+table)
		var ddl string
		for _, row := range rows {
			for key, value := range row {
				if strings.EqualFold(key, "Create Table") {
					ddl = fmt.Sprint(value)
				}
			}
		}
		matches := constraint.FindAllStringSubmatch(ddl, -1)
		if len(matches) == 0 {
			r.t.Fatal("actual dependency foreign-key schema missing")
		}
		for _, match := range matches {
			r.command(w, "blocking-drop-"+match[1], binary, []string{"sql", "ALTER TABLE " + table + " DROP FOREIGN KEY `" + match[1] + "`", "--json"}, false)
		}
	}
	before := r.image(w, binary, "blocking-missing-before")
	r.command(w, "blocking-remove-hydration", binary, []string{"sql", "DELETE FROM " + w.ephemeralTable + " WHERE id='" + input.MissingID + "'", "--json"}, false)
	after := r.image(w, binary, "blocking-missing-after")
	for table, rows := range before {
		expected := rows
		if table == w.ephemeralTable {
			expected = nil
			removed := 0
			for _, row := range rows {
				if row["id"] == input.MissingID {
					removed++
				} else {
					expected = append(expected, row)
				}
			}
			if removed != 1 {
				r.t.Fatalf("actual hydration deletion removed%d rows", removed)
			}
		}
		if !reflect.DeepEqual(expected, after[table]) {
			r.t.Fatalf("missing hydration changed unexpected source rows in%s", table)
		}
	}
	r.requireEdges(w, binary, "blocking-missing", edges)
	partial := b06BlockingNativeCheck(r, w, binary, "blocking-missing", input, true)
	r.writeJSON(filepath.Join(r.root, "blocking-native-result.json"), map[string]any{"complete_paths": complete, "partial_paths": partial, "absent_hydration": input.MissingID, "ordinary_cycle_admission": false, "parallel_edge_count": len(edges), "private": "HARD_DISABLED", "official": "UNSPENT", "native_history": "UNQUALIFIED"})
	return [][][]string{complete, partial}
}

func TestB06MixedBlockingCanonicalNative(t *testing.T) {
	root, checkout := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR"), os.Getenv("PLUG_SOURCE_MODULE_ROOT")
	if !filepath.IsAbs(root) || !filepath.IsAbs(checkout) {
		t.Fatal("absolute evidence directory and candidate checkout required")
	}
	r := &nativeRecorder{t: t, root: root}
	for _, key := range []string{"PLUG_B06_REQUEST_SHA", "GITHUB_RUN_ID", "GITHUB_RUN_ATTEMPT", "GITHUB_SHA", "GITHUB_WORKFLOW_REF"} {
		if os.Getenv(key) == "" {
			t.Fatal("missing exact request/run binding " + key)
		}
	}
	head := strings.TrimSpace(string(r.buildCommand(checkout, "blocking-candidate-head", "/usr/bin/git", []string{"rev-parse", "HEAD"})))
	if head != os.Getenv("PLUG_B06_REQUEST_SHA") {
		t.Fatal("request/checkout mismatch")
	}
	protocol, err := os.ReadFile(filepath.Join(checkout, "spec/s1-b09-native/native_inputs.json"))
	if err != nil || hashBytes(protocol) != "a7ddd9047eb94e54cdaa138b1adf756c5088a50bc01605ee2c6eafbfb9d0d9c2" {
		t.Fatal("existing native build protocol mismatch")
	}
	var protocolInput struct {
		ExporterLabel string `json:"exporter_label"`
	}
	if json.Unmarshal(protocol, &protocolInput) != nil || protocolInput.ExporterLabel == "" {
		t.Fatal("missing existing native executable label")
	}
	r.target = protocolInput.ExporterLabel
	input := b06ReadBlockingNativeInput(t)
	b06Retain(t, "blocking-request-run-binding.json", map[string]any{"candidate": head, "target": os.Getenv("TEST_TARGET"), "run": os.Getenv("GITHUB_RUN_ID"), "attempt": os.Getenv("GITHUB_RUN_ATTEMPT"), "workflow_sha": os.Getenv("GITHUB_SHA"), "workflow_ref": os.Getenv("GITHUB_WORKFLOW_REF"), "native_source": pinnedSource})
	results := map[string][][][]string{}
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run("paired_"+engine, func(t *testing.T) { results[engine] = b06RunBlockingPlug(t, engine) })
	}
	binary := r.buildCanonicalSource(pinnedSource)
	var twins [][][][]string
	for _, twin := range []string{"a", "b"} {
		t.Run("independent_"+twin, func(t *testing.T) {
			dir := filepath.Join(root, "blocking-native-"+twin)
			if err := os.Mkdir(dir, 0700); err != nil {
				t.Fatal(err)
			}
			nr := &b06NativeRecorder{nativeRecorder: &nativeRecorder{t: t, root: dir, target: r.target}}
			twins = append(twins, b06RecordBlockingNative(nr, binary, publicnative.StartDolt(t, dir), input))
		})
	}
	if len(twins) != 2 || !reflect.DeepEqual(twins[0], twins[1]) {
		t.Fatal("independent mixed blocking native originals disagree")
	}
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run("compare_"+engine, func(t *testing.T) {
			plug := results[engine]
			if len(plug) != 2 || len(plug[0]) != 2 || len(plug[0][0]) != 4 || len(plug[0][1]) != 2 || len(plug[1]) != 2 || len(plug[1][0]) != 4 {
				t.Fatal("public blocking original not complete")
			}
			native := twins[0]
			if plug[0][0][0] != native[0][0][0] || plug[0][0][3] != native[0][0][3] || plug[0][1][0] != native[0][1][0] || plug[1][0][1] != "" || native[1][0][1] != input.MissingID || plug[1][0][2] != plug[0][0][2] || !reflect.DeepEqual(plug[1][1], plug[0][1]) {
				t.Fatal("mixed native/public raw positions or missing-role correspondence changed")
			}
			b06Retain(t, "blocking-native-public-pair.json", map[string]any{"native_originals": native, "public_originals": plug, "workflow_label_role_binding": map[string]string{native[0][0][1]: plug[0][0][1], native[0][0][2]: plug[0][0][2], native[0][1][1]: plug[0][1][1]}, "missing_position": 1, "disclosure_contract": "native preserves absent ID; public preserves unreadable position without disclosing ID", "native_history": "UNQUALIFIED", "full_card": "requires independent original focus/whole disposition"})
		})
	}
}
