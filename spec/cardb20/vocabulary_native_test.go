package cardb20

import (
	"archive/tar"
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/publicnative"
)

type b20VocabularyTwin struct {
	Name       string                    `json:"name"`
	Images     map[string]nativeImage    `json:"current_images"`
	Auxiliary  map[string]nativeImage    `json:"auxiliary_images"`
	Journal    []map[string]any          `json:"original_journal"`
	Projection []b20VocabularyProjection `json:"classified_projection"`
	// This is a derived classification of actual journal rows under the pinned
	// wire contract, not another native command or an observed projector run.
	WireProjection []b20VocabularyProjection `json:"derived_six_kind_projection"`
}

// Authenticate and parse the actual pinned declaration, not a sample list
// reconstructed from the output. A new, omitted or misclassified op refuses.
func b20CheckPinnedVocabulary(t *testing.T, checkout string) map[string]any {
	t.Helper()
	path := filepath.Join(checkout, "spec/s1-b09-native/testdata/beads-9d6fff.tar.gz")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if hashBytes(raw) != "8aa8d07655ec6903703a4c94a4b1aa12d4fb509ab647cd1958f73f8342d3fe6e" {
		t.Fatal("native vocabulary archive is not authenticated pinned source")
	}
	gz, err := gzip.NewReader(bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	defer gz.Close()
	tr := tar.NewReader(gz)
	var source []byte
	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if header.Name == "internal/storage/issueops/journal.go" {
			source, err = io.ReadAll(tr)
			if err != nil {
				t.Fatal(err)
			}
			break
		}
	}
	if len(source) == 0 {
		t.Fatal("pinned EventOp declaration absent")
	}
	file, err := parser.ParseFile(token.NewFileSet(), "journal.go", source, 0)
	if err != nil {
		t.Fatal(err)
	}
	var declared []string
	sets := map[string][]string{}
	for _, d := range file.Decls {
		g, ok := d.(*ast.GenDecl)
		if !ok {
			continue
		}
		for _, s := range g.Specs {
			v, ok := s.(*ast.ValueSpec)
			if !ok {
				continue
			}
			typ, typed := v.Type.(*ast.Ident)
			if typed && typ.Name == "EventOp" {
				for _, value := range v.Values {
					lit, ok := value.(*ast.BasicLit)
					if !ok {
						t.Fatal("EventOp is not an explicit frozen string")
					}
					text, err := strconv.Unquote(lit.Value)
					if err != nil {
						t.Fatal(err)
					}
					declared = append(declared, text)
				}
			}
			for i, name := range v.Names {
				if name.Name != "wireEventOps" && name.Name != "engineOnlyEventOps" {
					continue
				}
				literal, ok := v.Values[i].(*ast.CompositeLit)
				if !ok {
					t.Fatal("vocabulary classification is no longer a literal")
				}
				for _, entry := range literal.Elts {
					id, ok := entry.(*ast.Ident)
					if !ok {
						t.Fatal("unclassified native op")
					}
					sets[name.Name] = append(sets[name.Name], id.Name)
				}
			}
		}
	}
	var expected []string
	for _, kind := range b20VocabularyKinds {
		expected = append(expected, kind.Native)
	}
	if !reflect.DeepEqual(declared, expected) || !reflect.DeepEqual(sets["wireEventOps"], []string{"EventCreate", "EventUpdate", "EventClose", "EventDelete", "EventDepAdd", "EventDepRemove"}) || !reflect.DeepEqual(sets["engineOnlyEventOps"], []string{"EventCommentWrite"}) {
		t.Fatalf("exhaustive vocabulary drift: %v %v", declared, sets)
	}
	return map[string]any{"source": pinnedSource, "archive_sha256": hashBytes(raw), "declaration_path": "internal/storage/issueops/journal.go", "declaration_sha256": hashBytes(source), "declared": declared, "classification": sets, "plug_contract": "engdocs/design/plug/contracts/events.md:27-34", "comment_disposition": "NATIVE_ENGINE_ONLY_PLUG_DURABLE_COMMENT"}
}

func b20VocabularyLines(t *testing.T, raw []byte) []map[string]any {
	t.Helper()
	var rows []map[string]any
	s := bufio.NewScanner(bytes.NewReader(raw))
	s.Buffer(make([]byte, 4096), 8<<20)
	for s.Scan() {
		value, err := decodeNativeSQLDocument(s.Bytes())
		if err != nil {
			t.Fatal(err)
		}
		rows = append(rows, b20VocabularyObject(t, value))
	}
	if err := s.Err(); err != nil {
		t.Fatal(err)
	}
	return rows
}

func b20NativeVocabularyProjection(t *testing.T, rows []map[string]any) []b20VocabularyProjection {
	t.Helper()
	var result []b20VocabularyProjection
	prior := map[string]map[string]any{}
	for _, row := range rows {
		op, _ := row["op"].(string)
		id, _ := row["issue_id"].(string)
		if row["actor"] != "b20-recorder" {
			t.Fatalf("native original actor changed: %+v", row)
		}
		image := b20VocabularyObjectOrNil(row["issue"])
		if op == "delete" {
			if row["issue"] != nil {
				t.Fatal("native delete must carry null issue")
			}
			image = prior[id]
		} else {
			prior[id] = image
		}
		if image == nil {
			t.Fatal("native event has no authoritative image or prior delete image")
		}
		p := b20VocabularyProjection{Sequence: fmt.Sprint(row["seq"]), NativeOp: op, ID: id, Title: fmt.Sprint(image["title"]), Status: fmt.Sprint(image["status"])}
		if op == "dep_add" || op == "dep_remove" {
			dep := b20VocabularyObject(t, row["dep"])
			p.LinkKind, _ = dep["kind"].(string)
			p.LinkTarget, _ = dep["target"].(string)
		}
		if op == "comment" {
			comment := b20VocabularyObject(t, row["comment"])
			p.Comment, _ = comment["text"].(string)
			if comment["source"] != "structured" || comment["author"] != "b20-recorder" {
				t.Fatal("native comment classification/author differs")
			}
		}
		result = append(result, p)
	}
	return result
}

func b20VocabularyObjectOrNil(value any) map[string]any { m, _ := value.(map[string]any); return m }

func (r *nativeRecorder) recordB20VocabularyTwin(binary, name string) b20VocabularyTwin {
	t := r.t
	if err := os.MkdirAll("/tmp/plug-swarm/work", 0700); err != nil {
		t.Fatal(err)
	}
	workspace, err := os.MkdirTemp("/tmp/plug-swarm/work", "b20-vocabulary-"+name+"-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(workspace); err != nil {
			t.Error(err)
		}
	})
	home, dir := filepath.Join(workspace, "home"), filepath.Join(workspace, "project")
	for _, p := range []string{home, dir, filepath.Join(workspace, "tmp")} {
		if err := os.Mkdir(p, 0700); err != nil {
			t.Fatal(err)
		}
	}
	server := publicnative.StartDolt(t, r.root)
	base := []string{"PATH=/tmp/plug-swarm/bin/worker-safe:/usr/local/bin:/usr/bin:/bin", "HOME=" + home, "USER=b20-native", "TZ=UTC", "LC_ALL=C", "NO_COLOR=1", "CI=true", "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_AUTHOR_NAME=B20 Public", "GIT_AUTHOR_EMAIL=b20@example.invalid", "GIT_COMMITTER_NAME=B20 Public", "GIT_COMMITTER_EMAIL=b20@example.invalid", "TMPDIR=" + filepath.Join(workspace, "tmp"), "BEADS_ACTOR=b20-recorder", "BEADS_NO_DAEMON=1"}
	w := nativeWorkspace{dir: dir, env: server.ChildEnv(base)}
	r.command(w, "git-init", "/usr/bin/git", []string{"-c", "core.hooksPath=/dev/null", "init", "--quiet", "--template="}, false)
	version := r.command(w, "version", binary, []string{"version", "--json"}, false)
	var identity struct{ Commit, Build string }
	if json.Unmarshal(version, &identity) != nil || identity.Commit != pinnedSource.Commit || identity.Build != "b20-public-fixture-"+pinnedSource.Commit {
		t.Fatal("executed native identity differs")
	}
	r.command(w, "init", binary, []string{"init", "--server", "--external", "--server-host", "127.0.0.1", "--server-port", strconv.Itoa(server.Port), "--database", "b20_vocabulary", "--prefix", "b20", "--skip-hooks", "--skip-agents", "--non-interactive", "--quiet"}, false)
	server.RequireDatabaseDir(t, "b20_vocabulary")
	for _, kv := range [][2]string{{"events-journal", "true"}, {"events-journal-auto-prune", "false"}} {
		r.command(w, "config-set-"+kv[0], binary, []string{"config", "set", kv[0], kv[1], "--json"}, false)
		got := r.command(w, "config-get-"+kv[0], binary, []string{"config", "get", kv[0], "--json"}, false)
		var setting struct{ Key, Value string }
		if json.Unmarshal(got, &setting) != nil || setting.Key != kv[0] || setting.Value != kv[1] {
			t.Fatal("actual native journal activation differs")
		}
	}
	w.families = r.discoverFamilies(w, binary)
	twin := b20VocabularyTwin{Name: name, Images: map[string]nativeImage{}, Auxiliary: map[string]nativeImage{}}
	capture := func(label string) {
		twin.Images[label] = r.image(w, binary, label)
		aux := nativeImage{Rows: map[string][]map[string]any{}, RawSHA256: map[string]string{}}
		for _, family := range []string{"dependencies", "comments"} {
			query := "SELECT * FROM " + family + " ORDER BY issue_id"
			raw := r.command(w, "aux-"+label+"-"+family, binary, []string{"--readonly", "sql", query, "--json"}, false)
			rows, err := decodeNativeSQLRows(raw)
			if err != nil {
				t.Fatal(err)
			}
			if err := verifyNativeSQLRows(raw, rows); err != nil {
				t.Fatal(err)
			}
			aux.Rows[family] = rows
			aux.RawSHA256[family] = hashBytes(raw)
		}
		twin.Auxiliary[label] = aux
	}
	capture("empty")
	steps := [][]string{
		{"create", "--id", b20VocabularySubject, "--title", "vocabulary anchor", "--json"},
		{"create", "--id", b20VocabularyOther, "--title", "vocabulary target", "--json"},
		{"update", b20VocabularySubject, "--title", "vocabulary edited", "--json"},
		{"comments", "add", b20VocabularySubject, b20VocabularyComment, "--author", "b20-recorder", "--json"},
		{"dep", "add", b20VocabularySubject, b20VocabularyOther, "--type", "relates-to", "--json"},
		{"dep", "remove", b20VocabularySubject, b20VocabularyOther, "--json"},
		{"close", b20VocabularySubject, "--reason", "complete", "--json"},
		{"delete", b20VocabularySubject, "--force", "--json"},
	}
	want := b20VocabularyExpected()
	for i, args := range steps {
		label := fmt.Sprintf("step%02d", i+1)
		r.command(w, "mutation-"+label, binary, args, false)
		capture(label)
		raw := r.command(w, "prefix-"+label+"-export", binary, []string{"events", "export", "--json"}, false)
		rows := b20VocabularyLines(t, raw)
		compareSQLJournal(t, twin.Images[label].Rows["journal"], rows)
		projection := b20NativeVocabularyProjection(t, rows)
		if !reflect.DeepEqual(projection, want[:i+1]) {
			t.Fatalf("native mutation%d did not produce its independently expected classified row: %+v", i+1, projection)
		}
		seq := twin.Images[label].Rows["sequence"]
		if len(seq) != 1 || fmt.Sprint(seq[0]["next_seq"]) != fmt.Sprint(i+1) {
			t.Fatal("native commit sequence differs")
		}
		aux := twin.Auxiliary[label].Rows
		wantDeps := 0
		if i == 4 {
			wantDeps = 1
		}
		if len(aux["dependencies"]) != wantDeps {
			t.Fatal("native edge state did not persist exact add/remove")
		}
		wantComments := 0
		if i >= 3 && i < 7 {
			wantComments = 1
		}
		if len(aux["comments"]) != wantComments {
			t.Fatal("native comment row/cascade state differs")
		}
		if wantComments == 1 && (aux["comments"][0]["text"] != b20VocabularyComment || aux["comments"][0]["author"] != "b20-recorder") {
			t.Fatal("persisted native comment text/author differs")
		}
		for since := 0; since <= i+1; since++ {
			suffix := b20VocabularyLines(t, r.command(w, fmt.Sprintf("prefix-%s-since%d", label, since), binary, []string{"events", "tail", "--since", fmt.Sprint(since), "--limit", "0", "--json"}, false))
			if !reflect.DeepEqual(suffix, rows[since:]) && !(len(suffix) == 0 && since == len(rows)) {
				t.Fatal("native unbounded since-exclusive vocabulary suffix differs")
			}
		}
		if i == 4 {
			r.command(w, "same-value-update", binary, []string{"update", b20VocabularySubject, "--title", "vocabulary edited", "--json"}, false)
			capture("no-op")
			batch := []byte("update " + b20VocabularySubject + " title=rollback\nupdate b20-vocabulary-missing title=missing\n")
			batchPath := filepath.Join(workspace, "abort.txt")
			if err := os.WriteFile(batchPath, batch, 0600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(r.root, "abort.txt"), batch, 0600); err != nil {
				t.Fatal(err)
			}
			r.command(w, "abort-batch", binary, []string{"batch", "--file", batchPath, "--json"}, true)
			capture("abort")
			failure, err := os.ReadFile(filepath.Join(r.root, "abort-batch.stderr"))
			if err != nil {
				t.Fatal(err)
			}
			var refusal struct{ Code, Error string }
			if json.NewDecoder(bytes.NewReader(failure)).Decode(&refusal) != nil || refusal.Code != "batch_error" || !strings.Contains(refusal.Error, "line 2") {
				t.Fatal("native batch did not reach actual second-operation refusal")
			}
			for _, after := range []string{"no-op", "abort"} {
				if !reflect.DeepEqual(twin.Images[label].Rows, twin.Images[after].Rows) || !reflect.DeepEqual(twin.Auxiliary[label].Rows, twin.Auxiliary[after].Rows) {
					t.Fatal("native no-op/UOW rollback changed physical rows or sequence")
				}
			}
		}
		twin.Journal = rows
		twin.Projection = projection
	}
	for _, p := range twin.Projection {
		if p.NativeOp != "comment" {
			twin.WireProjection = append(twin.WireProjection, p)
		}
	}
	path := filepath.Join(r.root, "recording.json")
	r.writeJSON(path, twin)
	conservation, err := verifyNativeSQLRecordingFile(path, map[string]string{name: r.root})
	if err != nil {
		t.Fatal(err)
	}
	r.writeJSON(filepath.Join(r.root, "row-conservation.json"), conservation)
	b20VerifyVocabularyAuxiliary(t, r, path, map[string]string{name: r.root})
	return twin
}

// Independently read both completed saved forms. The new auxiliary images get
// the same raw/request/hash/type/lexeme binding as the unchanged four families.
// No new auxiliary SQL is smuggled into their already frozen inventory rules.
func b20VerifyVocabularyAuxiliary(t *testing.T, r *nativeRecorder, path string, roots map[string]string) {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	doc, err := decodeNativeSQLDocument(raw)
	if err != nil {
		t.Fatal(err)
	}
	values, ok := doc.([]any)
	if !ok {
		values = []any{doc}
	}
	if len(values) != len(roots) {
		t.Fatal("auxiliary twin inventory differs")
	}
	seen := map[string]bool{}
	checks := 0
	for _, value := range values {
		twin := b20VocabularyObject(t, value)
		name, _ := twin["name"].(string)
		root, ok := roots[name]
		if !ok || seen[name] {
			t.Fatal("unknown/duplicate auxiliary twin")
		}
		seen[name] = true
		images := b20VocabularyObject(t, twin["auxiliary_images"])
		current := b20VocabularyObject(t, twin["current_images"])
		if len(images) != len(current) {
			t.Fatal("auxiliary checkpoint inventory differs")
		}
		for label := range current {
			image := b20VocabularyObject(t, images[label])
			rows := b20VocabularyObject(t, image["rows"])
			hashes := b20VocabularyObject(t, image["raw_sha256"])
			if len(rows) != 2 || len(hashes) != 2 {
				t.Fatal("auxiliary family inventory differs")
			}
			for _, family := range []string{"dependencies", "comments"} {
				base := filepath.Join(root, "aux-"+label+"-"+family)
				original, err := os.ReadFile(base + ".stdout")
				if err != nil {
					t.Fatal(err)
				}
				request, err := os.ReadFile(base + ".request.json")
				if err != nil {
					t.Fatal(err)
				}
				decoded, err := decodeNativeSQLDocument(request)
				if err != nil {
					t.Fatal(err)
				}
				command := b20VocabularyObject(t, decoded)
				argv, ok := command["argv"].([]any)
				if !ok || len(argv) != 5 || argv[1] != "--readonly" || argv[2] != "sql" || argv[3] != "SELECT * FROM "+family+" ORDER BY issue_id" || argv[4] != "--json" || command["exit"] != json.Number("0") || command["stdout_sha256"] != hashBytes(original) || hashes[family] != hashBytes(original) {
					t.Fatal("saved auxiliary image is not bound to actual exact SQL receipt")
				}
				var saved []map[string]any
				if rows[family] != nil {
					array, ok := rows[family].([]any)
					if !ok {
						t.Fatal("saved auxiliary rows are not an array")
					}
					for _, row := range array {
						saved = append(saved, b20VocabularyObject(t, row))
					}
				}
				if err := verifyNativeSQLRows(original, saved); err != nil {
					t.Fatal(err)
				}
				checks++
			}
		}
	}
	r.writeJSON(path+".auxiliary-conservation.json", map[string]any{"saved_file": path, "saved_sha256": hashBytes(raw), "checks": checks, "twins": len(roots), "verdict": "EXACT_COMPLETED_SAVED_FILE_RAW_SQL_CONSERVED"})
}

func TestB20NativeMutationVocabularyTwins(t *testing.T) {
	root, checkout := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR"), os.Getenv("PLUG_SOURCE_MODULE_ROOT")
	if !filepath.IsAbs(root) || !filepath.IsAbs(checkout) {
		t.Fatal("actual declared output and checkout paths required")
	}
	r := &nativeRecorder{t: t, root: root}
	for _, key := range []string{"PLUG_B20_REQUEST_SHA", "GITHUB_RUN_ID", "GITHUB_RUN_ATTEMPT", "GITHUB_SHA", "GITHUB_WORKFLOW_REF"} {
		if os.Getenv(key) == "" {
			t.Fatalf("missing exact request/run binding %s", key)
		}
	}
	head := strings.TrimSpace(string(r.buildCommand(checkout, "candidate-head", "/usr/bin/git", []string{"rev-parse", "HEAD"})))
	if head != os.Getenv("PLUG_B20_REQUEST_SHA") {
		t.Fatal("vocabulary request and actual checkout differ")
	}
	input, err := os.ReadFile(filepath.Join(checkout, "spec/s1-b09-native/native_inputs.json"))
	if err != nil {
		t.Fatal(err)
	}
	if hashBytes(input) != "a7ddd9047eb94e54cdaa138b1adf756c5088a50bc01605ee2c6eafbfb9d0d9c2" {
		t.Fatal("canonical native build protocol changed")
	}
	var protocol struct {
		ExporterLabel string `json:"exporter_label"`
	}
	if json.Unmarshal(input, &protocol) != nil || protocol.ExporterLabel == "" {
		t.Fatal("canonical native target missing")
	}
	r.target = protocol.ExporterLabel
	provenance := b20CheckPinnedVocabulary(t, checkout)
	manifest, err := os.ReadFile(filepath.Join(checkout, "tools/lexicon/source_manifest.txt"))
	if err != nil {
		t.Fatal(err)
	}
	r.writeJSON(filepath.Join(root, "vocabulary-source-provenance.json"), provenance)
	r.writeJSON(filepath.Join(root, "request-run-binding.json"), map[string]any{"candidate": head, "run": os.Getenv("GITHUB_RUN_ID"), "attempt": os.Getenv("GITHUB_RUN_ATTEMPT"), "workflow_sha": os.Getenv("GITHUB_SHA"), "workflow_ref": os.Getenv("GITHUB_WORKFLOW_REF"), "target": os.Getenv("TEST_TARGET"), "manifest_sha256": hashBytes(manifest), "native_source": pinnedSource, "qualification": "NEW_PUBLIC_VOCABULARY_UNIT_NOT_AN_OFFICIAL_RUN"})
	binary := r.buildCanonicalSource(pinnedSource)
	var twins []b20VocabularyTwin
	roots := map[string]string{}
	for _, name := range []string{"a", "b"} {
		if !t.Run("independent_"+name, func(t *testing.T) {
			dir := filepath.Join(root, "vocabulary-native-"+name)
			if err := os.Mkdir(dir, 0700); err != nil {
				t.Fatal(err)
			}
			roots[name] = dir
			twins = append(twins, (&nativeRecorder{t: t, root: dir, target: r.target}).recordB20VocabularyTwin(binary, name))
		}) {
			t.Fatal("native vocabulary twin incomplete")
		}
	}
	if len(twins) != 2 || !reflect.DeepEqual(twins[0].Projection, twins[1].Projection) {
		t.Fatal("fresh independent native vocabulary projections diverge")
	}
	path := filepath.Join(root, "vocabulary-native-twins.json")
	r.writeJSON(path, twins)
	if !t.Run("completed_saved_rows_conserve_raw", func(t *testing.T) {
		report, err := verifyNativeSQLRecordingFile(path, roots)
		if err != nil {
			t.Fatal(err)
		}
		r.writeJSON(filepath.Join(root, "aggregate-row-conservation.json"), report)
		b20VerifyVocabularyAuxiliary(t, r, path, roots)
	}) {
		t.Fatal("aggregate vocabulary conservation incomplete")
	}
	for _, engine := range engines {
		t.Run("paired_"+engine, func(t *testing.T) {
			actual := b20ReplayVocabulary(t, engine)
			for _, twin := range twins {
				if !reflect.DeepEqual(actual, twin.Projection) {
					t.Fatalf("actual public %s differs from independently recorded native %s classified semantics", engine, twin.Name)
				}
				var public []b20VocabularyProjection
				for _, row := range actual {
					if row.NativeOp != "comment" {
						public = append(public, row)
					}
				}
				if !reflect.DeepEqual(public, twin.WireProjection) {
					t.Fatal("six-kind native public projection differs from corresponding Plug rows")
				}
			}
			retain(t, "vocabulary-"+engine+"-native-comparison.json", map[string]any{"kinds": b20VocabularyKinds, "native_a": twins[0].Projection, "native_b": twins[1].Projection, "actual_plug": actual, "verdict": "PASS_CLASSIFIED_ALL_SEVEN_NATIVE_KINDS", "comment_difference": "native comment is journaled engine-only; Plug emits adopted bead.commented, contracts/events.md:32", "prune": "UNCHANGED_ADOPTED_RETENTION_DIFFERENCE_NOT_EXERCISED_BY_THIS_NEW_CORPUS"})
		})
	}
}
