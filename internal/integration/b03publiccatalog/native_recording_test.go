package cardb20

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/publicnative"
)

type b03NativeWorkspace struct {
	dir string
	env []string
}
type b03NativeCatalog struct {
	Name     string            `json:"name"`
	Category ontology.Category `json:"category"`
}
type b03NativeIssue struct {
	ID      string     `json:"id"`
	Status  string     `json:"status"`
	Created time.Time  `json:"created_at"`
	Started *time.Time `json:"started_at"`
	Closed  *time.Time `json:"closed_at"`
}

func b03SortedCatalog(m map[string]ontology.Category) []b03NativeCatalog {
	out := make([]b03NativeCatalog, 0, len(m))
	for k, v := range m {
		out = append(out, b03NativeCatalog{k, v})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}
func (r *nativeRecorder) b03Command(w b03NativeWorkspace, label, binary string, args []string, wantFailure bool) []byte {
	r.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, binary, args...)
	cmd.Dir = w.dir
	cmd.Env = w.env
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	start := time.Now().UTC()
	err := cmd.Run()
	exit := 0
	if err != nil {
		exit = -2
		var e *exec.ExitError
		if errors.As(err, &e) {
			exit = e.ExitCode()
		}
	}
	for suffix, data := range map[string][]byte{"stdout": stdout.Bytes(), "stderr": stderr.Bytes()} {
		if err := os.WriteFile(filepath.Join(r.root, label+"."+suffix), data, 0600); err != nil {
			r.t.Fatal(err)
		}
	}
	r.writeJSON(filepath.Join(r.root, label+".request.json"), map[string]any{"argv": cmd.Args, "directory": w.dir, "environment": w.env, "started": start, "ended": time.Now().UTC(), "exit": exit, "error": fmt.Sprint(err), "stdout_sha256": hashBytes(stdout.Bytes()), "stderr_sha256": hashBytes(stderr.Bytes())})
	if ctx.Err() != nil || exit < 0 || wantFailure && exit == 0 || !wantFailure && exit != 0 {
		r.t.Fatalf("%s exit=%d wantFailure=%t error=%v stderr=%s", label, exit, wantFailure, err, stderr.Bytes())
	}
	return stdout.Bytes()
}
func (r *nativeRecorder) b03Statuses(w b03NativeWorkspace, binary, label string) []b03NativeCatalog {
	r.t.Helper()
	raw := r.b03Command(w, label, binary, []string{"statuses", "--json"}, false)
	var out struct {
		Custom []b03NativeCatalog `json:"custom_statuses"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		r.t.Fatal(err)
	}
	sort.Slice(out.Custom, func(i, j int) bool { return out.Custom[i].Name < out.Custom[j].Name })
	if out.Custom == nil {
		out.Custom = []b03NativeCatalog{}
	}
	return out.Custom
}
func (r *nativeRecorder) b03Show(w b03NativeWorkspace, binary, id, label string) b03NativeIssue {
	r.t.Helper()
	raw := r.b03Command(w, label, binary, []string{"show", id, "--json"}, false)
	var out []b03NativeIssue
	if err := json.Unmarshal(raw, &out); err != nil || len(out) != 1 || out[0].ID != id || out[0].Created.IsZero() {
		r.t.Fatalf("incomplete native show: %s %v", raw, err)
	}
	return out[0]
}

func TestB03CanonicalNativeNonemptyCatalog(t *testing.T) {
	root := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR")
	checkout := os.Getenv("PLUG_SOURCE_MODULE_ROOT")
	if !filepath.IsAbs(root) || !filepath.IsAbs(checkout) {
		t.Fatal("actual RunsOn checkout and outputs required")
	}
	r := &nativeRecorder{t: t, root: root}
	for _, key := range []string{"PLUG_B03_REQUEST_SHA", "GITHUB_RUN_ID", "GITHUB_RUN_ATTEMPT", "GITHUB_SHA", "GITHUB_WORKFLOW_REF"} {
		if os.Getenv(key) == "" {
			t.Fatalf("missing request binding %s", key)
		}
	}
	actual := strings.TrimSpace(string(r.buildCommand(checkout, "b03-candidate-head", "/usr/bin/git", []string{"rev-parse", "HEAD"})))
	if actual != os.Getenv("PLUG_B03_REQUEST_SHA") {
		t.Fatal("request/actual checkout mismatch")
	}
	protocol, err := os.ReadFile(filepath.Join(checkout, "spec/s1-b09-native/native_inputs.json"))
	if err != nil || hashBytes(protocol) != "a7ddd9047eb94e54cdaa138b1adf756c5088a50bc01605ee2c6eafbfb9d0d9c2" {
		t.Fatal("existing canonical B09 protocol changed")
	}
	var config struct {
		ExporterLabel string `json:"exporter_label"`
	}
	if json.Unmarshal(protocol, &config) != nil || config.ExporterLabel == "" {
		t.Fatal("missing existing canonical label")
	}
	r.target = config.ExporterLabel
	support, err := os.ReadFile(filepath.Join(checkout, "spec/cardb20/native_build_test.go"))
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := os.ReadFile(filepath.Join(checkout, "tools/lexicon/source_manifest.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(root, "b03-candidate-source-manifest.txt"), manifest, 0600); err != nil {
		t.Fatal(err)
	}
	r.writeJSON(filepath.Join(root, "b03-request-run.json"), map[string]any{"candidate": actual, "run": os.Getenv("GITHUB_RUN_ID"), "attempt": os.Getenv("GITHUB_RUN_ATTEMPT"), "workflow_head": os.Getenv("GITHUB_SHA"), "workflow_ref": os.Getenv("GITHUB_WORKFLOW_REF"), "target": os.Getenv("TEST_TARGET"), "source": pinnedSource, "unchanged_build_support_sha256": hashBytes(support), "source_manifest_sha256": hashBytes(manifest), "qualification": "PUBLIC_OPERATIONAL_NATIVE_RECORDING_NOT_OFFICIAL_ADMISSION", "claims": "EXCLUDED"})
	binary := r.buildCanonicalSource(pinnedSource)
	// The existing exact-source recipe is compiled unchanged; no builder fork.
	server := publicnative.StartDolt(t, root)
	workspace := t.TempDir()
	home := filepath.Join(workspace, "home")
	project := filepath.Join(workspace, "project")
	tmp := filepath.Join(workspace, "tmp")
	for _, p := range []string{home, project, tmp} {
		if err = os.Mkdir(p, 0700); err != nil {
			t.Fatal(err)
		}
	}
	w := b03NativeWorkspace{project, server.ChildEnv([]string{"PATH=/tmp/plug-swarm/bin/worker-safe:/usr/bin:/bin", "HOME=" + home, "USER=b03-public", "TZ=UTC", "LC_ALL=C", "NO_COLOR=1", "CI=true", "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_AUTHOR_NAME=B03 Public", "GIT_AUTHOR_EMAIL=b03@example.invalid", "GIT_COMMITTER_NAME=B03 Public", "GIT_COMMITTER_EMAIL=b03@example.invalid", "TMPDIR=" + tmp, "BEADS_ACTOR=b03-recorder", "BEADS_NO_DAEMON=1"})}
	r.b03Command(w, "b03-git-init", "/usr/bin/git", []string{"-c", "core.hooksPath=/dev/null", "init", "--quiet", "--template="}, false)
	version := r.b03Command(w, "b03-version", binary, []string{"version", "--json"}, false)
	var identity struct{ Commit, Build string }
	if json.Unmarshal(version, &identity) != nil || identity.Commit != pinnedSource.Commit || identity.Build != "b20-public-fixture-"+pinnedSource.Commit {
		t.Fatalf("actual canonical ELF version mismatch: %s", version)
	}
	r.b03Command(w, "b03-init", binary, []string{"init", "--server", "--external", "--server-host", "127.0.0.1", "--server-port", strconv.Itoa(server.Port), "--database", "b03_catalog", "--prefix", "b03", "--skip-hooks", "--skip-agents", "--non-interactive", "--quiet"}, false)
	server.RequireDatabaseDir(t, "b03_catalog")
	set := func(label, value string, wantFailure bool) {
		r.b03Command(w, label, binary, []string{"config", "set", "status.custom", value, "--json"}, wantFailure)
	}
	catalog := b03SortedCatalog(b03Categories)
	set("b03-config-nonempty", b03Catalog, false)
	if got := r.b03Statuses(w, binary, "b03-catalog-nonempty"); !reflect.DeepEqual(got, catalog) {
		t.Fatalf("literal native catalog changed: %+v", got)
	}
	// Empty values are accepted empty catalogs, not malformed nonblank rows.
	for i, value := range []string{"", "  ", ", ,"} {
		label := fmt.Sprintf("b03-empty-%d", i)
		set(label, value, false)
		if got := r.b03Statuses(w, binary, label+"-catalog"); len(got) != 0 {
			t.Fatalf("empty native catalog retained rows: %+v", got)
		}
		r.b03Command(w, label+"-config", binary, []string{"config", "get", "status.custom", "--json"}, false)
		r.b03Command(w, label+"-custom-use", binary, []string{"create", "--id", "b03-empty-custom", "--title", "must refuse absent catalog", "--status", b03Active, "--json"}, true)
		id := fmt.Sprintf("b03-empty-%d", i)
		r.b03Command(w, label+"-builtin-use", binary, []string{"create", "--id", id, "--title", "valid default catalog", "--json"}, false)
		if got := r.b03Show(w, binary, id, label+"-builtin-readback"); got.Status != "open" {
			t.Fatal("empty custom catalog changed valid built-in status")
		}
	}
	set("b03-config-restore", b03Catalog, false)
	r.b03Command(w, "b03-create-catalog-anchor", binary, []string{"create", "--id", "b03-anchor", "--title", "effective catalog use", "--status", b03Active, "--json"}, false)
	malformed := []struct{ name, value string }{
		{"unknown-category", "bad:unknown"}, {"category-case", "bad:ACTIVE"}, {"trailing-colon", "bad:"},
		{"blank-name", ":active"}, {"space-name", "bad name:active"}, {"duplicate", "duplicate:active,duplicate:frozen"},
		{"builtin", "open:active"}, {"builtin-case", "Open:active"}, {"case-collision", "review:active,Review:wip"},
	}
	for _, tc := range malformed {
		t.Run(tc.name, func(t *testing.T) {
			child := &nativeRecorder{t: t, root: root}
			before := child.b03Command(w, "b03-"+tc.name+"-before-config", binary, []string{"config", "get", "status.custom", "--json"}, false)
			child.b03Command(w, "b03-"+tc.name, binary, []string{"config", "set", "status.custom", tc.value, "--json"}, true)
			after := child.b03Command(w, "b03-"+tc.name+"-after-config", binary, []string{"config", "get", "status.custom", "--json"}, false)
			if !bytes.Equal(before, after) || !reflect.DeepEqual(child.b03Statuses(w, binary, "b03-"+tc.name+"-catalog"), catalog) {
				t.Fatal("refused native config altered effective literal catalog")
			}
			child.b03Command(w, "b03-"+tc.name+"-valid-use", binary, []string{"update", "b03-anchor", "--status", b03Active, "--json"}, false)
			if got := child.b03Show(w, binary, "b03-anchor", "b03-"+tc.name+"-use-readback"); got.Status != b03Active {
				t.Fatal("refused config changed accepted normal status use")
			}
		})
	}
	names := make([]string, 51)
	want50 := map[string]ontology.Category{}
	for i := range names {
		name := fmt.Sprintf("catalog_%02d", i)
		names[i] = name + ":active"
		if i < 50 {
			want50[name] = ontology.Active
		}
	}
	set("b03-boundary-50", strings.Join(names[:50], ","), false)
	if !reflect.DeepEqual(r.b03Statuses(w, binary, "b03-boundary-50-catalog"), b03SortedCatalog(want50)) {
		t.Fatal("native exact 50 not installed")
	}
	before51 := r.b03Command(w, "b03-boundary-51-before-config", binary, []string{"config", "get", "status.custom", "--json"}, false)
	r.b03Command(w, "b03-boundary-50-use", binary, []string{"create", "--id", "b03-limit", "--title", "fiftieth status use", "--status", "catalog_49", "--json"}, false)
	set("b03-boundary-51", strings.Join(names, ","), true)
	r.b03Command(w, "b03-boundary-51-use-refusal", binary, []string{"update", "b03-limit", "--status", "catalog_50", "--json"}, true)
	if got := r.b03Show(w, binary, "b03-limit", "b03-boundary-refusal-use-readback"); got.Status != "catalog_49" {
		t.Fatal("native 51 refusal changed fiftieth-status row")
	}
	after51 := r.b03Command(w, "b03-boundary-51-after-config", binary, []string{"config", "get", "status.custom", "--json"}, false)
	if !bytes.Equal(before51, after51) {
		t.Fatal("native refused51 changed accepted config string")
	}
	if !reflect.DeepEqual(r.b03Statuses(w, binary, "b03-boundary-51-refused-catalog"), b03SortedCatalog(want50)) {
		t.Fatal("native refused51 changed accepted50")
	}
	set("b03-config-lifecycle", b03Catalog, false)
	var issueIDs []string
	var initials []b03NativeIssue
	for i, status := range []string{b03Active, b03WIP, b03Done, b03Frozen} {
		id := fmt.Sprintf("b03-%d", i+1)
		issueIDs = append(issueIDs, id)
		r.b03Command(w, "b03-create-"+status, binary, []string{"create", "--id", id, "--title", "literal category " + status, "--status", status, "--json"}, false)
		got := r.b03Show(w, binary, id, "b03-show-"+status)
		if got.Status != status {
			t.Fatal("native create substituted literal status")
		}
		initials = append(initials, got)
	}
	r.b03Command(w, "b03-create-unknown-status", binary, []string{"create", "--id", "b03-unknown", "--title", "must refuse", "--status", "unconfigured", "--json"}, true)
	r.b03Command(w, "b03-unknown-absent", binary, []string{"show", "b03-unknown", "--json"}, true)
	r.b03Command(w, "b03-create-builtin-open", binary, []string{"create", "--id", "b03-5", "--title", "builtin readiness control", "--json"}, false)
	if got := r.b03Show(w, binary, "b03-5", "b03-show-builtin-open"); got.Status != "open" {
		t.Fatal("native default initial status changed")
	}
	readyRaw := r.b03Command(w, "b03-default-ready", binary, []string{"ready", "--limit", "0", "--json"}, false)
	var ready []b03NativeIssue
	if err = json.Unmarshal(readyRaw, &ready); err != nil {
		t.Fatal(err)
	}
	readySet := map[string]bool{}
	for _, row := range ready {
		readySet[row.ID] = true
	}
	// Record the native default separately: its public ready listing may omit
	// custom active despite the category. No claim-path equivalence is asserted.
	if !readySet["b03-5"] {
		t.Fatal("native default ready lost unblocked built-in open control")
	}
	for i := 1; i < 4; i++ {
		if readySet[issueIDs[i]] {
			t.Fatal("native default ready included custom wip/done/frozen")
		}
	}
	primary := issueIDs[0]
	var trace []b03NativeIssue
	observe := func(label, status string) b03NativeIssue {
		p := r.b03Show(w, binary, primary, "b03-lifecycle-"+label)
		if p.Status != status {
			t.Fatalf("%s status=%s want=%s", label, p.Status, status)
		}
		trace = append(trace, p)
		return p
	}
	observe("created", b03Active)
	r.b03Command(w, "b03-update-wip", binary, []string{"update", primary, "--status", b03WIP, "--json"}, false)
	observe("wip", b03WIP)
	r.b03Command(w, "b03-update-done", binary, []string{"update", primary, "--status", b03Done, "--json"}, false)
	observe("custom-done", b03Done)
	r.b03Command(w, "b03-close", binary, []string{"close", primary, "--reason", "B03 ordinary close", "--json"}, false)
	closed := observe("closed", "closed")
	r.b03Command(w, "b03-repeat-close", binary, []string{"close", primary, "--json"}, false)
	repeat := observe("repeat-close", "closed")
	if closed.Closed == nil || closed.Closed.IsZero() || !reflect.DeepEqual(closed.Closed, repeat.Closed) {
		t.Fatal("native first close time changed")
	}
	r.b03Command(w, "b03-reopen", binary, []string{"reopen", primary, "--json"}, false)
	reopened := observe("reopened", "open")
	if reopened.Closed != nil {
		t.Fatal("native reopen retained close timestamp")
	}
	for _, p := range trace {
		if !p.Created.Equal(trace[0].Created) {
			t.Fatal("native lifecycle rewrote creation time")
		}
	}
	// Reopen directly from a configured done category is an additional source
	// path; it must not be silently covered by reopening literal closed alone.
	r.b03Command(w, "b03-configured-done-reopen", binary, []string{"reopen", issueIDs[2], "--json"}, false)
	if got := r.b03Show(w, binary, issueIDs[2], "b03-configured-done-reopened"); got.Status != "open" || got.Closed != nil {
		t.Fatalf("native configured-done reopen=%+v", got)
	}
	// Each call above is a fresh process against the actual owned Dolt root.
	if !reflect.DeepEqual(r.b03Statuses(w, binary, "b03-final-catalog"), catalog) {
		t.Fatal("lifecycle changed installed native catalog")
	}
	if err = server.Stop(); err != nil {
		t.Fatal(err)
	}
	r.writeJSON(filepath.Join(root, "b03-native-outcomes.json"), map[string]any{"complete": !t.Failed(), "catalog": catalog, "initials": initials, "default_ready": ready, "default_custom_active_ready": readySet[primary], "lifecycle": trace, "configured_done_reopen": "open", "native_boundary": map[string]bool{"50_accepted": true, "51_rejected": true}, "scope": "ACTUAL_CANONICAL_PUBLIC_OPERATIONAL_NATIVE_NOT_OFFICIAL_ADMISSION", "claims": "EXCLUDED"})
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run("paired_"+engine, func(t *testing.T) {
			points := b03PlugCatalog(t, engine)
			for i, p := range points[:4] {
				if p.Status != initials[i].Status || p.Category != b03Categories[initials[i].Status] {
					t.Fatal("literal custom status/category correspondence changed")
				}
			}
			r.writeJSON(filepath.Join(root, "b03-comparison-"+engine+".json"), map[string]any{"complete": !t.Failed(), "native_literal_catalog": catalog, "plug_literal_categories": b03Categories, "native_default_custom_active_ready": readySet[primary], "plug_default_custom_active_ready": points[0].Ready, "native_close_reopen": []string{"closed", "open"}, "plug_close_reopen": []string{b03Done, b03Active}, "adopted_differences": []string{"A3 configured Plug workflow slots replace literal native close/reopen destinations", "P10/P41/D24 immutable catalog keys and unbounded catalog replace native 50-row parser limit", "B03 Plug category-based default readiness versus separately recorded native default ready"}, "claims": "EXCLUDED"})
		})
	}
}
