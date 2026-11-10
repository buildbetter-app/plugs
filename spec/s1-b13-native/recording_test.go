// Package b13native_test records B13 against the accepted executable and the
// unchanged public Plug boundaries. This manual target grants no dispatch
// authority. Every invocation still needs its own exact-source finite route.
package b13native_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"debug/buildinfo"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/api/wire"
	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/publicnative"
)

const (
	b13Producer     = "c8eb3ba7d4177e111a6f5c8aebe06b5d0b1d5724"
	b13ProducerTree = "930405e863284a3955cbd35e73a6097b606b91a5"
	b13AcceptedRun  = "41800330346"
	b13ManifestSHA  = "ac028e4b1be9609415dde0d9f2a30e12f37f3be5f3f02e4a89b356749fc4e25a"
	b13SourceBase   = "7a43c3c625a5c08d3a4364a321cd47d6357ef30e"
	b13BaseTree     = "be93a595dc4c44ef4d94d2333060788fd344dca0"
	b13BaseParent   = "5812dd80f0c6e4d1e9136604941c7f54eef3238a"
	b13ModuleSHA    = "744bc17b41d8afec00b4e339540dea0d68e6677b9dcca5daef2e9e2725b774c2"
)

var (
	b13Required  = flag.Bool("b13-native-recording", false, "require the accepted, same-run prepared native executable")
	b13Recording = flag.String("b13-native-recording-id", "", "the separately authorized finite recording identity")
	b13SQLName   = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)
)

type b13Command struct {
	Argv, Env                                    []string
	Directory                                    string
	Started, Ended                               time.Time
	Exit                                         int
	ErrorType, Error, StdoutSHA256, StderrSHA256 string
}

type b13Native struct {
	t                       *testing.T
	ctx                     context.Context
	root, workspace, binary string
	env                     []string
	sequence                int
}

type b13Physical struct {
	Tables            map[string][]map[string]any
	Schema            map[string][]map[string]any
	Head, Log, Status []map[string]any
}

// The native CLI is the source of these observations. No authored archive or
// substituted source record enters the public replay path.
func TestB13NativeMemoryRecording(t *testing.T) {
	if !*b13Required || (*b13Recording != "recording1" && *b13Recording != "recording2") {
		t.Fatal("B13 requires one of the two explicit, separately retained recording identities")
	}
	output := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR")
	if output == "" || !filepath.IsAbs(output) {
		t.Fatal("B13 requires retained Bazel undeclared outputs")
	}
	root := filepath.Join(output, "b13-native-"+*b13Recording)
	if err := os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}
	source := b13ProductSource(t, root)
	b13Save(t, root, "claim.json", map[string]any{"schema": "plug-b13-native-claim/1", "recording": *b13Recording, "pid": os.Getpid(), "created_utc": time.Now().UTC(), "source": source, "producer": b13Producer})
	binary := b13Admit(t, root)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Minute)
	defer cancel()
	if err := os.MkdirAll("/tmp/plug-swarm/work", 0700); err != nil {
		t.Fatal(err)
	}
	workspace, err := os.MkdirTemp("/tmp/plug-swarm/work", "b13-native-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(workspace); err != nil {
			t.Error(err)
		}
	})
	server := publicnative.StartDolt(t, root)
	home := filepath.Join(workspace, "home")
	if err := os.MkdirAll(filepath.Join(home, ".config"), 0700); err != nil {
		t.Fatal(err)
	}
	// The pinned init writes workspace discovery metadata when it resolves the
	// isolated cwd itself; an explicit workspace-dir override plus the external
	// ChildEnv data root selects its metadata-free initialization route.
	base := []string{"PATH=/usr/bin:/bin", "HOME=" + home, "XDG_CONFIG_HOME=" + filepath.Join(home, ".config"), "BEADS_NO_DAEMON=1", "BEADS_ACTOR=b13-native-recorder", "CI=true", "DO_NOT_TRACK=1", "LANG=C.UTF-8", "TZ=UTC", "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_AUTHOR_NAME=B13 recorder", "GIT_AUTHOR_EMAIL=b13@example.invalid", "GIT_COMMITTER_NAME=B13 recorder", "GIT_COMMITTER_EMAIL=b13@example.invalid"}
	// No routing, envelope, expected pin or credential is appended after ChildEnv.
	n := &b13Native{t: t, ctx: ctx, root: root, workspace: workspace, binary: binary, env: server.ChildEnv(base)}
	n.must("git-init", []string{"git", "-c", "init.defaultBranch=main", "init", "--quiet"})
	version, code := n.cli("version", "version", "--json")
	if code != 0 || b13Object(t, version)["commit"] != b13Producer {
		t.Fatal("accepted native version mismatch")
	}
	db := "b13_" + b13SHA([]byte(workspace))[:24]
	n.mustCLI("init", "init", "--backend", "dolt", "--server", "--external", "--server-host", "127.0.0.1", "--server-port", strconv.Itoa(server.Port), "--database", db, "--prefix", "b13", "--quiet", "--skip-hooks", "--skip-agents", "--non-interactive")
	metadata, err := os.ReadFile(filepath.Join(workspace, ".beads", "metadata.json"))
	if err != nil {
		t.Fatal(err)
	}
	b13Bytes(t, root, "native-workspace-metadata.json", metadata)
	dir := server.RequireDatabaseDir(t, db)
	b13Save(t, root, "database.json", map[string]any{"database": db, "physical_directory": dir, "pid": server.PID, "port": server.Port, "data_root": server.DataRoot})
	n.mustCLI("ordinary-bead", "--json", "create", "--id", "b13-c0ffee", "--type", "task", "--title", "B13 settings-plane survivor")
	// Existing empty values cannot be created by remember's nonblank-content
	// contract. Seed one explicitly through the accepted public SQL CLI, and
	// retain that origin separately from all behavioral memory writes.
	n.mustCLI("legacy-empty-and-plane-seed", "--json", "sql", "INSERT INTO config (`key`,value) VALUES ('kv.memory.empty',''),('kv.b13-generic','generic survivor'),('b13.setting','settings survivor')")
	n.mustCLI("seed-commit", "dolt", "commit")
	expected := map[string]string{"empty": ""}
	n.read("empty-present", []string{"--json", "recall", "empty"}, 0, map[string]any{"key": "empty", "value": "", "found": true})
	n.read("absent", []string{"--json", "recall", "absent"}, 1, map[string]any{"key": "absent", "value": "", "found": false})
	n.read("initial-list", []string{"--json", "memories"}, 0, expected)
	for _, c := range []struct {
		label, key, content string
		derived             bool
	}{
		{"verbatim", "alpha", "  Line one\n\t東京 Ω raw < > &\r\nend  ", false},
		{"neighbor", "alpha-other", "Prefix neighbor survives exact deletion", false},
		{"explicit-spaced", "Explicit Key/Ω", "Exact Unicode/spaced key", false},
		{"eight-segments", "one-two-three-four-five-six-seven-eight", "ONE TWO three FOUR five SIX seven EIGHT nine ten", true},
		{"sixty-byte-cap", strings.Repeat("a", 60), strings.Repeat("A", 61) + " tail", true},
		{"cap-separator", strings.Repeat("b", 59), strings.Repeat("B", 59) + " c tail", true},
		{"collapsed-separators", "hello-world-preserve-exact-words", "Hello,_WORLD! Preserve\texact  words", true},
	} {
		args := []string{"--json", "remember", c.content}
		if !c.derived {
			args = append(args, "--key", c.key)
		}
		n.mutate(c.label, args, c.key, &c.content, map[string]any{"key": c.key, "value": c.content, "action": "remembered"})
		expected[c.key] = c.content
		n.read(c.label+"-recall", []string{"--json", "recall", c.key}, 0, map[string]any{"key": c.key, "value": c.content, "found": true})
	}
	for _, c := range []struct {
		label   string
		args    []string
		message string
	}{
		{"blank-content", []string{"--json", "remember", " \t\n ", "--key", "blank"}, "memory content cannot be empty"},
		{"blank-key", []string{"--json", "remember", "valid body", "--key", " \t "}, "memory key must not be empty"},
		{"punctuation", []string{"--json", "remember", "!!!"}, "could not generate key from content; use --key to specify one"},
		{"unicode-underivable", []string{"--json", "remember", "東京"}, "could not generate key from content; use --key to specify one"},
	} {
		n.read(c.label, c.args, 1, map[string]any{"error": c.message})
	}
	n.read("search-key", []string{"--json", "memories", "ALPHA"}, 0, map[string]string{"alpha": expected["alpha"], "alpha-other": expected["alpha-other"]})
	n.read("search-value", []string{"--json", "memories", "EXACT UNICODE"}, 0, map[string]string{"Explicit Key/Ω": expected["Explicit Key/Ω"]})
	n.read("search-miss", []string{"--json", "memories", "no-such-memory-needle"}, 0, map[string]string{})
	replacement := "Replacement body\n東京\tretained verbatim"
	n.mutate("replace", []string{"--json", "remember", replacement, "--key", "alpha"}, "alpha", &replacement, map[string]any{"key": "alpha", "value": replacement, "action": "updated"})
	expected["alpha"] = replacement
	n.read("complete-corpus", []string{"--json", "memories"}, 0, expected)
	// Preserve the entire corpus while empty is still present. Exporters see
	// all native rows/families; there is no role filter or synthetic input.
	inventory := filepath.Join(root, "inventory.json")
	archivePath := filepath.Join(root, "native-corpus.tar")
	exportBefore := n.image("export-before")
	n.mustCLI("inventory", "--readonly", "export", "--format", "beads-inventory/1", "--plug-capture-public", "-o", inventory)
	n.mustCLI("archive", "--readonly", "export", "--format", "plug-dump/1", "--plug-inventory", inventory, "-o", archivePath)
	exportAfter := n.image("export-after")
	if !reflect.DeepEqual(exportBefore, exportAfter) {
		t.Fatal("native inventory/archive capture changed complete source state/history")
	}
	archiveBytes, err := os.ReadFile(archivePath)
	if err != nil {
		t.Fatal(err)
	}
	archive, err := dumpformat.Read(bytes.NewReader(archiveBytes))
	if err != nil {
		t.Fatal(err)
	}
	b13Save(t, root, "native-corpus.json", map[string]any{"memories": expected, "archive_sha256": b13SHA(archiveBytes), "manifest": archive.Manifest(), "origin": "actual accepted CLI; legacy empty row explicitly seeded by recorded native SQL"})
	// Edge whitespace is legal native memory identity but the frozen archive
	// reader rejects edge whitespace in Record.ID. Exercise it AFTER the real
	// archive checkpoint, without trimming/filtering an archived source row.
	// This later native-only observation is not public import/reverse evidence.
	edgeContent := "Exact edge-spaced native identity"
	n.mutate("post-archive-edge-key", []string{"--json", "remember", edgeContent, "--key", "  Explicit Key/Ω  "}, "  Explicit Key/Ω  ", &edgeContent, map[string]any{"key": "  Explicit Key/Ω  ", "value": edgeContent, "action": "remembered"})
	n.read("post-archive-edge-recall", []string{"--json", "recall", "  Explicit Key/Ω  "}, 0, map[string]any{"key": "  Explicit Key/Ω  ", "value": edgeContent, "found": true})
	n.mutate("forget-alpha", []string{"--json", "forget", "alpha"}, "alpha", nil, map[string]any{"key": "alpha", "deleted": "true"})
	n.read("neighbor-survives", []string{"--json", "recall", "alpha-other"}, 0, map[string]any{"key": "alpha-other", "value": expected["alpha-other"], "found": true})
	n.mutate("forget-empty", []string{"--json", "forget", "empty"}, "empty", nil, map[string]any{"key": "empty", "deleted": "true"})
	n.read("forgotten-empty", []string{"--json", "recall", "empty"}, 1, map[string]any{"key": "empty", "value": "", "found": false})
	n.read("forget-miss", []string{"--json", "forget", "empty"}, 1, map[string]any{"key": "empty", "found": "false"})
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		if !t.Run("native-corpus-public-"+engine, func(t *testing.T) { b13Public(t, root, workspace, engine, archiveBytes, expected) }) {
			t.Fatalf("B13 stopped at actual %s public boundary; later engines are unproved", engine)
		}
	}
	if err := server.Stop(); err != nil {
		t.Fatal(err)
	}
	b13Save(t, root, "recording-complete.json", map[string]any{"schema": "plug-b13-native/1", "status": "RECORDING_COMPLETE_REQUIRES_INDEPENDENT_REVIEW", "producer": b13Producer, "source": source, "recording": *b13Recording, "archive_sha256": b13SHA(archiveBytes), "FullS1": "0/28", "NativeAdmission": "ADOPTED R" + b13AcceptedRun})
}

// The runner supplies this checkout path. Record the real commit object and
// ordered parents instead of attributing a new recording to an old control ref.
func b13ProductSource(t *testing.T, root string) map[string]any {
	t.Helper()
	sourceRoot := os.Getenv("PLUG_SOURCE_MODULE_ROOT")
	if !filepath.IsAbs(sourceRoot) {
		t.Fatal("B13 requires the runner's actual source checkout")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	n := &b13Native{t: t, ctx: ctx, root: filepath.Join(root, "product-source"), workspace: sourceRoot, env: []string{"PATH=/usr/bin:/bin", "LANG=C.UTF-8", "TZ=UTC", "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_OPTIONAL_LOCKS=0"}}
	git := func(label string, args ...string) []byte {
		return n.must(label, append([]string{"git"}, args...))
	}
	head := strings.TrimSpace(string(git("head", "rev-parse", "--verify", "HEAD")))
	tree := strings.TrimSpace(string(git("tree", "rev-parse", "--verify", "HEAD^{tree}")))
	raw := git("commit", "cat-file", "commit", head)
	shallow := strings.TrimSpace(string(git("shallow", "rev-parse", "--is-shallow-repository")))
	if len(head) != 40 || len(tree) != 40 || (shallow != "true" && shallow != "false") {
		t.Fatal("actual product source identity is incomplete")
	}
	headers := strings.SplitN(string(raw), "\n\n", 2)[0]
	var objectTree string
	var objectParents []string
	for _, line := range strings.Split(headers, "\n") {
		if strings.HasPrefix(line, "tree ") {
			objectTree = strings.TrimPrefix(line, "tree ")
		}
		if strings.HasPrefix(line, "parent ") {
			objectParents = append(objectParents, strings.TrimPrefix(line, "parent "))
		}
	}
	// A depth-one checkout still retains every original parent header. Traversal
	// commands intentionally hide those parents and cannot prove base ancestry.
	// The author dispatch and independent source review verify that separate
	// ancestry boundary in their complete object stores; never fetch here.
	if objectTree != tree || len(objectParents) == 0 {
		t.Fatal("actual raw commit object lacks its source tree or ordered parents")
	}
	for _, parent := range objectParents {
		if ok, _ := regexp.MatchString("^[0-9a-f]{40}$", parent); !ok {
			t.Fatal("actual raw commit has a malformed parent header")
		}
	}
	source := map[string]any{"commit": head, "tree": tree, "ordered_parents": objectParents, "raw_commit_sha256": b13SHA(raw), "checkout_is_shallow": shallow == "true", "base_commit": b13SourceBase, "base_tree": b13BaseTree, "base_parent": b13BaseParent, "base_ancestry_proof": "separate author dispatch and independent source review; not inferred from shallow checkout"}
	b13Save(t, root, "product-source.json", source)
	return source
}

func b13Admit(t *testing.T, root string) string {
	t.Helper()
	exports := map[string]string{}
	for _, key := range []string{"PLUG_PUBLIC_BEADS_BINARY", "PLUG_PUBLIC_BEADS_SHA256", "PLUG_PUBLIC_BEADS_BUILD_RUN", "PLUG_PUBLIC_BEADS_MANIFEST"} {
		exports[key] = os.Getenv(key)
		if exports[key] == "" {
			t.Fatalf("missing own preparation export %s", key)
		}
	}
	b13Save(t, root, "preparation-exports.json", exports)
	path := exports["PLUG_PUBLIC_BEADS_BINARY"]
	manifestPath := exports["PLUG_PUBLIC_BEADS_MANIFEST"]
	if !filepath.IsAbs(path) || !filepath.IsAbs(manifestPath) || exports["PLUG_PUBLIC_BEADS_BUILD_RUN"] != b13AcceptedRun {
		t.Fatal("preparation paths/run are not the accepted tuple")
	}
	data, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	b13Bytes(t, root, "accepted-manifest.json", data)
	if b13SHA(data) != b13ManifestSHA {
		t.Fatal("accepted manifest digest mismatch")
	}
	var m struct {
		Schema    string                                    `json:"schema"`
		Source    map[string]struct{ Commit, Tree string }  `json:"source"`
		Targets   map[string]struct{ Label, Binary string } `json:"targets"`
		Artifacts map[string]struct {
			SHA256 string `json:"sha256"`
			Size   int64  `json:"size"`
			Mode   string `json:"mode"`
		} `json:"artifacts"`
		Build struct {
			RunID string `json:"run_id"`
		} `json:"build"`
		Workflow   struct{ Ref, SHA, Profile string } `json:"workflow"`
		Resolution map[string]json.RawMessage         `json:"resolution"`
	}
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatal(err)
	}
	var v struct{ Commit, Build string }
	if err := json.Unmarshal(m.Resolution["bd_export_version"], &v); err != nil {
		t.Fatal(err)
	}
	a := m.Artifacts["bin/bd-export"]
	target := m.Targets["beads_exporter"]
	if m.Schema != "plug-public-native/1" || m.Source["beads"].Commit != b13Producer || m.Source["beads"].Tree != b13ProducerTree || m.Build.RunID != b13AcceptedRun || m.Workflow.SHA != "d758fdf42a426e9e2a4b707dbcebb22ec68b264e" || m.Workflow.Ref != "refs/heads/swarm/native-producer-census-control-c8eb-20261104" || m.Workflow.Profile != m.Schema || target.Label != "//cmd/bd:bd" || target.Binary != "bin/bd-export" || a.SHA256 != exports["PLUG_PUBLIC_BEADS_SHA256"] || a.Mode != "0o755" || v.Commit != b13Producer || v.Build != "public-"+b13Producer {
		t.Fatal("accepted manifest identity differs")
	}
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0755 || info.Size() != a.Size || a.Size <= 0 {
		t.Fatalf("accepted executable stat: %v", err)
	}
	b, err := os.ReadFile(path)
	if err != nil || b13SHA(b) != a.SHA256 {
		t.Fatal("accepted executable byte/hash mismatch")
	}
	build, err := buildinfo.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	b13Save(t, root, "accepted-build-info.json", build)
	if !strings.HasSuffix(build.Path, "/cmd/bd") || b13SHA([]byte(strings.TrimSuffix(build.Path, "/cmd/bd"))) != b13ModuleSHA || (build.Main.Path != "" && b13SHA([]byte(build.Main.Path)) != b13ModuleSHA) {
		t.Fatal("main binary module/package differs from accepted exporter")
	}
	return path
}

func (n *b13Native) command(label string, argv []string) ([]byte, int) {
	n.t.Helper()
	n.sequence++
	prefix := fmt.Sprintf("commands/%05d-%s", n.sequence, label)
	cmd := exec.CommandContext(n.ctx, argv[0], argv[1:]...)
	cmd.Dir = n.workspace
	cmd.Env = append([]string(nil), n.env...)
	cmd.WaitDelay = 5 * time.Second
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	started := time.Now().UTC()
	err := cmd.Run()
	ended := time.Now().UTC()
	exit := 0
	if err != nil {
		exit = -1
		var e *exec.ExitError
		if errors.As(err, &e) {
			exit = e.ExitCode()
		}
	}
	// First save original streams, then record metadata. Parse/assert later.
	b13Bytes(n.t, n.root, prefix+".stdout", stdout.Bytes())
	b13Bytes(n.t, n.root, prefix+".stderr", stderr.Bytes())
	r := b13Command{Argv: argv, Env: cmd.Env, Directory: cmd.Dir, Started: started, Ended: ended, Exit: exit, ErrorType: fmt.Sprintf("%T", err), StdoutSHA256: b13SHA(stdout.Bytes()), StderrSHA256: b13SHA(stderr.Bytes())}
	if err != nil {
		r.Error = err.Error()
	}
	b13Save(n.t, n.root, prefix+".json", r)
	return stdout.Bytes(), exit
}
func (n *b13Native) cli(label string, args ...string) ([]byte, int) {
	return n.command(label, append([]string{n.binary}, args...))
}
func (n *b13Native) must(label string, argv []string) []byte {
	out, exit := n.command(label, argv)
	if exit != 0 {
		n.t.Fatalf("native setup/capture %s exit=%d", label, exit)
	}
	return out
}
func (n *b13Native) mustCLI(label string, args ...string) []byte {
	return n.must(label, append([]string{n.binary}, args...))
}

func (n *b13Native) sql(label, query string) []map[string]any {
	data := n.mustCLI(label, "--readonly", "--json", "sql", query)
	value, err := wire.DecodeJSON(data)
	if err != nil {
		n.t.Fatalf("native SQL JSON %s: %v", label, err)
	}
	if value == nil {
		return nil
	} // Preserve the actual SQL CLI's null/empty distinction.
	items, ok := value.([]any)
	if !ok {
		n.t.Fatalf("native SQL %s not an array: %T", label, value)
	}
	rows := make([]map[string]any, len(items))
	for i, v := range items {
		var valid bool
		rows[i], valid = v.(map[string]any)
		if !valid {
			n.t.Fatal("SQL row is not an object")
		}
	}
	sort.Slice(rows, func(i, j int) bool { return string(b13JSON(n.t, rows[i])) < string(b13JSON(n.t, rows[j])) })
	return rows
}
func (n *b13Native) image(label string) b13Physical {
	n.t.Helper()
	image := b13Physical{Tables: map[string][]map[string]any{}, Schema: map[string][]map[string]any{}}
	for _, row := range n.sql(label+"-table-census", "SHOW TABLES") {
		if len(row) != 1 {
			n.t.Fatal("SHOW TABLES row changed")
		}
		var table string
		for _, v := range row {
			table, _ = v.(string)
		}
		if !b13SQLName.MatchString(table) {
			n.t.Fatal("invalid actual table name")
		}
		if _, ok := image.Tables[table]; ok {
			n.t.Fatal("duplicate actual table")
		}
		image.Tables[table] = n.sql(label+"-"+table, "SELECT * FROM `"+table+"`")
		image.Schema[table] = n.sql(label+"-schema-"+table, "SHOW CREATE TABLE `"+table+"`")
	}
	if len(image.Tables) == 0 {
		n.t.Fatal("no native table census")
	}
	image.Head = n.sql(label+"-head", "SELECT DOLT_HASHOF('HEAD') AS head, DOLT_HASHOF('HEAD~1') AS parent")
	image.Log = n.sql(label+"-log", "SELECT * FROM dolt_log")
	image.Status = n.sql(label+"-status", "SELECT * FROM dolt_status")
	b13Save(n.t, n.root, label+".json", image)
	b13RawSave(n.t, n.root, label+".raw.json", image)
	return image
}
func (n *b13Native) read(label string, args []string, wantExit int, want any) {
	before := n.image(label + "-before")
	out, exit := n.cli(label, args...)
	after := n.image(label + "-after")
	got, err := wire.DecodeJSON(out)
	if exit != wantExit || err != nil || !reflect.DeepEqual(got, b13ExpectedCLI(n.t, want)) || !reflect.DeepEqual(before, after) {
		n.t.Fatalf("native B13 read/refusal %s response/exit/full conservation failed: exit=%d parse=%v", label, exit, err)
	}
}
func (n *b13Native) mutate(label string, args []string, key string, content *string, want any) {
	before := n.image(label + "-before")
	out, exit := n.cli(label, args...)
	after := n.image(label + "-after")
	got, err := wire.DecodeJSON(out)
	if exit != 0 || err != nil || !reflect.DeepEqual(got, b13ExpectedCLI(n.t, want)) {
		n.t.Fatalf("native B13 mutation %s failed: exit=%d parse=%v", label, exit, err)
	}
	if err := b13NativeDelta(before, after, key, content); err != nil {
		n.t.Fatalf("native B13 mutation %s: %v", label, err)
	}
}
func b13NativeDelta(before, after b13Physical, key string, content *string) error {
	if _, present := before.Tables["config"]; !present {
		return fmt.Errorf("native config table is absent")
	}
	if !reflect.DeepEqual(before.Schema, after.Schema) || !reflect.DeepEqual(before.Status, after.Status) || len(before.Tables) != len(after.Tables) {
		return fmt.Errorf("schema/status/table census changed")
	}
	for table, rows := range before.Tables {
		other, ok := after.Tables[table]
		if !ok {
			return fmt.Errorf("table missing: %s", table)
		}
		if table != "config" {
			if !reflect.DeepEqual(rows, other) {
				return fmt.Errorf("unrelated native table changed: %s", table)
			}
			continue
		}
		storageKey := "kv.memory." + key
		without := func(items []map[string]any) ([]map[string]any, []map[string]any) {
			keep := []map[string]any{}
			target := []map[string]any{}
			for _, row := range items {
				if row["key"] == storageKey {
					target = append(target, row)
				} else {
					keep = append(keep, row)
				}
			}
			return keep, target
		}
		keep, old := without(rows)
		next, actual := without(other)
		if !reflect.DeepEqual(keep, next) || len(old) > 1 {
			return fmt.Errorf("non-target config row changed or duplicate target")
		}
		if content == nil {
			if len(old) != 1 || len(actual) != 0 {
				return fmt.Errorf("exact memory deletion not observed")
			}
		} else {
			if len(actual) != 1 {
				return fmt.Errorf("exact memory row missing")
			}
			expected := map[string]any{"key": storageKey, "value": *content}
			if len(old) == 1 {
				expected = map[string]any{}
				for k, v := range old[0] {
					expected[k] = v
				}
				expected["value"] = *content
			}
			if !reflect.DeepEqual(expected, actual[0]) {
				return fmt.Errorf("memory row changed undeclared columns or content")
			}
		}
	}
	if len(before.Head) != 1 || len(after.Head) != 1 || after.Head[0]["head"] == before.Head[0]["head"] || after.Head[0]["parent"] != before.Head[0]["head"] || len(after.Log) != len(before.Log)+1 {
		return fmt.Errorf("one native commit with exact old parent was not observed")
	}
	oldHead, oldOK := before.Head[0]["head"].(string)
	newHead, newOK := after.Head[0]["head"].(string)
	if !oldOK || !newOK || oldHead == "" || newHead == "" {
		return fmt.Errorf("native history head is not a nonempty identity")
	}
	return b13NativeHistoryDelta(before.Log, after.Log, oldHead, newHead)
}

func b13NativeHistoryDelta(before, after []map[string]any, oldHead, newHead string) error {
	if oldHead == "" || newHead == "" || oldHead == newHead || len(after) != len(before)+1 {
		return fmt.Errorf("native history delta is not one new head")
	}
	prior := map[string]map[string]any{}
	for _, row := range before {
		hash, ok := row["commit_hash"].(string)
		if !ok || hash == "" || prior[hash] != nil {
			return fmt.Errorf("prior native history identity is invalid or duplicated")
		}
		prior[hash] = row
	}
	if prior[oldHead] == nil || prior[oldHead]["refs"] != "HEAD -> main" || prior[newHead] != nil {
		return fmt.Errorf("native history head/reference boundary is invalid")
	}
	seen := map[string]bool{}
	for _, row := range after {
		hash, ok := row["commit_hash"].(string)
		if !ok || hash == "" || seen[hash] {
			return fmt.Errorf("current native history identity is invalid or duplicated")
		}
		seen[hash] = true
		if hash == newHead {
			if row["parents"] != oldHead || row["refs"] != "HEAD -> main" {
				return fmt.Errorf("new native history row differs from actual head/parent/reference")
			}
			continue
		}
		old, exists := prior[hash]
		if !exists {
			return fmt.Errorf("native history contains an undeclared new row")
		}
		// refs is a live branch decoration, not immutable commit data. Retain
		// both raw rows and require its sole exact move from old to new HEAD.
		expected := old
		if hash == oldHead {
			expected = make(map[string]any, len(old))
			for key, value := range old {
				expected[key] = value
			}
			expected["refs"] = ""
		}
		if !reflect.DeepEqual(expected, row) {
			return fmt.Errorf("prior native history changed outside the exact head-reference move")
		}
	}
	if !seen[newHead] || len(seen) != len(prior)+1 {
		return fmt.Errorf("native history omitted a prior row or the new head")
	}
	return nil
}

func TestB13NativeHistoryReferenceConservation(t *testing.T) {
	fixture := func() ([]map[string]any, []map[string]any) {
		before := []map[string]any{
			{"commit_hash": "older", "parents": "", "refs": "", "message": "original ancestor", "extra": json.Number("17")},
			{"commit_hash": "old", "parents": "older", "refs": "HEAD -> main", "message": "original head", "extra": json.Number("23")},
		}
		after := []map[string]any{
			{"commit_hash": "older", "parents": "", "refs": "", "message": "original ancestor", "extra": json.Number("17")},
			{"commit_hash": "old", "parents": "older", "refs": "", "message": "original head", "extra": json.Number("23")},
			{"commit_hash": "new", "parents": "old", "refs": "HEAD -> main", "message": "new operation", "extra": json.Number("29")},
		}
		return before, after
	}
	before, after := fixture()
	if err := b13NativeHistoryDelta(before, after, "old", "new"); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		mutate func([]map[string]any, []map[string]any)
	}{
		{"changed-ancestor", func(_, after []map[string]any) { after[0]["message"] = "rewritten" }},
		{"changed-old-head", func(_, after []map[string]any) { after[1]["extra"] = json.Number("24") }},
		{"lost-prior-field", func(_, after []map[string]any) { delete(after[1], "extra") }},
		{"extra-prior-field", func(_, after []map[string]any) { after[0]["new_field"] = true }},
		{"wrong-new-parent", func(_, after []map[string]any) { after[2]["parents"] = "older" }},
		{"unknown-new-head", func(_, after []map[string]any) { after[2]["commit_hash"] = "other" }},
		{"duplicate-prior-row", func(_, after []map[string]any) { after[0] = after[1] }},
		{"retained-old-reference", func(_, after []map[string]any) { after[1]["refs"] = "HEAD -> main" }},
		{"wrong-new-reference", func(_, after []map[string]any) { after[2]["refs"] = "" }},
		{"unrelated-reference", func(_, after []map[string]any) { after[0]["refs"] = "HEAD -> main" }},
		{"missing-old-reference", func(before, _ []map[string]any) { before[1]["refs"] = "" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before, after := fixture()
			tc.mutate(before, after)
			if err := b13NativeHistoryDelta(before, after, "old", "new"); err == nil {
				t.Fatal("history corruption or an inexact reference move was accepted")
			}
		})
	}
}

func b13SHA(data []byte) string { s := sha256.Sum256(data); return hex.EncodeToString(s[:]) }

// The accepted c8 CLI's default object response adds schema_version=1 to
// successes, maps and failures. ChildEnv does not admit BD_JSON_ENVELOPE.
// Build the entire expected response; do not remove fields from actual bytes.
func b13ExpectedCLI(t *testing.T, value any) map[string]any {
	t.Helper()
	object, ok := b13Decoded(t, value).(map[string]any)
	if !ok {
		t.Fatal("B13 expected CLI response must be an object")
	}
	object["schema_version"] = json.Number("1")
	return object
}

func b13JSON(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}
func b13Decoded(t *testing.T, value any) any {
	t.Helper()
	v, err := wire.DecodeJSON(b13JSON(t, value))
	if err != nil {
		t.Fatal(err)
	}
	return v
}
func b13Object(t *testing.T, data []byte) map[string]any {
	t.Helper()
	v, err := wire.DecodeJSON(data)
	if err != nil {
		t.Fatal(err)
	}
	m, ok := v.(map[string]any)
	if !ok {
		t.Fatal("expected JSON object")
	}
	return m
}
func b13Mkdir(path string) error { return os.Mkdir(path, 0700) }
func b13Write(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	_, writeErr := f.Write(data)
	return errors.Join(writeErr, f.Sync(), f.Close())
}
func b13Bytes(t *testing.T, root, name string, data []byte) {
	t.Helper()
	if err := b13Write(filepath.Join(root, name), data); err != nil {
		t.Fatal(err)
	}
}
func b13Save(t *testing.T, root, name string, value any) {
	t.Helper()
	b13Bytes(t, root, name, append(b13JSON(t, value), '\n'))
}

// Method-free raw trees preserve all actual fields, named Go types and nil
// containers, even when an invalid returned model refuses public JSON. Numeric
// scalars stay exact decimal text; byte slices remain typed, lossless base64.
func b13Raw(value reflect.Value) any {
	if !value.IsValid() {
		return map[string]any{"type": "<invalid>", "nil": true}
	}
	node := map[string]any{"type": value.Type().String(), "kind": value.Kind().String()}
	if value.CanInterface() {
		if stamp, ok := value.Interface().(time.Time); ok {
			node["utc"] = stamp.UTC().Format(time.RFC3339Nano)
			return node
		}
	}
	switch value.Kind() {
	case reflect.Interface, reflect.Pointer:
		node["nil"] = value.IsNil()
		if !value.IsNil() {
			node["value"] = b13Raw(value.Elem())
		}
	case reflect.Struct:
		fields := map[string]any{}
		for i := 0; i < value.NumField(); i++ {
			fields[value.Type().Field(i).Name] = b13Raw(value.Field(i))
		}
		node["fields"] = fields
	case reflect.Slice, reflect.Array:
		if value.Kind() == reflect.Slice {
			node["nil"] = value.IsNil()
			if value.Type().Elem().Kind() == reflect.Uint8 {
				// Encode actual bytes losslessly without invoking RawMessage or
				// model JSON. Named type, nil/empty and length remain explicit.
				node["bytes_base64"] = base64.StdEncoding.EncodeToString(value.Bytes())
				node["length"] = value.Len()
				node["sha256"] = b13SHA(value.Bytes())
				return node
			}
		}
		items := make([]any, value.Len())
		for i := range items {
			items[i] = b13Raw(value.Index(i))
		}
		node["items"] = items
	case reflect.Map:
		node["nil"] = value.IsNil()
		items := []any{}
		for _, key := range value.MapKeys() {
			items = append(items, map[string]any{"key": b13Raw(key), "value": b13Raw(value.MapIndex(key))})
		}
		sort.Slice(items, func(i, j int) bool {
			a, _ := json.Marshal(items[i])
			b, _ := json.Marshal(items[j])
			return bytes.Compare(a, b) < 0
		})
		node["entries"] = items
	case reflect.String:
		node["value"] = value.String()
	case reflect.Bool:
		node["value"] = value.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		node["decimal"] = strconv.FormatInt(value.Int(), 10)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		node["decimal"] = strconv.FormatUint(value.Uint(), 10)
	case reflect.Float32, reflect.Float64:
		node["decimal"] = strconv.FormatFloat(value.Float(), 'g', -1, value.Type().Bits())
	default:
		node["unsupported"] = true
	}
	return node
}
func b13RawSave(t *testing.T, root, name string, value any) {
	t.Helper()
	b13Save(t, root, name, b13Raw(reflect.ValueOf(value)))
}
