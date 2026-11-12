package s1b21b24

// This workload earns B23 observations independently of the closed B22 units.
// Every row enters through the pinned engine's real public batch endpoint.
import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/viewops"
	"github.com/buildbetter-app/plugs/internal/integration/converterroundtrip"
	"github.com/buildbetter-app/plugs/internal/publicnative"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type nativeB23Corpus struct {
	t                        *testing.T
	root, work, binary, name string
	env                      []string
	commands                 []nativeCommandReceipt
	http                     *nativeHTTPServer
}

type nativeB23Batch struct {
	label string
	items []any
}

func nativeB23Open(t *testing.T, root string, server *publicnative.Server, artifact nativePublicArtifact, name string) *nativeB23Corpus {
	t.Helper()
	c := &nativeB23Corpus{t: t, root: root, name: name}
	if err := os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}
	c.work = nativeMakeRecordingDirectory(t, server.Root, "b23-"+name+"-")
	if err := os.MkdirAll(filepath.Join(c.work, "home", ".config"), 0700); err != nil {
		t.Fatal(err)
	}
	c.binary = filepath.Join(c.work, "native-pinned")
	nativeCopyPinned(t, artifact.Binary, c.binary, artifact.SHA256, artifact.Size)
	c.env = server.ChildEnv(nativeRecordingEnv(server.DataRoot)(c.work))
	nativeRunCommand(t, root, &c.commands, "git-init", name, c.work, c.env, []string{"git", "-c", "init.defaultBranch=main", "init", "--quiet", "--template="}, nil, 0)
	c.run("version", "version", "--json")
	db := "s1_b23_" + *nativeRecordingID + "_" + name + "_" + strconv.Itoa(os.Getpid())
	c.run("init", "init", "--backend", "dolt", "--server", "--external", "--server-host", "127.0.0.1", "--server-port", strconv.Itoa(server.Port), "--database", db, "--prefix", project, "--quiet", "--skip-hooks", "--skip-agents", "--non-interactive")
	physical := server.RequireDatabaseDir(t, db)
	nativeWriteJSON(t, filepath.Join(root, "database-binding.json"), map[string]any{"database": db, "physical_root": physical, "server_root": server.DataRoot, "port": server.Port, "pid": server.PID, "child_env": c.env})
	c.run("disable-auto-export", "config", "set", "export.auto", "false")
	c.run("enable-versions", "config", "set", "versioned-history.enabled", "true")
	c.run("enable-journal", "config", "set", "events-journal", "true")
	c.run("settle-initial", "dolt", "commit")
	c.http = nativeStartHTTP(t, root, c.binary, c.work, c.env)
	return c
}
func (c *nativeB23Corpus) run(label string, args ...string) []byte {
	return nativeRunCommand(c.t, c.root, &c.commands, label, c.name, c.work, c.env, append([]string{c.binary}, args...), nil, 0)
}
func (c *nativeB23Corpus) sql(label, query string) []map[string]any {
	raw := c.run(label, "--readonly", "sql", query, "--json")
	var rows []map[string]any
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	if err := d.Decode(&rows); err != nil {
		c.t.Fatalf("B23 SQL %s: %v", label, err)
	}
	return rows
}
func (c *nativeB23Corpus) census(label string) map[string]any {
	state := map[string]any{}
	for i, row := range c.sql(label+"-tables", "SHOW TABLES") {
		if len(row) != 1 {
			c.t.Fatal("ambiguous native table name")
		}
		for _, value := range row {
			table, ok := value.(string)
			if !ok || strings.Trim(table, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_") != "" {
				c.t.Fatal("unsafe table name")
			}
			rows := c.sql(fmt.Sprintf("%s-table-%03d", label, i), "SELECT * FROM `"+table+"`")
			sort.Slice(rows, func(i, j int) bool { return nativeCanonical(rows[i]) < nativeCanonical(rows[j]) })
			state[table] = rows
		}
	}
	state["@dolt_log"] = c.sql(label+"-log", "SELECT commit_hash, committer, email, date, message FROM dolt_log ORDER BY commit_hash")
	nativeWriteJSON(c.t, filepath.Join(c.root, label+"-census.json"), state)
	return state
}
func (c *nativeB23Corpus) apply(label string, items ...any) {
	body := nativeRequireHTTP(c.t, c.http.request(label, "POST", "/v0/beads/issues:batchApply", nativeBatch(items...)), 200, "")
	if len(nativeItems(body)) != len(items) {
		c.t.Fatal("native B23 batch lost items")
	}
	for _, v := range nativeItems(body) {
		if v.(map[string]any)["changed"] != true {
			c.t.Fatal("native B23 seed was not a genuine create")
		}
	}
}

// The cap and exchange corpora keep their exact canonical row counts and
// independent 1,000-row public batches. The pinned public server has one
// mutation writer: overlapping batch requests produce its literal 503 busy
// refusal. Send the independent batches serially so this fixture preserves the
// full workload and observes each real endpoint response without retries or
// backoff hiding a refusal. Responses are checked only after all requests
// complete so one batch cannot hide another's result.
func (c *nativeB23Corpus) applyBatches(batches []nativeB23Batch, workers int) {
	c.t.Helper()
	if len(batches) == 0 || workers < 1 {
		c.t.Fatal("native B23 bulk batches require positive work")
	}
	if workers > len(batches) {
		workers = len(batches)
	}
	receipts := make([]nativeHTTPReceipt, len(batches))
	start := make(chan struct{})
	sem := make(chan struct{}, workers)
	var wg sync.WaitGroup
	for i, batch := range batches {
		wg.Add(1)
		go func(i int, batch nativeB23Batch) {
			defer wg.Done()
			<-start
			sem <- struct{}{}
			defer func() { <-sem }()
			receipts[i] = c.http.request(batch.label, "POST", "/v0/beads/issues:batchApply", nativeBatch(batch.items...))
		}(i, batch)
	}
	close(start)
	wg.Wait()
	for i, batch := range batches {
		body := nativeRequireHTTP(c.t, receipts[i], 200, "")
		if len(nativeItems(body)) != len(batch.items) {
			c.t.Fatalf("native B23 bulk batch %s lost items: got=%d want=%d", batch.label, len(nativeItems(body)), len(batch.items))
		}
		for _, v := range nativeItems(body) {
			if v.(map[string]any)["changed"] != true {
				c.t.Fatalf("native B23 bulk batch %s was not a genuine create", batch.label)
			}
		}
	}
}
func nativeB23Seed() []any {
	rows := []struct {
		name, title, typ, status, description, assignee string
		priority                                        int
		labels                                          []string
	}{
		{"alphaa", "Needle Alpha", "task", "open", "body Alpha", "alice", 0, []string{"Alpha", "shared", "two words"}},
		{"betaaa", "Needle beta", "task", "in_progress", "body beta", "bob", 1, []string{"alpha", "shared"}},
		{"gammaa", "ordinary gamma", "task", "blocked", "", "", 2, []string{"gamma"}},
		{"bugrow", "Needle bug", "bug", "open", "bug body", "alice", 3, []string{"Alpha", "gamma"}},
		{"closed", "completed needle", "task", "closed", "", "", 2, []string{"done"}},
		{"plainx", "ordinary blank", "task", "open", "", "", 4, []string{}},
	}
	items := []any{}
	for _, r := range rows {
		items = append(items, map[string]any{"kind": "create", "create": map[string]any{"id": nativeID(r.name), "title": r.title, "issue_type": r.typ, "status": r.status, "description": r.description, "assignee": r.assignee, "priority": r.priority, "labels": r.labels}})
	}
	return items
}
func nativeB23IDs(t *testing.T, body map[string]any) []string {
	t.Helper()
	ids := []string{}
	for _, v := range nativeItems(body) {
		ids = append(ids, v.(map[string]any)["id"].(string))
	}
	return ids
}
func nativeB23Want(names ...string) []string {
	ids := []string{}
	for _, n := range names {
		ids = append(ids, nativeID(n))
	}
	sort.Strings(ids)
	return ids
}

func TestNativeB23CountMatrix(t *testing.T) {
	if *nativeRecordingID != "recording1" && *nativeRecordingID != "recording2" {
		t.Fatal("explicit independent native_recording required")
	}
	root := nativeMakeRecordingRoot(t)
	request := os.Getenv("PLUG_B23_REQUEST_SHA")
	source := os.Getenv("PLUG_SOURCE_MODULE_ROOT")
	if len(request) != 40 || strings.Trim(request, "0123456789abcdef") != "" || !filepath.IsAbs(source) {
		t.Fatal("native B23 matrix requires its exact request SHA and declared source root")
	}
	cmd := exec.CommandContext(t.Context(), "/usr/bin/git", "-C", source, "show", "-s", "--format=%H%n%T%n%P", "HEAD")
	cmd.Env = []string{"PATH=/tmp/plug-swarm/bin/worker-safe:/usr/local/bin:/usr/bin:/bin", "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null"}
	raw, err := cmd.Output()
	tuple := strings.Split(strings.TrimSpace(string(raw)), "\n")
	if err != nil || len(tuple) != 3 || tuple[0] != request {
		t.Fatalf("native B23 matrix request/source mismatch: %q %v", raw, err)
	}
	nativeWriteJSON(t, filepath.Join(root, "request-binding.json"), map[string]any{"request_sha": request, "head": tuple[0], "tree": tuple[1], "ordered_parents": strings.Fields(tuple[2]), "recording": *nativeRecordingID, "target": os.Getenv("TEST_TARGET")})
	t.Logf("NATIVE_B23_REQUEST head=%s tree=%s parents=%s recording=%s", tuple[0], tuple[1], tuple[2], *nativeRecordingID)
	t.Logf("NATIVE_B23_OUTPUT %s", root)
	var profile nativeRecordingProfile
	var protocol nativeRecordingFixture
	if err := json.Unmarshal(nativeRunfile(t, "profile.json"), &profile); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(nativeRunfile(t, "protocol.json"), &protocol); err != nil || protocol.SourceCommit != nativeBeadsCommit || protocol.SourceTree != nativeBeadsTree {
		t.Fatal("invalid pinned B23 protocol", err)
	}
	nativeWriteRecordingBytes(t, filepath.Join(root, "profile.json"), nativeRunfile(t, "profile.json"))
	nativeWriteRecordingBytes(t, filepath.Join(root, "protocol.json"), nativeRunfile(t, "protocol.json"))
	artifact := nativeVerifyPublicArtifact(t, profile)
	nativeRecordBuildInfo(t, root, artifact, profile)
	server := publicnative.StartDolt(t, root)
	if !t.Run("native-operator-plane", func(t *testing.T) {
		c := nativeB23Open(t, filepath.Join(root, "plane"), server, artifact, "plane")
		c.apply("matrix-seed", nativeB23Seed()...)
		for _, r := range []struct{ name, flag, label string }{{"ephema", "ephemeral", "Alpha"}, {"nohsty", "no_history", "gamma"}} {
			c.apply("seed-"+r.name, map[string]any{"kind": "create", "create": map[string]any{"id": nativeID(r.name), "title": "plane " + r.name, "issue_type": "task", r.flag: true, "labels": []string{r.label}}})
		}
		// Keep the original omitted-type refusal as a distinct validation probe.
		// A duplicate probe must first carry a valid workspace issue type.
		invalidBefore := c.census("missing-type-before")
		invalid := nativeRequireHTTP(t, c.http.request("missing-type-validation", "POST", "/v0/beads/issues:batchApply", nativeBatch(map[string]any{"kind": "create", "create": map[string]any{"id": nativeID("alphaa"), "title": "duplicate", "ephemeral": true}})), 400, "invalid_argument")
		if invalid["reason"] != "invalid_value" || invalid["param"] != "items" || invalid["item_index"] != float64(0) || invalid["item_kind"] != "create" {
			t.Fatal("native omitted-type refusal lost its exact item validation", invalid)
		}
		if !reflect.DeepEqual(invalidBefore, c.census("missing-type-after")) {
			t.Fatal("native omitted-type refusal changed physical state")
		}
		// Public create refuses a cross-plane duplicate; no corrupt fixture is inserted.
		before := c.census("operator-before")
		refusal := nativeRequireHTTP(t, c.http.request("cross-plane-duplicate", "POST", "/v0/beads/issues:batchApply", nativeBatch(map[string]any{"kind": "create", "create": map[string]any{"id": nativeID("alphaa"), "title": "duplicate", "issue_type": "task", "ephemeral": true}})), 409, "already_exists")
		if refusal["param"] != "items[0].create.id" || refusal["item_index"] != float64(0) || refusal["item_kind"] != "create" {
			t.Fatal("duplicate did not refuse")
		}
		if !reflect.DeepEqual(before, c.census("duplicate-after")) {
			t.Fatal("native duplicate refusal changed physical state")
		}
		vectors := []struct {
			name, query string
			want        []string
			count       bool
		}{
			{"durable-all", "", nativeB23Want("alphaa", "betaaa", "gammaa", "bugrow", "closed", "plainx"), true},
			{"ephemeral-union", "include_ephemeral=true", nativeB23Want("alphaa", "betaaa", "gammaa", "bugrow", "closed", "plainx", "ephema", "nohsty"), true},
			{"infra-union", "include_infra=true", nativeB23Want("alphaa", "betaaa", "gammaa", "bugrow", "closed", "plainx", "ephema", "nohsty"), true},
			{"both-switches", "include_infra=true&include_ephemeral=true", nativeB23Want("alphaa", "betaaa", "gammaa", "bugrow", "closed", "plainx", "ephema", "nohsty"), true},
			{"label-all", "label=Alpha&label=gamma", nativeB23Want("bugrow"), true},
			{"label-any", "label_any=alpha&label_any=gamma", nativeB23Want("betaaa", "gammaa", "bugrow"), true},
			{"label-exclude", "exclude_label=Alpha", nativeB23Want("betaaa", "gammaa", "closed", "plainx"), false},
			{"label-case-upper", "label=Alpha", nativeB23Want("alphaa", "bugrow"), true},
			{"label-case-lower", "label=alpha", nativeB23Want("betaaa"), true},
			{"plane-label", "label=Alpha&include_ephemeral=true", nativeB23Want("alphaa", "bugrow", "ephema"), true},
			{"title-case-insensitive", "title=needle", nativeB23Want("alphaa", "betaaa", "bugrow", "closed"), true},
			{"description", "desc_contains=body", nativeB23Want("alphaa", "betaaa", "bugrow"), true},
			{"scalar-intersection", "status=open&type=bug&label=Alpha", nativeB23Want("bugrow"), true},
			{"status-or-list-only", "status=open,in_progress", nativeB23Want("alphaa", "betaaa", "bugrow", "plainx"), false},
			{"label-empty-set", "label=never-present", nativeB23Want(), true},
		}
		observations := []map[string]any{}
		for _, v := range vectors {
			body := nativeRequireHTTP(t, c.http.request(v.name+"-list", "GET", "/v0/beads/issues?all=true&limit=0&"+v.query, nil), 200, "")
			ids := nativeB23IDs(t, body)
			sort.Strings(ids)
			if !reflect.DeepEqual(ids, v.want) {
				t.Fatalf("native %s got=%v want=%v", v.name, ids, v.want)
			}
			entry := map[string]any{"name": v.name, "query": v.query, "expected": v.want, "ids": ids}
			if v.count {
				count := nativeRequireHTTP(t, c.http.request(v.name+"-count", "GET", "/v0/beads/issues:count?"+v.query, nil), 200, "")
				if count["total"] != float64(len(v.want)) {
					t.Fatalf("native full predicate count %s: %v", v.name, count)
				}
				entry["count"] = count
			}
			observations = append(observations, entry)
		}
		for _, q := range []string{"status=unknown-status", "type=unknown-type"} {
			body := nativeRequireHTTP(t, c.http.request("unknown-"+q, "GET", "/v0/beads/issues:count?"+q, nil), 200, "")
			if body["total"] != float64(0) {
				t.Fatal("unknown native count must be zero")
			}
		}
		// Both documented keyset orders traverse the exact mixed-plane set.
		for _, order := range []string{"priority", "created"} {
			ids := []string{}
			seen := map[string]bool{}
			cursor := ""
			for i := 0; i < 10; i++ {
				body := nativeRequireHTTP(t, c.http.request(fmt.Sprintf("keyset-%s-%d", order, i), "GET", "/v0/beads/issues?all=true&include_ephemeral=true&limit=2&sort="+order+"&cursor="+url.QueryEscape(cursor), nil), 200, "")
				ids = append(ids, nativeB23IDs(t, body)...)
				if body["has_more"] == false {
					cursor = ""
					break
				}
				cursor, _ = body["next_cursor"].(string)
				if cursor == "" || seen[cursor] {
					t.Fatal("native keyset did not advance")
				}
				seen[cursor] = true
			}
			sort.Strings(ids)
			if cursor != "" || !reflect.DeepEqual(ids, nativeB23Want("alphaa", "betaaa", "gammaa", "bugrow", "closed", "plainx", "ephema", "nohsty")) {
				t.Fatal("native keyset lost or repeated rows", ids)
			}
		}
		if !reflect.DeepEqual(before, c.census("operator-after")) {
			t.Fatal("native B23 read/refusal matrix mutated physical state")
		}
		nativeWriteJSON(t, filepath.Join(c.root, "operator-matrix.json"), observations)
		c.http.Stop()
	}) {
		return
	}
	if !t.Run("native-20001-cap", func(t *testing.T) {
		c := nativeB23Open(t, filepath.Join(root, "cap"), server, artifact, "cap")
		c.apply("matrix-seed", nativeB23Seed()...)
		batches := make([]nativeB23Batch, 0, 20)
		for start := 0; start < 19995; start += 1000 {
			items := []any{}
			end := start + 1000
			if end > 19995 {
				end = 19995
			}
			for i := start; i < end; i++ {
				items = append(items, nativeCreate(fmt.Sprintf("b%05d", i), "bulk ordinary"))
			}
			batches = append(batches, nativeB23Batch{label: fmt.Sprintf("bulk-%05d", start), items: items})
		}
		c.applyBatches(batches, 1)
		before := c.census("cap-before")
		if len(before["issues"].([]map[string]any)) != 20001 {
			t.Fatal("native cap corpus is not exactly 20001 durable issues")
		}
		count := nativeRequireHTTP(t, c.http.request("cap-full-count", "GET", "/v0/beads/issues:count", nil), 200, "")
		if count["total"] != float64(20001) {
			t.Fatal("native scalar count was truncated", count)
		}
		out := nativeRunCommand(t, c.root, &c.commands, "cap-list-refusal", c.name, c.work, c.env, []string{c.binary, "--readonly", "--json", "list", "--all", "--limit", "0", "--max-rows", "20000"}, nil, 2)
		stderr, err := os.ReadFile(c.commands[len(c.commands)-1].StderrPath)
		if err != nil || len(out) != 0 || !strings.Contains(string(stderr), "Error: too many rows: 20001 found, --max-rows=20000 exceeded.") {
			t.Fatal("native cap did not refuse without partial stdout", err, string(stderr))
		}
		filtered := c.run("cap-filtered-list", "--readonly", "--json", "list", "--all", "--title", "Needle", "--limit", "0", "--max-rows", "20000")
		var rows []map[string]any
		if err := json.Unmarshal(filtered, &rows); err != nil || len(rows) != 4 {
			t.Fatal("native cap preceded full predicate", err, len(rows))
		}
		if !reflect.DeepEqual(before, c.census("cap-after")) {
			t.Fatal("native cap/read changed physical state")
		}
		c.http.Stop()
		nativeB23ExportAndReplay(t, c, protocol, 20001)
	}) {
		return
	}
	// The 20001 archive remains intact and earns only its real inspection
	// refusal. A fresh independently initialized 20000-row corpus exercises
	// the supported import, then one genuine public Plug create crosses its cap.
	if !t.Run("supported-20000-import", func(t *testing.T) {
		c := nativeB23Open(t, filepath.Join(root, "exchange"), server, artifact, "exchange")
		c.apply("matrix-seed", nativeB23Seed()...)
		batches := make([]nativeB23Batch, 0, 20)
		for start := 0; start < 19994; start += 1000 {
			end := start + 1000
			if end > 19994 {
				end = 19994
			}
			items := []any{}
			for i := start; i < end; i++ {
				items = append(items, nativeCreate(fmt.Sprintf("b%05d", i), "bulk ordinary"))
			}
			batches = append(batches, nativeB23Batch{label: fmt.Sprintf("bulk-%05d", start), items: items})
		}
		c.applyBatches(batches, 1)
		state := c.census("source-final")
		if len(state["issues"].([]map[string]any)) != 20000 {
			t.Fatal("separate supported source is not exactly 20000 rows")
		}
		c.http.Stop()
		nativeB23ExportAndReplay(t, c, protocol, 20000)
	}) {
		return
	}
	if err := server.Stop(); err != nil {
		t.Fatal(err)
	}
	nativeWriteJSON(t, filepath.Join(root, "scope.json"), map[string]any{"recording": *nativeRecordingID, "card": "B23", "durable_native_rows": 20001, "separate_supported_source_rows": 20000, "imported_cap_route": "20000 genuine native rows plus one actual public Plug create; no 20001-native import claim", "plane_corpus": "separate real durable/ephemeral/no-history stores", "legacy_dual_resident_input": "NOT_FABRICATED_PUBLIC_CREATE_REFUSES", "whole_role_export": false, "source_history": false, "protected_source_stale": false, "native_admission": nil})
}

func nativeB23ExportAndReplay(t *testing.T, c *nativeB23Corpus, protocol nativeRecordingFixture, sourceCount int) {
	t.Helper()
	c.run("settle-export", "dolt", "commit")
	ordinaryPath := filepath.Join(c.root, "source.jsonl")
	c.run("ordinary-export", "--readonly", "export", "--include-memories", "--output", ordinaryPath)
	ordinary, err := os.ReadFile(ordinaryPath)
	if err != nil {
		t.Fatal(err)
	}
	projected, err := nativeProjectJSONL(ordinary, protocol.ProjectionFields, protocol.StatusCategories)
	if err != nil {
		t.Fatal(err)
	}
	nativeWriteRecordingBytes(t, filepath.Join(c.root, "source-projection.json"), projected)
	settings, err := nativeReadPublicConfigSettings(c.run("settings-readback", "--json", "config", "list"))
	if err != nil {
		t.Fatal(err)
	}
	metadata, err := nativeReadPublicMetadata(nativeFlatEnvelope(c.run("metadata-readback", "--readonly", "sql", "SELECT `key`, value FROM metadata ORDER BY `key`", "--json")))
	if err != nil {
		t.Fatal(err)
	}
	expected, err := nativeExpectPublicConfig(append(settings, metadata...), projected)
	if err != nil {
		t.Fatal(err)
	}
	nativeWriteJSON(t, filepath.Join(c.root, "independent-config-expectation.json"), expected)
	inv := filepath.Join(c.root, "inventory.json")
	c.run("inventory", "--readonly", "export", "--format", "beads-inventory/1", "--plug-capture-public", "-o", inv)
	invData, err := os.ReadFile(inv)
	if err != nil {
		t.Fatal(err)
	}
	if err := nativeValidateInventory(invData, protocol); err != nil {
		t.Fatal(err)
	}
	archivePath := filepath.Join(c.root, "source.tar")
	c.run("archive", "--readonly", "export", "--format", "plug-dump/1", "--plug-inventory", inv, "-o", archivePath)
	archive, err := dumpformat.ReadFile(archivePath)
	if err != nil {
		t.Fatal(err)
	}
	nativeAssertPublicConfigArchive(t, archive, inv, expected)
	projection, err := nativeProjectArchive(archive, protocol.ProjectionFields)
	if err != nil {
		t.Fatal(err)
	}
	nativeWriteRecordingBytes(t, filepath.Join(c.root, "archive-projection.json"), projection)
	if !bytes.Equal(projected, projection) || len(archive.Records("bead")) != sourceCount {
		t.Fatal("native B23 source/archive projection is incomplete")
	}
	data, err := os.ReadFile(archivePath)
	if err != nil {
		t.Fatal(err)
	}
	inspection, err := converterroundtrip.InspectImport(context.Background(), bytes.NewReader(data))
	if sourceCount == 20001 {
		if err == nil || err.Error() != "restore graph exceeds local service limit" {
			t.Fatal("native 20001 inspection boundary", err)
		}
		nativeWriteJSON(t, filepath.Join(c.root, "inspection-refusal.json"), map[string]any{"error": err.Error(), "archive_sha256": nativeHashBytes(data), "actual_source_records": sourceCount, "inspection": inspection, "target_opened": false, "imported_native_20001_claim": false})
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	profile := driver.RestoreImportProfile{Admission: driver.RestoreImportAdmission{Mode: "authenticated", Actor: model.PrincipalRef{Kind: "user", ID: "s1-native-recorder"}, Authority: "restore-admin", AdmissionDigest: nativeHashBytes([]byte("s1-b23 actual native count corpus"))}, SourceStoreID: inspection.SourceStoreID, Method: "offline.restore.import", IdempotencyKey: "s1-b23-" + *nativeRecordingID, ArchiveDigest: inspection.ArchiveSHA256, DerivedDigest: inspection.DerivedSHA256, HistoryMode: driver.RestoreHistoryModeSnapshot}
	nativeWriteJSON(t, filepath.Join(c.root, "import-profile.json"), profile)
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		if !t.Run("imported-"+engine, func(t *testing.T) {
			f := newFixture(t, engine)
			if err := f.lease.Release(f.ctx); err != nil {
				t.Fatal(err)
			}
			f.lease = nil
			f.clock.Advance(t, time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC).Sub(f.clock.Now()))
			f.bind(t)
			o := &nativePostObserver{leaseRoot: c.root}
			t.Cleanup(func() {
				o.mu.Lock()
				observations := append([]nativePostObservation(nil), o.observations...)
				o.mu.Unlock()
				nativeWriteJSON(t, filepath.Join(c.root, engine+"-b23-subscriber.json"), map[string]any{"count": len(observations), "observations": observations})
			})
			o.startHostLease(t, f)
			t.Cleanup(func() { o.stopHostLease(t) })
			report, err := converterroundtrip.RestoreImportWithLease(f.ctx, bytes.NewReader(data), f.store, f.lease, profile)
			if err != nil {
				t.Fatal("real native B23 import", err)
			}
			nativeWriteJSON(t, filepath.Join(c.root, engine+"-restore.json"), report.Restored)
			var denied atomic.Bool
			nativeBindImported(t, f, o, &denied)
			nativeAssertSnapshotHistoryBoundary(t, c.root, f)
			nativeB23ImportedMatrix(t, c.root, f, o, &denied)
		}) {
			return
		}
	}
}

func nativeB23ImportedMatrix(t *testing.T, root string, f *fixture, o *nativePostObserver, denied *atomic.Bool) {
	t.Helper()
	before := nativeImportedState(t, f)
	nativeWriteJSON(t, filepath.Join(root, f.engine+"-b23-before.json"), nativeRecordState(t, before))
	resources := nativeB23Want("alphaa", "betaaa", "gammaa", "bugrow", "closed", "plainx")
	queries := []struct {
		query string
		want  []string
	}{
		{"", resources}, {"  ", resources}, {"label=Alpha", nativeB23Want("alphaa", "bugrow")}, {"label=alpha", nativeB23Want("betaaa")},
		{"label=Alpha AND label=gamma", nativeB23Want("bugrow")}, {"label=alpha OR label=gamma", nativeB23Want("betaaa", "gammaa", "bugrow")},
		{"NOT label=Alpha", nativeB23Want("betaaa", "gammaa", "closed", "plainx")}, {"(label=Alpha OR label=alpha) AND NOT label=gamma", nativeB23Want("alphaa", "betaaa")},
		{"label=\"two words\"", nativeB23Want("alphaa")}, {"labels=shared", nativeB23Want("alphaa", "betaaa")},
		{"title=Needle", nativeB23Want("alphaa", "betaaa", "bugrow")}, {"title=needle", nativeB23Want("closed")},
		{"status=todo", nativeB23Want("alphaa", "bugrow", "plainx")}, {"status=done", nativeB23Want("closed")}, {"status=backlog", nativeB23Want("gammaa")},
		{"status=in_progress", nativeB23Want("betaaa")}, {"type=issue AND status=todo", nativeB23Want("alphaa", "bugrow", "plainx")}, {"type=bug", nativeB23Want()}, {"type=unknown", nativeB23Want()}, {"status=unknown", nativeB23Want()},
	}
	for i, q := range queries {
		def := nativeViewDefinition(q.query, resources, 1000, "id")
		ids, cursor := nativeViewIDs(t, f.call("view.run", fmt.Sprintf("b23-matrix-list-%d", i), map[string]any{"definition": def}))
		if cursor != "" || !reflect.DeepEqual(ids, q.want) {
			t.Fatalf("imported %s query %q got=%v want=%v", f.engine, q.query, ids, q.want)
		}
		def["budgetTokens"] = 1
		nativeB23Count(t, f, fmt.Sprintf("b23-matrix-count-%d", i), def, len(q.want))
	}
	for i, q := range []string{"status=todo OR status=done", "NOT status=todo", "label=Alpha XOR label=gamma", "label=", "label=Alpha AND", "priority=2"} {
		want := "invalid_params"
		if q == "priority=2" {
			want = "unknown_field"
		}
		r := f.call("view.run", fmt.Sprintf("b23-invalid-%d", i), map[string]any{"countOnly": true, "definition": nativeViewDefinition(q, resources, 1, "id")})
		if r.Error == nil || r.Error.Data.Code != want || len(r.Result) != 0 {
			t.Fatal("imported query refusal lost typed/no-result contract", q, r.Error, string(r.Result))
		}
	}
	sameState(t, before, nativeImportedState(t, f))
	def20k := nativeViewDefinition("", []string{}, 1, "id")
	nativeB23Count(t, f, "b23-imported-20000-control", def20k, 20000)
	// Frozen conversion maps native built-in task/bug to actual type issue.
	// A real production create crosses the imported 20000 image to 20001;
	// it is not relabeled as a 20001-native-source import.
	// Restore installs the actual random allocator. Bind its returned identity;
	// the ordinary fixture's fallback ID sequence does not control this route.
	allocatorBefore := nativeB23Allocator(t, f, before.Meta)
	fallbackBefore := f.idIndex
	ref, created := f.create(t, "b23-live-cap-member", "actual public Plug cap member")
	if err := allocatorBefore.ValidateCandidate("", ref.ID); err != nil || !ref.IsID() || !ref.Branch.IsZero() || f.idIndex != fallbackBefore || created.Revision != model.Natural64(2) || before.Meta.Head != model.Natural64(1) || len(created.Warnings) != 0 {
		t.Fatal("B23 real public create failed its restored allocator/commit binding", ref, created, err)
	}
	capBefore := nativeImportedState(t, f)
	if len(capBefore.Records) != len(before.Records)+1 || len(capBefore.Changesets) != 1 || o.count() != 1 {
		t.Fatal("B23 real create did not add exactly one record/history/subscriber")
	}
	currentByID := map[string]driver.Record{}
	for _, record := range capBefore.Records {
		if _, exists := currentByID[record.Ref.ID]; exists {
			t.Fatal("duplicate imported census identity", record.Ref.ID)
		}
		currentByID[record.Ref.ID] = record
	}
	for _, old := range before.Records {
		current, exists := currentByID[old.Ref.ID]
		if !exists || !reflect.DeepEqual(old, current) || !reflect.DeepEqual(before.Versions[old.Ref.ID], capBefore.Versions[old.Ref.ID]) {
			t.Fatal("cap create changed/lost prior native record/version", old.Ref.ID)
		}
	}

	for _, old := range before.Records {
		if old.Ref.ID == ref.ID {
			t.Fatal("B23 allocator reused an imported identity", ref)
		}
	}
	record, exists := currentByID[ref.ID]
	var body struct {
		ID, Type, Project string
		Fields            map[string]any
	}
	if !exists || record.Ref.ID != ref.ID || !record.Ref.IsID() || !record.Ref.Branch.IsZero() || record.Kind != "bead" || record.Revision != created.Revision || capBefore.Meta.Head != created.Revision || capBefore.Meta.HistoryFloor != before.Meta.HistoryFloor || len(created.Results) != 1 || created.Results[0].Ref != ref || created.Results[0].Revision != created.Revision || !bytes.Equal(record.Body, created.Results[0].Record) {
		t.Fatal("B23 cap member lost its exact returned/stored after image", ref, created)
	}
	if err := json.Unmarshal(record.Body, &body); err != nil || body.ID != ref.ID || body.Type != "issue" || body.Project != project || body.Fields["title"] != "actual public Plug cap member" {
		t.Fatal("B23 cap member does not match the actual project/type/title predicate", body, err)
	}
	allocatorAfter := nativeB23Allocator(t, f, capBefore.Meta)
	if !reflect.DeepEqual(allocatorBefore, allocatorAfter) {
		t.Fatal("B23 random allocator changed its counter-free profile")
	}
	nativeWriteJSON(t, filepath.Join(root, f.engine+"-b23-allocator.json"), map[string]any{"before": allocatorBefore, "after": allocatorAfter, "created_ref": ref, "commit": created, "fallback_calls_before": fallbackBefore, "fallback_calls_after": f.idIndex})
	versions := capBefore.Versions[ref.ID]
	if len(versions) != 1 || versions[0].Ref.ID != ref.ID || !versions[0].Ref.IsID() || !versions[0].Ref.Branch.IsZero() || versions[0].Revision != created.Revision || !versions[0].Base || versions[0].Deleted || !bytes.Equal(versions[0].Body, record.Body) {
		t.Fatal("B23 cap member lacks its one complete creation version")
	}
	receipt, ok := capBefore.Receipts["bead.create/"+key("b23-live-cap-member")]
	if !ok || len(capBefore.Receipts) != len(before.Receipts)+1 || !reflect.DeepEqual(receipt.Commit, created) || receipt.RequestHash == "" || receipt.ResponsePending || !reflect.DeepEqual(receipt.Changeset, capBefore.Changesets[0]) {
		t.Fatal("B23 create receipt lost the full winning commit/history")
	}
	requestBytes, err := json.Marshal(struct {
		ChangeSet               model.ChangeSet
		Method, Project, Return string
		DryRun                  bool
	}{model.ChangeSet{Ops: []model.Op{{Kind: model.OpCreate, Create: &model.CreateOp{Type: "issue", Fields: map[string]any{"title": "actual public Plug cap member"}, Project: project}}}}, "bead.create", project, "full", false})
	if err != nil || receipt.RequestHash != nativeHashBytes(requestBytes) {
		t.Fatal("B23 create receipt does not bind the actual source-defined request", receipt.RequestHash, err)
	}
	o.mu.Lock()
	observation := o.observations[0]
	o.mu.Unlock()
	if observation.Snapshot.At == nil || *observation.Snapshot.At != created.Revision || observation.Snapshot.Incarnation != capBefore.Meta.Incarnation || observation.Change.Revision != created.Revision || len(observation.Change.Changes) != 1 || len(observation.Rows) != 1 || !reflect.DeepEqual(observation.Rows[0], record) {
		t.Fatal("B23 one create callback lost its actual committed snapshot/full record")
	}
	change := observation.Change.Changes[0]
	if change.Resource.Kind != "bead" || change.Resource.Ref.ID != ref.ID || !change.Resource.Ref.IsID() || !change.Resource.Ref.Branch.IsZero() || change.Op.Kind != model.OpCreate || len(change.Before) != 0 || !bytes.Equal(change.After, record.Body) {
		t.Fatal("B23 create callback lost its full source-defined change")
	}
	visible := nativeViewDefinition("type=issue", []string{ref.ID}, 1000, "id")
	visibleIDs, visibleCursor := nativeViewIDs(t, f.call("view.run", "b23-created-current-visible", map[string]any{"definition": visible}))
	if !reflect.DeepEqual(visibleIDs, []string{ref.ID}) || visibleCursor != "" {
		t.Fatal("B23 cap member is not currently visible in the same issue set", visibleIDs, visibleCursor)
	}
	visible["budgetTokens"] = 1
	nativeB23Count(t, f, "b23-created-current-visible-count", visible, 1)

	nativeWriteJSON(t, filepath.Join(root, f.engine+"-b23-cap-entry.json"), nativeRecordState(t, capBefore))
	// Both ordinary modes refuse the real 20001 authorized set; fields=[]
	// grants no cap exception.
	def := nativeViewDefinition("", []string{}, 1, "id")
	for _, count := range []bool{false, true} {
		for _, empty := range []bool{false, true} {
			if empty {
				def["fields"] = []string{}
			} else {
				def["fields"] = []string{"id"}
			}
			r := f.call("view.run", fmt.Sprintf("b23-cap-%t-%t", count, empty), map[string]any{"countOnly": count, "definition": def})
			if r.Error == nil || r.Error.Data.Code != "scope_too_large" || len(r.Result) != 0 {
				t.Fatal("imported 20001 cap returned partial result", r.Error, string(r.Result))
			}
		}
	}
	// Filtering occurs before the cap. Excluding the single done label leaves exactly 20000.
	def = nativeViewDefinition("NOT label=done", []string{}, 1, "id")
	nativeB23Count(t, f, "b23-cap-filter", def, 20000)
	// The same real beta denial used by the independently pinned test policy
	// leaves exactly 20000 authorized rows, without a count or cap trace.
	denied.Store(true)
	def = nativeViewDefinition("", []string{}, 1, "id")
	nativeB23Count(t, f, "b23-cap-current-denial", def, 20000)
	denied.Store(false)
	historical := viewops.AsOf{Revision: before.Meta.Head, Incarnation: before.Meta.Incarnation}
	nativeB23HistoricalCap(t, f, "b23-live", historical, ref, denied)
	nativeReopenImported(t, f, o, denied)
	nativeB23HistoricalCap(t, f, "b23-reopened", historical, ref, denied)
	def = nativeViewDefinition("NOT label=done", []string{}, 1, "id")
	nativeB23Count(t, f, "b23-reopened-count", def, 20000)
	after := nativeImportedState(t, f)
	nativeWriteJSON(t, filepath.Join(root, f.engine+"-b23-after.json"), nativeRecordState(t, after))
	sameState(t, capBefore, after)
	if o.count() != 1 {
		t.Fatal("imported B23 reads posted beyond the one real create")
	}
}

// Read the real restored allocator at the same immutable census head.
func nativeB23Allocator(t *testing.T, f *fixture, meta driver.Meta) driver.OfflineAllocator {
	t.Helper()
	at := meta.Head
	r, err := f.store.Read(f.ctx, driver.View{Incarnation: meta.Incarnation, At: &at})
	if err != nil {
		t.Fatal(err)
	}
	reader, ok := r.(driver.AllocatorReader)
	if !ok {
		_ = r.Close()
		t.Fatal("B23 restored store has no allocator reader")
	}
	allocator, readErr := reader.LoadAllocator(f.ctx, project)
	closeErr := r.Close()
	if readErr != nil || closeErr != nil || allocator.Validate() != nil || !reflect.DeepEqual(allocator, driver.NewRandomAllocator(project)) {
		t.Fatal("B23 actual restored allocator differs from its admitted random profile", allocator, readErr, closeErr)
	}
	return allocator
}

func nativeB23Count(t *testing.T, f *fixture, label string, definition map[string]any, want int) {
	t.Helper()
	nativeB23CountAt(t, f, label, definition, want, nil)
}

func nativeB23CountAt(t *testing.T, f *fixture, label string, definition map[string]any, want int, historical *viewops.AsOf) {
	t.Helper()
	meta, err := f.store.Meta(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	wantSnapshot := viewops.AsOf{Revision: meta.Head, Incarnation: meta.Incarnation}
	params := map[string]any{"countOnly": true, "definition": definition}
	if historical != nil {
		wantSnapshot = *historical
		params["asOf"] = *historical
	}
	r := f.call("view.run", label, params)
	if r.Error != nil {
		t.Fatal("B23 exact count refused", label, r.Error)
	}
	var body map[string]json.RawMessage
	if err := json.Unmarshal(r.Result, &body); err != nil {
		t.Fatal(err)
	}
	var count int
	var items []any
	if err := json.Unmarshal(body["count"], &count); err != nil || count != want {
		t.Fatal("B23 full predicate count", label, count, want, err)
	}
	if err := json.Unmarshal(body["items"], &items); err != nil || len(items) != 0 || !bytes.Equal(bytes.TrimSpace(body["items"]), []byte("[]")) {
		t.Fatal("B23 count emitted items", label, err)
	}
	for _, key := range []string{"groups", "elisions", "nextCursor"} {
		if _, ok := body[key]; ok {
			t.Fatal("B23 count emitted page/group output", key)
		}
	}
	var snapshot viewops.AsOf
	if err := json.Unmarshal(body["asOf"], &snapshot); err != nil || snapshot != wantSnapshot {
		t.Fatal("B23 count changed its selected content snapshot", label, snapshot, wantSnapshot, err)
	}
}

// Content stays pinned while every request re-evaluates the current read
// policy. In particular, a later 20001st row cannot contaminate a 20000-row
// historical count, and a current denial must change both snapshots' counts.
// Run this same oracle on genuine native imports and independently created
// public Plug state; the latter supplies no native-source qualification.
func nativeB23HistoricalCap(t *testing.T, f *fixture, label string, historical viewops.AsOf, created model.Ref, denied *atomic.Bool) {
	t.Helper()
	meta, err := f.store.Meta(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	head, headErr := meta.Head.Uint64()
	old, oldErr := historical.Revision.Uint64()
	if headErr != nil || oldErr != nil || meta.Incarnation != historical.Incarnation || head <= old {
		t.Fatal("historical cap needs an earlier snapshot in this incarnation", meta, historical, err)
	}
	for _, empty := range []bool{false, true} {
		def := nativeViewDefinition("", []string{}, 1, "id")
		if empty {
			def["fields"] = []string{}
		}
		prefix := fmt.Sprintf("%s-fields-empty-%t", label, empty)
		denied.Store(false)
		nativeB23CountAt(t, f, prefix+"-historical-allowed", def, 20000, &historical)
		denied.Store(true)
		nativeB23CountAt(t, f, prefix+"-historical-current-denial", def, 19999, &historical)
		nativeB23Count(t, f, prefix+"-head-current-denial", def, 20000)
		denied.Store(false)
		nativeB23CountAt(t, f, prefix+"-historical-regrant", def, 20000, &historical)
		for _, countOnly := range []bool{false, true} {
			r := f.call("view.run", fmt.Sprintf("%s-head-regrant-%t", prefix, countOnly), map[string]any{"countOnly": countOnly, "definition": def})
			if r.Error == nil || r.Error.Data.Code != "scope_too_large" || len(r.Result) != 0 {
				t.Fatal("current regrant must restore the full cap refusal, without a partial answer", r.Error, string(r.Result))
			}
		}
	}
	def := nativeViewDefinition("", []string{created.ID}, 1, "id")
	nativeB23CountAt(t, f, label+"-new-member-historically-absent", def, 0, &historical)
	nativeB23Count(t, f, label+"-new-member-currently-present", def, 1)
}

// This ordinary control creates every member through the real public wire.
// It does not dispatch Beads, synthesize an import archive, or stand in for the
// separately pinned native 20001 + fresh 20000 recording and replay workload.
func TestB23HistoricalCapCurrentAuthorizationAcrossStores(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			// Each engine owns its store, handler and receipts. Overlap the
			// full public workloads under the unchanged parent deadline.
			t.Parallel()
			var denied atomic.Bool
			f := newFixtureWithConfig(t, engine, nil, func(_ context.Context, _ model.PrincipalRef, resource model.ResourceRef, _ string) error {
				if denied.Load() && resource.Ref.ID == "b23-public-00000" {
					return &model.Error{Data: model.ErrorData{Code: "forbidden"}}
				}
				return nil
			})
			// The full public workload outlives the SQL host lease TTL. Use
			// the existing keeper and drain it before each physical reopen.
			o := &nativePostObserver{leaseRoot: t.TempDir()}
			if outputs := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR"); outputs != "" {
				o.leaseRoot = outputs
			}
			o.startHostLease(t, f)
			t.Cleanup(func() { o.stopHostLease(t) })
			f.ids = make([]string, 20001)
			for i := range f.ids {
				f.ids[i] = fmt.Sprintf("b23-public-%05d", i)
			}
			const batchSize = 100
			for start := 0; start < 20000; start += batchSize {
				ops := make([]any, batchSize)
				for i := range ops {
					ops[i] = map[string]any{"kind": "create", "type": "issue", "fields": map[string]any{"title": f.ids[start+i]}}
				}
				c := commit(t, f.call("tx.apply", fmt.Sprintf("b23-public-setup-%05d", start), map[string]any{"changeSet": map[string]any{"ops": ops}}))
				if c.Revision != model.Natural64(uint64(start/batchSize+1)) || len(c.Results) != batchSize {
					t.Fatal("public cap setup lost a batch revision/member", start, c)
				}
				for i, result := range c.Results {
					if !result.Ref.IsID() || !result.Ref.Branch.IsZero() || result.Ref.ID != f.ids[start+i] || result.Revision != c.Revision {
						t.Fatal("public cap setup lost an exact returned identity/revision", start+i, result)
					}
				}
			}
			before := nativeImportedState(t, f)
			if len(before.Records) != 20000 || len(before.Changesets) != 200 || len(before.Receipts) != 200 {
				t.Fatal("ordinary public cap setup did not create the full 20000-row history", len(before.Records), len(before.Changesets), len(before.Receipts))
			}
			historical := viewops.AsOf{Revision: before.Meta.Head, Incarnation: before.Meta.Incarnation}
			ref, created := f.create(t, "b23-public-cross-cap", "actual public cap member")
			if created.Revision != model.Natural64(201) || ref.ID != f.ids[20000] {
				t.Fatal("public create did not cross the historical cap boundary", ref, created)
			}
			capBefore := nativeImportedState(t, f)
			if len(capBefore.Records) != 20001 || len(capBefore.Changesets) != 201 || len(capBefore.Receipts) != 201 {
				t.Fatal("public cap crossing did not add exactly one row/commit/receipt")
			}
			nativeB23HistoricalCap(t, f, "b23-public-live", historical, ref, &denied)
			sameState(t, capBefore, nativeImportedState(t, f))
			o.stopHostLease(t)
			f.reopen(t)
			o.startHostLease(t, f)
			nativeB23HistoricalCap(t, f, "b23-public-reopened", historical, ref, &denied)
			sameState(t, capBefore, nativeImportedState(t, f))
			t.Logf("B23 public historical cap PASS: engine=%s current=20001 historical=20000 current-denial=20000/19999; regrant refusal and full reader state conserved", engine)
		})
	}
}
