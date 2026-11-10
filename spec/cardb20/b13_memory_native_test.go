package cardb20

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/publicnative"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// B13 shares this package solely to consume native_build_test.go and the
// original nativeRecorder.command/workspace support without copying a builder.
// Neither existing B20 test nor helper behavior is modified or re-executed.
const b13PublicKey = "  Public Key/Ω e\u0301  "
const b13TrimmedPublicKey = "Public Key/Ω e\u0301"
const b13TrimmedPublicBody = "Exact trimmed native neighbor"
const b13PublicBody = " \tVerbatim native\n東京 e\u0301  "
const b13PublicReplacement = "\nReplaced Ω\t東京 "
const b13DerivedBody = "Keep these exact\nwords."

// Independent literal key vectors cover Unicode identity and both derivation
// limits against the actual pinned public implementation.
var b13KeyVectors = []struct{ name, explicit, content, effective string }{
	{"composed", "Unicode é", "composed value", "Unicode é"},
	{"decomposed", "Unicode e\u0301", "decomposed value", "Unicode e\u0301"},
	{"eight-segments", "", "One two three four five six seven eight NINE ten ELEVEN", "one-two-three-four-five-six-seven-eight"},
	{"sixty-bytes", "", strings.Repeat("A", 66) + " tail", strings.Repeat("a", 60)},
	{"non-ascii-collapse", "", "Ä ONE/二 TWO THREE", "one-two-three"},
	{"explicit-underivable", "東京 key", "東京 !!!", "東京 key"},
}

func b13ExpectedDerived(body string) string {
	if body == b13DerivedBody {
		return "keep-these-exact-words"
	}
	for _, vector := range b13KeyVectors {
		if vector.explicit == "" && vector.content == body {
			return vector.effective
		}
	}
	panic("missing literal B13 derived-key vector")
}

func b13Decode(t *testing.T, data []byte, target any) {
	t.Helper()
	d := json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()
	if err := d.Decode(target); err != nil {
		t.Fatalf("decode actual response: %v: %s", err, data)
	}
}

func b13Same(t *testing.T, got, want any) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("B13 exact comparison: got=%+v want=%+v", got, want)
	}
}

type b13Reference struct {
	r           *nativeRecorder
	w           nativeWorkspace
	binary      string
	projections map[string]map[string]string
}

func (n *b13Reference) call(label string, args []string, exit int) []byte {
	n.r.t.Helper()
	out := n.r.command(n.w, label, n.binary, args, exit != 0)
	var receipt struct {
		Exit int `json:"exit"`
	}
	data, err := os.ReadFile(filepath.Join(n.r.root, label+".request.json"))
	if err != nil {
		n.r.t.Fatal(err)
	}
	b13Decode(n.r.t, data, &receipt)
	if receipt.Exit != exit {
		n.r.t.Fatalf("native %s exit=%d want%d", label, receipt.Exit, exit)
	}
	return out
}

func (n *b13Reference) sql(label, query string) []map[string]any {
	n.r.t.Helper()
	var rows []map[string]any
	b13Decode(n.r.t, n.call(label, []string{"--json", "sql", query}, 0), &rows)
	if rows == nil {
		rows = []map[string]any{}
	}
	sort.Slice(rows, func(i, j int) bool {
		a, _ := json.Marshal(rows[i])
		b, _ := json.Marshal(rows[j])
		return string(a) < string(b)
	})
	return rows
}

// Every native physical table, current Dolt status and real history identity
// are captured for conservation. Values remain public fixture values only.
func (n *b13Reference) image(label string) map[string]any {
	n.r.t.Helper()
	rows := map[string][]map[string]any{}
	valid := regexp.MustCompile(`^[A-Za-z0-9_]+$`)
	for _, row := range n.sql(label+"-tables", "SHOW TABLES") {
		if len(row) != 1 {
			n.r.t.Fatal("native table census shape changed")
		}
		for _, value := range row {
			name, ok := value.(string)
			if !ok || !valid.MatchString(name) {
				n.r.t.Fatal("unsafe native table identity")
			}
			rows[name] = n.sql(label+"-table-"+name, "SELECT * FROM `"+name+"`")
		}
	}
	if len(rows) == 0 {
		n.r.t.Fatal("native physical image is empty")
	}
	image := map[string]any{"tables": rows, "head": n.sql(label+"-head", "SELECT DOLT_HASHOF('HEAD') AS head"), "history": n.sql(label+"-history", "SELECT * FROM dolt_log"), "status": n.sql(label+"-status", "SELECT * FROM dolt_status")}
	n.r.writeJSON(filepath.Join(n.r.root, label+".image.json"), image)
	return image
}

func (n *b13Reference) view(label string, want map[string]string) {
	n.r.t.Helper()
	var wire map[string]any
	b13Decode(n.r.t, n.call(label+"-memories", []string{"--json", "memories"}, 0), &wire)
	if wire == nil || wire["schema_version"] != json.Number("1") {
		n.r.t.Fatal("native memories missing schema_version=1")
	}
	got := map[string]string{}
	for key, value := range wire {
		if key == "schema_version" {
			continue // Checked native wire metadata, not a memory value.
		}
		text, ok := value.(string)
		if !ok {
			n.r.t.Fatal("native memory value is not a string")
		}
		got[key] = text
	}
	b13Same(n.r.t, got, want)
	n.r.writeJSON(filepath.Join(n.r.root, label+"-memory-wire-and-projection.json"), map[string]any{"literal_native_wire": wire, "actual_memory_values": got, "wire_metadata": map[string]any{"schema_version": json.Number("1")}})
	n.projections[label] = got
}

func TestB13CanonicalPublicMemoryCounterparts(t *testing.T) {
	root := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR")
	if !filepath.IsAbs(root) {
		t.Fatal("absolute retained outputs required")
	}
	checkout := os.Getenv("PLUG_SOURCE_MODULE_ROOT")
	r := &nativeRecorder{t: t, root: root}
	actual := strings.TrimSpace(string(r.buildCommand(checkout, "b13-candidate-head", "/usr/bin/git", []string{"rev-parse", "HEAD"})))
	if actual == "" || actual != os.Getenv("PLUG_B13_REQUEST_SHA") {
		t.Fatal("B13 request/actual checkout mismatch")
	}
	protocol, err := os.ReadFile(filepath.Join(checkout, "spec/s1-b09-native/native_inputs.json"))
	if err != nil || hashBytes(protocol) != "a7ddd9047eb94e54cdaa138b1adf756c5088a50bc01605ee2c6eafbfb9d0d9c2" {
		t.Fatal("existing canonical source protocol differs", err)
	}
	var input struct {
		ExporterLabel string `json:"exporter_label"`
	}
	b13Decode(t, protocol, &input)
	if input.ExporterLabel == "" {
		t.Fatal("canonical production executable label absent")
	}
	r.target = input.ExporterLabel
	r.writeJSON(filepath.Join(root, "b13-request-binding.json"), map[string]any{"candidate": actual, "target": os.Getenv("TEST_TARGET"), "run": os.Getenv("GITHUB_RUN_ID"), "attempt": os.Getenv("GITHUB_RUN_ATTEMPT"), "workflow_sha": os.Getenv("GITHUB_SHA"), "workflow_ref": os.Getenv("GITHUB_WORKFLOW_REF"), "native_source": pinnedSource, "source_protocol_sha256": hashBytes(protocol), "scope": "PUBLIC_OPERATIONAL_MEMORY_ONLY", "official_admission": "NOT_RUN", "imported_replace_forget": "DECIDED_OVERLAY_PUBLIC_COMPARISON"})
	binary := r.buildCanonicalSource(pinnedSource) // Exact existing B20 recipe, unchanged.
	var projection map[string]map[string]string
	if !t.Run("native-reference", func(t *testing.T) {
		dir := filepath.Join(root, "b13-native")
		if err := os.Mkdir(dir, 0700); err != nil {
			t.Fatal(err)
		}
		nr := &nativeRecorder{t: t, root: dir, target: r.target}
		workspace := t.TempDir()
		home, project := filepath.Join(workspace, "home"), filepath.Join(workspace, "project")
		for _, p := range []string{home, project, filepath.Join(workspace, "tmp")} {
			if err := os.Mkdir(p, 0700); err != nil {
				t.Fatal(err)
			}
		}
		server := publicnative.StartDolt(t, dir)
		base := []string{"PATH=/tmp/plug-swarm/bin/worker-safe:/usr/bin:/bin", "HOME=" + home, "USER=b13-public", "TZ=UTC", "LC_ALL=C", "NO_COLOR=1", "CI=true", "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_AUTHOR_NAME=B13 Public", "GIT_AUTHOR_EMAIL=b13@example.invalid", "GIT_COMMITTER_NAME=B13 Public", "GIT_COMMITTER_EMAIL=b13@example.invalid", "TMPDIR=" + filepath.Join(workspace, "tmp"), "BEADS_ACTOR=b13-public-recorder", "BEADS_NO_DAEMON=1"}
		n := &b13Reference{r: nr, w: nativeWorkspace{dir: project, env: server.ChildEnv(base)}, binary: binary, projections: map[string]map[string]string{}}
		nr.command(n.w, "git-init", "/usr/bin/git", []string{"-c", "core.hooksPath=/dev/null", "init", "--quiet", "--template="}, false)
		var version struct{ Commit, Build string }
		b13Decode(t, n.call("version", []string{"version", "--json"}, 0), &version)
		if version.Commit != pinnedSource.Commit || version.Build != "b20-public-fixture-"+pinnedSource.Commit {
			t.Fatal("actual binary/source version mismatch")
		}
		n.call("init", []string{"init", "--server", "--external", "--server-host", "127.0.0.1", "--server-port", strconv.Itoa(server.Port), "--database", "b13_memory", "--prefix", "b13", "--skip-hooks", "--skip-agents", "--non-interactive", "--quiet"}, 0)
		server.RequireDatabaseDir(t, "b13_memory")
		n.call("seed-empty-and-settings", []string{"--json", "sql", "INSERT INTO config (`key`,value) VALUES ('kv.memory.empty',''),('kv.b13-generic','generic survivor'),('b13.setting','settings survivor')"}, 0)
		// Publish the explicit present-empty/settings setup before operational
		// memory writes. Their normal CLI commit guard refuses unrelated dirty
		// internal settings; this baseline is setup, not native remember.
		n.call("publish-seeded-settings", []string{"dolt", "commit", "--message", "B13 public memory fixture baseline"}, 0)
		n.image("published-source-baseline")
		t.Run("empty-versus-absent", func(t *testing.T) {
			for _, v := range []struct {
				label, key, value string
				found             bool
				exit              int
			}{{"empty", "empty", "", true, 0}, {"absent", "absent", "", false, 1}} {
				var got map[string]any
				b13Decode(t, n.call("recall-"+v.label, []string{"--json", "recall", v.key}, v.exit), &got)
				b13Same(t, got, map[string]any{"schema_version": json.Number("1"), "key": v.key, "value": v.value, "found": v.found})
			}
			n.view("initial", map[string]string{"empty": ""})
		})
		remember := func(label, key, content, action string) {
			args := []string{"--json", "remember", content}
			if key != "" {
				args = append(args, "--key", key)
			}
			var got map[string]any
			b13Decode(t, n.call(label, args, 0), &got)
			wantKey := key
			if wantKey == "" {
				wantKey = b13ExpectedDerived(content)
			}
			b13Same(t, got, map[string]any{"schema_version": json.Number("1"), "key": wantKey, "value": content, "action": action})
		}
		t.Run("explicit-and-derived-unicode-keys", func(t *testing.T) {
			remember("explicit", b13PublicKey, b13PublicBody, "remembered")
			remember("neighbor", b13PublicKey+"neighbor", "exact neighbor", "remembered")
			remember("trimmed-neighbor", b13TrimmedPublicKey, b13TrimmedPublicBody, "remembered")
			remember("derived", "", b13DerivedBody, "remembered")
			n.view("created", map[string]string{"empty": "", b13PublicKey: b13PublicBody, b13TrimmedPublicKey: b13TrimmedPublicBody, b13PublicKey + "neighbor": "exact neighbor", "keep-these-exact-words": b13DerivedBody})
		})
		t.Run("replacement-and-noop-conservation", func(t *testing.T) {
			remember("replace", b13PublicKey, b13PublicReplacement, "updated")
			n.view("replaced", map[string]string{"empty": "", b13PublicKey: b13PublicReplacement, b13TrimmedPublicKey: b13TrimmedPublicBody, b13PublicKey + "neighbor": "exact neighbor", "keep-these-exact-words": b13DerivedBody})
			before := n.image("before-noop")
			remember("same-value", b13PublicKey, b13PublicReplacement, "updated")
			after := n.image("after-noop")
			b13Same(t, after, before)
		})
		t.Run("refusals-preserve-all-native-tables-and-history", func(t *testing.T) {
			before := n.image("before-refusals")
			for _, v := range []struct {
				label string
				args  []string
			}{{"blank", []string{"--json", "remember", " \t\n", "--key", b13PublicKey}}, {"underivable", []string{"--json", "remember", "東京 !!!"}}, {"blank-key", []string{"--json", "remember", "valid", "--key", " \t "}}} {
				n.call("refuse-"+v.label, v.args, 1)
			}
			b13Same(t, n.image("after-refusals"), before)
		})
		t.Run("forget-exact-and-empty-keeps-settings", func(t *testing.T) {
			for i, key := range []string{b13PublicKey, "empty"} {
				var got map[string]any
				b13Decode(t, n.call(fmt.Sprintf("forget-%d", i), []string{"--json", "forget", key}, 0), &got)
				b13Same(t, got, map[string]any{"schema_version": json.Number("1"), "key": key, "deleted": "true"})
			}
			n.view("forgotten", map[string]string{b13TrimmedPublicKey: b13TrimmedPublicBody, b13PublicKey + "neighbor": "exact neighbor", "keep-these-exact-words": b13DerivedBody})
			before := n.image("before-forget-miss")
			n.call("forget-miss", []string{"--json", "forget", "empty"}, 1)
			b13Same(t, n.image("after-forget-miss"), before)
			var found map[string]any
			b13Decode(t, n.call("empty-now-absent", []string{"--json", "recall", "empty"}, 1), &found)
			b13Same(t, found, map[string]any{"schema_version": json.Number("1"), "key": "empty", "value": "", "found": false})
			settings := n.sql("settings-survive", "SELECT `key`,value FROM config WHERE `key` IN ('kv.b13-generic','b13.setting') ORDER BY `key`")
			b13Same(t, settings, []map[string]any{{"key": "b13.setting", "value": "settings survivor"}, {"key": "kv.b13-generic", "value": "generic survivor"}})
		})
		t.Run("forget-then-re-remember", func(t *testing.T) {
			remember("re-remember", b13PublicKey, b13PublicBody, "remembered")
			n.view("remembered", map[string]string{b13PublicKey: b13PublicBody, b13TrimmedPublicKey: b13TrimmedPublicBody, b13PublicKey + "neighbor": "exact neighbor", "keep-these-exact-words": b13DerivedBody})
		})
		t.Run("unicode-identity-and-derived-key-limits", func(t *testing.T) {
			want := map[string]string{}
			for key, value := range n.projections["remembered"] {
				want[key] = value
			}
			for _, vector := range b13KeyVectors {
				remember("key-vector-"+vector.name, vector.explicit, vector.content, "remembered")
				var got map[string]any
				b13Decode(t, n.call("key-recall-"+vector.name, []string{"--json", "recall", vector.effective}, 0), &got)
				b13Same(t, got, map[string]any{"schema_version": json.Number("1"), "key": vector.effective, "value": vector.content, "found": true})
				want[vector.effective] = vector.content
			}
			n.view("fidelity", want)
		})
		t.Run("empty-corpus-keeps-settings", func(t *testing.T) {
			var keys []string
			for key := range n.projections["fidelity"] {
				keys = append(keys, key)
			}
			sort.Strings(keys)
			for i, key := range keys {
				var got map[string]any
				b13Decode(t, n.call(fmt.Sprintf("forget-all-%d", i), []string{"--json", "forget", key}, 0), &got)
				b13Same(t, got, map[string]any{"schema_version": json.Number("1"), "key": key, "deleted": "true"})
			}
			n.view("empty-corpus", map[string]string{})
			settings := n.sql("empty-corpus-settings-survive", "SELECT `key`,value FROM config WHERE `key` IN ('kv.b13-generic','b13.setting') ORDER BY `key`")
			b13Same(t, settings, []map[string]any{{"key": "b13.setting", "value": "settings survivor"}, {"key": "kv.b13-generic", "value": "generic survivor"}})
		})

		projection = n.projections
		nr.writeJSON(filepath.Join(dir, "actual-memory-projections.json"), projection)
	}) {
		t.Fatal("canonical public reference incomplete; no counterpart credit")
	}
	for _, engine := range engines {
		t.Run("paired-core-"+engine, func(t *testing.T) { b13ComparePlug(t, root, engine, projection, false) })
		t.Run("paired-imported-"+engine, func(t *testing.T) { b13ComparePlug(t, root, engine, projection, true) })
	}
}

type b13PlugState struct {
	Meta     driver.Meta
	Rows     []driver.Record
	Versions map[string][]driver.Version
	History  []model.Changeset
	Derived  map[driver.DerivedTable][]driver.DerivedRow
}

func b13PlugImage(t *testing.T, store driver.Store) b13PlugState {
	t.Helper()
	ctx := context.Background()
	meta, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	r, err := store.Read(ctx, driver.View{Incarnation: meta.Incarnation, At: &meta.Head})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	page, err := r.Query(ctx, driver.Query{Limit: 1000})
	if err != nil || page.NextCursor != nil {
		t.Fatal("incomplete B13 public state", err)
	}
	state := b13PlugState{Meta: meta, Rows: page.Items, Versions: map[string][]driver.Version{}, Derived: map[driver.DerivedTable][]driver.DerivedRow{}}
	ids := map[string]bool{"b13-empty": true, "b13-setting": true}
	for i := 1; i <= 32; i++ {
		ids[fmt.Sprintf("b13-compare-%d", i)] = true
	}
	for _, row := range page.Items {
		ids[row.Ref.ID] = true
	}
	for id := range ids {
		p, e := r.Versions(ctx, model.RefByID(id, model.Natural64(0)), driver.PageReq{Limit: 1000})
		if e != nil || p.NextCursor != nil {
			t.Fatal(e)
		}
		state.Versions[id] = p.Items
	}
	state.History, err = r.Changesets(ctx, nil, 1000)
	if err != nil || len(state.History) >= 1000 {
		t.Fatal(err)
	}
	for _, table := range []driver.DerivedTable{driver.DerivedReadyState, driver.DerivedAncestorClosure, driver.DerivedCutClosure, driver.DerivedAttribute, driver.DerivedSortKey, driver.DerivedReferenceIndex, driver.DerivedGrant, driver.DerivedOntology, driver.DerivedPlugins} {
		state.Derived[table], err = r.DerivedRows(ctx, table, "")
		if err != nil {
			t.Fatal(err)
		}
	}
	return state
}

func b13ComparePlug(t *testing.T, root, engine string, native map[string]map[string]string, imported bool) {
	f := openStore(t, engine)
	store := f.store
	ctx := context.Background()
	var lease driver.ActiveLease
	var server *rpc.Server
	serial, requests := 0, 0
	var frames []map[string]any
	var stop context.CancelFunc
	var renewed chan error
	bind := func() {
		var err error
		lease, err = store.Lease(ctx, "b13-public-counterpart", driver.LeaseTTL)
		if err != nil {
			t.Fatal(err)
		}
		live, cancel := context.WithCancel(ctx)
		stop = cancel
		renewed = make(chan error, 1)
		active, done := lease, renewed
		go func() {
			ticker := time.NewTicker(driver.LeaseRenewEvery)
			defer ticker.Stop()
			for {
				select {
				case <-live.Done():
					done <- nil
					return
				case <-ticker.C:
					if e := active.Renew(live); e != nil {
						if live.Err() != nil {
							e = nil
						}
						done <- e
						return
					}
				}
			}
		}()
		ontology := service.NewOntology(nil)
		writer := pipeline.New(store, lease, pipeline.WithOntology(ontology), pipeline.WithIDGenerator(func() (string, error) {
			serial++
			if serial > 32 {
				return "", fmt.Errorf("B13 ID inventory exhausted")
			}
			return fmt.Sprintf("b13-compare-%d", serial), nil
		}))
		domain := service.New(store, lease, writer, service.WithOntology(ontology))
		registry, e := rpc.NewRegistry()
		if e != nil {
			t.Fatal(e)
		}
		if e = rpc.BindService(registry, domain); e != nil {
			t.Fatal(e)
		}
		meta, e := store.Meta(ctx)
		if e != nil {
			t.Fatal(e)
		}
		server, e = rpc.New(registry, rpc.Config{DefaultProject: "b13compare", Incarnation: meta.Incarnation, CursorKey: []byte("b13-native-compare-cursor-00000001")})
		if e != nil {
			t.Fatal(e)
		}
	}
	closeStore := func() {
		if stop != nil {
			stop()
			if e := <-renewed; e != nil {
				t.Error(e)
			}
			stop = nil
		}
		if lease != nil {
			if e := lease.Release(ctx); e != nil {
				t.Error(e)
			}
			lease = nil
		}
		if store != nil {
			if e := store.Close(); e != nil {
				t.Error(e)
			}
			store = nil
		}
	}
	t.Cleanup(closeStore)
	bind()
	mode := "core"
	if imported {
		mode = "imported"
	}
	defer func() { retain(t, "b13-plug-"+mode+"-"+engine+"-frames.json", frames) }()
	call := func(label, method string, params map[string]any, key int, want string) json.RawMessage {
		t.Helper()
		requests++
		copied := map[string]any{}
		for k, v := range params {
			copied[k] = v
		}
		meta := map[string]any{"project": "b13compare"}
		if key != 0 {
			meta["idempotencyKey"] = fmt.Sprintf("%032d", key)
		}
		copied["_meta"] = meta
		request, e := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": requests, "method": method, "params": copied})
		if e != nil {
			t.Fatal(e)
		}
		raw := server.Dispatch(rpc.WithPrincipal(ctx, model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}), request)
		frames = append(frames, map[string]any{"label": label, "request": json.RawMessage(request), "response": json.RawMessage(raw), "request_sha256": hashBytes(request), "response_sha256": hashBytes(raw)})
		var reply response
		b13Decode(t, raw, &reply)
		if want != "" {
			if reply.Error == nil || reply.Error.Data.Code != want || reply.Result != nil {
				t.Fatalf("%s want%s: %s", label, want, raw)
			}
		} else if reply.Error != nil {
			t.Fatalf("%s: %s", label, raw)
		}
		return reply.Result
	}
	rows := []conformance.FixtureChange{
		{Ref: model.RefByID("b13-empty", model.Natural64(0)), Kind: "memory", Op: model.OpCreate, Body: []byte(`{"id":"b13-empty","key":"empty","content":""}`)},
		{Ref: model.RefByID("b13-setting", model.Natural64(0)), Kind: "config", Op: model.OpCreate, Body: []byte(`{"id":"b13-setting","key":"kv.memory.decoy","value":"generic survivor","content":"settings survivor"}`)},
	}
	if imported {
		rows = append(rows[1:], b13ConstructedSourceRows(t, store, engine)...)
	}
	_, err := conformance.Seed(ctx, store, lease, driver.ChangesetHeader{}, rows)
	if err != nil {
		t.Fatal(err)
	}
	initial := b13PlugImage(t, store)
	ids := map[string]string{"empty": "b13-empty"}
	if imported {
		for _, key := range []string{"empty", b13PublicKey} {
			ids[key] = driver.SourcePlaneMirrorIDV1("memory", "", "config", key)
		}
	}
	forgotten := map[string]bool{}
	conserve := func() {
		if !imported {
			return
		}
		after := b13PlugImage(t, store)
		found := map[string]driver.Record{}
		for _, row := range after.Rows {
			found[row.Ref.ID] = row
		}
		for _, original := range initial.Rows {
			b13Same(t, found[original.Ref.ID], original)
			b13Same(t, after.Versions[original.Ref.ID], initial.Versions[original.Ref.ID])
		}
	}

	view := func(label string) {
		t.Helper()
		before := b13PlugImage(t, store)
		got := map[string]string{}
		items := []service.Memory{}
		cursor := ""
		for pages := 0; ; pages++ {
			if pages > 32 {
				t.Fatal("B13 paging did not end")
			}
			params := map[string]any{"limit": 1}
			if cursor != "" {
				params["cursor"] = cursor
			}
			var page struct {
				Items []service.Memory `json:"items"`
				Next  string           `json:"nextCursor"`
			}
			b13Decode(t, call(label+"-list", "memory.list", params, 0, ""), &page)
			if page.Items == nil {
				t.Fatal("memory page missing")
			}
			for _, m := range page.Items {
				if _, exists := got[m.Key]; exists {
					t.Fatal("duplicate memory key")
				}
				got[m.Key] = m.Content
				items = append(items, m)
			}
			if page.Next == "" {
				break
			}
			if page.Next == cursor {
				t.Fatal("stuck memory cursor")
			}
			cursor = page.Next
		}
		b13Same(t, got, native[label])
		var brief struct {
			Memories []service.Memory `json:"memories"`
		}
		b13Decode(t, call(label+"-brief", "agent.brief", map[string]any{}, 0, ""), &brief)
		b13Same(t, brief.Memories, items)
		for _, item := range items {
			var point service.Memory
			b13Decode(t, call(label+"-get", "memory.get", map[string]any{"id": item.ID}, 0, ""), &point)
			b13Same(t, point, item)
		}
		b13Same(t, b13PlugImage(t, store), before)
		conserve()
		retain(t, "b13-plug-"+mode+"-"+engine+"-"+label+".json", map[string]any{"actual_native": native[label], "actual_Plug": got, "state": before, "adopted_differences": []string{"P85 core memory methods and agent.brief versus config-backed CLI", "typed not_found versus native found:false/exit1", "Plug IDs/revisions versus native keys/Dolt commits", "immutable source and attributed overlays versus overwritten native config rows"}})
	}
	save := func(label, key, body string, receipt int) model.Commit {
		params := map[string]any{"content": body}
		if key != "" {
			params["key"] = key
		}
		var commit model.Commit
		b13Decode(t, call(label, "memory.save", params, receipt, ""), &commit)
		if len(commit.Results) != 1 {
			t.Fatal("memory save missing identity")
		}
		wantKey := key
		if wantKey == "" {
			wantKey = b13ExpectedDerived(body)
		}
		if previous := ids[wantKey]; previous != "" && previous != commit.Results[0].Ref.ID && !forgotten[wantKey] {
			if !imported || commit.Results[0].Ref.ID != pipeline.MemoryOverlayRef(model.RefByID(previous, model.Natural64(0))).ID {
				t.Fatal("replacement changed an unrelated identity")
			}
		}
		forgotten[wantKey] = false
		ids[wantKey] = commit.Results[0].Ref.ID
		return commit
	}
	t.Run("empty-versus-absent", func(t *testing.T) {
		if !imported {
			view("initial")
		} else {
			var m service.Memory
			b13Decode(t, call("source-empty", "memory.get", map[string]any{"id": ids["empty"]}, 0, ""), &m)
			if m.Content != "" || m.Key != "empty" {
				t.Fatal("constructed source empty lost")
			}
		}
		before := b13PlugImage(t, store)
		call("absent", "memory.get", map[string]any{"id": "b13-absent"}, 0, "not_found")
		b13Same(t, b13PlugImage(t, store), before)
	})
	t.Run("explicit-and-derived-unicode-keys", func(t *testing.T) {
		save("explicit", b13PublicKey, b13PublicBody, 701)
		save("neighbor", b13PublicKey+"neighbor", "exact neighbor", 702)
		save("trimmed-neighbor", b13TrimmedPublicKey, b13TrimmedPublicBody, 706)
		save("derived", "", b13DerivedBody, 703)
		view("created")
	})
	t.Run("replacement-noop-and-refusals", func(t *testing.T) {
		save("replace", b13PublicKey, b13PublicReplacement, 704)
		view("replaced")
		before := b13PlugImage(t, store)
		save("same-value", b13PublicKey, b13PublicReplacement, 705)
		b13Same(t, b13PlugImage(t, store), before)
		for i, v := range []map[string]any{{"key": b13PublicKey, "content": " \t\n"}, {"content": "東京 !!!"}, {"key": " \t ", "content": "valid"}} {
			before := b13PlugImage(t, store)
			call(fmt.Sprintf("refusal%d", i), "memory.save", v, 711+i, "invalid_params")
			b13Same(t, b13PlugImage(t, store), before)
		}
	})
	reopen := func(label string) {
		before := b13PlugImage(t, store)
		if engine == "memory" {
			stop()
			if e := <-renewed; e != nil {
				t.Fatal(e)
			}
			stop = nil
			if e := lease.Release(ctx); e != nil {
				t.Fatal(e)
			}
			bind()
		} else {
			closeStore()
			var e error
			store, e = f.open()
			if e != nil {
				t.Fatal(e)
			}
			bind()
		}
		b13Same(t, b13PlugImage(t, store), before)
		t.Logf("B13 public native counterpart engine=%s phase=%s SQL_reopen=%t", engine, label, engine != "memory")
	}
	t.Run("sql-reopen-before-forget", func(t *testing.T) { reopen("before-forget"); view("replaced") })
	t.Run("forget-exact-and-empty-keeps-settings", func(t *testing.T) {
		for i, key := range []string{b13PublicKey, "empty"} {
			call("forget", "memory.delete", map[string]any{"id": ids[key]}, 721+i, "")
			forgotten[key] = true
		}
		view("forgotten")
		before := b13PlugImage(t, store)
		call("forget-miss", "memory.delete", map[string]any{"id": ids["empty"]}, 723, "not_found")
		b13Same(t, b13PlugImage(t, store), before)
		reopen("after-forget")
		view("forgotten")
		after := b13PlugImage(t, store)
		var setting driver.Record
		for _, row := range after.Rows {
			if row.Ref.ID == "b13-setting" {
				setting = row
			}
		}
		for _, row := range initial.Rows {
			if row.Ref.ID == "b13-setting" {
				b13Same(t, setting, row)
			}
		}
	})
	t.Run("forget-then-re-remember", func(t *testing.T) {
		save("re-remember", b13PublicKey, b13PublicBody, 731)
		view("remembered")
		reopen("after-re-remember")
		view("remembered")
		conserve()
	})
	t.Run("unicode-identity-and-derived-key-limits", func(t *testing.T) {
		for i, vector := range b13KeyVectors {
			save("key-vector-"+vector.name, vector.explicit, vector.content, 741+i)
		}
		view("fidelity")
		reopen("key-fidelity")
		view("fidelity")
	})
	t.Run("empty-corpus-and-normal-brief-through-sql-reopen", func(t *testing.T) {
		var keys []string
		for key := range native["fidelity"] {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for i, key := range keys {
			call("forget-all", "memory.delete", map[string]any{"id": ids[key]}, 761+i, "")
			call("forgotten-get", "memory.get", map[string]any{"id": ids[key]}, 0, "not_found")
		}
		view("empty-corpus")
		reopen("empty-corpus")
		view("empty-corpus")
		conserve()
	})

}

// These exact public rows are constructed view fixtures, not a restore receipt,
// physical capture or producer admission. The typed source-only plan deliberately
// declares no active entities. Real import/reverse controls run in converterroundtrip.
func b13ConstructedSourceRows(t *testing.T, store driver.Store, engine string) []conformance.FixtureChange {
	t.Helper()
	meta, err := store.Meta(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var changes []conformance.FixtureChange
	for _, entry := range []struct{ key, value string }{{"empty", ""}, {b13PublicKey, b13PublicBody}} {
		id := driver.SourcePlaneMirrorIDV1("memory", "", "config", entry.key)
		body, err := json.Marshal(map[string]any{"id": id, "key": entry.key, "content": entry.value, "source_id": entry.key, "source_project": "", "source_table": "config", "source": map[string]any{"key": "kv.memory." + entry.key, "value": entry.value}, "value": entry.value, "classification": "public", "metadata": map[string]any{"beads.classification": "public", "beads.memory.value_encoding": "text"}})
		if err != nil {
			t.Fatal(err)
		}
		changes = append(changes, conformance.FixtureChange{Ref: model.RefByID(id, model.Natural64(0)), Kind: "memory", Op: model.OpCreate, Body: body})
	}
	raw, err := json.Marshal(changes)
	if err != nil {
		t.Fatal(err)
	}
	digest := hashBytes(raw)
	lifetime := driver.CheckpointLifetimeProcess
	if engine != "memory" {
		lifetime = driver.CheckpointLifetimeRestart
	}
	plan := driver.SourcePlanePlanV1{Version: driver.SourcePlanePlanVersionV1, AddressContract: driver.SourcePlaneAddressContractV1, SourceStoreID: "b13-public-constructed-source", ArchiveSHA256: digest, Target: driver.SourcePlaneTargetV1{StoreID: string(meta.StoreID), Incarnation: meta.Incarnation, RequireEmpty: true, Lifetime: lifetime, RuntimeContract: driver.SourcePlaneRuntimeContractV1}, Providers: []driver.SourcePlaneProviderV1{{Alias: "workflow", Instance: "b13-memory-fixture", Namespace: "plugin/workflow/b13-memory-fixture/durable", Schema: "public-memory-fixture/1", Codec: "json/1", RuntimeContract: driver.SourcePlaneRuntimeContractV1, Lifetime: lifetime, LogicalImageSHA256: hashBytes([]byte(`{}`)), LogicalImageSize: 2, OriginAdapter: "constructed-memory-view/1"}}}
	binding, err := driver.NewSourcePlaneImportBindingV1(plan)
	if err != nil {
		t.Fatal(err)
	}
	profile := driver.RestoreImportProfile{Admission: driver.RestoreImportAdmission{Mode: "authenticated", Actor: model.PrincipalRef{Kind: "user", ID: "constructed-public-fixture"}, Authority: "restore-admin", AdmissionDigest: digest}, SourceStoreID: driver.StoreID(plan.SourceStoreID), Method: "offline.restore.import", IdempotencyKey: strings.Repeat("b", 32), ArchiveDigest: digest, DerivedDigest: digest, HistoryMode: driver.RestoreHistoryModeSnapshot, SourcePlanes: &binding}
	request, err := profile.RequestHash()
	if err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(map[string]any{"profile": profile, "request_hash": request})
	if err != nil {
		t.Fatal(err)
	}
	changes = append(changes, conformance.FixtureChange{Ref: model.RefByID("instance_config/restore-import/binding", model.Natural64(0)), Kind: "instance_config", Op: model.OpCreate, Body: body})
	return changes
}
