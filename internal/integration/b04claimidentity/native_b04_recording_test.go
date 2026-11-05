package b04claimidentity_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/publicnative"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type b04NativeCapture struct {
	RunID              string                     `json:"runID"`
	Database           string                     `json:"database"`
	SourceIDs          map[string]string          `json:"sourceIDs"`
	ReadyBefore        []string                   `json:"readyBefore"`
	Direct             string                     `json:"direct"`
	SameOwner          string                     `json:"sameOwner"`
	CustomActive       string                     `json:"customActive"`
	ForeignHeld        string                     `json:"foreignHeld"`
	DirectPost         b04NativeStoredIssue       `json:"directPost"`
	SameOwnerPost      b04NativeStoredIssue       `json:"sameOwnerPost"`
	CustomActivePost   b04NativeStoredIssue       `json:"customActivePost"`
	ForeignHeldPost    b04NativeStoredIssue       `json:"foreignHeldPost"`
	Foreign            b04NativeForeignProjection `json:"foreign"`
	NoOpHistorySame    bool                       `json:"noOpHistorySame"`
	NoOpVersionsSame   bool                       `json:"noOpVersionsSame"`
	NoOpStateSHA256    string                     `json:"noOpStateSHA256"`
	ForeignStateSHA256 string                     `json:"foreignStateSHA256"`
	ClaimNext          []b04NativeIssueProjection `json:"claimNext"`
	ClaimNextIDs       []string                   `json:"claimNextIDs"`
	EmptyJSON          json.RawMessage            `json:"emptyJSON"`
	Inventory          map[string]any             `json:"inventory"`
	DumpBytes          int64                      `json:"dumpBytes"`
	DumpRefusal        string                     `json:"dumpRefusal,omitempty"`
	Commands           int                        `json:"commands"`
	Output             string                     `json:"output"`
}

// b04NativeIssueProjection is deliberately smaller than the raw native
// payload. The complete stdout/stderr and command receipt are retained by
// b04NativeCommand; this projection records the observed identity and the
// hydrated claim postimage without turning an expected source ID into proof.
type b04NativeIssueProjection struct {
	ID        string `json:"id"`
	Status    string `json:"status"`
	Assignee  string `json:"assignee"`
	StartedAt string `json:"startedAt,omitempty"`
}

// Update returns claim fields without a revision. Show independently exposes
// the stored revision; keep that observation separate and require it.
type b04NativeStoredIssue struct {
	b04NativeIssueProjection
	Revision string `json:"revision"`
}

type b04NativeForeignProjection struct {
	SchemaVersion string `json:"schemaVersion"`
	FailedID      string `json:"failedID"`
	Message       string `json:"message"`
	Diagnostic    string `json:"diagnostic"`
}

// This is the only B04 manual target. Ordinary focused/whole tests never
// invoke a native executable or consume this capture; they may only validate
// the embedded source contract.
func TestB04NativeClaimAndReplay(t *testing.T) {
	inputs := loadB04NativeInputs(t)
	retained := b04NativeEnv(t, "TEST_UNDECLARED_OUTPUTS_DIR")
	root, err := os.MkdirTemp(retained, "b04-native-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		inventory, invErr := b04NativeInventory(root)
		b04NativeJSON(t, filepath.Join(root, "output-inventory.json"), map[string]any{
			"entries": inventory, "complete": invErr == nil, "passed": !t.Failed(),
			"exclusions":    []string{"poll.log excluded by basename before stat/read/hash", "output-inventory.json self"},
			"qualification": "bounded B04 native/public replay only; no B05/B22/full-S1 qualification",
		})
		if invErr != nil {
			t.Errorf("native output inventory: %v", invErr)
		}
		t.Logf("B04_NATIVE_OUTPUT root=%s", root)
	})
	b04NativeWrite(t, filepath.Join(root, "native_b04_inputs.json"), b04NativeInputsBytes)
	binary := b04NativePrepared(t, root, inputs)

	captures := make([]b04NativeCapture, 0, len(inputs.Vector.Runs))
	for _, run := range inputs.Vector.Runs {
		run := run
		if !t.Run(run.ID, func(t *testing.T) {
			capture := recordB04NativeStore(t, binary, root, inputs, run.ID, run.Database)
			captures = append(captures, capture)
		}) {
			t.Fatal("native capture failed; remaining native store/import/replay/contention NOT_RUN")
		}
		if capture := captures[len(captures)-1]; capture.DumpRefusal != "" {
			b04NativeJSON(t, filepath.Join(root, "downstream-not-run.json"), map[string]any{
				"run": capture.RunID, "refusal": capture.DumpRefusal,
				"nextNativeStore": "NOT_RUN", "twinComparison": "NOT_RUN", "import": "NOT_RUN", "replay": "NOT_RUN", "contention": "NOT_RUN",
				"reason": "the unchanged accepted producer refuses the real custom-status/live-claim source; no source clearing, synthetic archive or fixture may replace it",
			})
			t.Fatal("B04 native archive unavailable: observed producer refusal retained; downstream NOT_RUN")
		}
	}
	if len(captures) != 2 {
		t.Fatalf("native twin capture count=%d want=2", len(captures))
	}
	// Compare only the semantic disposition. Raw commands, timestamps, server
	// ports and database bytes remain separate immutable receipts.
	semantic := func(c b04NativeCapture) any {
		return struct {
			IDs          map[string]string
			Ready        []string
			Direct       string
			SameOwner    string
			CustomActive string
			Foreign      b04NativeForeignProjection
			Next         []string
			Empty        string
		}{c.SourceIDs, c.ReadyBefore, c.Direct, c.SameOwner, c.CustomActive, c.Foreign, c.ClaimNextIDs, string(c.EmptyJSON)}
	}
	if !reflect.DeepEqual(semantic(captures[0]), semantic(captures[1])) {
		t.Fatalf("native semantic twins differ: A=%+v B=%+v", semantic(captures[0]), semantic(captures[1]))
	}
	b04NativeJSON(t, filepath.Join(root, "native-twin-comparison.json"), map[string]any{
		"equal": true, "runA": captures[0], "runB": captures[1],
		"projection": "observed source IDs/ready order/direct/same-owner/custom-active/typed CLI refusal/distinct full-walk winners/empty; all claim postimages are individually asserted and wall clocks retained separately",
	})
	// Recorded native outcomes address explicitly configured Plug test rows on
	// three backends. This is bounded outcome replay, not native archive import.
	for _, backend := range []string{"memory", "sqlite_wal", "postgres"} {
		backend := backend
		t.Run("replay/"+backend, func(t *testing.T) {
			eachStore(t, false, func(t *testing.T, e engine) {
				if e.name != backend {
					return
				}
				replayB04Native(t, captures[0], e, filepath.Join(root, "replay-"+backend), backend == "memory")
			})
		})
	}

}

func recordB04NativeStore(t *testing.T, binary, root string, inputs b04NativeInputs, runID, database string) b04NativeCapture {
	t.Helper()
	output := filepath.Join(root, runID)
	if err := os.MkdirAll(output, 0o700); err != nil {
		t.Fatal(err)
	}
	server := publicnative.StartDolt(t, output)
	workspace := t.TempDir()
	home := filepath.Join(workspace, "home")
	if err := os.MkdirAll(home, 0o700); err != nil {
		t.Fatal(err)
	}
	env := server.ChildEnv([]string{
		"PATH=" + filepath.Dir(server.DoltBinary) + ":/usr/bin:/bin",
		"HOME=" + home, "USERPROFILE=" + home, "XDG_CONFIG_HOME=" + filepath.Join(home, ".config"),
		"USER=public-native-b04", "BEADS_ACTOR=s1-b04-native-owner", "BEADS_NO_DAEMON=1",
		"TZ=UTC", "LC_ALL=C", "NO_COLOR=1", "CI=true", "GIT_CONFIG_NOSYSTEM=1",
	})
	c := &b04NativeCommand{t: t, binary: binary, workspace: workspace, output: output, env: env}
	var version struct {
		Commit string `json:"commit"`
		Build  string `json:"build"`
	}
	if err := b04NativeDecode(c.run("version", "version", "--json"), &version); err != nil {
		t.Fatalf("native version JSON: %v", err)
	}
	if version.Commit != inputs.Source || version.Build != "public-"+inputs.Source {
		t.Fatalf("native version commit=%s build=%s want source=%s", version.Commit, version.Build, inputs.Source)
	}
	c.run("init-server", "init", "--backend", "dolt", "--server", "--external", "--server-host", "127.0.0.1", "--server-port", strconv.Itoa(server.Port), "--database", database, "--prefix", "b04", "--quiet", "--skip-hooks", "--skip-agents", "--non-interactive")
	c.run("config-custom", "config", "set", "status.custom", "awaiting_review:active")
	clockColumns := b04NativeRows(t, c.run("claim-clock-columns", "--readonly", "sql", "SHOW COLUMNS FROM `issues`", "--json"))
	for _, field := range []string{"started_at", "updated_at"} {
		found := false
		for _, column := range clockColumns {
			if column["Field"] == field {
				found = true
				if column["Type"] != "datetime" {
					t.Fatalf("native %s precision changed: %+v", field, column)
				}
			}
		}
		if !found {
			t.Fatalf("native claim clock column %s is absent", field)
		}
	}
	ids := inputs.Vector.SourceIDs
	create := func(label, id, title, status string, priority int) {
		args := []string{"create", "--id", id, "--title", title, "--type", "task", "--priority", strconv.Itoa(priority), "--status", status, "--json"}
		c.run(label, args...)
	}
	create("create-direct", ids["direct"], "B04 direct", "open", 0)
	create("create-custom-active", ids["customActive"], "B04 custom active", "awaiting_review", 1)
	create("create-foreign-held", ids["foreignHeld"], "B04 foreign holder", "open", 2)
	create("create-next", ids["next"], "B04 next", "open", 3)
	create("create-empty", ids["empty"], "B04 empty front", "open", 4)
	readyBefore := c.run("ready-before", "ready", "--json", "--sort", "hybrid", "--limit", "0")
	readyIDs := b04NativeIDs(t, readyBefore)
	direct := b04NativeClaim(t, c, "claim-direct", ids["direct"], true)
	directPost := b04NativeShow(t, c, "show-direct", direct.ID)
	if direct != directPost.b04NativeIssueProjection {
		t.Fatal("direct claim response differs from its hydrated stored postimage")
	}
	beforeNoOp := b04NativeState(t, c, "same-owner-before")
	historyBefore := c.run("same-owner-history-before", "--readonly", "history", direct.ID, "--events", "--limit", "0", "--json")
	versionsBefore := b04NativeVersions(t, c, "same-owner-versions-before", direct.ID)
	sameOwner := b04NativeClaim(t, c, "claim-direct-same-owner", direct.ID, false)
	sameOwnerPost := b04NativeShow(t, c, "show-same-owner", direct.ID)
	historyAfter := c.run("same-owner-history-after", "--readonly", "history", direct.ID, "--events", "--limit", "0", "--json")
	versionsAfter := b04NativeVersions(t, c, "same-owner-versions-after", direct.ID)
	afterNoOp := b04NativeState(t, c, "same-owner-after")
	if direct != sameOwner || directPost != sameOwnerPost ||
		!bytes.Equal(b04NativeCanonical(t, historyBefore), b04NativeCanonical(t, historyAfter)) ||
		!reflect.DeepEqual(versionsBefore, versionsAfter) || !reflect.DeepEqual(beforeNoOp, afterNoOp) {
		t.Fatal("same-owner native claim changed its complete postimage/history/versions/physical state")
	}
	custom := b04NativeClaim(t, c, "claim-custom-active", ids["customActive"], true)
	customPost := b04NativeShow(t, c, "show-custom-active", custom.ID)
	if custom != customPost.b04NativeIssueProjection {
		t.Fatal("custom-active claim response differs from its hydrated stored postimage")
	}
	held := b04NativeClaim(t, c, "claim-foreign-held", ids["foreignHeld"], true)
	heldPost := b04NativeShow(t, c, "show-foreign-held", held.ID)
	if held != heldPost.b04NativeIssueProjection {
		t.Fatal("foreign-holder initial claim did not return its stored postimage")
	}
	beforeForeign := b04NativeState(t, c, "foreign-before")
	foreignEnv := make([]string, 0, len(c.env))
	for _, value := range c.env {
		if !strings.HasPrefix(value, "BEADS_ACTOR=") {
			foreignEnv = append(foreignEnv, value)
		}
	}
	foreignEnv = append(foreignEnv, "BEADS_ACTOR=s1-b04-foreign")
	foreign := &b04NativeCommand{t: t, binary: c.binary, workspace: c.workspace, output: filepath.Join(c.output, "foreign"), env: foreignEnv}
	if err := os.MkdirAll(foreign.output, 0o700); err != nil {
		t.Fatal(err)
	}
	foreignResult := foreign.expect("foreign-claim-refusal", 1, "update", held.ID, "--claim", "--json")
	foreignProjection := b04NativeForeign(t, foreignResult, held.ID)
	afterForeign := b04NativeState(t, c, "foreign-after")
	if !reflect.DeepEqual(beforeForeign, afterForeign) || heldPost != b04NativeShow(t, c, "show-foreign-after", held.ID) {
		t.Fatal("native foreign refusal changed full physical state or holder postimage")
	}
	// Consume exactly the remaining eligible ready rows. Every winner must
	// match the independently observed front, be unique, and hydrate its own
	// stored postimage; never populate results from expected source labels.
	remaining := b04NativeIDs(t, c.run("ready-after-direct", "--readonly", "ready", "--json", "--sort", "hybrid", "--limit", "0"))
	if len(remaining) != 2 || remaining[0] != ids["next"] || remaining[1] != ids["empty"] {
		t.Fatalf("remaining native front=%v want next/empty in priority order", remaining)
	}
	claimNextIDs := []string{}
	claimNext := []b04NativeIssueProjection{}
	seen := map[string]bool{}
	for i, expected := range remaining {
		result := c.expect(fmt.Sprintf("claim-next-%d", i+1), 0, "ready", "--claim", "--json")
		winner := b04NativeClaimed(t, result.Stdout, expected)
		if seen[winner.ID] || winner != b04NativeShow(t, c, fmt.Sprintf("show-next-%d", i+1), winner.ID).b04NativeIssueProjection {
			t.Fatal("claimNext repeated a winner or returned a different stored postimage")
		}
		b04NativeStartedDuring(t, winner, result)
		seen[winner.ID] = true
		claimNextIDs = append(claimNextIDs, winner.ID)
		claimNext = append(claimNext, winner)
	}
	beforeEmpty := b04NativeState(t, c, "empty-before")
	empty := c.run("claim-next-empty", "ready", "--claim", "--json")
	// The next call is retained as an explicit terminal. Some
	// producer versions return JSON null and others return an empty array; both
	// are accepted only as a successful nil front, never as a fabricated row.
	var emptyValue any
	if err := b04NativeDecode(empty, &emptyValue); err != nil {
		t.Fatalf("claimNext empty response is not closed JSON: %v", err)
	}
	if !b04NativeEmptyResult(emptyValue) {
		t.Fatalf("claimNext empty response is not nil/empty: %s", empty)
	}
	if !reflect.DeepEqual(beforeEmpty, b04NativeState(t, c, "empty-after")) {
		t.Fatal("native empty committed front changed physical records/history/operational state")
	}
	c.run("quiesce-commit", "dolt", "commit", "-m", "B04 native claim capture "+runID)
	databaseDir := server.RequireDatabaseDir(t, database)
	info, err := os.Lstat(databaseDir)
	if err != nil || info.Mode()&os.ModeSymlink != 0 {
		t.Fatalf("native database identity is not a real directory: %v", err)
	}
	b04NativeJSON(t, filepath.Join(output, "physical-database.json"), map[string]any{
		"path": databaseDir, "mode": info.Mode().String(), "pid": server.PID, "port": server.Port,
		"dataRoot": server.DataRoot, "childEnv": env,
	})
	inventoryPath := filepath.Join(output, "inventory.json")
	c.run("public-inventory", "--readonly", "export", "--format", "beads-inventory/1", "--plug-capture-public", "-o", inventoryPath)
	if _, err := os.Stat(inventoryPath + ".sources"); err != nil {
		t.Fatalf("native inventory sidecar tree missing: %v", err)
	}
	sidecarBefore, err := b04NativeInventory(inventoryPath + ".sources")
	if err != nil {
		t.Fatal(err)
	}
	dumpPath := filepath.Join(output, "readback.tar")
	beforeDump := b04NativeState(t, c, "dump-before")
	dump := c.execute("public-dump", "--readonly", "export", "--format", "plug-dump/1", "--plug-inventory", inventoryPath, "-o", dumpPath)
	afterDump := b04NativeState(t, c, "dump-after")
	if dump.TimedOut || !reflect.DeepEqual(beforeDump, afterDump) {
		t.Fatal("native readonly dump timed out or changed the source")
	}
	dumpRefusal := ""
	var dumpBytes int64
	if dump.Exit != 0 {
		// c8eb Build iterates a map and may report either unsupported family
		// first. Both must be present in the actual preserved physical source;
		// their refusal is a real portability dependency, never an archive.
		diagnostic := strings.TrimSpace(string(dump.Stderr))
		for _, family := range []string{"custom_statuses", "leases"} {
			want := "unsupported nonempty family " + family + " (live claims must be resolved)"
			if strings.Contains(diagnostic, want) {
				dumpRefusal = want
			}
		}
		if dump.Exit != 1 || dumpRefusal == "" || len(bytes.TrimSpace(dump.Stdout)) != 0 {
			t.Fatalf("unexpected native dump refusal exit=%d stderr=%s", dump.Exit, diagnostic)
		}
		if _, err := os.Lstat(dumpPath); !os.IsNotExist(err) {
			t.Fatal("refused dump left an archive path")
		}
	} else {
		_, dumpBytes, err = b04NativeFileSHA(dumpPath)
		if err != nil || dumpBytes == 0 {
			t.Fatalf("successful native dump absent/empty: %v", err)
		}
	}
	sidecarAfter, err := b04NativeInventory(inventoryPath + ".sources")
	if err != nil || !reflect.DeepEqual(sidecarBefore, sidecarAfter) {
		t.Fatalf("native dump changed source sidecars: err=%v", err)
	}
	projection := b04NativeCapture{
		RunID: runID, Database: database, SourceIDs: ids, ReadyBefore: readyIDs,
		Direct: direct.ID, SameOwner: sameOwner.ID, CustomActive: custom.ID, ForeignHeld: held.ID,
		DirectPost: directPost, SameOwnerPost: sameOwnerPost, CustomActivePost: customPost, ForeignHeldPost: heldPost,
		Foreign: foreignProjection, NoOpHistorySame: true, NoOpVersionsSame: true,
		NoOpStateSHA256: b04NativeSHA(b04NativeEncoded(t, beforeNoOp)), ForeignStateSHA256: b04NativeSHA(b04NativeEncoded(t, beforeForeign)),
		ClaimNext: claimNext, ClaimNextIDs: claimNextIDs, EmptyJSON: append(json.RawMessage(nil), bytes.TrimSpace(empty)...),
		Inventory: map[string]any{"path": inventoryPath, "sidecars": sidecarBefore, "database": databaseDir},
		DumpBytes: dumpBytes, DumpRefusal: dumpRefusal, Commands: c.count + foreign.count, Output: output,
	}
	b04NativeJSON(t, filepath.Join(output, "observed-projection.json"), projection)
	b04NativeJSON(t, filepath.Join(output, "source-to-actual-binding.json"), map[string]any{
		"nativeObserved": map[string]any{"direct": direct, "sameOwner": sameOwner, "customActive": custom, "foreignHeld": held, "claimNext": claimNext},
		"plugBindings":   "NOT_RUN until actual native archive is available; replay writes only returned Plug refs",
	})
	return projection
}

func b04NativeEncoded(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func b04NativeCanonical(t *testing.T, data []byte) []byte {
	t.Helper()
	var value any
	if err := b04NativeDecode(data, &value); err != nil {
		t.Fatal(err)
	}
	return b04NativeEncoded(t, value)
}

func b04NativeRows(t *testing.T, data []byte) []map[string]any {
	t.Helper()
	var value any
	if err := b04NativeDecode(data, &value); err != nil {
		t.Fatal(err)
	}
	if object, ok := value.(map[string]any); ok {
		if wrapped, exists := object["data"]; exists {
			value = wrapped
		} else {
			return []map[string]any{object}
		}
	}
	rows, ok := value.([]any)
	if !ok {
		t.Fatalf("native row response is not an array/object: %s", data)
	}
	result := make([]map[string]any, 0, len(rows))
	for _, raw := range rows {
		row, ok := raw.(map[string]any)
		if !ok {
			t.Fatalf("native row is not an object: %v", raw)
		}
		result = append(result, row)
	}
	return result
}

func b04NativeClaimed(t *testing.T, data []byte, expected string) b04NativeIssueProjection {
	t.Helper()
	rows := b04NativeRows(t, data)
	if len(rows) != 1 {
		t.Fatalf("claim response has %d rows, want one", len(rows))
	}
	row := rows[0]
	text := func(key string) string {
		if row[key] == nil {
			return ""
		}
		return fmt.Sprint(row[key])
	}
	got := b04NativeIssueProjection{ID: text("id"), Status: text("status"), Assignee: text("assignee"), StartedAt: text("started_at")}
	if got.ID != expected || got.Status != "in_progress" || got.Assignee != "s1-b04-native-owner" || got.StartedAt == "" {
		t.Fatalf("native claim did not atomically return the expected hydrated owner/status/start: %+v", got)
	}
	if _, err := time.Parse(time.RFC3339Nano, got.StartedAt); err != nil {
		t.Fatalf("native started_at: %v", err)
	}
	return got
}

func b04NativeStartedDuring(t *testing.T, got b04NativeIssueProjection, result b04NativeResult) {
	t.Helper()
	started, err := time.Parse(time.RFC3339Nano, got.StartedAt)
	// The observed issues schema is DATETIME(0). Dolt rounds values to its
	// nearest second (the admitted producer pins that behavior in its 0069
	// precision replay). Project both actual command endpoints into that same
	// domain; retain the raw nanosecond bounds in the command receipt.
	lower, upper := result.StartedUTC.Round(time.Second), result.EndedUTC.Round(time.Second)
	if err != nil || started.Nanosecond() != 0 || started.Before(lower) || started.After(upper) {
		t.Fatalf("native start is outside the actual claim command: %+v %+v", got, result)
	}
}

func b04NativeClaim(t *testing.T, c *b04NativeCommand, label, id string, fresh bool) b04NativeIssueProjection {
	t.Helper()
	result := c.expect(label, 0, "update", id, "--claim", "--json")
	got := b04NativeClaimed(t, result.Stdout, id)
	if fresh {
		b04NativeStartedDuring(t, got, result)
	}
	return got
}

func b04NativeShow(t *testing.T, c *b04NativeCommand, label, id string) b04NativeStoredIssue {
	t.Helper()
	data := c.run(label, "--readonly", "show", id, "--json")
	claim := b04NativeClaimed(t, data, id)
	row := b04NativeRows(t, data)[0]
	// The admitted producer's RevisionToken is an opaque decimal int64 string,
	// including negative values and the legacy "0". Require its exact canonical
	// spelling; never normalize the token or invent one from update output.
	revision, present := row["revision"].(string)
	value, err := strconv.ParseInt(revision, 10, 64)
	if !present || err != nil || strconv.FormatInt(value, 10) != revision {
		t.Fatalf("native show has no genuine canonical signed revision token: %s", data)
	}
	return b04NativeStoredIssue{b04NativeIssueProjection: claim, Revision: revision}
}

func b04NativeVersions(t *testing.T, c *b04NativeCommand, label, id string) map[string]any {
	t.Helper()
	result := c.execute(label, "--readonly", "versions", id, "--json")
	if result.TimedOut || (result.Exit != 0 && result.Exit != 1) {
		t.Fatalf("native versions did not complete: %+v", result)
	}
	if result.Exit == 1 && !bytes.Contains(append(append([]byte{}, result.Stdout...), result.Stderr...), []byte("versioned history is not being recorded on this store")) {
		t.Fatal("native versions failed for a reason other than the actual default history-off refusal")
	}
	// Do not enable or disable native history to obtain a convenient result.
	// The raw version rows or literal default refusal must be unchanged.
	return map[string]any{"exit": result.Exit, "stdout": string(result.Stdout), "stderr": string(result.Stderr)}
}

func b04NativeForeign(t *testing.T, result b04NativeResult, id string) b04NativeForeignProjection {
	t.Helper()
	lines := bytes.Split(bytes.TrimSpace(result.Stderr), []byte("\n"))
	var envelope map[string]any
	if len(bytes.TrimSpace(result.Stdout)) != 0 || len(lines) == 0 || b04NativeDecode(lines[len(lines)-1], &envelope) != nil {
		t.Fatal("native foreign claim did not return its actual failed-ID error envelope")
	}
	if len(envelope) != 3 || fmt.Sprint(envelope["schema_version"]) != "1" || envelope["error"] != "1 of 1 issues failed to update" {
		t.Fatalf("native foreign error schema/message differs: %+v", envelope)
	}
	failed, ok := envelope["failed"].([]any)
	if !ok || len(failed) != 1 {
		t.Fatalf("native foreign failed IDs=%+v", envelope["failed"])
	}
	row, ok := failed[0].(map[string]any)
	want := "updating issue: issue already claimed by s1-b04-native-owner"
	if !ok || len(row) != 2 || row["id"] != id || row["error"] != want {
		t.Fatalf("native foreign refusal lost exact ID/holder/diagnostic: %+v", row)
	}
	// This accepted CLI schema has no traceId field. The public Plug RPC
	// envelope is independently validated for its catalog schema and traceId.
	return b04NativeForeignProjection{SchemaVersion: "1", FailedID: fmt.Sprint(row["id"]), Message: fmt.Sprint(envelope["error"]), Diagnostic: want}
}

func b04NativeState(t *testing.T, c *b04NativeCommand, label string) map[string]any {
	t.Helper()
	tables := b04NativeRows(t, c.run(label+"-tables", "--readonly", "sql", "SHOW TABLES", "--json"))
	names := []string{}
	for _, row := range tables {
		if len(row) != 1 {
			t.Fatalf("unexpected SHOW TABLES row: %+v", row)
		}
		for _, value := range row {
			name, ok := value.(string)
			if !ok || name == "" || strings.ContainsAny(name, "`\x00") {
				t.Fatalf("invalid actual native table name %v", value)
			}
			names = append(names, name)
		}
	}
	sort.Strings(names)
	if len(names) == 0 {
		t.Fatal("native physical census found no tables")
	}
	state := map[string]any{"tables": names}
	for _, name := range names {
		rows := b04NativeRows(t, c.run(label+"-rows-"+name, "--readonly", "sql", "SELECT * FROM `"+name+"`", "--json"))
		encoded := make([]string, 0, len(rows))
		for _, row := range rows {
			encoded = append(encoded, string(b04NativeEncoded(t, row)))
		}
		sort.Strings(encoded)
		columns := c.run(label+"-columns-"+name, "--readonly", "sql", "SHOW COLUMNS FROM `"+name+"`", "--json")
		state[name] = map[string]any{"rows": encoded, "columns": string(b04NativeCanonical(t, columns))}
	}
	state["dolt_log"] = string(b04NativeCanonical(t, c.run(label+"-commits", "--readonly", "sql", "SELECT * FROM dolt_log", "--json")))
	b04NativeJSON(t, filepath.Join(c.output, label+"-physical-state.json"), state)
	return state
}

func b04NativeIDs(t *testing.T, data []byte) []string {
	t.Helper()
	var value any
	if err := b04NativeDecode(data, &value); err != nil {
		t.Fatalf("native ready response JSON: %v", err)
	}
	return b04NativeIDsValue(t, value)
}

func b04NativeIDsValue(t *testing.T, value any) []string {
	t.Helper()
	var rows []any
	switch v := value.(type) {
	case []any:
		rows = v
	case map[string]any:
		if raw, ok := v["issues"]; ok {
			rows, _ = raw.([]any)
		} else if raw, ok := v["items"]; ok {
			rows, _ = raw.([]any)
		}
	}
	ids := make([]string, 0, len(rows))
	for _, raw := range rows {
		if row, ok := raw.(map[string]any); ok {
			if id, ok := row["id"].(string); ok && id != "" {
				ids = append(ids, id)
			}
		}
	}
	return ids
}

func b04NativeFirstID(t *testing.T, data []byte) string {
	ids := b04NativeIDs(t, data)
	if len(ids) > 0 {
		return ids[0]
	}
	var value any
	if err := b04NativeDecode(data, &value); err != nil {
		t.Fatal(err)
	}
	if object, ok := value.(map[string]any); ok {
		for _, key := range []string{"id", "issueID", "issue_id"} {
			if id, ok := object[key].(string); ok {
				return id
			}
		}
	}
	return ""
}

func b04NativeEmptyResult(value any) bool {
	if value == nil {
		return true
	}
	switch v := value.(type) {
	case []any:
		return len(v) == 0
	case map[string]any:
		for _, key := range []string{"issues", "items", "result"} {
			if raw, ok := v[key]; ok {
				return b04NativeEmptyResult(raw)
			}
		}
	}
	return false
}

func replayB04Native(t *testing.T, capture b04NativeCapture, e engine, output string, contention bool) {
	t.Helper()
	if err := os.MkdirAll(output, 0o700); err != nil {
		t.Fatal(err)
	}
	// This configured outcome fixture is intentionally distinct from archive
	// import. Its results can establish only bounded RPC outcome replay; the
	// accepted producer refusal above keeps it NOT_RUN for the present input.
	document := ontology.DefaultDocument()
	document.Statuses["awaiting_review"] = ontology.Status{Name: "Awaiting review", Category: ontology.Unstarted}
	workflow := document.Workflows["issue"]
	workflow.Statuses = append(workflow.Statuses, "awaiting_review")
	workflow.Transitions = append(workflow.Transitions, ontology.Rule{From: []string{"awaiting_review"}, To: "in_progress", Guards: []ontology.Guard{{Kind: ontology.NotTemplate}, {Kind: ontology.NoOpenBlockers}, {Kind: ontology.Permission}}})
	document.Workflows["issue"] = workflow
	f := newFixtureWithSetup(t, e, document, map[string]string{"a2": "awaiting_review"})
	owner := f.issue("agent:b04-native-owner", "user:pool-a", nil)
	foreign := f.issue("agent:b04-native-foreign", "user:pool-a", nil)
	bindings := map[string]model.Ref{"direct": f.work["a1"], "customActive": f.work["a2"]}
	for i, role := range []string{"foreignHeld", "next", "empty"} {
		reply := f.request(f.bootstrap, credential{}, "bead.create", poolA, f.key("native-seed-"+role), "setup", map[string]any{"type": "issue", "fields": map[string]any{"title": "native replay " + role, "description": "hydrated " + role, "priority": i + 2}})
		commit := reply.commit(t)
		if len(commit.Results) != 1 {
			t.Fatal("native replay setup did not return one actual ref")
		}
		bindings[role] = commit.Results[0].Ref
	}
	observed := map[string]string{"direct": capture.Direct, "sameOwner": capture.SameOwner, "customActive": capture.CustomActive, "foreignHeld": capture.ForeignHeld}
	nativeToPlug := map[string]model.Ref{}
	seenRefs := map[string]bool{}
	for role, plugRef := range bindings {
		nativeID := capture.SourceIDs[role]
		if nativeID == "" || nativeID == plugRef.ID || seenRefs[plugRef.ID] {
			t.Fatal("missing/reused native-to-Plug identity binding")
		}
		if role != "next" && role != "empty" && observed[role] != nativeID {
			t.Fatalf("%s observation differs from its source binding", role)
		}
		nativeToPlug[nativeID] = plugRef
		seenRefs[plugRef.ID] = true
	}
	if observed["sameOwner"] != observed["direct"] {
		t.Fatal("native same-owner response changed identity")
	}
	// Persist actual returned references alongside observed command identities,
	// then use this very mapping to address every RPC and inspect its result.
	b04NativeJSON(t, filepath.Join(output, "source-to-plug-refs.json"), map[string]any{"observedNative": observed, "claimNext": capture.ClaimNext, "returnedPlugRefs": bindings, "nativeToPlug": nativeToPlug, "scope": "configured outcome fixture only, not native archive import"})
	keyDirect := f.key("native-direct")
	before := f.state()
	first := f.call(owner, "bead.claim", poolA, keyDirect, "b04-native-owner", target(nativeToPlug[capture.Direct]))
	claimed := f.winner(before, owner, "bead.claim", keyDirect, "b04-native-owner", nativeToPlug[capture.Direct], first, true)
	repeatKey := f.key("native-direct-same-owner")
	noOpScope := f.remember(owner, "bead.claim", repeatKey, "different-display")
	beforeNoOp := f.state()
	repeat := f.call(owner, "bead.claim", poolA, repeatKey, "different-display", target(nativeToPlug[capture.Direct]))
	repeatCommit := repeat.commit(t)
	sameClaimState(t, claimed, repeatCommit)
	onlyNoopReceipt(t, beforeNoOp, f.state(), noOpScope, repeatCommit)
	customKey := f.key("native-custom-active")
	customBefore := f.state()
	customRef := nativeToPlug[capture.CustomActive]
	customDoc, err := pipeline.DecodeDocument(customBefore.record(t, customRef).Body)
	if err != nil || customDoc.Fields["status"] != "awaiting_review" {
		t.Fatal("custom-active replay lacks its explicitly configured preimage")
	}
	customReply := f.call(owner, "bead.claim", poolA, customKey, "b04-native-owner", target(customRef))
	f.winner(customBefore, owner, "bead.claim", customKey, "b04-native-owner", customRef, customReply, true)
	heldKey := f.key("native-held")
	heldBefore := f.state()
	heldRef := nativeToPlug[capture.ForeignHeld]
	heldReply := f.call(owner, "bead.claim", poolA, heldKey, "b04-native-owner", target(heldRef))
	f.winner(heldBefore, owner, "bead.claim", heldKey, "b04-native-owner", heldRef, heldReply, true)
	refusalKey := f.key("native-foreign")
	f.remember(foreign, "bead.claim", refusalKey, "b04-native-foreign")
	refusalBefore := f.state()
	refusal := f.call(foreign, "bead.claim", poolA, refusalKey, "b04-native-foreign", target(nativeToPlug[capture.Foreign.FailedID]))
	if refusal.code != "busy" || refusal.error == nil || len(refusal.envelope["result"]) != 0 {
		t.Fatal("foreign outcome did not map to the typed public refusal")
	}
	sameState(t, refusalBefore, f.state())
	b04NativeWrite(t, filepath.Join(output, "foreign-public-envelope.json"), refusal.raw)
	// f.request validates the closed public error schema, canonical retryable
	// value and nonempty transport traceId on this actual saved envelope.
	if len(capture.ClaimNextIDs) != 2 {
		t.Fatal("native complete walk was not observed")
	}
	nextReplies := []json.RawMessage{}
	winners := map[string]bool{}
	for i, nativeID := range capture.ClaimNextIDs {
		expected, ok := nativeToPlug[nativeID]
		if !ok || winners[expected.ID] {
			t.Fatal("native walk has an unmapped/duplicate winner")
		}
		nextKey := f.key(fmt.Sprintf("native-claim-next-%d", i))
		beforeNext := f.state()
		next := f.call(owner, "bead.claimNext", poolA, nextKey, "b04-native-owner", nil)
		f.winner(beforeNext, owner, "bead.claimNext", nextKey, "b04-native-owner", expected, next, true)
		nextReplies = append(nextReplies, next.raw)
		winners[expected.ID] = true
	}
	b04NativeJSON(t, filepath.Join(output, "claim-next-public-envelopes.json"), nextReplies)
	emptyKey := f.key("native-claim-next-empty")
	emptyScope := f.remember(owner, "bead.claimNext", emptyKey, "b04-native-owner")
	emptyBefore := f.state()
	emptyReply := f.call(owner, "bead.claimNext", poolA, emptyKey, "b04-native-owner", nil)
	if string(emptyReply.envelope["result"]) != "null" {
		t.Fatalf("claimNext empty result=%s want JSON null", emptyReply.raw)
	}
	emptyAfter := f.state()
	head, err := emptyBefore.Meta.Head.Uint64()
	if err != nil {
		t.Fatal(err)
	}
	entry := emptyAfter.Receipts[emptyScope]
	if emptyAfter.Meta.Head != model.Natural64(head+1) || len(emptyAfter.Changesets) != len(emptyBefore.Changesets)+1 || entry == nil || entry.ResponsePending || entry.Commit.Revision != emptyAfter.Meta.Head || len(entry.Commit.Results) != 0 {
		t.Fatal("empty claimNext did not publish exactly its H+1 terminal receipt")
	}
	change := emptyAfter.Changesets[len(emptyAfter.Changesets)-1]
	if len(change.Changes) != 0 || !reflect.DeepEqual(entry.Changeset, change) {
		t.Fatal("empty claimNext wrote a bead or receipt differs from its changeset")
	}
	allowed := emptyAfter
	allowed.Meta.Head = emptyBefore.Meta.Head
	allowed.Changesets = allowed.Changesets[:len(allowed.Changesets)-1]
	allowed.Receipts = make(map[receiptScope]*driver.IdempotencyRecord, len(emptyAfter.Receipts))
	for key, value := range emptyAfter.Receipts {
		allowed.Receipts[key] = value
	}
	allowed.Receipts[emptyScope] = nil
	sameState(t, emptyBefore, allowed)
	b04NativeWrite(t, filepath.Join(output, "empty-public-envelope.json"), emptyReply.raw)
	assertEmptyReplay := func(label string) {
		beforeReplay := f.state()
		replayed := f.call(owner, "bead.claimNext", poolA, emptyKey, label, nil)
		var meta struct {
			Replayed bool `json:"replayed"`
		}
		if err := json.Unmarshal(replayed.envelope["_meta"], &meta); err != nil || !meta.Replayed || !bytes.Equal(replayed.envelope["result"], emptyReply.envelope["result"]) {
			t.Fatalf("empty replay changed result: %s", replayed.raw)
		}
		sameState(t, beforeReplay, f.state())
	}
	assertEmptyReplay("same-key")
	if e.name != "memory" {
		f.reopen()
		assertEmptyReplay("after-reopen")
	}
	if contention {
		b04RunContention(t, f)
	}
	b04NativeJSON(t, filepath.Join(output, "replay-disposition.json"), map[string]any{
		"backend": e.name, "direct": nativeToPlug[capture.Direct], "customActive": nativeToPlug[capture.CustomActive], "claimNext": nextReplies,
		"empty": "null", "contention": contention,
		"nativeSourceIDs": capture.SourceIDs,
		"qualification":   "actual bounded replay and conservation; no FullS1/native admission promotion",
	})
}

func b04RunContention(t *testing.T, f *fixture) {
	t.Helper()
	const agents = 200
	credentials := make([]credential, agents)
	for i := range credentials {
		credentials[i] = f.issue(fmt.Sprintf("agent:b04-contender-%03d", i), "user:pool-b", nil)
	}
	targetID := f.work["b1"]
	type outcome struct {
		code   string
		result bool
	}
	outcomes := make(chan outcome, agents)
	var wg sync.WaitGroup
	for i, by := range credentials {
		wg.Add(1)
		go func(i int, by credential) {
			defer wg.Done()
			params := map[string]any{"id": targetID, "_meta": map[string]any{"project": poolB, "agent": fmt.Sprintf("contender-%03d", i), "idempotencyKey": fmt.Sprintf("b04-contention-%03d", i)}}
			raw, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": i + 1, "method": "bead.claim", "params": params})
			if err != nil {
				outcomes <- outcome{code: "encode"}
				return
			}
			req := httptest.NewRequest(http.MethodPost, "http://127.0.0.1/rpc/v1", bytes.NewReader(raw)).WithContext(f.ctx)
			req.Header.Set("Plug-RPC", "1")
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Authorization", "Bearer "+by.key.Token)
			rec := httptest.NewRecorder()
			f.app.Handler.ServeHTTP(rec, req)
			var envelope map[string]json.RawMessage
			if json.Unmarshal(rec.Body.Bytes(), &envelope) != nil {
				outcomes <- outcome{code: "invalid-json"}
				return
			}
			if rawErr := envelope["error"]; len(rawErr) != 0 {
				var wireErr struct {
					Data struct {
						Code string `json:"code"`
					} `json:"data"`
				}
				if json.Unmarshal(rawErr, &wireErr) == nil {
					outcomes <- outcome{code: wireErr.Data.Code}
					return
				}
			}
			outcomes <- outcome{result: len(envelope["result"]) != 0 && string(envelope["result"]) != "null"}
		}(i, by)
	}
	wg.Wait()
	close(outcomes)
	winners, busy := 0, 0
	for got := range outcomes {
		if got.result {
			winners++
		}
		if got.code == "busy" {
			busy++
		}
	}
	if winners != 1 || busy != agents-1 {
		t.Fatalf("200-agent claim contention winners=%d busy=%d want 1/%d", winners, busy, agents-1)
	}
	t.Logf("B04 native replay contention: agents=%d winner=%d busy=%d", agents, winners, busy)
}
