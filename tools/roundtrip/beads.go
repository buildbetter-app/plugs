package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/reverseconverter"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/postgres"
)

// These identify the supported importer/exporter interface inspected for this
// integration. The archive's older beads_reference is its schema pin, not the
// executable's build provenance. A future executable requires a new review.
const beadsCommit = "682ca798dab448803ea9b74f7da3715c21d8c3ae"
const beadsTree = "9d026de307ee915953b247cf42758d0c53303574"

type beadsOptions struct {
	Binary, SHA256, BuildRun, Inventory string
	CapturePublicInventory              bool
}

type commandEvidence struct {
	Args         []string `json:"args"`
	Exit         int      `json:"exit"`
	StdoutSHA256 string   `json:"stdout_sha256"`
	StderrSHA256 string   `json:"stderr_sha256"`
}

func hash(data []byte) string { sum := sha256.Sum256(data); return hex.EncodeToString(sum[:]) }

// runBeads has no build, download, workflow or PATH fallback. It consumes an
// independently delivered, hash-pinned public executable on the runtime host.
// Every subprocess is real; tests do not substitute a successful bd factory.
func runBeads(ctx context.Context, options beadsOptions, output string, source *dumpformat.Archive, live reverseconverter.Result, result *outcome) {
	result.Stage = "beads_artifact"
	result.Status = "WAIT"
	result.Reason = "pinned_beads_artifact_required"
	if options.Binary == "" || options.SHA256 == "" || options.BuildRun == "" {
		return
	}
	buildRun, err := strconv.ParseUint(options.BuildRun, 10, 64)
	if err != nil || buildRun == 0 || len(options.SHA256) != 64 {
		result.Status, result.Reason = "BLOCK", "invalid_beads_provenance"
		return
	}
	binary, err := filepath.Abs(options.Binary)
	if err != nil {
		result.Status, result.Reason = "BLOCK", "beads_artifact_refused"
		return
	}
	info, err := os.Lstat(binary)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o111 == 0 {
		result.Status, result.Reason = "BLOCK", "beads_artifact_refused"
		return
	}
	f, err := os.Open(binary)
	if err != nil {
		result.Status, result.Reason = "BLOCK", "beads_artifact_refused"
		return
	}
	// Execute only the verified copy in the owned 0700 directory, so a later
	// replacement of the delivered path cannot change the bytes being run.
	pinned := filepath.Join(output, "beads-pinned")
	copy, err := os.OpenFile(pinned, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o500)
	if err != nil {
		_ = f.Close()
		result.Status, result.Reason = "BLOCK", "beads_artifact_refused"
		return
	}
	h := sha256.New()
	_, err = io.Copy(io.MultiWriter(copy, h), f)
	_ = f.Close()
	closeErr := copy.Close()
	if err != nil || closeErr != nil || hex.EncodeToString(h.Sum(nil)) != options.SHA256 {
		_ = os.Remove(pinned)
		result.Status, result.Reason = "BLOCK", "beads_artifact_digest_mismatch"
		return
	}
	binary = pinned
	projects := source.Manifest().Projects
	if len(projects) != 1 {
		result.Status, result.Reason = "BLOCK", "beads_single_project_required"
		return
	}
	workspace := filepath.Join(output, "beads-target")
	if err := os.Mkdir(workspace, 0o700); err != nil {
		result.Status, result.Reason = "BLOCK", "new_beads_target_required"
		return
	}
	childHome := filepath.Join(workspace, "home")
	if err := os.Mkdir(childHome, 0o700); err != nil {
		result.Status, result.Reason = "BLOCK", "new_beads_target_required"
		return
	}
	result.BeadsBinarySHA256, result.BeadsBuildRun = options.SHA256, options.BuildRun
	result.BeadsCommit, result.BeadsTree = beadsCommit, beadsTree
	commands := []commandEvidence{}
	run := func(args ...string) ([]byte, error) {
		cmd := exec.CommandContext(ctx, binary, args...)
		cmd.Dir = workspace
		// Do not inherit a user's workspace, credentials, config or plugins.
		cmd.Env = []string{"PATH=/usr/bin:/bin", "HOME=" + childHome, "XDG_CONFIG_HOME=" + filepath.Join(childHome, ".config"), "BEADS_DIR=" + filepath.Join(workspace, ".beads"), "DO_NOT_TRACK=1", "GIT_CONFIG_NOSYSTEM=1", "GIT_AUTHOR_NAME=Public roundtrip", "GIT_AUTHOR_EMAIL=public@example.invalid", "GIT_COMMITTER_NAME=Public roundtrip", "GIT_COMMITTER_EMAIL=public@example.invalid"}
		var stdout, stderr bytes.Buffer
		cmd.Stdout, cmd.Stderr = &stdout, &stderr
		err := cmd.Run()
		exitCode := 0
		if err != nil {
			exitCode = -1
			if cmd.ProcessState != nil {
				exitCode = cmd.ProcessState.ExitCode()
			}
		}
		commands = append(commands, commandEvidence{Args: args, Exit: exitCode, StdoutSHA256: hash(stdout.Bytes()), StderrSHA256: hash(stderr.Bytes())})
		n := len(commands)
		if e := writePrivate(output, fmt.Sprintf("beads-%d.stdout", n), stdout.Bytes()); e != nil {
			return nil, e
		}
		if e := writePrivate(output, fmt.Sprintf("beads-%d.stderr", n), stderr.Bytes()); e != nil {
			return nil, e
		}
		return stdout.Bytes(), err
	}
	defer func() {
		data, err := json.Marshal(commands)
		if err != nil || writePrivate(output, "beads-commands.json", data) != nil {
			result.Status, result.Reason = "BLOCK", "beads_command_evidence_refused"
		}
	}()
	version, err := run("version", "--json")
	var identity struct {
		Commit string `json:"commit"`
	}
	if err != nil || json.Unmarshal(version, &identity) != nil || identity.Commit != beadsCommit {
		result.Status, result.Reason = "BLOCK", "beads_executable_pin_mismatch"
		return
	}
	result.Stage = "beads_init"
	if _, err = run("init", "--prefix", projects[0].Prefix, "--skip-hooks", "--skip-agents", "--non-interactive", "--quiet"); err != nil {
		result.Status, result.Reason = "BLOCK", "beads_init_refused"
		return
	}
	result.Stage = "beads_reimport"
	if _, err = run("import", filepath.Join(output, "reverse-live.jsonl"), "--json"); err != nil {
		result.Status, result.Reason, result.BeadsReimport = "BLOCK", "beads_import_refused", "FAIL"
		return
	}
	result.BeadsReimport = "PASS"
	// Readback inventory describes this actual initialized/imported target.
	// Its absence must not hide artifact failures or discard a real import.
	result.Stage = "beads_inventory"
	result.Reason = "beads_inventory_required"
	inventoryInput := options.Inventory
	if options.CapturePublicInventory {
		// Capture only the fresh target through the same verified executable.
		// A source inventory cannot attest to this newly imported workspace.
		inventoryInput = filepath.Join(output, "beads-generated-inventory.json")
		if _, err = run("--readonly", "export", "--format", "beads-inventory/1", "--plug-capture-public", "--output", inventoryInput); err != nil {
			result.Status, result.Reason, result.BeadsReadback = "BLOCK", "beads_inventory_capture_refused", "FAIL"
			return
		}
	}
	if inventoryInput == "" {
		return
	}
	inventory, err := filepath.Abs(inventoryInput)
	if err != nil {
		result.Status = "BLOCK"
		return
	}
	if stat, err := os.Stat(inventory); err != nil || !stat.Mode().IsRegular() || stat.Size() > 1<<20 {
		result.Status = "BLOCK"
		return
	}
	inventoryBytes, err := os.ReadFile(inventory)
	if err != nil || len(inventoryBytes) > 1<<20 || writePrivate(output, "beads-inventory.json", inventoryBytes) != nil {
		result.Status = "BLOCK"
		return
	}
	inventory = filepath.Join(output, "beads-inventory.json")
	result.Stage = "beads_readback"
	readback := filepath.Join(output, "beads-raw-readback.tar")
	if _, err = run("--readonly", "export", "--format", "plug-dump/1", "--plug-inventory", inventory, "--output", readback); err != nil {
		result.Status, result.Reason, result.BeadsReadback = "BLOCK", "beads_raw_export_refused", "FAIL"
		return
	}
	actual, err := dumpformat.ReadFile(readback)
	if err != nil {
		result.Status, result.Reason, result.BeadsReadback = "BLOCK", "beads_raw_readback_invalid", "FAIL"
		return
	}
	result.BeadsReadbackSHA256 = actual.SHA256()
	result.SourceFieldLosses = nativeOpaqueLosses(source, actual)
	if reason := compareRawReadback(live.JSONL, actual); reason != "" {
		result.Status, result.Reason, result.BeadsReadback = "BLOCK", reason, "FAIL"
		return
	}
	result.BeadsReadback = "PASS"
	// Native opaque column fidelity is a separate assertion from retaining the
	// same values as beads.* metadata. Never silently certify that relocation.
	if len(result.SourceFieldLosses) != 0 {
		result.Status, result.Reason, result.BeadsReadback = "BLOCK", "beads_native_opaque_fields_changed", "FAIL"
		return
	}
	result.Status, result.Stage, result.Reason = "PASS", "public_snapshot_roundtrip_complete", ""
}

func sameJSON(a, b any) bool {
	x, e := json.Marshal(a)
	y, f := json.Marshal(b)
	return e == nil && f == nil && bytes.Equal(x, y)
}

func nativeOpaqueLosses(source, actual *dumpformat.Archive) []string {
	rows := map[string]dumpformat.Record{}
	losses := map[string]bool{}
	for _, row := range actual.Records("bead") {
		rows[row.ID] = row
	}
	for _, row := range source.Records("bead") {
		got, ok := rows[row.ID]
		if !ok {
			losses["missing_issue"] = true
			continue
		}
		for _, field := range dumpformat.OpaqueIssueFields() {
			want, present := row.Source.Fields[field]
			value, stored := got.Source.Fields[field]
			if present != stored || !sameJSON(want, value) {
				losses[field] = true
			}
		}
	}
	names := []string{}
	for field := range losses {
		names = append(names, field)
	}
	sort.Strings(names)
	return names
}

// Compare the actual exporter's raw SQL field images to the bytes submitted
// to bd import. Projection and the original source archive are not readback.
func compareRawReadback(expected []byte, actual *dumpformat.Archive) string {
	rows := map[string]dumpformat.Record{}
	for _, row := range actual.Records("bead") {
		rows[row.ID] = row
	}
	decoder := json.NewDecoder(bytes.NewReader(expected))
	decoder.UseNumber()
	issues := 0
	edges, comments, memories := map[string]map[string]any{}, map[string]map[string]any{}, map[string]any{}
	for {
		var line map[string]any
		err := decoder.Decode(&line)
		if err == io.EOF {
			break
		}
		if err != nil {
			return "beads_expected_jsonl_invalid"
		}
		if line["_type"] == "memory" {
			key, _ := line["key"].(string)
			if _, exists := memories[key]; exists {
				return "beads_expected_memory_duplicate"
			}
			memories[key] = line["value"]
			continue
		}
		if line["_type"] != "issue" {
			continue
		}
		id, _ := line["id"].(string)
		stored, ok := rows[id]
		if !ok {
			return "beads_raw_issue_missing"
		}
		delete(rows, id)
		issues++
		for _, field := range []string{"id", "title", "description", "design", "acceptance_criteria", "notes", "spec_id", "status", "priority", "issue_type", "assignee", "owner", "created_by", "external_ref", "source_system", "close_reason", "closed_by_session", "metadata"} {
			want, included := line[field]
			got := stored.Source.Fields[field]
			if !included {
				if got == nil || got == "" {
					continue
				}
				return "beads_raw_field_default_changed"
			}
			if !sameJSON(want, got) {
				return "beads_raw_field_changed"
			}
		}
		for _, field := range []string{"created_at", "updated_at", "started_at", "closed_at", "due_at", "defer_until"} {
			want, _ := line[field].(string)
			got, _ := stored.Source.Fields[field].(string)
			if want == "" && got == "" {
				continue
			}
			a, e := time.Parse(time.RFC3339Nano, want)
			b, f := time.Parse(time.RFC3339Nano, got)
			if e != nil || f != nil || !a.Equal(b) {
				return "beads_raw_timestamp_changed"
			}
		}
		if !sameStringSet(line["labels"], stored.Body["labels"]) {
			return "beads_raw_labels_changed"
		}
		wantPinned := line["pinned"] == true
		gotPinned := stored.Source.Fields["pinned"]
		if !samePinnedValue(wantPinned, gotPinned) {
			return "beads_raw_pinned_changed"
		}
		for _, raw := range array(line["dependencies"]) {
			dep, ok := raw.(map[string]any)
			if !ok {
				return "beads_expected_edge_invalid"
			}
			key := edgeKey(dep["issue_id"], dep["depends_on_id"], dep["type"])
			if _, exists := edges[key]; exists {
				return "beads_expected_edge_duplicate"
			}
			edges[key] = dep
		}
		for _, raw := range array(line["comments"]) {
			comment, ok := raw.(map[string]any)
			if !ok {
				return "beads_expected_comment_invalid"
			}
			key, _ := comment["id"].(string)
			if _, exists := comments[key]; exists {
				return "beads_expected_comment_duplicate"
			}
			comments[key] = comment
		}
	}
	if len(rows) != 0 || issues == 0 {
		return "beads_raw_issue_census_changed"
	}
	for _, row := range actual.Records("edge") {
		s := row.Source.Fields
		key := edgeKey(s["issue_id"], s["depends_on_issue_id"], s["type"])
		want, ok := edges[key]
		if !ok {
			return "beads_raw_edge_census_changed"
		}
		delete(edges, key)
		// Source dependency IDs may be reallocated. Its retained envelope must
		// still preserve the exact original identity and metadata.
		var envelope any
		if text, ok := want["metadata"].(string); !ok || decodeNumberJSON([]byte(text), &envelope) != nil {
			return "beads_expected_edge_invalid"
		}
		if !sameJSON(envelope, s["metadata"]) {
			return "beads_raw_edge_metadata_changed"
		}
		for _, field := range []string{"created_by", "thread_id"} {
			if !sameOptionalText(want[field], s[field]) {
				return "beads_raw_edge_field_changed"
			}
		}
		if !sameInstant(want["created_at"], s["created_at"]) {
			return "beads_raw_edge_timestamp_changed"
		}
	}
	if len(edges) != 0 {
		return "beads_raw_edge_missing"
	}
	for _, row := range actual.Records("comment") {
		s := row.Source.Fields
		id, _ := s["id"].(string)
		want, ok := comments[id]
		if !ok {
			return "beads_raw_comment_census_changed"
		}
		delete(comments, id)
		for _, field := range []string{"id", "issue_id", "author", "text"} {
			if !sameJSON(want[field], s[field]) {
				return "beads_raw_comment_field_changed"
			}
		}
		if !sameInstant(want["created_at"], s["created_at"]) {
			return "beads_raw_comment_timestamp_changed"
		}
	}
	if len(comments) != 0 {
		return "beads_raw_comment_missing"
	}
	for _, row := range actual.Records("memory") {
		key, _ := row.Body["key"].(string)
		want, ok := memories[key]
		if !ok || !sameJSON(want, row.Body["value"]) {
			return "beads_raw_memory_changed"
		}
		delete(memories, key)
	}
	if len(memories) != 0 {
		return "beads_raw_memory_missing"
	}
	return ""
}

func array(v any) []any { a, _ := v.([]any); return a }
func edgeKey(from, to, kind any) string {
	b, _ := json.Marshal([]any{from, to, kind})
	return string(b)
}
func sameOptionalText(a, b any) bool {
	if a == nil {
		a = ""
	}
	if b == nil {
		b = ""
	}
	return sameJSON(a, b)
}
func sameInstant(a, b any) bool {
	x, _ := a.(string)
	y, _ := b.(string)
	if x == "" && y == "" {
		return true
	}
	t, e := time.Parse(time.RFC3339Nano, x)
	u, f := time.Parse(time.RFC3339Nano, y)
	return e == nil && f == nil && t.Equal(u)
}
func sameStringSet(a, b any) bool {
	strings := func(v any) ([]string, bool) {
		r := []string{}
		for _, x := range array(v) {
			s, ok := x.(string)
			if !ok {
				return nil, false
			}
			r = append(r, s)
		}
		sort.Strings(r)
		return r, true
	}
	x, e := strings(a)
	y, f := strings(b)
	return e && f && sameJSON(x, y)
}
func decodeNumberJSON(data []byte, value any) error {
	d := json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()
	if err := d.Decode(value); err != nil {
		return err
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return fmt.Errorf("trailing JSON")
	}
	return nil
}

func openDisposablePostgres(ctx context.Context, dsn string) (driver.Store, error) {
	u, err := url.Parse(dsn)
	if err != nil || (u.Scheme != "postgres" && u.Scheme != "postgresql") {
		return nil, fmt.Errorf("invalid disposable DSN")
	}
	ip := net.ParseIP(u.Hostname())
	if ip == nil || !ip.IsLoopback() || !strings.HasPrefix(u.Path, "/plug_roundtrip_") || len(u.Path) <= len("/plug_roundtrip_") {
		return nil, fmt.Errorf("disposable loopback database required")
	}
	// URI query parameters must not override the checked destination.
	for key := range u.Query() {
		if key != "sslmode" && key != "connect_timeout" {
			return nil, fmt.Errorf("DSN override refused")
		}
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, err
	}
	var tables int
	err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM pg_catalog.pg_tables WHERE schemaname NOT IN ('pg_catalog', 'information_schema')").Scan(&tables)
	_ = db.Close()
	if err != nil || tables != 0 {
		return nil, fmt.Errorf("empty disposable database required")
	}
	return (postgres.Driver{}).Open(ctx, driver.Config{DSN: dsn})
}

// samePinnedValue validates the raw SQL representation independently of the
// converter. Unknown stored values must not collapse into an expected false.
func samePinnedValue(want bool, raw any) bool {
	switch value := raw.(type) {
	case bool:
		return want == value
	case json.Number:
		return value == "1" && want || value == "0" && !want
	case string:
		return value == "1" && want || value == "0" && !want
	default:
		return false
	}
}
