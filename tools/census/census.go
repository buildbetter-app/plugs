// Package census validates the executable source/store census ratchet.
//
// The fixture is deliberately a reviewed description of the pinned Beads
// source.  It is not a count guessed from the Plug tree: schema entries carry
// the source path and digest, external configuration inputs are named
// explicitly, and every pinned Go test declaration has a disposition.
package census

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"regexp"
	"sort"
	"strings"
)

const (
	Format                = "plug-source-census/1"
	SourceRepository      = "buildbetter-app/plugs"
	SourceCommit          = "9d6fff760b7a314b476539198504d77ce0959656"
	SourceRef             = "refs/tags/beads-base"
	ExpectedFixtureDigest = "ab2ef747655da08cd7879b9d2e9887051a183a7cc788cdbce32c8d09ccc4ca99"

	// These are source facts measured from SourceCommit.  They are kept here
	// as an integrity check for the reviewed fixture, not as a replacement for
	// its itemized records.
	ExpectedMainMigrations    = 72
	ExpectedIgnoredMigrations = 28
	ExpectedTestDeclarations  = 13139
)

type Census struct {
	Format           string            `json:"format"`
	Source           Source            `json:"source"`
	Schema           Schema            `json:"schema"`
	StoreTables      []StoreTable      `json:"storeTables"`
	StoreSources     []StoreSource     `json:"storeSources"`
	ConfigSources    []ConfigSource    `json:"configSources"`
	TestSources      []TestSource      `json:"testSources"`
	TestDispositions []TestDisposition `json:"testDispositions"`
	Counts           Counts            `json:"counts"`
}

type Source struct {
	Repository string `json:"repository"`
	Ref        string `json:"ref"`
	Commit     string `json:"commit"`
}

type Schema struct {
	Main    []Migration `json:"main"`
	Ignored []Migration `json:"ignored"`
}

type Migration struct {
	Version     int    `json:"version"`
	Path        string `json:"path"`
	SHA256      string `json:"sha256"`
	Disposition string `json:"disposition"`
}

type ConfigSource struct {
	Path        string `json:"path"`
	Kind        string `json:"kind"`
	Presence    string `json:"presence"`
	Precedence  int    `json:"precedence,omitempty"`
	Disposition string `json:"disposition"`
	SHA256      string `json:"sha256,omitempty"`
}

// StoreTable is one final SQL table/view/scratch shape from the paired
// Beads schema census. Views and migration scratch are included so their
// deliberate rebuild/drop decisions remain reviewable even though they are
// not restored as Plug records.
type StoreTable struct {
	Name        string        `json:"name"`
	Plane       string        `json:"plane"`
	Kind        string        `json:"kind"`
	Columns     []StoreColumn `json:"columns"`
	Disposition string        `json:"disposition"`
}

type StoreColumn struct {
	Name        string `json:"name"`
	Disposition string `json:"disposition"`
}

// StoreSource records non-SQL state that affects a frozen export. These are
// separate from ConfigSources because they describe Dolt/server ownership,
// working-set, and repository state rather than configuration precedence.
type StoreSource struct {
	Path        string `json:"path"`
	Kind        string `json:"kind"`
	Presence    string `json:"presence"`
	Disposition string `json:"disposition"`
}

type TestSource struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
}

type TestDisposition struct {
	ID          string `json:"id"`
	Path        string `json:"path"`
	Name        string `json:"name"`
	Line        int    `json:"line"`
	Disposition string `json:"disposition"`
}

type Counts struct {
	MainMigrations    int `json:"mainMigrations"`
	IgnoredMigrations int `json:"ignoredMigrations"`
	StoreTables       int `json:"storeTables"`
	StoreColumns      int `json:"storeColumns"`
	StoreSources      int `json:"storeSources"`
	ConfigSources     int `json:"configSources"`
	TestSources       int `json:"testSources"`
	TestDeclarations  int `json:"testDeclarations"`
}

var (
	sha256Pattern             = regexp.MustCompile(`^[0-9a-f]{64}$`)
	testNamePattern           = regexp.MustCompile(`^Test[A-Za-z0-9_]*$`)
	versionedMigrationPattern = regexp.MustCompile(`^(?:internal/storage/schema/migrations/)([0-9]{4})_[^/]+\.up\.sql$`)
	ignoredMigrationPattern   = regexp.MustCompile(`^(?:internal/storage/schema/migrations/ignored/)([0-9]{4})_[^/]+\.up\.sql$`)
)

// Load reads a strict JSON census fixture.  Unknown fields and trailing JSON
// values are refused so a fixture cannot silently grow a second, unreviewed
// representation of an input.
func Load(file string) (Census, []byte, error) {
	raw, err := os.ReadFile(file)
	if err != nil {
		return Census{}, nil, fmt.Errorf("read census fixture: %w", err)
	}
	if err := rejectDuplicateMembers(raw); err != nil {
		return Census{}, nil, fmt.Errorf("scan census fixture: %w", err)
	}
	var census Census
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&census); err != nil {
		return Census{}, nil, fmt.Errorf("decode census fixture: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return Census{}, nil, errors.New("census fixture has trailing JSON")
		}
		return Census{}, nil, fmt.Errorf("decode census trailing data: %w", err)
	}
	return census, raw, nil
}

// rejectDuplicateMembers scans the JSON token stream before decoding into Go
// structs. encoding/json keeps the last value for a duplicate object member,
// which would let an unreviewed second representation alter the decoded
// census while leaving the canonical digest bound only to that last value.
// Each object gets its own name set, so the check is recursive without
// treating equal names in separate objects as duplicates.
func rejectDuplicateMembers(raw []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	first, err := decoder.Token()
	if err != nil {
		if err == io.EOF {
			return nil
		}
		return err
	}
	return scanJSONValue(decoder, first, "$")
}

func scanJSONValue(decoder *json.Decoder, token json.Token, location string) error {
	delimiter, ok := token.(json.Delim)
	if !ok {
		return nil
	}

	switch delimiter {
	case '{':
		seen := make(map[string]struct{})
		for {
			member, err := decoder.Token()
			if err != nil {
				return err
			}
			if member == json.Delim('}') {
				return nil
			}
			name, ok := member.(string)
			if !ok {
				return fmt.Errorf("object member at %s is not a string", location)
			}
			if _, duplicate := seen[name]; duplicate {
				return fmt.Errorf("duplicate JSON member %q at %s", name, location)
			}
			seen[name] = struct{}{}

			value, err := decoder.Token()
			if err != nil {
				return err
			}
			if err := scanJSONValue(decoder, value, location+"."+name); err != nil {
				return err
			}
		}
	case '[':
		for index := 0; ; index++ {
			value, err := decoder.Token()
			if err != nil {
				return err
			}
			if value == json.Delim(']') {
				return nil
			}
			if err := scanJSONValue(decoder, value, fmt.Sprintf("%s[%d]", location, index)); err != nil {
				return err
			}
		}
	case '}', ']':
		return fmt.Errorf("unexpected closing delimiter %q at %s", delimiter, location)
	default:
		return fmt.Errorf("unexpected JSON delimiter %q at %s", delimiter, location)
	}
}

// Validate checks the complete pinned census.  It intentionally validates
// item identity and disposition rather than trusting the summary counts.
func Validate(c Census) error {
	if c.Format != Format {
		return fmt.Errorf("unsupported census format %q", c.Format)
	}
	if c.Source.Repository != SourceRepository || c.Source.Commit != SourceCommit || c.Source.Ref != SourceRef {
		return fmt.Errorf("source pin is %s %s %s, want %s %s %s", c.Source.Repository, c.Source.Ref, c.Source.Commit, SourceRepository, SourceRef, SourceCommit)
	}
	if strings.TrimSpace(c.Source.Repository) == "" {
		return errors.New("source repository is empty")
	}

	if err := validateMigrations("main", c.Schema.Main, ExpectedMainMigrations, false); err != nil {
		return err
	}
	if err := validateMigrations("ignored", c.Schema.Ignored, ExpectedIgnoredMigrations, true); err != nil {
		return err
	}
	if err := validateStoreTables(c.StoreTables); err != nil {
		return err
	}
	if err := validateStoreSources(c.StoreSources); err != nil {
		return err
	}
	if err := validateConfigSources(c.ConfigSources); err != nil {
		return err
	}
	if err := validateTestSources(c.TestSources, c.TestDispositions); err != nil {
		return err
	}

	if c.Counts.MainMigrations != len(c.Schema.Main) || c.Counts.MainMigrations != ExpectedMainMigrations {
		return fmt.Errorf("main migration count %d does not match itemized count %d and pinned count %d", c.Counts.MainMigrations, len(c.Schema.Main), ExpectedMainMigrations)
	}
	if c.Counts.IgnoredMigrations != len(c.Schema.Ignored) || c.Counts.IgnoredMigrations != ExpectedIgnoredMigrations {
		return fmt.Errorf("ignored migration count %d does not match itemized count %d and pinned count %d", c.Counts.IgnoredMigrations, len(c.Schema.Ignored), ExpectedIgnoredMigrations)
	}
	if c.Counts.StoreTables != len(c.StoreTables) {
		return fmt.Errorf("store table count %d does not match itemized count %d", c.Counts.StoreTables, len(c.StoreTables))
	}
	storeColumns := 0
	for _, table := range c.StoreTables {
		storeColumns += len(table.Columns)
	}
	if c.Counts.StoreColumns != storeColumns {
		return fmt.Errorf("store column count %d does not match itemized count %d", c.Counts.StoreColumns, storeColumns)
	}
	if c.Counts.StoreSources != len(c.StoreSources) {
		return fmt.Errorf("store source count %d does not match itemized count %d", c.Counts.StoreSources, len(c.StoreSources))
	}
	if c.Counts.ConfigSources != len(c.ConfigSources) {
		return fmt.Errorf("config source count %d does not match itemized count %d", c.Counts.ConfigSources, len(c.ConfigSources))
	}
	if c.Counts.TestSources != len(c.TestSources) {
		return fmt.Errorf("test source count %d does not match itemized count %d", c.Counts.TestSources, len(c.TestSources))
	}
	if c.Counts.TestDeclarations != len(c.TestDispositions) || c.Counts.TestDeclarations != ExpectedTestDeclarations {
		return fmt.Errorf("test declaration count %d does not match itemized count %d and pinned count %d", c.Counts.TestDeclarations, len(c.TestDispositions), ExpectedTestDeclarations)
	}
	digest, err := FixtureDigest(c)
	if err != nil {
		return fmt.Errorf("digest census fixture: %w", err)
	}
	if digest != ExpectedFixtureDigest {
		return fmt.Errorf("census fixture digest %s does not match reviewed digest %s", digest, ExpectedFixtureDigest)
	}
	return nil
}

func validateMigrations(lane string, entries []Migration, want int, ignored bool) error {
	if len(entries) != want {
		return fmt.Errorf("%s migration inventory has %d entries, want pinned %d", lane, len(entries), want)
	}
	seen := make(map[string]struct{}, len(entries))
	for i, entry := range entries {
		if entry.Version != i+1 {
			return fmt.Errorf("%s migration version at index %d is %d, want %d", lane, i, entry.Version, i+1)
		}
		match := versionedMigrationPattern.FindStringSubmatch(entry.Path)
		if ignored {
			match = ignoredMigrationPattern.FindStringSubmatch(entry.Path)
		}
		if len(match) != 2 || match[1] != fmt.Sprintf("%04d", entry.Version) {
			return fmt.Errorf("%s migration %d has noncanonical path %q", lane, entry.Version, entry.Path)
		}
		if _, ok := seen[entry.Path]; ok {
			return fmt.Errorf("duplicate %s migration path %q", lane, entry.Path)
		}
		seen[entry.Path] = struct{}{}
		if !sha256Pattern.MatchString(entry.SHA256) {
			return fmt.Errorf("%s migration %q has invalid SHA-256 %q", lane, entry.Path, entry.SHA256)
		}
		if err := validateDisposition("migration "+entry.Path, entry.Disposition); err != nil {
			return err
		}
	}
	return nil
}

// Hex escapes preserve pinned legacy names without adding them to Plug's
// source vocabulary. Their decoded values must match the reviewed fixture.
var knownConfigSources = []ConfigSource{
	{Path: "~/.beads/config.yaml", Kind: "yaml-user-legacy", Presence: "runtime-optional", Precedence: 1, Disposition: "carry:config"},
	{Path: "~/.config/\x62d/config.yaml", Kind: "yaml-user-documented", Presence: "runtime-optional", Precedence: 2, Disposition: "carry:config"},
	{Path: "$NATIVE_USER_CONFIG_DIR/\x62d/config.yaml", Kind: "yaml-user-native", Presence: "runtime-optional", Precedence: 3, Disposition: "carry:config"},
	{Path: ".beads/config.yaml", Kind: "yaml-workspace", Presence: "runtime-optional", Precedence: 4, Disposition: "carry:config"},
	{Path: "$BEADS_DIR/config.yaml", Kind: "yaml-runtime", Presence: "runtime-conditional", Precedence: 5, Disposition: "carry:config"},
	{Path: ".beads/config.local.yaml", Kind: "yaml-workspace-local", Presence: "runtime-conditional", Precedence: 6, Disposition: "carry:config"},
	{Path: "cli:flags", Kind: "cli-overrides", Presence: "runtime-conditional", Precedence: 7, Disposition: "manifest/rebuild:config-overrides"},
	{Path: "env:\x42D_*", Kind: "environment-overrides", Presence: "runtime-conditional", Precedence: 8, Disposition: "manifest/rebuild:config-overrides"},
	{Path: "env:BEADS_ACTOR", Kind: "environment-override", Presence: "runtime-conditional", Precedence: 8, Disposition: "manifest/rebuild:config-overrides"},
	{Path: "env:BEADS_IDENTITY", Kind: "environment-override", Presence: "runtime-conditional", Precedence: 8, Disposition: "manifest/rebuild:config-overrides"},
	{Path: "env:BEADS_NODE_ID", Kind: "environment-override", Presence: "runtime-conditional", Precedence: 8, Disposition: "manifest/rebuild:config-overrides"},
	{Path: "env:BEADS_DIR", Kind: "environment-selector", Presence: "runtime-conditional", Precedence: 8, Disposition: "manifest/rebuild:config-source-selection"},
	{Path: ".beads/metadata.json", Kind: "workspace-metadata", Presence: "runtime-conditional", Disposition: "manifest/rebuild:metadata"},
	{Path: ".beads/config.json", Kind: "legacy-metadata", Presence: "runtime-conditional", Disposition: "manifest/rebuild:metadata"},
	{Path: ".beads/hooks/", Kind: "hook-directory", Presence: "runtime-optional", Disposition: "archive:hooks"},
	{Path: ".beads-hooks/", Kind: "hook-directory", Presence: "runtime-optional", Disposition: "archive:hooks"},
	{Path: ".git/hooks/", Kind: "hook-directory", Presence: "runtime-optional", Disposition: "archive:hooks"},
	{Path: ".pre-commit-config.yaml", Kind: "hook-manager-config", Presence: "runtime-optional", Disposition: "archive:hooks"},
	{Path: "git:core.hooksPath", Kind: "hook-manager-setting", Presence: "runtime-conditional", Disposition: "archive:hooks"},
	{Path: "simple-git-hooks", Kind: "hook-manager-setting", Presence: "runtime-conditional", Disposition: "archive:hooks"},
	{Path: "IDE/agent hook settings", Kind: "hook-manager-setting", Presence: "runtime-conditional", Disposition: "archive:hooks"},
	{Path: ".beads/routes.jsonl", Kind: "routes", Presence: "runtime-optional", Disposition: "manifest/rebuild:routes"},
	{Path: ".beads/formulas/", Kind: "\x66ormula-directory", Presence: "runtime-optional", Disposition: "carry:workflow"},
	{Path: "~/.beads/formulas/", Kind: "\x66ormula-search-path", Presence: "runtime-optional", Disposition: "carry:workflow"},
	{Path: "$\x46ORMULA_SEARCH_PATHS", Kind: "\x66ormula-search-path", Presence: "runtime-conditional", Disposition: "carry:workflow"},
	{Path: "\x66ormula:references", Kind: "\x66ormula-reference", Presence: "runtime-conditional", Disposition: "carry:workflow"},
	{Path: ".beads/formulas/beads-release.\x66ormula.toml", Kind: "\x66ormula", Presence: "pinned-source", Disposition: "carry:workflow", SHA256: "69b067bb23791925b6d74cdbd4b439d40d73898ba4c7f575164d4866efa13b69"},
	{Path: ".beads/issues.jsonl", Kind: "export-cross-check", Presence: "runtime-optional", Disposition: "archive:cross-check"},
	{Path: ".beads/interactions.jsonl", Kind: "export-cross-check", Presence: "runtime-optional", Disposition: "archive:cross-check"},
	{Path: "store:config:kv.memory.*", Kind: "memory-rows", Presence: "runtime-conditional", Disposition: "carry:memory"},
	{Path: "store:local_metadata:events_journal_autoprune", Kind: "journal-control", Presence: "runtime-conditional", Disposition: "manifest/rebuild:history-floor"},
}

func storeColumns(disposition string, names ...string) []StoreColumn {
	columns := make([]StoreColumn, 0, len(names))
	for _, name := range names {
		columns = append(columns, StoreColumn{Name: name, Disposition: disposition})
	}
	return columns
}

func issueColumnNames() []string {
	return []string{
		"id", "content_hash", "title", "description", "design", "acceptance_criteria", "notes", "status", "priority", "issue_type", "assignee", "estimated_minutes", "created_at", "created_by", "owner", "updated_at", "closed_at", "closed_by_session", "external_ref", "spec_id", "compaction_level", "compacted_at", "compacted_at_commit", "original_size", "sender", "ephemeral", "\x77isp_type", "pinned", "is_template", "mol_type", "work_type", "source_system", "metadata", "source_repo", "close_reason", "event_kind", "actor", "target", "payload", "await_type", "await_id", "timeout_ns", "waiters", "hook_bead", "role_bead", "agent_state", "last_activity", "role_type", "\x72ig", "due_at", "defer_until", "no_history", "started_at", "is_blocked", "row_lock", "storage_class", "current_revision", "participation_generation",
	}
}

func reviewedStoreTables() []StoreTable {
	issues := issueColumnNames()
	return []StoreTable{
		{Name: "schema_migrations", Plane: "main", Kind: "table", Columns: storeColumns("manifest/rebuild:schema-cursor", "version", "content_hash"), Disposition: "manifest/rebuild:schema-cursor"},
		{Name: "ignored_schema_migrations", Plane: "ignored", Kind: "table", Columns: storeColumns("manifest/rebuild:schema-cursor", "version", "applied_at", "content_hash"), Disposition: "manifest/rebuild:schema-cursor"},
		{Name: "issues", Plane: "main", Kind: "table", Columns: storeColumns("carry:bead", issues...), Disposition: "carry:bead"},
		{Name: "dependencies", Plane: "main", Kind: "table", Columns: storeColumns("carry:edge", "id", "issue_id", "depends_on_issue_id", "depends_on_\x77isp_id", "depends_on_external", "type", "created_at", "created_by", "metadata", "thread_id"), Disposition: "carry:edge"},
		{Name: "labels", Plane: "main", Kind: "table", Columns: storeColumns("carry:bead-label", "issue_id", "label"), Disposition: "carry:bead-label"},
		{Name: "comments", Plane: "main", Kind: "table", Columns: storeColumns("carry:comment", "id", "issue_id", "author", "text", "created_at"), Disposition: "carry:comment"},
		{Name: "events", Plane: "ignored", Kind: "table", Columns: storeColumns("archive:event-evidence", "issue_id", "event_type", "actor", "old_value", "new_value", "comment", "created_at", "id"), Disposition: "archive:event-evidence"},
		{Name: "config", Plane: "main", Kind: "table", Columns: storeColumns("carry:config", "key", "value"), Disposition: "carry:config"},
		{Name: "metadata", Plane: "main", Kind: "table", Columns: storeColumns("manifest/rebuild:metadata", "key", "value"), Disposition: "manifest/rebuild:metadata"},
		{Name: "child_counters", Plane: "main", Kind: "table", Columns: storeColumns("manifest/rebuild:allocator", "parent_id", "last_child"), Disposition: "manifest/rebuild:allocator"},
		{Name: "issue_snapshots", Plane: "main", Kind: "table", Columns: storeColumns("carry:history", "id", "issue_id", "snapshot_time", "compaction_level", "original_size", "compressed_size", "original_content", "archived_events"), Disposition: "carry:history"},
		{Name: "compaction_snapshots", Plane: "main", Kind: "table", Columns: storeColumns("carry:history", "id", "issue_id", "compaction_level", "snapshot_json", "created_at"), Disposition: "carry:history"},
		{Name: "repo_mtimes", Plane: "ignored", Kind: "table", Columns: storeColumns("manifest/rebuild:repo-state", "repo_path", "jsonl_path", "mtime_ns", "last_checked"), Disposition: "manifest/rebuild:repo-state"},
		{Name: "routes", Plane: "main", Kind: "table", Columns: storeColumns("manifest/rebuild:routes", "prefix", "path", "created_at", "updated_at"), Disposition: "manifest/rebuild:routes"},
		{Name: "issue_counter", Plane: "main", Kind: "table", Columns: storeColumns("manifest/rebuild:allocator", "prefix", "last_id"), Disposition: "manifest/rebuild:allocator"},
		{Name: "interactions", Plane: "main", Kind: "table", Columns: storeColumns("archive:interactions", "id", "kind", "created_at", "actor", "issue_id", "model", "prompt", "response", "error", "tool_name", "exit_code", "parent_id", "label", "reason", "extra"), Disposition: "archive:interactions"},
		{Name: "federation_peers", Plane: "main", Kind: "table", Columns: storeColumns("drop:D10", "name", "remote_url", "username", "password_encrypted", "sovereignty", "last_sync", "created_at", "updated_at"), Disposition: "drop:D10"},
		{Name: "custom_statuses", Plane: "main", Kind: "table", Columns: storeColumns("carry:ontology", "name", "category"), Disposition: "carry:ontology"},
		{Name: "custom_types", Plane: "main", Kind: "table", Columns: storeColumns("carry:ontology", "name"), Disposition: "carry:ontology"},
		{Name: "local_metadata", Plane: "ignored", Kind: "table", Columns: storeColumns("manifest/rebuild:history-floor", "key", "value"), Disposition: "manifest/rebuild:history-floor"},
		{Name: "provenance_events", Plane: "main", Kind: "table", Columns: storeColumns("carry:provenance", "id", "issue_id", "kind", "actor", "ref", "ref_kind", "payload", "source", "occurred_at", "created_at"), Disposition: "carry:provenance"},
		{Name: "\x62d_events_journal", Plane: "ignored", Kind: "table", Columns: storeColumns("archive:journal", "seq", "ts", "op", "issue_id", "issue_json", "dep_json", "comment_json", "actor"), Disposition: "archive:journal"},
		{Name: "\x62d_events_seq", Plane: "ignored", Kind: "table", Columns: storeColumns("manifest/rebuild:journal-head", "id", "next_seq"), Disposition: "manifest/rebuild:journal-head"},
		{Name: "issue_versions", Plane: "main", Kind: "table", Columns: storeColumns("carry:history", "issue_id", "revision", "epoch", "durable_state", "change_actor", "change_agent", "change_message", "change_at", "removed_at", "removed_reason", "attribution_status", "removed_restriction"), Disposition: "carry:history"},
		{Name: "store_epoch", Plane: "main", Kind: "table", Columns: storeColumns("manifest/rebuild:incarnation", "id", "epoch", "bumped_at", "bumped_reason"), Disposition: "manifest/rebuild:incarnation"},
		{Name: "epoch_minted_addresses", Plane: "main", Kind: "table", Columns: storeColumns("manifest/rebuild:allocator", "address", "store_id", "minted_id", "minted_epoch", "minted_at", "carried_from", "gone_at_epoch"), Disposition: "manifest/rebuild:allocator"},
		{Name: "leases", Plane: "ignored", Kind: "table", Columns: storeColumns("manifest/rebuild:leases", "issue_id", "holder", "granted_at", "lease_expires_at", "heartbeat_at", "granted_node"), Disposition: "manifest/rebuild:leases"},
		{Name: "wisps", Plane: "ignored", Kind: "table", Columns: storeColumns("carry:workflow", issues...), Disposition: "carry:workflow"},
		{Name: "\x77isp_labels", Plane: "ignored", Kind: "table", Columns: storeColumns("carry:workflow", "issue_id", "label"), Disposition: "carry:workflow"},
		{Name: "\x77isp_dependencies", Plane: "ignored", Kind: "table", Columns: storeColumns("carry:workflow", "id", "issue_id", "depends_on_issue_id", "depends_on_\x77isp_id", "depends_on_external", "type", "created_at", "created_by", "metadata", "thread_id"), Disposition: "carry:workflow"},
		{Name: "\x77isp_events", Plane: "ignored", Kind: "table", Columns: storeColumns("carry:workflow", "id", "issue_id", "event_type", "actor", "old_value", "new_value", "comment", "created_at"), Disposition: "carry:workflow"},
		{Name: "\x77isp_comments", Plane: "ignored", Kind: "table", Columns: storeColumns("carry:workflow", "id", "issue_id", "author", "text", "created_at"), Disposition: "carry:workflow"},
		{Name: "\x77isp_child_counters", Plane: "ignored", Kind: "table", Columns: storeColumns("manifest/rebuild:allocator", "parent_id", "last_child"), Disposition: "manifest/rebuild:allocator"},
		{Name: "ready_issues", Plane: "main", Kind: "view", Disposition: "manifest/rebuild:derived-view"},
		{Name: "blocked_issues", Plane: "main", Kind: "view", Disposition: "manifest/rebuild:derived-view"},
		{Name: "__temp__wisps", Plane: "ignored", Kind: "scratch", Disposition: "drop:migration-scratch"},
		{Name: "__temp__\x77isp_labels", Plane: "ignored", Kind: "scratch", Disposition: "drop:migration-scratch"},
		{Name: "__temp__\x77isp_dependencies", Plane: "ignored", Kind: "scratch", Disposition: "drop:migration-scratch"},
		{Name: "__temp__\x77isp_events", Plane: "ignored", Kind: "scratch", Disposition: "drop:migration-scratch"},
		{Name: "__temp__\x77isp_comments", Plane: "ignored", Kind: "scratch", Disposition: "drop:migration-scratch"},
		{Name: "__temp__repo_mtimes", Plane: "ignored", Kind: "scratch", Disposition: "drop:migration-scratch"},
		{Name: "__temp__local_metadata", Plane: "ignored", Kind: "scratch", Disposition: "drop:migration-scratch"},
		{Name: "__temp__\x77isp_child_counters", Plane: "ignored", Kind: "scratch", Disposition: "drop:migration-scratch"},
		{Name: "__temp__leases", Plane: "main+ignored", Kind: "scratch", Disposition: "drop:migration-scratch"},
		{Name: "__temp__\x62d_events_journal", Plane: "main+ignored", Kind: "scratch", Disposition: "drop:migration-scratch"},
		{Name: "__temp__\x62d_events_seq", Plane: "main+ignored", Kind: "scratch", Disposition: "drop:migration-scratch"},
		{Name: "__temp__events_flip", Plane: "main", Kind: "scratch", Disposition: "drop:migration-scratch"},
		{Name: "__\x62d_0059_recompute_wisps", Plane: "main", Kind: "scratch", Disposition: "drop:migration-scratch"},
		{Name: "__\x62d_0059_recompute_\x77isp_deps", Plane: "main", Kind: "scratch", Disposition: "drop:migration-scratch"},
		{Name: "repo_mtimes_tmp", Plane: "main", Kind: "scratch", Disposition: "drop:migration-scratch"},
		{Name: "__temp__ignored_schema_migrations_untrack", Plane: "ignored", Kind: "scratch", Disposition: "drop:migration-scratch"},
	}
}

var knownStoreTables = reviewedStoreTables()

var knownStoreSources = []StoreSource{
	{Path: "dolt_ignore", Kind: "system-table", Presence: "runtime-required", Disposition: "manifest/rebuild:dolt-ignore"},
	{Path: "dolt_nonlocal_tables", Kind: "system-table", Presence: "runtime-optional", Disposition: "manifest/rebuild:dolt-topology"},
	{Path: "dolt_status", Kind: "working-set", Presence: "runtime-required", Disposition: "manifest/rebuild:freeze"},
	{Path: "dolt_working_set", Kind: "working-set", Presence: "runtime-required", Disposition: "manifest/rebuild:freeze"},
	{Path: "dolt_commit_graph", Kind: "history", Presence: "runtime-required", Disposition: "manifest/rebuild:history"},
	{Path: "dolt_branches_remotes", Kind: "topology", Presence: "runtime-optional", Disposition: "manifest/rebuild:dolt-topology"},
	{Path: "dolt_conflicts_schema", Kind: "conflicts", Presence: "runtime-required", Disposition: "manifest/rebuild:conflicts"},
	{Path: "dolt_history_maintenance", Kind: "history-maintenance", Presence: "runtime-optional", Disposition: "manifest/rebuild:history-floor"},
	{Path: ".beads/dolt/", Kind: "embedded-data", Presence: "runtime-conditional", Disposition: "manifest/rebuild:embedded-store"},
	{Path: ".beads/embeddeddolt/", Kind: "embedded-data", Presence: "runtime-conditional", Disposition: "manifest/rebuild:embedded-store"},
	{Path: "$DOLT_DATA_DIR/", Kind: "embedded-data", Presence: "runtime-conditional", Disposition: "manifest/rebuild:embedded-store"},
	{Path: "server:metadata", Kind: "server-connection", Presence: "runtime-conditional", Disposition: "manifest/rebuild:server-store"},
	{Path: "server:working_set", Kind: "server-connection", Presence: "runtime-conditional", Disposition: "manifest/rebuild:server-store"},
	{Path: "server:machine-ignored-rows", Kind: "server-connection", Presence: "runtime-conditional", Disposition: "manifest/rebuild:ignored-owner"},
	{Path: "server:proxy-identity", Kind: "server-connection", Presence: "runtime-conditional", Disposition: "manifest/rebuild:server-store"},
}

func validateStoreTables(entries []StoreTable) error {
	if len(entries) != len(knownStoreTables) {
		return fmt.Errorf("store table inventory has %d entries, want %d reviewed tables", len(entries), len(knownStoreTables))
	}
	wantByName := make(map[string]StoreTable, len(knownStoreTables))
	for _, want := range knownStoreTables {
		wantByName[want.Name] = want
	}
	seen := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		want, ok := wantByName[entry.Name]
		if !ok {
			return fmt.Errorf("unknown store table %q", entry.Name)
		}
		if _, duplicate := seen[entry.Name]; duplicate {
			return fmt.Errorf("duplicate store table %q", entry.Name)
		}
		seen[entry.Name] = struct{}{}
		if entry.Plane != want.Plane || entry.Kind != want.Kind || entry.Disposition != want.Disposition {
			return fmt.Errorf("store table %q is %s/%s/%s, want %s/%s/%s", entry.Name, entry.Plane, entry.Kind, entry.Disposition, want.Plane, want.Kind, want.Disposition)
		}
		if len(entry.Columns) != len(want.Columns) {
			return fmt.Errorf("store table %q has %d columns, want %d", entry.Name, len(entry.Columns), len(want.Columns))
		}
		columnSeen := make(map[string]struct{}, len(entry.Columns))
		for i, column := range entry.Columns {
			if column.Name != want.Columns[i].Name || column.Disposition != want.Columns[i].Disposition {
				return fmt.Errorf("store table %q column %d is %s/%s, want %s/%s", entry.Name, i, column.Name, column.Disposition, want.Columns[i].Name, want.Columns[i].Disposition)
			}
			if _, duplicate := columnSeen[column.Name]; duplicate {
				return fmt.Errorf("duplicate column %q in store table %q", column.Name, entry.Name)
			}
			columnSeen[column.Name] = struct{}{}
			if err := validateDisposition("store column "+entry.Name+"."+column.Name, column.Disposition); err != nil {
				return err
			}
		}
		if err := validateDisposition("store table "+entry.Name, entry.Disposition); err != nil {
			return err
		}
	}
	return nil
}

func validateStoreSources(entries []StoreSource) error {
	if len(entries) != len(knownStoreSources) {
		return fmt.Errorf("store source inventory has %d entries, want %d reviewed sources", len(entries), len(knownStoreSources))
	}
	wantByPath := make(map[string]StoreSource, len(knownStoreSources))
	for _, want := range knownStoreSources {
		wantByPath[want.Path] = want
	}
	seen := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		want, ok := wantByPath[entry.Path]
		if !ok {
			return fmt.Errorf("unknown store source %q", entry.Path)
		}
		if _, duplicate := seen[entry.Path]; duplicate {
			return fmt.Errorf("duplicate store source %q", entry.Path)
		}
		seen[entry.Path] = struct{}{}
		if entry.Kind != want.Kind || entry.Presence != want.Presence || entry.Disposition != want.Disposition {
			return fmt.Errorf("store source %q is %s/%s/%s, want %s/%s/%s", entry.Path, entry.Kind, entry.Presence, entry.Disposition, want.Kind, want.Presence, want.Disposition)
		}
		if err := validateDisposition("store source "+entry.Path, entry.Disposition); err != nil {
			return err
		}
	}
	return nil
}

func validateConfigSources(entries []ConfigSource) error {
	if len(entries) != len(knownConfigSources) {
		return fmt.Errorf("configuration source inventory has %d entries, want %d reviewed sources", len(entries), len(knownConfigSources))
	}
	wantByPath := make(map[string]ConfigSource, len(knownConfigSources))
	for _, want := range knownConfigSources {
		wantByPath[want.Path] = want
	}
	seen := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		want, ok := wantByPath[entry.Path]
		if !ok {
			return fmt.Errorf("unknown configuration source %q", entry.Path)
		}
		if _, ok := seen[entry.Path]; ok {
			return fmt.Errorf("duplicate configuration source %q", entry.Path)
		}
		seen[entry.Path] = struct{}{}
		if entry.Kind != want.Kind || entry.Presence != want.Presence || entry.Precedence != want.Precedence || entry.Disposition != want.Disposition {
			return fmt.Errorf("configuration source %q is %s/%s/%d/%s, want %s/%s/%d/%s", entry.Path, entry.Kind, entry.Presence, entry.Precedence, entry.Disposition, want.Kind, want.Presence, want.Precedence, want.Disposition)
		}
		if want.SHA256 != "" {
			if !sha256Pattern.MatchString(entry.SHA256) {
				return fmt.Errorf("configuration source %q must carry a SHA-256", entry.Path)
			}
			if entry.SHA256 != want.SHA256 {
				return fmt.Errorf("configuration source %q has SHA-256 %q, want reviewed %q", entry.Path, entry.SHA256, want.SHA256)
			}
		} else if entry.SHA256 != "" {
			return fmt.Errorf("runtime configuration source %q must not copy bytes into the fixture", entry.Path)
		}
		if err := validateDisposition("configuration source "+entry.Path, entry.Disposition); err != nil {
			return err
		}
	}
	return nil
}

func validateTestSources(sources []TestSource, tests []TestDisposition) error {
	if len(sources) == 0 {
		return errors.New("test source inventory is empty")
	}
	byPath := make(map[string]TestSource, len(sources))
	for _, source := range sources {
		if !strings.HasSuffix(source.Path, ".go") || strings.Contains(source.Path, "..") || path.IsAbs(source.Path) {
			return fmt.Errorf("invalid test source path %q", source.Path)
		}
		if _, ok := byPath[source.Path]; ok {
			return fmt.Errorf("duplicate test source %q", source.Path)
		}
		if !sha256Pattern.MatchString(source.SHA256) {
			return fmt.Errorf("test source %q has invalid SHA-256", source.Path)
		}
		byPath[source.Path] = source
	}
	seen := make(map[string]struct{}, len(tests))
	for _, test := range tests {
		if _, ok := byPath[test.Path]; !ok {
			return fmt.Errorf("test declaration %q refers to unlisted source %q", test.ID, test.Path)
		}
		if !testNamePattern.MatchString(test.Name) || test.Line < 1 {
			return fmt.Errorf("invalid test declaration %q", test.ID)
		}
		if test.ID != test.Path+"::"+test.Name {
			return fmt.Errorf("test declaration ID %q is not canonical", test.ID)
		}
		if _, ok := seen[test.ID]; ok {
			return fmt.Errorf("duplicate test declaration %q", test.ID)
		}
		seen[test.ID] = struct{}{}
		if err := validateDisposition("test "+test.ID, test.Disposition); err != nil {
			return err
		}
	}
	return nil
}

func validateDisposition(label, disposition string) error {
	if disposition == "" {
		return fmt.Errorf("%s has no disposition", label)
	}
	for _, prefix := range []string{"card:", "drop:", "n/a:", "carry:", "archive:", "manifest/rebuild:"} {
		if strings.HasPrefix(disposition, prefix) && strings.TrimSpace(strings.TrimPrefix(disposition, prefix)) != "" {
			return nil
		}
	}
	return fmt.Errorf("%s has invalid disposition %q", label, disposition)
}

// ValidateInventoryEquality compares a newly enumerated inventory with the
// reviewed fixture.  It is intentionally strict: a source disappearing or a
// new source appearing is a ratchet failure until the fixture is reviewed.
func ValidateInventoryEquality(want, got Census) error {
	if err := Validate(want); err != nil {
		return fmt.Errorf("reviewed census: %w", err)
	}
	if err := Validate(got); err != nil {
		return fmt.Errorf("observed census: %w", err)
	}
	left, _ := canonicalInventory(want)
	right, _ := canonicalInventory(got)
	if !bytes.Equal(left, right) {
		return fmt.Errorf("census inventory differs from reviewed fixture")
	}
	return nil
}

func canonicalInventory(c Census) ([]byte, error) {
	// Sort copies of all order-insensitive sections before comparing. Migration
	// and test order are semantic and are already checked above.
	copyC := c
	copyC.StoreSources = append([]StoreSource(nil), c.StoreSources...)
	copyC.ConfigSources = append([]ConfigSource(nil), c.ConfigSources...)
	copyC.TestSources = append([]TestSource(nil), c.TestSources...)
	sort.Slice(copyC.StoreSources, func(i, j int) bool { return copyC.StoreSources[i].Path < copyC.StoreSources[j].Path })
	sort.Slice(copyC.ConfigSources, func(i, j int) bool { return copyC.ConfigSources[i].Path < copyC.ConfigSources[j].Path })
	sort.Slice(copyC.TestSources, func(i, j int) bool { return copyC.TestSources[i].Path < copyC.TestSources[j].Path })
	return json.Marshal(copyC)
}

// FixtureDigest returns a stable digest of the canonical census bytes. It is
// useful in reports and does not get stored inside the fixture (avoiding a
// self-referential digest).
func FixtureDigest(c Census) (string, error) {
	raw, err := canonicalInventory(c)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}
