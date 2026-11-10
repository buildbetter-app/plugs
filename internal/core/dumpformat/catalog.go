package dumpformat

// Physical source names are data-only protocol vocabulary shared by archive
// validation and source-bound provider preparation. They imply no target
// lifetime, principal, status mapping or permission to replay source events.
const (
	DurableEntityTable         = "issues"
	EphemeralPlaneWireField    = "wisp_plane"
	EphemeralTypeField         = "wisp_type"
	EphemeralEntityTable       = "wisps"
	DurableDependencyTable     = "dependencies"
	SourceEventJournalTable    = "bd_events_journal"
	SourceEventSequenceTable   = "bd_events_seq"
	EphemeralDependencyTable   = "wisp_dependencies"
	EphemeralLabelTable        = "wisp_labels"
	EphemeralCommentTable      = "wisp_comments"
	EphemeralEventTable        = "wisp_events"
	EphemeralChildCounterTable = "wisp_child_counters"
)

// Wire census for exporter 10953a8536c3a90b7cf54896d2ed5513b48a8e99,
// internal/plugdump/catalog.go. These names describe source data only.
var issueColumns = "id content_hash title description design acceptance_criteria notes status priority issue_type assignee estimated_minutes created_at created_by owner updated_at closed_at closed_by_session external_ref spec_id compaction_level compacted_at compacted_at_commit original_size sender ephemeral wisp_type pinned is_template mol_type work_type source_system metadata source_repo close_reason event_kind actor target payload await_type await_id timeout_ns waiters hook_bead role_bead agent_state last_activity role_type rig due_at defer_until no_history started_at is_blocked row_lock storage_class current_revision participation_generation"

// OpaqueIssueFields are Beads issue columns without a native Plug field. They
// are carried as reserved beads.<field> metadata and are never interpreted by
// Plug. Keep this list in the dump-format package so restore and reverse
// conversion cannot silently drift apart.
var opaqueIssueFields = []string{
	"agent_state", "await_id", "await_type", "ephemeral", "event_kind",
	"hook_bead", "is_template", "mol_type", "payload", "role_bead",
	"role_type", "target", "timeout_ns", "waiters", "wisp_type", "work_type",
}

func OpaqueIssueFields() []string { return append([]string(nil), opaqueIssueFields...) }

var tableColumns = map[string]string{
	"schema_migrations":         "version content_hash",
	"ignored_schema_migrations": "version applied_at content_hash",
	DurableEntityTable:          issueColumns,
	EphemeralEntityTable:        issueColumns,
	DurableDependencyTable:      "id issue_id depends_on_issue_id depends_on_wisp_id depends_on_external type created_at created_by metadata thread_id",
	EphemeralDependencyTable:    "id issue_id depends_on_issue_id depends_on_wisp_id depends_on_external type created_at created_by metadata thread_id",
	"labels":                    "issue_id label",
	EphemeralLabelTable:         "issue_id label",
	"comments":                  "id issue_id author text created_at",
	EphemeralCommentTable:       "id issue_id author text created_at",
	"events":                    "id issue_id event_type actor old_value new_value comment created_at",
	EphemeralEventTable:         "id issue_id event_type actor old_value new_value comment created_at",
	"config":                    "key value",
	"metadata":                  "key value",
	"local_metadata":            "key value",
	"child_counters":            "parent_id last_child",
	EphemeralChildCounterTable:  "parent_id last_child",
	"issue_counter":             "prefix last_id",
	"issue_snapshots":           "id issue_id snapshot_time compaction_level original_size compressed_size original_content archived_events",
	"compaction_snapshots":      "id issue_id compaction_level snapshot_json created_at",
	"repo_mtimes":               "repo_path jsonl_path mtime_ns last_checked",
	"routes":                    "prefix path created_at updated_at",
	"interactions":              "id kind created_at actor issue_id model prompt response error tool_name exit_code parent_id label reason extra",
	"federation_peers":          "name remote_url username password_encrypted sovereignty last_sync created_at updated_at",
	"custom_statuses":           "name category",
	"custom_types":              "name",
	"provenance_events":         "id issue_id kind actor ref ref_kind payload source occurred_at created_at",
	"bd_events_journal":         "seq ts op issue_id issue_json dep_json comment_json actor",
	"bd_events_seq":             "id next_seq",
	"issue_versions":            "issue_id revision epoch durable_state change_actor change_agent change_message change_at removed_at removed_reason attribution_status removed_restriction",
	"store_epoch":               "id epoch bumped_at bumped_reason",
	"epoch_minted_addresses":    "address store_id minted_id minted_epoch minted_at carried_from gone_at_epoch",
	"leases":                    "issue_id holder granted_at lease_expires_at heartbeat_at granted_node",
}

func actionFor(table string) string {
	switch table {
	case "issues", "dependencies", "labels", "comments", "config", "metadata", "child_counters", "issue_counter":
		return "carry"
	case "schema_migrations", "ignored_schema_migrations", "store_epoch", "epoch_minted_addresses", "bd_events_seq", "local_metadata", "repo_mtimes", "leases":
		return "manifest/rebuild"
	case "wisps", "wisp_labels", "wisp_dependencies", "wisp_comments", "wisp_events", "wisp_child_counters", "custom_statuses", "custom_types", "interactions", "federation_peers", "routes":
		return "refuse-if-nonempty"
	default:
		return "archive"
	}
}

// sourcePlaneFileFamily classifies the existing physical capture families.
// All source vocabulary remains visible in this authoritative catalog.
func sourcePlaneFileFamily(family string) bool {
	switch family {
	case "user-legacy-config", "user-xdg-config", "user-native-config", "project-config", "beads-dir-config", "project-local-config", "metadata-json", "legacy-config-json", "beads-hooks", "legacy-hooks", "git-hooks", "external-hook-managers", "routes-jsonl", "formulas", "formula-search-paths", "issues-jsonl", "interactions-jsonl":
		return true
	}
	return false
}
