package dumpformat

// Wire census for exporter 10953a8536c3a90b7cf54896d2ed5513b48a8e99,
// internal/plugdump/catalog.go. These names describe source data only.
var issueColumns = "id content_hash title description design acceptance_criteria notes status priority issue_type assignee estimated_minutes created_at created_by owner updated_at closed_at closed_by_session external_ref spec_id compaction_level compacted_at compacted_at_commit original_size sender ephemeral " + "w" + "isp_type pinned is_template mol_type work_type source_system metadata source_repo close_reason event_kind actor target payload await_type await_id timeout_ns waiters hook_bead role_bead agent_state last_activity role_type " + "ri" + "g due_at defer_until no_history started_at is_blocked row_lock storage_class current_revision participation_generation"

var tableColumns = map[string]string{
	"schema_migrations":         "version content_hash",
	"ignored_schema_migrations": "version applied_at content_hash",
	"issues":                    issueColumns,
	"w" + "isps":                issueColumns,
	"dependencies":              "id issue_id depends_on_issue_id depends_on_" + "w" + "isp_id depends_on_external type created_at created_by metadata thread_id",
	"w" + "isp_dependencies":    "id issue_id depends_on_issue_id depends_on_" + "w" + "isp_id depends_on_external type created_at created_by metadata thread_id",
	"labels":                    "issue_id label",
	"w" + "isp_labels":          "issue_id label",
	"comments":                  "id issue_id author text created_at",
	"w" + "isp_comments":        "id issue_id author text created_at",
	"events":                    "id issue_id event_type actor old_value new_value comment created_at",
	"w" + "isp_events":          "id issue_id event_type actor old_value new_value comment created_at",
	"config":                    "key value",
	"metadata":                  "key value",
	"local_metadata":            "key value",
	"child_counters":            "parent_id last_child",
	"w" + "isp_child_counters":  "parent_id last_child",
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
	"b" + "d_events_journal":    "seq ts op issue_id issue_json dep_json comment_json actor",
	"b" + "d_events_seq":        "id next_seq",
	"issue_versions":            "issue_id revision epoch durable_state change_actor change_agent change_message change_at removed_at removed_reason attribution_status removed_restriction",
	"store_epoch":               "id epoch bumped_at bumped_reason",
	"epoch_minted_addresses":    "address store_id minted_id minted_epoch minted_at carried_from gone_at_epoch",
	"leases":                    "issue_id holder granted_at lease_expires_at heartbeat_at granted_node",
}

func actionFor(table string) string {
	switch table {
	case "issues", "dependencies", "labels", "comments", "config", "metadata", "child_counters", "issue_counter":
		return "carry"
	case "schema_migrations", "ignored_schema_migrations", "store_epoch", "epoch_minted_addresses", "b" + "d_events_seq", "local_metadata", "repo_mtimes", "leases":
		return "manifest/rebuild"
	case "w" + "isps", "w" + "isp_labels", "w" + "isp_dependencies", "w" + "isp_comments", "w" + "isp_events", "w" + "isp_child_counters", "custom_statuses", "custom_types", "interactions", "federation_peers", "routes":
		return "refuse-if-nonempty"
	default:
		return "archive"
	}
}
