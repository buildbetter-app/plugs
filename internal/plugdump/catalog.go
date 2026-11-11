package plugdump

import "strings"

// This is the paired main=72/ignored=28 census, not the JSONL projection.
// All columns are read, including columns types.Issue does not serialize.
var issueColumns = "id content_hash title description design acceptance_criteria notes status priority issue_type assignee estimated_minutes created_at created_by owner updated_at closed_at closed_by_session external_ref spec_id compaction_level compacted_at compacted_at_commit original_size sender ephemeral wisp_type pinned is_template mol_type work_type source_system metadata source_repo close_reason event_kind actor target payload await_type await_id timeout_ns waiters hook_bead role_bead agent_state last_activity role_type rig due_at defer_until no_history started_at is_blocked row_lock storage_class current_revision participation_generation"

var tableColumns = map[string]string{
	"schema_migrations":         "version content_hash",
	"ignored_schema_migrations": "version applied_at content_hash",
	"issues":                    issueColumns,
	"wisps":                     issueColumns,
	"dependencies":              "id issue_id depends_on_issue_id depends_on_wisp_id depends_on_external type created_at created_by metadata thread_id",
	"wisp_dependencies":         "id issue_id depends_on_issue_id depends_on_wisp_id depends_on_external type created_at created_by metadata thread_id",
	"labels":                    "issue_id label",
	"wisp_labels":               "issue_id label",
	"comments":                  "id issue_id author text created_at",
	"wisp_comments":             "id issue_id author text created_at",
	"events":                    "id issue_id event_type actor old_value new_value comment created_at",
	"wisp_events":               "id issue_id event_type actor old_value new_value comment created_at",
	"config":                    "key value",
	"metadata":                  "key value",
	"local_metadata":            "key value",
	"child_counters":            "parent_id last_child",
	"wisp_child_counters":       "parent_id last_child",
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

// Every family needs an explicit input entry. Directory families may contain
// multiple entries; absence is checked at the supplied path. The caller must
// supply all configured search paths, not just the default path.
var externalFamilies = []string{
	"user-legacy-config", "user-xdg-config", "user-native-config",
	"project-config", "beads-dir-config", "project-local-config",
	"environment-and-flags", "metadata-json", "legacy-config-json",
	"beads-hooks", "legacy-hooks", "git-hooks", "external-hook-managers",
	"routes-jsonl", "formulas", "formula-search-paths", "issues-jsonl",
	"interactions-jsonl", "dolt-ignore", "dolt-nonlocal-tables",
	"dolt-working-set", "dolt-commit-graph", "dolt-remotes",
	"dolt-conflicts", "dolt-history-maintenance", "store-files-and-quiescence",
}

func actionFor(table string) (string, string) {
	switch table {
	case "issues", "dependencies", "labels", "comments", "config", "metadata", "child_counters", "issue_counter":
		return "carry", "head state and source provenance"
	case "schema_migrations", "ignored_schema_migrations", "store_epoch", "epoch_minted_addresses", "bd_events_seq", "local_metadata":
		return "manifest/rebuild", "source identity, cursor or history-floor evidence; never a Plug revision"
	case "repo_mtimes", "leases":
		return "manifest/rebuild", "operational state excluded; leases must be empty"
	case "wisps", "wisp_labels", "wisp_dependencies", "wisp_comments", "wisp_events", "wisp_child_counters", "custom_statuses", "custom_types":
		return "archive", "read-only workflow/catalog source evidence; no durable Plug history or live lease is invented"
	case "interactions", "federation_peers", "routes":
		return "refuse-if-nonempty", "workflow/overlay/routing/audit/credential mapping is unsupported in this slice"
	default:
		return "archive", "read-only source evidence; no verified A6 history or attribution reconstruction"
	}
}

func columnsFor(name string) []string { return strings.Fields(tableColumns[name]) }

var knownKinds = map[string]bool{
	"bead": true, "edge": true, "comment": true, "memory": true,
	"config": true, "beads-archive": true,
}
