package model

import (
	"encoding/json"
	"fmt"
)

type ErrorCode string
type WarningCode string

func validErrorCode(code ErrorCode) bool {
	switch code {
	case "parse_error", "invalid_request", "method_not_found", "invalid_params", "internal_error",
		"not_found", "unauthenticated", "token_expired", "authn_unavailable", "forbidden",
		"authz_unavailable", "authz_policy_invalid", "authz_postfilter_cap", "authz_link_allow_not_last",
		"authz_not_installed", "ontology_guard_failed", "closure_too_large", "protected_record",
		"subscriber_unavailable", "precondition_failed", "write_conflict", "busy", "rate_limited",
		"cursor_invalid", "cursor_expired", "idempotency_key_reused", "lease_not_held", "lease_expired",
		"project_required", "incarnation_changed", "field_owned", "ontology_plan_stale",
		"ontology_migration_too_large", "ontology_drifted", "ontology_unknown_field", "ontology_use_slot_verb",
		"authz_requires_authn", "lockout_refused", "plugin_busy", "plugin_host_write_in_sync_call",
		"plugin_egress_in_hot_path", "plugin_reentrant_write", "plugin_patch_out_of_scope", "subscriber_skipped",
		"post_subscriber_failed", "history_gone", "visibility_lost", "too_many_sessions", "filter_too_broad",
		"scope_too_large", "destination_paused", "ref_moved", "unknown_field", "restore_requires_empty",
		"restore_checksum_failed", "restore_violation", "human_presence_required", "authz_verify_failed",
		"invalid_cursor", "compaction_unavailable", "stale_snapshot", "plugin_durable_state_unavailable":
		return true
	default:
		return false
	}
}

func validWarningCode(code WarningCode) bool {
	switch code {
	case "ontology_drifted", "subscriber_skipped", "post_subscriber_failed", "visibility_lost":
		return true
	default:
		return false
	}
}

func (code ErrorCode) Validate() error {
	if !validErrorCode(code) {
		return fmt.Errorf("unknown error code %q", code)
	}
	return nil
}

func (code ErrorCode) MarshalJSON() ([]byte, error) {
	if err := code.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(string(code))
}

func (code *ErrorCode) UnmarshalJSON(data []byte) error {
	var value string
	if err := decodeJSON(data, &value); err != nil {
		return err
	}
	decoded := ErrorCode(value)
	if err := decoded.Validate(); err != nil {
		return err
	}
	*code = decoded
	return nil
}

func (code WarningCode) Validate() error {
	if !validWarningCode(code) {
		return fmt.Errorf("unknown warning code %q", code)
	}
	return nil
}

func (code WarningCode) MarshalJSON() ([]byte, error) {
	if err := code.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(string(code))
}

func (code *WarningCode) UnmarshalJSON(data []byte) error {
	var value string
	if err := decodeJSON(data, &value); err != nil {
		return err
	}
	decoded := WarningCode(value)
	if err := decoded.Validate(); err != nil {
		return err
	}
	*code = decoded
	return nil
}
