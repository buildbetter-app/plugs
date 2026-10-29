# Plug error catalog

This is the closed wire error catalog for Plug JSON-RPC 2.0. Each error has a
stable machine code, a JSON-RPC integer, retryability metadata, an audience
classification, and a flag indicating whether it may be returned as a
`result.warnings` entry. The JSON schemas for the common error data envelope
and each named error live in [`openrpc.json`](openrpc.json).

A committed write returns warnings inside `result.warnings`; it never returns
both a committed result and a JSON-RPC `error`. The `audience` value guides
diagnostics and publication policy; P69 requires warnings to remain visible
to end users. Internal and host-only entries remain in this closed catalog
for implementation and telemetry mapping.

## Catalog

| Code | JSON-RPC | Retryable | Audience | Warning-capable |
| --- | ---: | :---: | --- | :---: |
| `parse_error` | `-32700` | no | `client` | no |
| `invalid_request` | `-32600` | no | `client` | no |
| `method_not_found` | `-32601` | no | `client` | no |
| `invalid_params` | `-32602` | no | `client` | no |
| `internal_error` | `-32603` | no | `operator` | no |
| `not_found` | `-32000` | no | `client` | no |
| `unauthenticated` | `-32001` | no | `client` | no |
| `token_expired` | `-32002` | no | `client` | no |
| `authn_unavailable` | `-32003` | yes | `operator` | no |
| `forbidden` | `-32004` | no | `client` | no |
| `authz_unavailable` | `-32005` | yes | `operator` | no |
| `authz_policy_invalid` | `-32006` | no | `operator` | no |
| `authz_postfilter_cap` | `-32007` | no | `operator` | no |
| `authz_link_allow_not_last` | `-32008` | no | `operator` | no |
| `authz_not_installed` | `-32009` | no | `client` | no |
| `ontology_guard_failed` | `-32010` | no | `client` | no |
| `closure_too_large` | `-32011` | no | `operator` | no |
| `protected_record` | `-32012` | no | `client` | no |
| `subscriber_unavailable` | `-32013` | yes | `operator` | no |
| `precondition_failed` | `-32014` | no | `client` | no |
| `write_conflict` | `-32015` | yes | `client` | no |
| `busy` | `-32016` | yes | `client` | no |
| `rate_limited` | `-32017` | yes | `client` | no |
| `cursor_invalid` | `-32018` | no | `client` | no |
| `cursor_expired` | `-32019` | no | `client` | no |
| `idempotency_key_reused` | `-32020` | no | `client` | no |
| `lease_not_held` | `-32021` | yes | `client` | no |
| `lease_expired` | `-32022` | yes | `client` | no |
| `project_required` | `-32023` | no | `client` | no |
| `incarnation_changed` | `-32024` | no | `client` | no |
| `field_owned` | `-32025` | no | `operator` | no |
| `ontology_plan_stale` | `-32026` | no | `client` | no |
| `ontology_migration_too_large` | `-32027` | no | `operator` | no |
| `ontology_drifted` | `-32028` | no | `operator` | yes |
| `ontology_unknown_field` | `-32029` | no | `client` | no |
| `ontology_use_slot_verb` | `-32030` | no | `client` | no |
| `authz_requires_authn` | `-32031` | no | `operator` | no |
| `lockout_refused` | `-32032` | no | `operator` | no |
| `plugin_busy` | `-32033` | yes | `plugin-host` | no |
| `plugin_host_write_in_sync_call` | `-32034` | no | `plugin-host` | no |
| `plugin_egress_in_hot_path` | `-32035` | no | `plugin-host` | no |
| `plugin_reentrant_write` | `-32036` | no | `plugin-host` | no |
| `plugin_patch_out_of_scope` | `-32037` | no | `plugin-host` | no |
| `subscriber_skipped` | `-32038` | no | `client` | yes |
| `post_subscriber_failed` | `-32039` | no | `client` | yes |
| `history_gone` | `-32040` | no | `client` | no |
| `visibility_lost` | `-32041` | no | `client` | yes |
| `too_many_sessions` | `-32042` | yes | `client` | no |
| `filter_too_broad` | `-32043` | no | `client` | no |
| `scope_too_large` | `-32044` | no | `client` | no |
| `destination_paused` | `-32045` | no | `operator` | no |
| `ref_moved` | `-32046` | no | `operator` | no |
| `unknown_field` | `-32047` | no | `client` | no |
| `restore_requires_empty` | `-32048` | no | `operator` | no |
| `restore_checksum_failed` | `-32049` | no | `operator` | no |
| `restore_violation` | `-32050` | no | `operator` | no |
| `human_presence_required` | `-32051` | no | `client` | no |
| `authz_verify_failed` | `-32052` | no | `operator` | no |
| `invalid_cursor` | `-32053` | no | `client` | no |
| `compaction_unavailable` | `-32054` | yes | `operator` | no |
| `stale_snapshot` | `-32055` | yes | `internal` | no |

The protocol range is the standard JSON-RPC allocation (`-32700` through
`-32603`). Plug application errors use `-32000` through `-32055`; the
allocation is contiguous and must remain stable once clients depend on it.

## Runtime mapping

- `code` is the stable wire string carried in `error.data.code` or a warning.
- `jsonRpc` is the JSON-RPC integer in the error object.
- `retryable` indicates that the operation may be retried after the caller
  applies the method-specific idempotency and cursor rules. Retryable does
  not imply that a retry is safe without the original idempotency key.
- `audience` is one of `client`, `operator`, `plugin-host`, or `internal`.
- `warningCapable` marks codes that can appear in `result.warnings` after a
  write commits. Other codes are JSON-RPC errors.

Error detail members are method- or code-specific and may be carried in the
open `details` object. The common envelope requires `code` and `retryable`
and permits `retryAfterMs`, `hint`, `traceId`, and `details`.
