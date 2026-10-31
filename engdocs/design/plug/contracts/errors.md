# Plug error catalog

This is the closed wire error catalog for Plug JSON-RPC 2.0. Each error has a
stable machine code, a JSON-RPC integer, retryability metadata, an audience
classification, and a flag indicating whether it may be returned as a
`result.warnings` entry. The JSON schemas for the common error data envelope,
the closed `ErrorCode` enum, and each named error live in
[`openrpc.json`](openrpc.json). A payload with a code outside that enum is
invalid, and a warning must use the closed warning-capable subset;
implementations must not pass through an unregistered string. Each named
JSON-RPC error component additionally pins `data.code` to its catalog row, so
a `Forbidden` payload cannot carry a different registered code.

This PR is a **bounded contract snapshot**. It no longer treats the OpenRPC
document as evidence that generated the Markdown table. Independent source
hashes, the method-selection rule, the allocation owner, the bounded method
registry artifact, and the reproduction commands are recorded in
`openrpc.json` under `x-source-evidence` and `x-method-registry`. The artifact
must validate the method inventory and descriptor parity before implementation
PRs consume it; a future generator may replace it only with equivalent checks.
Decisions #17/#18 select fixed project/team permissions and the bounded X1–X9
package; controller v2 authorizes its implementation. This snapshot does not
yet describe the complete X1–X9 contract. Its recorded source hashes retain
the historical inputs rather than claiming parity with later design edits.
`x-contract-gaps` also records C7's unrestricted restore-manifest parameter,
so typed restore consumers remain unsupported.

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
`-32603`). Plug application errors use `-32000` through `-32055`. The bounded
contract registry owns this range: values are append-only, never renumbered
after publication, and new codes use the next unused value. The
`x-error-catalog` array and the `ErrorCode` enum are the machine-readable
allocation for this snapshot; any future registry or generator must preserve
every existing row.

## Parent/child refusal boundary

The post-stop parent/child contract reuses this closed error vocabulary. A
parented create or reparent that is not yet admitted by the implementation
continues to return the current `invalid_params`/`forbidden` refusal. Once a
future slice is separately admitted, a missing or hidden parent remains
non-disclosing: return `not_found` after disclosure checks or the existing
authorization denial, without a count or error that distinguishes hidden
content. A self/descendant cycle or other child invariant failure is
`ontology_guard_failed`; an over-limit closure rewrite is
`closure_too_large`; an over-limit authorized progress input is
`scope_too_large`. Refused attempts publish no partial hierarchy, closure,
event or receipt state. This text records the conditional contract only; it
does not claim runtime support or qualification.

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

## Driver-to-wire mappings

Driver sentinels never cross the JSON-RPC boundary as Go errors. The service
maps them to these closed wire errors and preserves the typed detail shown
below:

| Driver error | Wire error | Typed `details` | Retry/handling rule |
| --- | --- | --- | --- |
| `ErrFenced` | `lease_not_held` (`-32021`) | `{expectedEpoch, observedEpoch}` | Retry only after acquiring a new active lease; the stale writer is never replayed. |
| `ErrTruncated{Floor}` | `cursor_expired` (`-32019`) | `{floor}` | Reopen from a current snapshot or the returned gap cursor. |
| `ErrIncarnationChanged{From, To}` | `incarnation_changed` (`-32024`) | `{fromIncarnation, toIncarnation}` | Discard the old cursor/view and reopen under the new incarnation. |
| `ErrIndeterminate{IdempotencyKey}` | `stale_snapshot` (`-32055`, internal) | `{idempotencyKey, outcome: "unknown"}` | Resolve the commit by idempotency key before deciding whether a retry is safe; never duplicate the write. |

The named OpenRPC error components for these wire codes reference
`FencedDetails`, `TruncatedDetails`, `IncarnationChangedDetails`, and
`IndeterminateDetails`, respectively. `stale_snapshot` remains an internal
catalog entry even though its mapping is required for driver conformance.

## Bounded registry validation

`openrpc.json` contains a separately stored, human-maintained
`x-method-registry` artifact with the complete method inventory, every
parameter/result schema declaration, error attachments and descriptor fields.
Its name and descriptor digests bind the stored artifact. The validator below
reads that artifact independently; it never constructs its expected registry
from `methods`. Deleting a method, dropping a descriptor or changing an error
attachment on either side must fail. The separate wire inventory also checks
the operation/event/error names against the schemas, Go and Markdown. The
validator rejects duplicate JSON members and checks every named error's code
and number plus every Markdown row's retryability, audience and warning flag.
It also requires decision #14's `session.open` and `session.close`, even when
both inventories and their digests are changed together.

Run from this directory. The optional second argument checks the recorded
source hashes against a checkout of those historical inputs; the live design
checkout has since changed. Decision #19 and controller v2 select exact-head
local Bazel checks plus independent review as the current merge gate. The
historical authorization latency failure remains a failure; this contract
check does not establish runtime or performance acceptance.

```sh
python3 - openrpc.json <<'PY'
import hashlib, json, pathlib, re, sys

def unique_members(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise SystemExit("duplicate JSON member: " + key)
        result[key] = value
    return result

path = pathlib.Path(sys.argv[1] if len(sys.argv) > 1 else "openrpc.json")
doc = json.loads(path.read_text(), object_pairs_hook=unique_members)
registry = doc["x-method-registry"]
schemas = doc["components"]["schemas"]

def require(condition, message):
    if not condition:
        raise SystemExit(message)

def digest(value):
    raw = json.dumps(value, separators=(",", ":"), ensure_ascii=False).encode()
    return hashlib.sha256(raw).hexdigest()

def pointer(ref):
    require(ref.startswith("#/"), "external reference: " + ref)
    value = doc
    for part in ref[2:].split("/"):
        key = part.replace("~1", "/").replace("~0", "~")
        value = value[int(key)] if isinstance(value, list) else value[key]
    return value

def check_refs(value):
    if isinstance(value, dict):
        if "$ref" in value:
            pointer(value["$ref"])
        for child in value.values():
            check_refs(child)
    elif isinstance(value, list):
        for child in value:
            check_refs(child)

entries = registry["methods"]
names = [entry["name"] for entry in entries]
methods = {method["name"]: method for method in doc["methods"]}
require({"session.open", "session.close"} <= set(methods), "decision #14 requires session.open and session.close")
require(len(names) == len(set(names)) == registry["methodCount"], "registry names/count")
require(len(doc["methods"]) == len(methods) == len(entries), "method names/count")
require(set(names) == set(methods), "missing or unexpected method")
require(hashlib.sha256(("\n".join(names) + "\n").encode()).hexdigest() == registry["nameSha256"], "inventory hash")
require(digest(entries) == registry["descriptorSha256"], "descriptor hash")
for entry in entries:
    method = methods[entry["name"]]
    require(set(entry) == set(registry["requiredDescriptorFields"]), "descriptor fields")
    params = [{"name": p["name"], "required": p.get("required", False), "schema": p["schema"]} for p in method["params"]]
    expected = {"name": method["name"], "params": params, "resultSchema": method["result"]["schema"], "errors": [e["$ref"] for e in method.get("errors", [])]}
    for key, extension in [("effect", "x-effect"), ("permission", "x-permission"), ("idempotency", "x-idempotency"), ("stability", "x-stability"), ("agentCore", "x-agent-core")]:
        expected[key] = method[extension]
    require(entry == expected, "descriptor mismatch: " + entry["name"])
    for key in ("effect", "permission", "idempotency"):
        require(entry[key] in doc["x-method-descriptor"][key], "unknown " + key)
    require(entry["effect"] in registry["validation"]["closedEffects"], "registry effect")
    require(entry["stability"] and type(entry["agentCore"]) is bool, "stability/agentCore")
    require(len(params) == len({p["name"] for p in params}), "duplicate parameter")
    require(all(type(p["required"]) is bool and isinstance(p["schema"], dict) for p in params), "parameter schema")
    require(isinstance(entry["resultSchema"], dict), "result schema")
    require(len(entry["errors"]) == len(set(entry["errors"])), "duplicate error")
    require(entry["errors"] or entry["effect"] == "event", "missing errors")
    require(all(e.startswith("#/components/errors/") for e in entry["errors"]), "error declaration")
    if entry["effect"] in ("write", "restore"):
        require(all("#/components/errors/" + e in entry["errors"] for e in ("LeaseNotHeld", "LeaseExpired")), "lease mapping")
check_refs(doc)
wire = registry["wireInventory"]
require(wire["durableEventTypes"] == schemas["DurableEvent"]["properties"]["type"]["enum"], "durable inventory")
require(wire["controlEventTypes"] == [v["properties"]["type"]["const"] for v in schemas["ControlEvent"]["oneOf"]], "control inventory")
for key, schema in [("errorCodes", "ErrorCode"), ("warningCodes", "WarningCode")]:
    require(wire[key] == schemas[schema]["enum"], schema + " inventory")
require(wire["errorCodes"] == [r["code"] for r in doc["x-error-catalog"]], "error allocation")
require(wire["warningCodes"] == [r["code"] for r in doc["x-error-catalog"] if r["warningCapable"]], "warning allocation")
catalog = {row["code"]: row for row in doc["x-error-catalog"]}
require(len(catalog) == len(doc["x-error-catalog"]), "duplicate catalog error")
for code, row in catalog.items():
    require(type(row["jsonRpc"]) is int, "catalog error number: " + code)
    require(type(row["retryable"]) is bool and type(row["warningCapable"]) is bool, "catalog error flags: " + code)
    require(row["audience"] in ("client", "operator", "plugin-host", "internal"), "catalog error audience: " + code)
require(len({row["jsonRpc"] for row in catalog.values()}) == len(catalog), "duplicate error number")
components = doc["components"]["errors"]
require(len(components) == len(catalog), "error component count")
component_codes = []
for name, component in components.items():
    data = component["data"]
    code = data["properties"]["code"]["const"]
    require(code in catalog, "unknown component error: " + name)
    require(name.lower() == code.replace("_", ""), "error component name: " + name)
    require(type(component["code"]) is int and component["code"] == catalog[code]["jsonRpc"], "error number: " + name)
    require({"code", "retryable"} <= set(data["required"]), "error data members: " + name)
    component_codes.append(code)
require(len(set(component_codes)) == len(catalog), "duplicate component error")
ops = schemas["ChangeSet"]["properties"]["ops"]["items"]["oneOf"]
kinds = []
for variant in ops:
    kind = pointer(variant["$ref"])["properties"]["kind"]
    kinds.extend(kind.get("enum", [kind.get("const")]))
require(set(kinds) == set(wire["operationKinds"]), "operation inventory")
go = (path.parent / "go-sketches.md").read_text()
for typ, key in [("OpKind", "operationKinds"), ("DurableEventType", "durableEventTypes"), ("ControlEventType", "controlEventTypes")]:
    require(set(re.findall(r'\b\w+\s+' + typ + r'\s*=\s*"([^"]+)"', go)) == set(wire[key]), "Go " + typ)
for function, key in [("validErrorCode", "errorCodes"), ("validWarningCode", "warningCodes")]:
    body = go.split("func " + function + "(", 1)[1].split("\n}", 1)[0]
    require(set(re.findall(r'"([^"]+)"', body)) == set(wire[key]), "Go " + function)
error_rows = []
for line in (path.parent / "errors.md").read_text().splitlines():
    if re.match(r'^\| `[^`]+` \| `-\d+`', line):
        code, number, retryable, audience, warning = [part.strip().strip("`") for part in line.split("|")[1:-1]]
        require(retryable in ("yes", "no") and warning in ("yes", "no"), "Markdown error flags")
        error_rows.append({"code": code, "jsonRpc": int(number), "retryable": retryable == "yes", "audience": audience, "warningCapable": warning == "yes"})
require(error_rows == doc["x-error-catalog"], "Markdown error metadata")
event_rows = re.findall(r'^\| `([^`]+)` \|', (path.parent / "events.md").read_text(), re.M)
require(set(event_rows) == set(wire["durableEventTypes"] + wire["controlEventTypes"]), "Markdown events")
if len(sys.argv) > 2:
    root = pathlib.Path(sys.argv[2])
    for item in doc["x-source-evidence"]["inputs"]:
        require(hashlib.sha256((root / item["path"]).read_bytes()).hexdigest() == item["sha256"], "source hash: " + item["path"])
print(f"PASS: {len(entries)} methods; descriptor, wire, Go, Markdown and local reference parity")
PY
```
