# Plug Go contract sketches

These are public-shape sketches, not implementation code. `pluginapi` and
`sdk/go` are the only public Go surfaces (P47); the driver and core model
packages are internal. Names and fields are deliberately wire-oriented so a
native plugin and a source-built WASM plugin receive the same JSON-RPC
messages.

**Completion Contract.** Done = native and WASM clients share the published
operation, event, reference and principal wire domains, proven by schema-to-Go
and Go-to-schema checks, self-roundtrips and registry validation; compilation
or one accepted corpus alone does not count. This is a bounded contract
artifact, with implementation and deferred contract gaps gated separately.

String requirements check member presence and type; they do not add a minimum
length absent from OpenRPC. References retain which member was present even
when its string is empty. `Natural` preserves the schemas' unbounded
nonnegative integers, including integral decimal and exponent spellings;
engine-sized conversions must return an overflow error. `EventCause.depth`
still has the schema's maximum of 8. The public principal codec validates the
same shape and closed kinds as the model; transport authentication still
stamps the effective principal.

## Session display names

The adopted E3 decision (2026-10-30 08:30Z) adds optional `displayName` to
`session.open` input and each `Presence` member. The `Session` result, principal,
`onBehalfOf`, `_meta.agent` and all existing session ownership and expiry rules
stay as defined. There is no session-refresh method in this catalog;
`presence.set` renews idle expiry without changing the display name. No new
method is introduced.

**Completion Contract.** Done = the canonical OpenRPC and embedded catalog
describe the same optional display field, normalization and trust rules,
proven by schema/catalog conformance and the complete owning-library reverse
and full lexicon gates; these checks do not count as runtime or UI acceptance.
Scope = this contract and its wire schema validation; collab and UI consumers
follow only after the contract lands.

The field additions have these Go wire shapes. `Meta` and `PrincipalRef`
retain their existing closed OpenRPC schemas; `json.RawMessage` below is a
placeholder for the existing `Meta` codec, not permission for extra members.
These are shape sketches, with no new normalization or storage implementation.

```go
type SessionOpenParams struct {
	OnBehalfOf  *string         `json:"onBehalfOf,omitempty"`
	DisplayName *string         `json:"displayName,omitempty"`
	Meta        json.RawMessage `json:"_meta,omitempty"`
}

type Presence struct {
	SessionID   string       `json:"sessionId"`
	Principal   PrincipalRef `json:"principal"`
	Focus       *string      `json:"focus,omitempty"`
	WorkingOn   []string     `json:"workingOn,omitempty"`
	OnBehalfOf  *string      `json:"onBehalfOf,omitempty"`
	DisplayName *string      `json:"displayName,omitempty"`
}
```

Normative normalization: trim leading and trailing Unicode `White_Space`.
Reject Unicode control (`Cc`) **and** format (`Cf`) characters with typed
`invalid_params`. Permit at most **64 Unicode scalar values after trimming**,
not 64 bytes or grapheme clusters. Empty after trimming means absent, including
when returning a Presence member; it is never a JSON `null` or an empty result
string. Omitting the input preserves existing behavior.

`DisplayNameInput` accepts a raw string, including empty or padded input; it
deliberately has no `maxLength` before normalization. `DisplayName` describes
the normalized output with `minLength: 1` and `maxLength: 64`. JSON Schema does
not transform the input. Trimming, empty-to-absent, category rejection and the
input bound after trimming remain normative consumer requirements in
`x-display-name-contract`, not behavior established by schema validation.

`displayName` is caller-asserted, **untrusted display-only data**. It never
participates in authorization, attribution, audit actor, idempotency, leases or
dedup. Clients and the web UI always render it next to the authenticated
principal: the user name, API key name or "anonymous". It never replaces or
impersonates that principal. Two sessions of the same principal remain two
members identified by `sessionId`, each with its own `displayName`. Compact
`onBehalfOf` presentation must preserve those member identities; it may not
merge their labels or synthesize a trusted group identity. Existing sponsorship,
resource masking, session expiry/close and no graph/log-write rules still apply.

After the contract lands, the collab consumer must prove normalization and
typed refusals through real bound RPCs, including both `Cc` and `Cf`, Unicode
whitespace, empty/omitted input, 64/65 scalar boundaries and padded 64-scalar
input. It must also prove unchanged masking, expiry/close, ownership and trusted
identity behavior, with two sessions of the same principal kept separate. The
UI must prove display beside the authenticated principal. Only then can E3
qualify with 50 simulated agents given distinct `displayName` values. This
contract PR supplies none of those runtime or browser proofs.

## Restore import comments and allocator profile

The restore-import contract adds a typed result shape without changing the
ordinary `CommentOp`, create, patch, metadata or transport metadata inputs.
`bead.get` may include an optional `comments` array on its existing
`BeadGetResult` result. It uses the existing authorization and as-of rules, introduces no RPC,
and does not claim that the current service or browser already reads comments.

```go
type Comment struct {
	Body         string         `json:"body"`
	Principal    PrincipalRef   `json:"principal"` // verified actor
	Time         time.Time      `json:"time"`      // restore commit time
	Origin       *CommentOrigin `json:"origin,omitempty"`
	SourceAuthor *string        `json:"sourceAuthor,omitempty"`
	SourceTime   *time.Time     `json:"sourceTime,omitempty"`
}

type CommentOrigin string

const CommentOriginImport CommentOrigin = "import"

// LegacyComment is the unchanged body/principal/time shape. ImportedComment
// is the closed shape used when origin is import and sourceTime is present.
type ImportedComment struct {
	Body         string        `json:"body"`
	Principal    PrincipalRef  `json:"principal"` // trusted restoring actor
	Time         time.Time     `json:"time"`      // restore commit time
	Origin       CommentOrigin `json:"origin"`    // always import
	SourceAuthor *string       `json:"sourceAuthor,omitempty"` // untrusted
	SourceTime   time.Time     `json:"sourceTime"`              // source provenance
}
```

An imported comment's `Principal` is the actor admitted by the trusted restore
host. `sourceAuthor` is provenance only: trim Unicode `White_Space`, reject
Unicode `Cc` and `Cf`, count at most 256 Unicode scalar values after trimming,
and omit it when the normalized value is empty. Render it beside the verified
principal, like `displayName`; never use it for authorization, audit identity,
or attribution of a new action. `sourceTime` retains the original source
timestamp, while `time` is the restore commit time. Old comments decode without
an origin or source fields and remain unchanged. Missing or mismatched trusted
admission refuses before writes; no source author or typed principal is
fabricated.

The allocator declaration and target state are deliberately separate:

`hash_length` is retained exactly as emitted by the source archive: hash mode
accepts 1..64 and sequential mode accepts 0..64. The field is explicit even
for sequential declarations; these source bounds are independent of the live
target driver's hash-mode minimum.

```go
type SourceAllocatorProvenance struct {
	Prefix        string            `json:"prefix"`
	IDMode        string            `json:"id_mode"` // sequential or hash
	HashLength    uint32            `json:"hash_length"` // 1..64 for hash, 0..64 for sequential
	HashNamespaces []string          `json:"hash_namespaces,omitempty"`
	LastID        uint64            `json:"last_id"`
	ChildCounters map[string]uint64 `json:"child_counters,omitempty"`
}

type TargetHashAllocator struct {
	Prefix           string            `json:"prefix"`
	IDMode           string            `json:"id_mode"` // hash
	HashLength       uint32            `json:"hash_length"`
	MinHashLength    uint32            `json:"min_hash_length"`
	MaxHashLength    uint32            `json:"max_hash_length"`
	MaxCollisionProb float64           `json:"max_collision_prob"`
	LastID           uint64            `json:"last_id"`
	ChildCounters    map[string]uint64 `json:"child_counters,omitempty"`
}

type RestoreAllocatorProfile struct {
	SourceAllocator SourceAllocatorProvenance `json:"sourceAllocator"`
	TargetAllocator TargetHashAllocator     `json:"targetAllocator"`
}
```

The target emits Beads adaptive base36 hashes and `${parent}.N` children.
The source config selects min/max length and collision probability; absent
settings use 3, 8 and 0.25. Each length gets ten nonce attempts before advancing.
Restore seeds counters from both stored and observed direct-child maxima.
Candidates are checked against every existing, restored and staged ID in the
store, including other kinds and projects. Consumption, counter changes,
records and receipts commit together under the complete lock set. Rollback
consumes nothing. The original source allocator declaration and IDs remain
unchanged in source provenance; they do not replace the adaptive config.

The prepared import profile is host-private and transaction-local. It binds the
trusted admission to the restore operation and recovery:

```go
type RestoreImportAdmission struct {
	Mode            string       `json:"mode"` // authenticated or anonymous-local
	Actor           PrincipalRef `json:"actor"`
	Scope           string       `json:"scope"`
	Authority       string       `json:"authority"` // restore-admin or trusted-human-presence
	AdmissionDigest string       `json:"admissionDigest"`
}

type RestoreImportProfile struct {
	Admission      RestoreImportAdmission `json:"admission"`
	SourceStoreID  string                 `json:"sourceStoreId"`
	Method         string                 `json:"method"` // offline.restore.import
	IdempotencyKey string                 `json:"idempotencyKey"`
	ArchiveDigest  string                 `json:"archiveDigest"`
	DerivedDigest  string                 `json:"derivedDigest"`
	HistoryMode    string                 `json:"historyMode,omitempty"` // full (default), partial, snapshot
}
```

`historyMode` is part of the admitted profile and its request hash. Omitted
input is strict `full`; `partial` and `snapshot` require an explicit operator
choice. A full import refuses unavailable, unverified, non-contiguous or
unmappable source history with a typed reason. Partial history records a
verified range, exact gaps and a floor only when replay is available; until
then it returns typed `not-yet-supported`. Snapshot imports the current image,
sets the floor to revision 1 and starts a new incarnation.

Authenticated mode requires a live interactive user, restore-admin authority,
and operation/archive-bound D32; its normalized G4 agent scope is empty.
Anonymous-local mode records trusted human
presence and an honest anonymous actor/agent scope (the normalized G4
`scope`, including empty); it does not promote an
operator label into a verified user. The host creates the profile after that
admission; ordinary JSON-RPC input, `PrincipalRef`, lease, archive field or
digest cannot mint it. Actor, scope, method, idempotency key, source store,
archive digest and derived digest must match in preparation, header, imported
comments, receipt and read-only recovery. Recovery must compare the complete
durable profile and the live target allocator plus immutable source provenance;
coarse principal-key lookup is insufficient.

## Shared model and `ChangeSet`

```go
// package model (internal/core/model)
package model

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/hex"
	"unicode/utf8"
	"fmt"
	"io"
	"math/big"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// Natural is the lossless nonnegative JSON-integer wire domain. A zero Go
// value represents zero. It deliberately has no uint32/int64/uint64 ceiling:
// the accepted OpenRPC schemas specify no such maximum. Drivers must check
// conversion to engine-sized integers and may never silently narrow a value.
// Direct casts are checked again by Validate and MarshalJSON.
type Natural string

func Natural64(value uint64) Natural { return Natural(strconv.FormatUint(value, 10)) }

func (value Natural) Canonical() (string, error) {
	text := string(value)
	if text == "" {
		text = "0"
	}
	canonical, err := canonicalInteger([]byte(text))
	if err != nil {
		return "", err
	}
	if strings.HasPrefix(canonical, "-") {
		return "", fmt.Errorf("expected nonnegative integer")
	}
	return canonical, nil
}

func (value Natural) Validate() error {
	_, err := value.Canonical()
	return err
}

func (value Natural) IsZero() bool {
	canonical, err := value.Canonical()
	return err == nil && canonical == "0"
}

func (value Natural) String() string {
	canonical, err := value.Canonical()
	if err != nil {
		return "<invalid integer>"
	}
	return canonical
}

func (value Natural) Uint64() (uint64, error) {
	canonical, err := value.Canonical()
	if err != nil {
		return 0, err
	}
	// Canonical values of at most 20 digits are expanded. An exponent here
	// therefore cannot fit uint64; ParseUint rejects it without allocation.
	return strconv.ParseUint(canonical, 10, 64)
}

func (value Natural) MarshalJSON() ([]byte, error) {
	canonical, err := value.Canonical()
	return []byte(canonical), err
}

func (value *Natural) UnmarshalJSON(data []byte) error {
	canonical, err := canonicalInteger(data)
	if err != nil {
		return err
	}
	decoded := Natural(canonical)
	if err := decoded.Validate(); err != nil {
		return err
	}
	*value = decoded
	return nil
}

// canonicalInteger recognizes mathematical JSON integers, including 1.0,
// 1e0, and large integral exponents, without float64 rounding. It normalizes
// equivalent spellings. Exponents stay symbolic so a short input such as
// 1e1000000000 does not allocate a billion-digit integer.
func canonicalInteger(data []byte) (string, error) {
	text := strings.TrimSpace(string(data))
	if text == "" || !json.Valid([]byte(text)) || (text[0] != '-' && (text[0] < '0' || text[0] > '9')) {
		return "", fmt.Errorf("expected JSON integer")
	}
	negative := strings.HasPrefix(text, "-")
	if negative {
		text = text[1:]
	}
	exponent := new(big.Int)
	if index := strings.IndexAny(text, "eE"); index >= 0 {
		if _, ok := exponent.SetString(text[index+1:], 10); !ok {
			return "", fmt.Errorf("invalid integer exponent")
		}
		text = text[:index]
	}
	fractionDigits := 0
	if index := strings.IndexByte(text, '.'); index >= 0 {
		fractionDigits = len(text) - index - 1
	}
	digits := strings.TrimLeft(strings.ReplaceAll(text, ".", ""), "0")
	if digits == "" {
		return "0", nil
	}
	coefficient := strings.TrimRight(digits, "0")
	trailingZeros := len(digits) - len(coefficient)
	exponent.Sub(exponent, big.NewInt(int64(fractionDigits-trailingZeros)))
	if exponent.Sign() < 0 {
		return "", fmt.Errorf("JSON number is not integral")
	}
	canonical := coefficient
	if exponent.Sign() > 0 {
		if exponent.IsInt64() && exponent.Int64() <= int64(20-len(coefficient)) {
			canonical += strings.Repeat("0", int(exponent.Int64()))
		} else {
			canonical += "e" + exponent.String()
		}
	}
	if negative {
		canonical = "-" + canonical
	}
	return canonical, nil
}

type Revision = Natural
type Branch = Natural
type ErrorCode string   // closed by components.schemas.ErrorCode in OpenRPC
type WarningCode string // closed by components.schemas.WarningCode in OpenRPC

// decodeStrict is the one JSON boundary used by every custom wire codec in
// this sketch. OpenRPC closes these objects, so ordinary json.Unmarshal is not
// sufficient: it silently drops unknown members and accepts a missing object
// as its zero value. The required list is checked separately because Go's
// zero-value fields cannot distinguish an omitted member from an explicit
// zero. Nested values invoke their own strict codecs through json.Decoder.
func decodeStrict(data []byte, dst any, required ...string) error {
	var fields map[string]json.RawMessage
	if err := decodeJSON(data, &fields); err != nil {
		return err
	}
	if fields == nil {
		return fmt.Errorf("expected JSON object")
	}
	for _, name := range required {
		if _, ok := fields[name]; !ok {
			return fmt.Errorf("missing required JSON member %q", name)
		}
	}
	if err := checkWireFields(fields, reflect.TypeOf(dst).Elem()); err != nil {
		return err
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	dec.UseNumber()
	if err := dec.Decode(dst); err != nil {
		return err
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		if err == nil {
			return fmt.Errorf("trailing JSON value")
		}
		return err
	}
	return nil
}

// encoding/json matches field names without regard to case and treats null
// as a no-op for scalar fields. Neither behavior matches JSON Schema. Check
// the exact member names and typed collection elements before decoding. Only
// sponsor is nullable; RawMessage and any deliberately retain arbitrary JSON.
func checkWireFields(fields map[string]json.RawMessage, typ reflect.Type) error {
	allowed := map[string]reflect.StructField{}
	var collect func(reflect.Type)
	collect = func(t reflect.Type) {
		for i := 0; i < t.NumField(); i++ {
			field := t.Field(i)
			if field.Anonymous {
				collect(field.Type)
				continue
			}
			name := strings.Split(field.Tag.Get("json"), ",")[0]
			if name != "-" && name != "" {
				allowed[name] = field
			}
		}
	}
	collect(typ)
	for name, raw := range fields {
		field, ok := allowed[name]
		if !ok {
			return fmt.Errorf("unknown JSON member %q", name)
		}
		if err := checkWireValue(raw, field.Type, field.Tag.Get("wire") == "nullable"); err != nil {
			return fmt.Errorf("JSON member %q: %w", name, err)
		}
	}
	return nil
}

func checkWireValue(raw json.RawMessage, typ reflect.Type, nullable bool) error {
	if typ == reflect.TypeFor[json.RawMessage]() || typ.Kind() == reflect.Interface {
		return nil
	}
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		if nullable {
			return nil
		}
		return fmt.Errorf("null is not allowed")
	}
	if typ.Kind() == reflect.Pointer {
		return checkWireValue(raw, typ.Elem(), false)
	}
	if reflect.PointerTo(typ).Implements(reflect.TypeFor[json.Unmarshaler]()) {
		// The nested custom codec checks its own tagged schema.
		return nil
	}
	switch typ.Kind() {
	case reflect.Slice:
		var values []json.RawMessage
		if err := decodeJSON(raw, &values); err != nil {
			return err
		}
		for _, value := range values {
			if err := checkWireValue(value, typ.Elem(), false); err != nil {
				return err
			}
		}
	case reflect.Map:
		var values map[string]json.RawMessage
		if err := decodeJSON(raw, &values); err != nil {
			return err
		}
		for _, value := range values {
			if err := checkWireValue(value, typ.Elem(), false); err != nil {
				return err
			}
		}
	case reflect.Struct:
		var fields map[string]json.RawMessage
		if err := decodeJSON(raw, &fields); err != nil {
			return err
		}
		return checkWireFields(fields, typ)
	}
	return nil
}

func decodeJSON(data []byte, dst any) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	if err := dec.Decode(dst); err != nil {
		return err
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		if err == nil {
			return fmt.Errorf("trailing JSON value")
		}
		return err
	}
	return nil
}

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
		"invalid_cursor", "compaction_unavailable", "stale_snapshot":
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

type Ref struct {
	// Presence selects the oneOf arm, including an explicit empty id or key.
	// Use the constructors to select an empty arm in native Go code.
	ID       string       `json:"id,omitempty"`
	Branch   Natural      `json:"branch,omitempty"`
	Key      string       `json:"key,omitempty"`
	selected refSelection `json:"-"`
}

type refSelection uint8

const (
	refUnselected refSelection = iota
	refID
	refKey
)

func RefByID(id string, branch Natural) Ref { return Ref{ID: id, Branch: branch, selected: refID} }
func RefByKey(key string) Ref               { return Ref{Key: key, selected: refKey} }

func (r Ref) selection() refSelection {
	if r.selected != refUnselected {
		return r.selected
	}
	if r.ID != "" || !r.Branch.IsZero() {
		return refID
	}
	if r.Key != "" {
		return refKey
	}
	return refUnselected
}

// IsID preserves the selected form even when the selected string is empty.
// Callers validate the reference before using it as an identity or lock key.
func (r Ref) IsID() bool { return r.selection() == refID }

func (r Ref) Validate() error {
	if err := r.Branch.Validate(); err != nil {
		return fmt.Errorf("ref branch: %w", err)
	}
	switch r.selection() {
	case refID:
		if r.Key != "" {
			return fmt.Errorf("ref cannot contain both id and key")
		}
	case refKey:
		if r.ID != "" || !r.Branch.IsZero() {
			return fmt.Errorf("ref key cannot carry an id or branch")
		}
	default:
		return fmt.Errorf("ref must contain exactly one of id or key")
	}
	return nil
}

func (r Ref) MarshalJSON() ([]byte, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}
	if r.IsID() {
		return json.Marshal(struct {
			ID     string  `json:"id"`
			Branch Natural `json:"branch,omitempty"`
		}{r.ID, r.Branch})
	}
	return json.Marshal(struct {
		Key string `json:"key"`
	}{r.Key})
}

func (r *Ref) UnmarshalJSON(data []byte) error {
	type wireRef Ref
	var value wireRef
	if err := decodeStrict(data, &value); err != nil {
		return err
	}
	var fields map[string]json.RawMessage
	if err := decodeJSON(data, &fields); err != nil {
		return err
	}
	_, hasID := fields["id"]
	_, hasKey := fields["key"]
	if hasID == hasKey {
		return fmt.Errorf("ref must contain exactly one of id or key")
	}
	if hasKey {
		if _, hasBranch := fields["branch"]; hasBranch {
			return fmt.Errorf("ref key cannot carry a branch")
		}
	}
	decoded := Ref(value)
	if hasID {
		decoded.selected = refID
	} else {
		decoded.selected = refKey
	}
	if err := decoded.Validate(); err != nil {
		return err
	}
	*r = decoded
	return nil
}

type ResourceRef struct {
	Kind string `json:"kind"`
	Ref  Ref    `json:"ref"`
}
type FieldSet []string

type Precondition struct {
	Ref      Ref       `json:"ref"`
	Revision *Revision `json:"revision,omitempty"`
	Field    string    `json:"field,omitempty"`
	Hash     string    `json:"hash,omitempty"`
}

type ChangeSet struct {
	Ops    []Op           `json:"ops"` // one atomic revision; up to 1,000 ops
	Expect []Precondition `json:"expect,omitempty"`
}

type Op struct {
	Kind     OpKind      `json:"-"`
	Create   *CreateOp   `json:"-"`
	Patch    *PatchOp    `json:"-"`
	Close    *CloseOp    `json:"-"`
	Reparent *ReparentOp `json:"-"`
	Link     *LinkOp     `json:"-"` // carries both link and unlink
	Comment  *CommentOp  `json:"-"`
	Attach   *AttachOp   `json:"-"`
	Delete   *DeleteOp   `json:"-"`
	SetMeta  *SetMetaOp  `json:"-"`
}

type OpKind string

const (
	OpCreate   OpKind = "create"
	OpPatch    OpKind = "patch"
	OpClose    OpKind = "close"
	OpReparent OpKind = "reparent"
	OpLink     OpKind = "link"
	OpUnlink   OpKind = "unlink"
	OpComment  OpKind = "comment"
	OpAttach   OpKind = "attach"
	OpDelete   OpKind = "delete"
	OpSetMeta  OpKind = "setMeta"
)

type CreateOp struct {
	Kind    OpKind         `json:"kind"`
	Key     string         `json:"key,omitempty"`
	Type    string         `json:"type"`
	Fields  map[string]any `json:"fields"`
	Parent  *Ref           `json:"parent,omitempty"`
	Project string         `json:"project,omitempty"`
}
type PatchOp struct {
	Kind   OpKind         `json:"kind"`
	Target Ref            `json:"target"`
	Fields map[string]any `json:"fields"`
}
// CloseOp is the per-operation close slot in tx.apply. Expect observes the
// ordered in-request image; ChangeSet.Expect keeps its snapshot semantics.
type CloseOp struct {
	Kind OpKind `json:"kind"`
	Target Ref `json:"target"`
	Reason string `json:"reason,omitempty"`
	Expect []Precondition `json:"expect,omitempty"`
}
type ReparentOp struct {
	Kind    OpKind `json:"kind"`
	Target  Ref    `json:"target"`
	Parent  *Ref   `json:"parent,omitempty"`
	Project string `json:"project,omitempty"`
}
// LinkTargetKind is local (the omitted default) or external. External IDs are
// exact, nonempty branch-zero ID references and never resolve to local beads.
type LinkTargetKind string

type LinkOp struct {
	TargetKind LinkTargetKind `json:"targetKind,omitempty"`
	Kind     OpKind `json:"kind"`
	From     Ref    `json:"from"`
	To       Ref    `json:"to"`
	LinkKind string `json:"linkKind"`
}
type CommentOp struct {
	Kind   OpKind `json:"kind"`
	Target Ref    `json:"target"`
	Body   string `json:"body"`
}
type AttachOp struct {
	Kind       OpKind     `json:"kind"`
	Target     Ref        `json:"target"`
	Attachment Attachment `json:"attachment"`
}
type DeleteOp struct {
	Kind   OpKind `json:"kind"`
	Target Ref    `json:"target"`
}
type SetMetaOp struct {
	Kind   OpKind          `json:"kind"`
	Target Ref             `json:"target"`
	Key    string          `json:"key"`
	Value  json.RawMessage `json:"value"`
}
type Attachment struct {
	Kind       string `json:"kind"`
	URI        string `json:"uri,omitempty"`
	ContentRef string `json:"contentRef,omitempty"`
	MediaType  string `json:"mediaType"`
	Summary    string `json:"summary"`
}

func (a Attachment) MarshalJSON() ([]byte, error) {
	switch a.Kind {
	case "link", "commit", "blob", "log":
	default:
		return nil, fmt.Errorf("unknown attachment kind %q", a.Kind)
	}
	type wireAttachment Attachment
	return json.Marshal(wireAttachment(a))
}

func (a *Attachment) UnmarshalJSON(data []byte) error {
	type wireAttachment Attachment
	var value wireAttachment
	if err := decodeStrict(data, &value, "kind", "mediaType", "summary"); err != nil {
		return err
	}
	switch value.Kind {
	case "link", "commit", "blob", "log":
	default:
		return fmt.Errorf("unknown attachment kind %q", value.Kind)
	}
	*a = Attachment(value)
	return nil
}

func requireNonNilMap(name string, value map[string]any) error {
	if value == nil {
		return fmt.Errorf("%s must be a JSON object", name)
	}
	return nil
}

func validateCreate(value CreateOp) error {
	if value.Kind != OpCreate {
		return fmt.Errorf("create operation has kind %q", value.Kind)
	}
	return requireNonNilMap("fields", value.Fields)
}

func validatePatch(value PatchOp) error {
	if value.Kind != OpPatch {
		return fmt.Errorf("patch operation has kind %q", value.Kind)
	}
	return requireNonNilMap("fields", value.Fields)
}

func validateClose(value CloseOp) error {
	if value.Kind != OpClose {
		return fmt.Errorf("close operation has kind %q", value.Kind)
	}
	return nil
}

func validateReparent(value ReparentOp) error {
	if value.Kind != OpReparent {
		return fmt.Errorf("reparent operation has kind %q", value.Kind)
	}
	return nil
}

func validateLink(value LinkOp) error {
	if value.Kind != OpLink && value.Kind != OpUnlink {
		return fmt.Errorf("link operation has kind %q", value.Kind)
	}
	return nil
}

func validateComment(value CommentOp) error {
	if value.Kind != OpComment {
		return fmt.Errorf("comment operation has kind %q", value.Kind)
	}
	return nil
}

func validateAttach(value AttachOp) error {
	if value.Kind != OpAttach {
		return fmt.Errorf("attach operation has kind %q", value.Kind)
	}
	return nil
}

func validateDelete(value DeleteOp) error {
	if value.Kind != OpDelete {
		return fmt.Errorf("delete operation has kind %q", value.Kind)
	}
	return nil
}

func validateSetMeta(value SetMetaOp) error {
	if value.Kind != OpSetMeta {
		return fmt.Errorf("setMeta operation has kind %q", value.Kind)
	}
	if len(value.Value) == 0 {
		return fmt.Errorf("setMeta operation requires a JSON value")
	}
	var raw any
	return decodeJSON(value.Value, &raw)
}

// MarshalJSON flattens the selected member. The result is exactly one of the
// OpenRPC ChangeSet.ops oneOf schemas; it is never a {kind, create: {...}}
// wrapper. UnmarshalJSON performs the inverse discriminator selection.
func (o Op) MarshalJSON() ([]byte, error) {
	selected := 0
	for _, present := range []bool{o.Create != nil, o.Patch != nil, o.Close != nil, o.Reparent != nil, o.Link != nil, o.Comment != nil, o.Attach != nil, o.Delete != nil, o.SetMeta != nil} {
		if present {
			selected++
		}
	}
	if selected != 1 {
		return nil, fmt.Errorf("operation must select exactly one payload")
	}
	switch o.Kind {
	case OpCreate:
		if o.Create == nil {
			return nil, fmt.Errorf("operation %q has no payload", o.Kind)
		}
		value := *o.Create
		value.Kind = OpCreate
		if err := validateCreate(value); err != nil {
			return nil, err
		}
		return json.Marshal(value)
	case OpPatch:
		if o.Patch == nil {
			return nil, fmt.Errorf("operation %q has no payload", o.Kind)
		}
		value := *o.Patch
		value.Kind = OpPatch
		if err := validatePatch(value); err != nil {
			return nil, err
		}
		return json.Marshal(value)
	case OpClose:
		if o.Close == nil {
			return nil, fmt.Errorf("operation %q has no payload", o.Kind)
		}
		value := *o.Close
		value.Kind = OpClose
		if err := validateClose(value); err != nil {
			return nil, err
		}
		return json.Marshal(value)
	case OpReparent:
		if o.Reparent == nil {
			return nil, fmt.Errorf("operation %q has no payload", o.Kind)
		}
		value := *o.Reparent
		value.Kind = OpReparent
		if err := validateReparent(value); err != nil {
			return nil, err
		}
		return json.Marshal(value)
	case OpLink, OpUnlink:
		if o.Link == nil {
			return nil, fmt.Errorf("operation %q has no payload", o.Kind)
		}
		value := *o.Link
		value.Kind = o.Kind
		if err := validateLink(value); err != nil {
			return nil, err
		}
		return json.Marshal(value)
	case OpComment:
		if o.Comment == nil {
			return nil, fmt.Errorf("operation %q has no payload", o.Kind)
		}
		value := *o.Comment
		value.Kind = OpComment
		if err := validateComment(value); err != nil {
			return nil, err
		}
		return json.Marshal(value)
	case OpAttach:
		if o.Attach == nil {
			return nil, fmt.Errorf("operation %q has no payload", o.Kind)
		}
		value := *o.Attach
		value.Kind = OpAttach
		if err := validateAttach(value); err != nil {
			return nil, err
		}
		return json.Marshal(value)
	case OpDelete:
		if o.Delete == nil {
			return nil, fmt.Errorf("operation %q has no payload", o.Kind)
		}
		value := *o.Delete
		value.Kind = OpDelete
		if err := validateDelete(value); err != nil {
			return nil, err
		}
		return json.Marshal(value)
	case OpSetMeta:
		if o.SetMeta == nil {
			return nil, fmt.Errorf("operation %q has no payload", o.Kind)
		}
		value := *o.SetMeta
		value.Kind = OpSetMeta
		if err := validateSetMeta(value); err != nil {
			return nil, err
		}
		return json.Marshal(value)
	default:
		return nil, fmt.Errorf("unknown operation kind %q", o.Kind)
	}
}

func (o *Op) UnmarshalJSON(data []byte) error {
	var fields map[string]json.RawMessage
	if err := decodeJSON(data, &fields); err != nil {
		return err
	}
	if fields == nil {
		return fmt.Errorf("operation must be a JSON object")
	}
	kindRaw, ok := fields["kind"]
	if !ok {
		return fmt.Errorf("operation requires kind")
	}
	var kind OpKind
	if err := decodeJSON(kindRaw, &kind); err != nil {
		return fmt.Errorf("operation kind: %w", err)
	}
	*o = Op{Kind: kind}
	switch kind {
	case OpCreate:
		var value CreateOp
		if err := decodeStrict(data, &value, "kind", "type", "fields"); err != nil {
			return err
		}
		if err := validateCreate(value); err != nil {
			return err
		}
		o.Create = &value
	case OpPatch:
		var value PatchOp
		if err := decodeStrict(data, &value, "kind", "target", "fields"); err != nil {
			return err
		}
		if err := validatePatch(value); err != nil {
			return err
		}
		o.Patch = &value
	case OpClose:
		var value CloseOp
		if err := decodeStrict(data, &value, "kind", "target"); err != nil {
			return err
		}
		if err := validateClose(value); err != nil {
			return err
		}
		o.Close = &value
	case OpReparent:
		var value ReparentOp
		if err := decodeStrict(data, &value, "kind", "target"); err != nil {
			return err
		}
		if err := validateReparent(value); err != nil {
			return err
		}
		o.Reparent = &value
	case OpLink, OpUnlink:
		var value LinkOp
		if err := decodeStrict(data, &value, "kind", "from", "to", "linkKind"); err != nil {
			return err
		}
		if err := validateLink(value); err != nil {
			return err
		}
		o.Link = &value
	case OpComment:
		var value CommentOp
		if err := decodeStrict(data, &value, "kind", "target", "body"); err != nil {
			return err
		}
		if err := validateComment(value); err != nil {
			return err
		}
		o.Comment = &value
	case OpAttach:
		var value AttachOp
		if err := decodeStrict(data, &value, "kind", "target", "attachment"); err != nil {
			return err
		}
		if err := validateAttach(value); err != nil {
			return err
		}
		o.Attach = &value
	case OpDelete:
		var value DeleteOp
		if err := decodeStrict(data, &value, "kind", "target"); err != nil {
			return err
		}
		if err := validateDelete(value); err != nil {
			return err
		}
		o.Delete = &value
	case OpSetMeta:
		var value SetMetaOp
		if err := decodeStrict(data, &value, "kind", "target", "key", "value"); err != nil {
			return err
		}
		if err := validateSetMeta(value); err != nil {
			return err
		}
		o.SetMeta = &value
	default:
		return fmt.Errorf("unknown operation kind %q", kind)
	}
	return nil
}

// AuditResource identifies a resource whose CURRENT read policy protects the
// descriptor. Audit identities are resolved, nonempty ID refs on branch zero;
// display keys and caller-supplied unresolved references are not log identities.
type AuditResource struct {
	Kind string `json:"kind"`
	Ref  Ref    `json:"ref"`
}

type EventImage struct {
	Ref      Ref      `json:"ref"`
	Revision Revision `json:"revision"`
}

// ConditionalEdge is a directed edge stored in the dependent's source image.
// (Dependent, Blocker, Kind) is its identity; Revision pins its membership.
type ConditionalEdge struct {
	Dependent Ref      `json:"dependent"`
	Blocker   Ref      `json:"blocker"`
	Kind      string   `json:"kind"`
	Revision  Revision `json:"revision"`
}

// OntologyPin names the protected ontology document containing the base and
// overlays. Digest pins the effective compilation used for this project. A nil
// Project explicitly means the server base (a projectless blocker).
type OntologyPin struct {
	Ref      Ref      `json:"ref"`
	Revision Revision `json:"revision"`
	Digest   string   `json:"digest"`
	Project  *Ref     `json:"project,omitempty"`
}

// ConditionDecision stores the resolved interpretation, never a live lookup.
// Canceled has no outcome; completed has success/failure. Only completed/failure
// satisfies conditional-blocks. Terminal and Satisfied are checked assertions.
type ConditionDecision struct {
	Status    string `json:"status"`
	Category  string `json:"category"`
	Outcome   string `json:"outcome"`
	Terminal  bool   `json:"terminal"`
	Satisfied bool   `json:"satisfied"`
}

type ConditionUnmetDetails struct {
	Dependent     EventImage        `json:"dependent"`
	BlockerBefore EventImage        `json:"blockerBefore"`
	BlockerAfter  EventImage        `json:"blockerAfter"`
	Edge          ConditionalEdge   `json:"edge"`
	Ontology      OntologyPin       `json:"ontology"`
	Before        ConditionDecision `json:"before"`
	After         ConditionDecision `json:"after"`
}

const maxAuditText = 1024

func auditText(value, name string) error {
	if value == "" || !utf8.ValidString(value) || utf8.RuneCountInString(value) > maxAuditText {
		return fmt.Errorf("%s must contain 1..%d Unicode characters", name, maxAuditText)
	}
	return nil
}

func auditRef(ref Ref) error {
	if err := ref.Validate(); err != nil {
		return err
	}
	if !ref.IsID() || !ref.Branch.IsZero() {
		return fmt.Errorf("audit reference must be a resolved branch-zero ID")
	}
	return auditText(ref.ID, "audit ID")
}

func sameAuditRef(a, b Ref) bool {
	return a.ID == b.ID && a.IsID() && b.IsID() && a.Branch.IsZero() && b.Branch.IsZero()
}

func (r AuditResource) Validate() error {
	switch r.Kind {
	case "bead", "ontology", "plugin", "view", "memory", "sync", "derived", "project":
	default:
		return fmt.Errorf("unknown audit resource kind %q", r.Kind)
	}
	return auditRef(r.Ref)
}
func (r AuditResource) MarshalJSON() ([]byte, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}
	type wire AuditResource
	return json.Marshal(wire(r))
}
func (r *AuditResource) UnmarshalJSON(data []byte) error {
	type wire AuditResource
	var v wire
	if err := decodeStrict(data, &v, "kind", "ref"); err != nil {
		return err
	}
	d := AuditResource(v)
	if err := d.Validate(); err != nil {
		return err
	}
	*r = d
	return nil
}

func (i EventImage) Validate() error {
	if err := auditRef(i.Ref); err != nil {
		return err
	}
	return i.Revision.Validate()
}
func (i EventImage) MarshalJSON() ([]byte, error) {
	if err := i.Validate(); err != nil {
		return nil, err
	}
	type wire EventImage
	return json.Marshal(wire(i))
}
func (i *EventImage) UnmarshalJSON(data []byte) error {
	type wire EventImage
	var v wire
	if err := decodeStrict(data, &v, "ref", "revision"); err != nil {
		return err
	}
	d := EventImage(v)
	if err := d.Validate(); err != nil {
		return err
	}
	*i = d
	return nil
}

func (e ConditionalEdge) Validate() error {
	if err := auditRef(e.Dependent); err != nil {
		return err
	}
	if err := auditRef(e.Blocker); err != nil {
		return err
	}
	if sameAuditRef(e.Dependent, e.Blocker) || e.Kind != "conditional-blocks" {
		return fmt.Errorf("invalid conditional edge")
	}
	return e.Revision.Validate()
}
func (e ConditionalEdge) MarshalJSON() ([]byte, error) {
	if err := e.Validate(); err != nil {
		return nil, err
	}
	type wire ConditionalEdge
	return json.Marshal(wire(e))
}
func (e *ConditionalEdge) UnmarshalJSON(data []byte) error {
	type wire ConditionalEdge
	var v wire
	if err := decodeStrict(data, &v, "dependent", "blocker", "kind", "revision"); err != nil {
		return err
	}
	d := ConditionalEdge(v)
	if err := d.Validate(); err != nil {
		return err
	}
	*e = d
	return nil
}

func (p OntologyPin) Validate() error {
	if err := auditRef(p.Ref); err != nil {
		return err
	}
	if err := p.Revision.Validate(); err != nil {
		return err
	}
	if p.Project != nil {
		if err := auditRef(*p.Project); err != nil {
			return err
		}
	}
	digest, err := hex.DecodeString(p.Digest)
	if err != nil || len(digest) != 32 || hex.EncodeToString(digest) != p.Digest {
		return fmt.Errorf("ontology digest must be canonical lowercase SHA-256 hex")
	}
	return nil
}
func (p OntologyPin) MarshalJSON() ([]byte, error) {
	if err := p.Validate(); err != nil {
		return nil, err
	}
	type wire OntologyPin
	return json.Marshal(wire(p))
}
func (p *OntologyPin) UnmarshalJSON(data []byte) error {
	type wire OntologyPin
	var v wire
	if err := decodeStrict(data, &v, "ref", "revision", "digest"); err != nil {
		return err
	}
	d := OntologyPin(v)
	if err := d.Validate(); err != nil {
		return err
	}
	*p = d
	return nil
}

func (d ConditionDecision) Validate() error {
	if err := auditText(d.Status, "status"); err != nil {
		return err
	}
	terminal, satisfied := false, false
	switch d.Category {
	case "triage", "backlog", "unstarted", "started":
		if d.Outcome != "" {
			return fmt.Errorf("nonterminal status has an outcome")
		}
	case "canceled":
		terminal = true
		if d.Outcome != "" {
			return fmt.Errorf("canceled status has an outcome")
		}
	case "completed":
		terminal = true
		if d.Outcome != "success" && d.Outcome != "failure" {
			return fmt.Errorf("completed status needs success or failure outcome")
		}
		satisfied = d.Outcome == "failure"
	default:
		return fmt.Errorf("unknown status category %q", d.Category)
	}
	if d.Terminal != terminal || d.Satisfied != satisfied {
		return fmt.Errorf("inconsistent terminal/condition decision")
	}
	return nil
}
func (d ConditionDecision) MarshalJSON() ([]byte, error) {
	if err := d.Validate(); err != nil {
		return nil, err
	}
	type wire ConditionDecision
	return json.Marshal(wire(d))
}
func (d *ConditionDecision) UnmarshalJSON(data []byte) error {
	type wire ConditionDecision
	var v wire
	if err := decodeStrict(data, &v, "status", "category", "outcome", "terminal", "satisfied"); err != nil {
		return err
	}
	decoded := ConditionDecision(v)
	if err := decoded.Validate(); err != nil {
		return err
	}
	*d = decoded
	return nil
}

func (d ConditionUnmetDetails) Validate() error {
	for _, image := range []EventImage{d.Dependent, d.BlockerBefore, d.BlockerAfter} {
		if err := image.Validate(); err != nil {
			return err
		}
	}
	if err := d.Edge.Validate(); err != nil {
		return err
	}
	if err := d.Ontology.Validate(); err != nil {
		return err
	}
	if err := d.Before.Validate(); err != nil {
		return err
	}
	if err := d.After.Validate(); err != nil {
		return err
	}
	if !sameAuditRef(d.BlockerBefore.Ref, d.BlockerAfter.Ref) || !sameAuditRef(d.Edge.Dependent, d.Dependent.Ref) || !sameAuditRef(d.Edge.Blocker, d.BlockerAfter.Ref) || d.Edge.Revision.String() != d.Dependent.Revision.String() {
		return fmt.Errorf("conditional edge/source image identity mismatch")
	}
	if d.Before.Status == d.After.Status || !d.After.Terminal || d.After.Satisfied {
		return fmt.Errorf("condition-unmet requires a changed status entering an unsatisfied terminal outcome")
	}
	if compareAuditRevision(d.BlockerBefore.Revision, d.BlockerAfter.Revision) >= 0 {
		return fmt.Errorf("condition-unmet requires a later blocker image")
	}
	if compareAuditRevision(d.Dependent.Revision, d.BlockerAfter.Revision) > 0 || compareAuditRevision(d.Ontology.Revision, d.BlockerAfter.Revision) > 0 {
		return fmt.Errorf("condition source image is from a future revision")
	}
	return nil
}

// Inputs have already passed Natural.Validate. Compare symbolic magnitudes;
// even 1e1000000000 never expands into a billion-digit allocation.
func compareAuditRevision(a, b Revision) int {
	left, right := a.String(), b.String()
	if left == right {
		return 0
	}
	if left == "0" {
		return -1
	}
	if right == "0" {
		return 1
	}
	magnitude := func(value string) (string, *big.Int) {
		coefficient, exponent, _ := strings.Cut(value, "e")
		size := new(big.Int)
		if exponent != "" {
			size.SetString(exponent, 10)
		}
		size.Add(size, big.NewInt(int64(len(coefficient))))
		return coefficient, size
	}
	l, lm := magnitude(left)
	r, rm := magnitude(right)
	if cmp := lm.Cmp(rm); cmp != 0 {
		return cmp
	}
	for i := 0; i < max(len(l), len(r)); i++ {
		lc, rc := byte('0'), byte('0')
		if i < len(l) {
			lc = l[i]
		}
		if i < len(r) {
			rc = r[i]
		}
		if lc < rc {
			return -1
		}
		if lc > rc {
			return 1
		}
	}
	return 0
}
func (d ConditionUnmetDetails) MarshalJSON() ([]byte, error) {
	if err := d.Validate(); err != nil {
		return nil, err
	}
	type wire ConditionUnmetDetails
	return json.Marshal(wire(d))
}
func (d *ConditionUnmetDetails) UnmarshalJSON(data []byte) error {
	type wire ConditionUnmetDetails
	var v wire
	if err := decodeStrict(data, &v, "dependent", "blockerBefore", "blockerAfter", "edge", "ontology", "before", "after"); err != nil {
		return err
	}
	decoded := ConditionUnmetDetails(v)
	if err := decoded.Validate(); err != nil {
		return err
	}
	*d = decoded
	return nil
}

type EventCategory string
type DurableEventType string
type ControlEventType string

const (
	EventDurable EventCategory = "durable"
	EventControl EventCategory = "control"

	EventBeadCreated              DurableEventType = "bead.created"
	EventBeadUpdated              DurableEventType = "bead.updated"
	EventBeadReparented           DurableEventType = "bead.reparented"
	EventBeadLinked               DurableEventType = "bead.linked"
	EventBeadUnlinked             DurableEventType = "bead.unlinked"
	EventBeadCommented            DurableEventType = "bead.commented"
	EventBeadAttached             DurableEventType = "bead.attached"
	EventBeadDeleted              DurableEventType = "bead.deleted"
	EventBeadClaimed              DurableEventType = "bead.claimed"
	EventBeadReleased             DurableEventType = "bead.released"
	EventBeadClosed               DurableEventType = "bead.closed"
	EventBeadFailed               DurableEventType = "bead.failed"
	EventBeadCanceled             DurableEventType = "bead.canceled"
	EventBeadReopened             DurableEventType = "bead.reopened"
	EventBeadDeferred             DurableEventType = "bead.deferred"
	EventBeadLeaseReclaimed       DurableEventType = "bead.lease.reclaimed"
	EventGateApproved             DurableEventType = "gate.approved"
	EventGateReported             DurableEventType = "gate.reported"
	EventGateEscalated            DurableEventType = "gate.escalated"
	EventOntologyApplied          DurableEventType = "ontology.applied"
	EventPluginApplied            DurableEventType = "plugin.applied"
	EventViewSaved                DurableEventType = "view.saved"
	EventViewDeleted              DurableEventType = "view.deleted"
	EventMemorySaved              DurableEventType = "memory.saved"
	EventMemoryDeleted            DurableEventType = "memory.deleted"
	EventSyncPushed               DurableEventType = "sync.pushed"
	EventSyncImported             DurableEventType = "sync.imported"
	EventDerivedUpdated           DurableEventType = "derived.updated"
	EventDependencyConditionUnmet DurableEventType = "dependency.conditionUnmet"

	EventGap            ControlEventType = "event.gap"
	EventWatchDelta     ControlEventType = "watch.delta"
	EventWatchReset     ControlEventType = "watch.reset"
	EventVisibilityLost ControlEventType = "visibility.lost"
	EventRPCRejected    ControlEventType = "rpc.rejected"
	EventStoreRestored  ControlEventType = "store.restored"
)

type EventPosition struct {
	Cursor      string   `json:"cursor"`
	Incarnation string   `json:"incarnation"`
	Revision    Revision `json:"revision"`
	Index       Natural  `json:"index"`
}
type EventCause struct {
	Root  string   `json:"root"`
	Depth Natural  `json:"depth"`
	Chain []string `json:"chain"`
}

func (c *EventCause) UnmarshalJSON(data []byte) error {
	type wireEventCause EventCause
	var value wireEventCause
	if err := decodeStrict(data, &value, "root", "depth", "chain"); err != nil {
		return err
	}
	if err := validateEventCause(EventCause(value)); err != nil {
		return err
	}
	*c = EventCause(value)
	return nil
}

func (c EventCause) MarshalJSON() ([]byte, error) {
	if err := validateEventCause(c); err != nil {
		return nil, err
	}
	type wireEventCause EventCause
	return json.Marshal(wireEventCause(c))
}

type DurableEvent struct {
	EventPosition
	Type      DurableEventType       `json:"type"`
	Resource  string                 `json:"resource"`
	Changed   []string               `json:"changed"`
	Before    map[string]any         `json:"before,omitempty"`
	After     map[string]any         `json:"after,omitempty"`
	Principal PrincipalRef           `json:"principal"`
	Chain     []string               `json:"chain"`
	SetBy     map[string]string      `json:"setBy"`
	Cause     EventCause             `json:"cause"`
	Derived   bool                   `json:"derived"`
	Details   *ConditionUnmetDetails `json:"details,omitempty"`
}
type ControlDetails interface{ controlDetails() }
type EventGapDetails struct {
	FromIncarnation string   `json:"fromIncarnation"`
	ToIncarnation   string   `json:"toIncarnation"`
	Floor           Revision `json:"floor"`
	Reason          string   `json:"reason"`
}

func (EventGapDetails) controlDetails() {}

func (d EventGapDetails) MarshalJSON() ([]byte, error) {
	if err := validateControlDetails(EventGap, d); err != nil {
		return nil, err
	}
	type wireDetails EventGapDetails
	return json.Marshal(wireDetails(d))
}

func (d *EventGapDetails) UnmarshalJSON(data []byte) error {
	type wireDetails EventGapDetails
	var value wireDetails
	if err := decodeStrict(data, &value, "fromIncarnation", "toIncarnation", "floor", "reason"); err != nil {
		return err
	}
	decoded := EventGapDetails(value)
	if err := validateControlDetails(EventGap, decoded); err != nil {
		return err
	}
	*d = decoded
	return nil
}

type StoreRestoredDetails struct {
	FromIncarnation string `json:"fromIncarnation"`
	ToIncarnation   string `json:"toIncarnation"`
	Reason          string `json:"reason"`
}

func (StoreRestoredDetails) controlDetails() {}

func (d StoreRestoredDetails) MarshalJSON() ([]byte, error) {
	if err := validateControlDetails(EventStoreRestored, d); err != nil {
		return nil, err
	}
	type wireDetails StoreRestoredDetails
	return json.Marshal(wireDetails(d))
}

func (d *StoreRestoredDetails) UnmarshalJSON(data []byte) error {
	type wireDetails StoreRestoredDetails
	var value wireDetails
	if err := decodeStrict(data, &value, "fromIncarnation", "toIncarnation", "reason"); err != nil {
		return err
	}
	decoded := StoreRestoredDetails(value)
	if err := validateControlDetails(EventStoreRestored, decoded); err != nil {
		return err
	}
	*d = decoded
	return nil
}

type WatchDeltaDetails struct {
	SubscriptionID string  `json:"subscriptionId"`
	FromCursor     string  `json:"fromCursor"`
	ToCursor       string  `json:"toCursor"`
	Events         []Event `json:"events"`
}

func (WatchDeltaDetails) controlDetails() {}

func (d WatchDeltaDetails) MarshalJSON() ([]byte, error) {
	if err := validateControlDetails(EventWatchDelta, d); err != nil {
		return nil, err
	}
	type wireDetails WatchDeltaDetails
	return json.Marshal(wireDetails(d))
}

func (d *WatchDeltaDetails) UnmarshalJSON(data []byte) error {
	type wireDetails WatchDeltaDetails
	var value wireDetails
	if err := decodeStrict(data, &value, "subscriptionId", "fromCursor", "toCursor", "events"); err != nil {
		return err
	}
	decoded := WatchDeltaDetails(value)
	if err := validateControlDetails(EventWatchDelta, decoded); err != nil {
		return err
	}
	*d = decoded
	return nil
}

type WatchResetDetails struct {
	SubscriptionID string `json:"subscriptionId"`
	Reason         string `json:"reason"`
	SnapshotCursor string `json:"snapshotCursor"`
}

func (WatchResetDetails) controlDetails() {}

func (d WatchResetDetails) MarshalJSON() ([]byte, error) {
	if err := validateControlDetails(EventWatchReset, d); err != nil {
		return nil, err
	}
	type wireDetails WatchResetDetails
	return json.Marshal(wireDetails(d))
}

func (d *WatchResetDetails) UnmarshalJSON(data []byte) error {
	type wireDetails WatchResetDetails
	var value wireDetails
	if err := decodeStrict(data, &value, "subscriptionId", "reason", "snapshotCursor"); err != nil {
		return err
	}
	decoded := WatchResetDetails(value)
	if err := validateControlDetails(EventWatchReset, decoded); err != nil {
		return err
	}
	*d = decoded
	return nil
}

type VisibilityLostDetails struct {
	Reason string `json:"reason"`
}

func (VisibilityLostDetails) controlDetails() {}

func (d VisibilityLostDetails) MarshalJSON() ([]byte, error) {
	if err := validateControlDetails(EventVisibilityLost, d); err != nil {
		return nil, err
	}
	type wireDetails VisibilityLostDetails
	return json.Marshal(wireDetails(d))
}

func (d *VisibilityLostDetails) UnmarshalJSON(data []byte) error {
	type wireDetails VisibilityLostDetails
	var value wireDetails
	if err := decodeStrict(data, &value, "reason"); err != nil {
		return err
	}
	decoded := VisibilityLostDetails(value)
	if err := validateControlDetails(EventVisibilityLost, decoded); err != nil {
		return err
	}
	*d = decoded
	return nil
}

type RPCRejectedDetails struct {
	RequestID any    `json:"requestId"`
	Reason    string `json:"reason"`
}

func (RPCRejectedDetails) controlDetails() {}

// marshalRequestID retains the complete string/integer schema domain. JSON
// numbers are checked mathematically, without int64 or float64 conversion.
func marshalRequestID(value any) ([]byte, error) {
	switch value.(type) {
	case string:
		return json.Marshal(value)
	case json.Number, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		raw, err := json.Marshal(value)
		if err != nil {
			return nil, err
		}
		canonical, err := canonicalInteger(raw)
		if err != nil {
			return nil, fmt.Errorf("rpc.rejected requestId must be an integer: %w", err)
		}
		return []byte(canonical), nil
	default:
		return nil, fmt.Errorf("rpc.rejected requestId must be a string or integer")
	}
}

func (d RPCRejectedDetails) MarshalJSON() ([]byte, error) {
	if err := validateControlDetails(EventRPCRejected, d); err != nil {
		return nil, err
	}
	requestID, err := marshalRequestID(d.RequestID)
	if err != nil {
		return nil, err
	}
	return json.Marshal(struct {
		RequestID json.RawMessage `json:"requestId"`
		Reason    string          `json:"reason"`
	}{requestID, d.Reason})
}

func (d *RPCRejectedDetails) UnmarshalJSON(data []byte) error {
	type wireDetails RPCRejectedDetails
	var value wireDetails
	if err := decodeStrict(data, &value, "requestId", "reason"); err != nil {
		return err
	}
	decoded := RPCRejectedDetails(value)
	if err := validateControlDetails(EventRPCRejected, decoded); err != nil {
		return err
	}
	*d = decoded
	return nil
}

type ControlEvent struct {
	EventPosition
	Type     ControlEventType `json:"type"`
	Resource string           `json:"resource,omitempty"`
	Details  ControlDetails   `json:"-"`
}
type Event struct {
	Category EventCategory `json:"-"`
	Durable  *DurableEvent `json:"-"`
	Control  *ControlEvent `json:"-"`
}

func validDurableEventType(kind DurableEventType) bool {
	switch kind {
	case EventBeadCreated, EventBeadUpdated, EventBeadReparented, EventBeadLinked, EventBeadUnlinked,
		EventBeadCommented, EventBeadAttached, EventBeadDeleted, EventBeadClaimed, EventBeadReleased,
		EventBeadClosed, EventBeadFailed, EventBeadCanceled, EventBeadReopened, EventBeadDeferred,
		EventBeadLeaseReclaimed, EventGateApproved, EventGateReported, EventGateEscalated, EventOntologyApplied,
		EventPluginApplied, EventViewSaved, EventViewDeleted, EventMemorySaved, EventMemoryDeleted,
		EventSyncPushed, EventSyncImported, EventDerivedUpdated, EventDependencyConditionUnmet:
		return true
	default:
		return false
	}
}

func validControlEventType(kind ControlEventType) bool {
	switch kind {
	case EventGap, EventWatchDelta, EventWatchReset, EventVisibilityLost, EventRPCRejected, EventStoreRestored:
		return true
	default:
		return false
	}
}

func validPrincipalKind(kind string) bool {
	switch kind {
	case "anonymous", "user", "agent", "service", "plugin", "system":
		return true
	default:
		return false
	}
}

func validGapReason(reason string) bool {
	switch reason {
	case "restore", "failover", "history_floor", "checkpoint":
		return true
	default:
		return false
	}
}

func validResetReason(reason string) bool {
	switch reason {
	case "policy_changed", "ontology_changed", "view_changed", "overflow", "lag", "visibility_changed", "view_deleted":
		return true
	default:
		return false
	}
}

func validVisibilityReason(reason string) bool {
	switch reason {
	case "authorization_changed", "redacted", "resource_deleted":
		return true
	default:
		return false
	}
}

func validateEventPosition(position EventPosition) error {
	if err := position.Revision.Validate(); err != nil {
		return fmt.Errorf("event revision: %w", err)
	}
	return position.Index.Validate()
}

func validateDurableEvent(value DurableEvent) error {
	if err := validateEventPosition(value.EventPosition); err != nil {
		return err
	}
	if !validDurableEventType(value.Type) {
		return fmt.Errorf("unknown durable event type %q", value.Type)
	}
	if value.Changed == nil || value.Chain == nil || value.SetBy == nil {
		return fmt.Errorf("durable event has a missing required collection")
	}
	if value.Type == EventDependencyConditionUnmet {
		if value.Details == nil {
			return fmt.Errorf("condition-unmet event needs details")
		}
		if err := value.Details.Validate(); err != nil {
			return err
		}
		if value.Resource != "bead/"+value.Details.Dependent.Ref.ID || len(value.Changed) != 0 || value.Before != nil || value.After != nil || value.Derived {
			return fmt.Errorf("condition notice forbids fake dependent mutation/images")
		}
		if value.Revision.String() != value.Details.BlockerAfter.Revision.String() { return fmt.Errorf("notice revision must pin final blocker image") }
	} else if value.Details != nil {
		return fmt.Errorf("ordinary event forbids condition details")
	}
	if err := validatePrincipal(value.Principal); err != nil {
		return err
	}
	return validateEventCause(value.Cause)
}

func validatePrincipal(value PrincipalRef) error {
	seen := map[*PrincipalRef]bool{}
	for {
		if !validPrincipalKind(value.Kind) {
			return fmt.Errorf("unknown principal kind %q", value.Kind)
		}
		if value.Sponsor == nil {
			return nil
		}
		if seen[value.Sponsor] {
			return fmt.Errorf("cyclic principal sponsor")
		}
		seen[value.Sponsor] = true
		value = *value.Sponsor
	}
}

func validateEventCause(value EventCause) error {
	depth, err := value.Depth.Uint64()
	if err != nil || depth > 8 || value.Chain == nil {
		return fmt.Errorf("invalid event cause")
	}
	return nil
}

func validateControlDetails(kind ControlEventType, details ControlDetails) error {
	switch kind {
	case EventGap:
		value, ok := details.(EventGapDetails)
		if !ok || !validGapReason(value.Reason) || value.Floor.Validate() != nil {
			return fmt.Errorf("invalid event.gap details")
		}
	case EventStoreRestored:
		value, ok := details.(StoreRestoredDetails)
		if !ok || value.Reason != "restore" {
			return fmt.Errorf("invalid store.restored details")
		}
	case EventWatchDelta:
		value, ok := details.(WatchDeltaDetails)
		if !ok || value.Events == nil {
			return fmt.Errorf("invalid watch.delta details")
		}
	case EventWatchReset:
		value, ok := details.(WatchResetDetails)
		if !ok || !validResetReason(value.Reason) {
			return fmt.Errorf("invalid watch.reset details")
		}
	case EventVisibilityLost:
		value, ok := details.(VisibilityLostDetails)
		if !ok || !validVisibilityReason(value.Reason) {
			return fmt.Errorf("invalid visibility.lost details")
		}
	case EventRPCRejected:
		value, ok := details.(RPCRejectedDetails)
		if !ok || value.Reason != "notification_no_result" || value.RequestID == nil {
			return fmt.Errorf("invalid rpc.rejected details")
		}
		if _, err := marshalRequestID(value.RequestID); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unknown control event type %q", kind)
	}
	return nil
}

func (e Event) MarshalJSON() ([]byte, error) {
	if (e.Durable == nil) == (e.Control == nil) {
		return nil, fmt.Errorf("event must select exactly one durable or control member")
	}
	if e.Durable != nil {
		if e.Category != "" && e.Category != EventDurable {
			return nil, fmt.Errorf("durable event has category %q", e.Category)
		}
		if err := validateDurableEvent(*e.Durable); err != nil {
			return nil, err
		}
		return json.Marshal(e.Durable)
	}
	if e.Category != "" && e.Category != EventControl {
		return nil, fmt.Errorf("control event has category %q", e.Category)
	}
	if !validControlEventType(e.Control.Type) || e.Control.Details == nil || !controlDetailsMatch(e.Control.Type, e.Control.Details) {
		return nil, fmt.Errorf("control event %q has the wrong details type", e.Control.Type)
	}
	if err := validateEventPosition(e.Control.EventPosition); err != nil {
		return nil, err
	}
	if err := validateControlDetails(e.Control.Type, e.Control.Details); err != nil {
		return nil, err
	}
	var resource *string
	if e.Control.Type == EventVisibilityLost {
		resource = &e.Control.Resource // required even when the string is empty
	} else if e.Control.Resource != "" {
		return nil, fmt.Errorf("only visibility.lost carries a resource")
	}
	details, err := json.Marshal(e.Control.Details)
	if err != nil {
		return nil, err
	}
	return json.Marshal(struct {
		Category    EventCategory    `json:"category"`
		Cursor      string           `json:"cursor"`
		Incarnation string           `json:"incarnation"`
		Revision    Revision         `json:"revision"`
		Index       Natural          `json:"index"`
		Type        ControlEventType `json:"type"`
		Resource    *string          `json:"resource,omitempty"`
		Details     json.RawMessage  `json:"details"`
		Derived     bool             `json:"derived"`
	}{EventControl, e.Control.Cursor, e.Control.Incarnation, e.Control.Revision, e.Control.Index, e.Control.Type, resource, details, false})
}

func controlDetailsMatch(kind ControlEventType, details ControlDetails) bool {
	switch kind {
	case EventGap:
		_, ok := details.(EventGapDetails)
		return ok
	case EventWatchDelta:
		_, ok := details.(WatchDeltaDetails)
		return ok
	case EventWatchReset:
		_, ok := details.(WatchResetDetails)
		return ok
	case EventVisibilityLost:
		_, ok := details.(VisibilityLostDetails)
		return ok
	case EventRPCRejected:
		_, ok := details.(RPCRejectedDetails)
		return ok
	case EventStoreRestored:
		_, ok := details.(StoreRestoredDetails)
		return ok
	default:
		return false
	}
}

func (d *DurableEvent) UnmarshalJSON(data []byte) error {
	var wire struct {
		Category EventCategory `json:"category"`
		EventPosition
		Type      DurableEventType       `json:"type"`
		Resource  string                 `json:"resource"`
		Changed   []string               `json:"changed"`
		Before    map[string]any         `json:"before,omitempty"`
		After     map[string]any         `json:"after,omitempty"`
		Principal PrincipalRef           `json:"principal"`
		Chain     []string               `json:"chain"`
		SetBy     map[string]string      `json:"setBy"`
		Cause     EventCause             `json:"cause"`
		Derived   bool                   `json:"derived"`
		Details   *ConditionUnmetDetails `json:"details,omitempty"`
	}
	if err := decodeStrict(data, &wire, "category", "cursor", "incarnation", "revision", "index", "type", "resource", "changed", "principal", "chain", "setBy", "cause", "derived"); err != nil {
		return err
	}
	var fields map[string]json.RawMessage
	if err := decodeJSON(data, &fields); err != nil {
		return err
	}
	for _, name := range []string{"before", "after"} {
		if raw, ok := fields[name]; ok && bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			return fmt.Errorf("durable event %s must be an object when present", name)
		}
	}
	if wire.Category != EventDurable {
		return fmt.Errorf("durable event has category %q", wire.Category)
	}
	value := DurableEvent{
		EventPosition: wire.EventPosition,
		Type:          wire.Type,
		Resource:      wire.Resource,
		Changed:       wire.Changed,
		Before:        wire.Before,
		After:         wire.After,
		Principal:     wire.Principal,
		Chain:         wire.Chain,
		SetBy:         wire.SetBy,
		Cause:         wire.Cause,
		Derived:       wire.Derived,
		Details:       wire.Details,
	}
	if err := validateDurableEvent(value); err != nil {
		return err
	}
	*d = value
	return nil
}

func (d DurableEvent) MarshalJSON() ([]byte, error) {
	if err := validateDurableEvent(d); err != nil {
		return nil, err
	}
	type wireDurableEvent DurableEvent
	return json.Marshal(struct {
		Category EventCategory `json:"category"`
		wireDurableEvent
	}{EventDurable, wireDurableEvent(d)})
}

func (e *Event) UnmarshalJSON(data []byte) error {
	var head struct {
		Category EventCategory `json:"category"`
		Type     string        `json:"type"`
	}
	if err := decodeJSON(data, &head); err != nil {
		return err
	}
	decoded := Event{Category: head.Category}
	switch head.Category {
	case EventDurable:
		var value DurableEvent
		if err := decodeJSON(data, &value); err != nil {
			return err
		}
		decoded.Durable = &value
	case EventControl:
		var envelope struct {
			Category EventCategory `json:"category"`
			EventPosition
			Type     ControlEventType `json:"type"`
			Resource string           `json:"resource,omitempty"`
			Details  json.RawMessage  `json:"details"`
			Derived  bool             `json:"derived"`
		}
		if err := decodeStrict(data, &envelope, "category", "cursor", "incarnation", "revision", "index", "type", "details", "derived"); err != nil {
			return err
		}
		if envelope.Category != EventControl || !validControlEventType(envelope.Type) {
			return fmt.Errorf("unknown control event type %q", envelope.Type)
		}
		var fields map[string]json.RawMessage
		if err := decodeJSON(data, &fields); err != nil {
			return err
		}
		_, hasResource := fields["resource"]
		if (envelope.Type == EventVisibilityLost) != hasResource {
			return fmt.Errorf("only visibility.lost carries a required resource")
		}
		if err := validateEventPosition(envelope.EventPosition); err != nil {
			return err
		}
		if envelope.Derived {
			return fmt.Errorf("control event cannot be derived")
		}
		value := ControlEvent{EventPosition: envelope.EventPosition, Type: envelope.Type, Resource: envelope.Resource}
		switch envelope.Type {
		case EventGap:
			var details EventGapDetails
			if err := decodeStrict(envelope.Details, &details, "fromIncarnation", "toIncarnation", "floor", "reason"); err != nil {
				return err
			}
			value.Details = details
		case EventStoreRestored:
			var details StoreRestoredDetails
			if err := decodeStrict(envelope.Details, &details, "fromIncarnation", "toIncarnation", "reason"); err != nil {
				return err
			}
			value.Details = details
		case EventWatchDelta:
			var details WatchDeltaDetails
			if err := decodeStrict(envelope.Details, &details, "subscriptionId", "fromCursor", "toCursor", "events"); err != nil {
				return err
			}
			value.Details = details
		case EventWatchReset:
			var details WatchResetDetails
			if err := decodeStrict(envelope.Details, &details, "subscriptionId", "reason", "snapshotCursor"); err != nil {
				return err
			}
			value.Details = details
		case EventVisibilityLost:
			var details VisibilityLostDetails
			if err := decodeStrict(envelope.Details, &details, "reason"); err != nil {
				return err
			}
			value.Details = details
		case EventRPCRejected:
			var details RPCRejectedDetails
			if err := decodeStrict(envelope.Details, &details, "requestId", "reason"); err != nil {
				return err
			}
			value.Details = details
		default:
			return fmt.Errorf("unknown control event type %q", envelope.Type)
		}
		decoded.Control = &value
		if err := validateControlDetails(value.Type, value.Details); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unknown event category %q", head.Category)
	}
	*e = decoded
	return nil
}

type Warning struct {
	Code    WarningCode    `json:"code"`
	Message string         `json:"message"`
	Details map[string]any `json:"details,omitempty"`
}

func (w Warning) MarshalJSON() ([]byte, error) {
	if !validWarningCode(w.Code) {
		return nil, fmt.Errorf("unknown warning code %q", w.Code)
	}
	type wireWarning Warning
	return json.Marshal(wireWarning(w))
}

func (w *Warning) UnmarshalJSON(data []byte) error {
	type wireWarning Warning
	var value wireWarning
	if err := decodeStrict(data, &value, "code", "message"); err != nil {
		return err
	}
	if !validWarningCode(value.Code) {
		return fmt.Errorf("unknown warning code %q", value.Code)
	}
	*w = Warning(value)
	return nil
}

type Violation struct {
	Guard   string         `json:"guard"`
	Message string         `json:"message"`
	Details map[string]any `json:"details,omitempty"`
}

// Writer is the only core entry point for graph writes. Named verbs lower to
// this same pipeline; tx.apply supplies ChangeSet directly.
type Writer interface {
	Apply(ctx context.Context, by PrincipalRef, cs ChangeSet, meta WriteMeta) (Commit, error)
}
type WriteMeta struct {
	Project        string
	IdempotencyKey string
	DryRun         bool
	Return         string // full or minimal
	Traceparent    string
	Agent          string
}

type ChangesetKind string

const (
	ChangesetWrite     ChangesetKind = "write"
	ChangesetDerived   ChangesetKind = "derived"
	ChangesetRevert    ChangesetKind = "revert"
	ChangesetMigration ChangesetKind = "migration"
	ChangesetRedact    ChangesetKind = "redact"
	ChangesetRestore   ChangesetKind = "restore"
)

// MaxChangesetEvents bounds the complete atomic event plan, ordinary events
// included. Overflow refuses the write; it never truncates condition notices.
const MaxChangesetEvents = 1000 // same bound as model.MaxChangeSetOps

// EventProvenance is stamped by the trusted Writer, not accepted from ChangeSet
// input. Slot is empty for a non-slot write. Chain records authorization links.
type EventProvenance struct {
	Method string            `json:"method"`
	Slot   string            `json:"slot"`
	SetBy  map[string]string `json:"setBy"`
	Cause  EventCause        `json:"cause"`
	Chain  []string          `json:"chain"`
}

// EventDescriptor is a closed durable event plan. ChangeIndex anchors an actual
// changed record, including notices whose resource is an unchanged dependent.
// Index is the position in Changeset.Events. Images are reconstructed from the
// pinned history, not duplicated here. Only conditionUnmet has Details.
type EventDescriptor struct {
	Index       Natural                `json:"index"`
	ChangeIndex Natural                `json:"changeIndex"`
	Type        DurableEventType       `json:"type"`
	Resource    AuditResource          `json:"resource"`
	Provenance  EventProvenance        `json:"provenance"`
	Details     *ConditionUnmetDetails `json:"details,omitempty"`
}


type Changeset struct {
	Revision       Revision       `json:"revision"`
	Branch         Branch         `json:"branch"`
	Incarnation    string         `json:"incarnation"`
	Kind           ChangesetKind  `json:"kind"`
	Time           time.Time      `json:"time"`
	Principal      PrincipalRef   `json:"principal"`
	Chain          []string       `json:"chain" wire:"nullable"`
	IdempotencyKey string         `json:"idempotencyKey,omitempty"`
	Mutators       []string       `json:"mutators,omitempty"`
	Changes        []ChangeRecord `json:"changes" wire:"nullable"`
	Events         []EventDescriptor `json:"events,omitempty"`
}
type ChangeRecord struct {
	Ref          Ref      `json:"ref"`
	Op           OpKind   `json:"op"`
	FromRevision Revision `json:"fromRevision"`
	ChangedPaths []string `json:"changedPaths" wire:"nullable"`
}
```

`Op.Kind` is a closed union: the selected pointer must match `Kind`, and
startup registry lint rejects an operation or method whose effect lowers to no
member. Protected plugin-set, lock, instance-config, and ontology records are
not legal targets for `tx.apply`, `setMeta`, or `history.revert`.

The ordered descriptor data shapes above are canonical model fields. Their
strict validation, complete-plan ordering, resource authorization, refusal caps,
atomic driver-copy follow-on and legacy fallback are specified in
[Atomic ordered audit descriptors](events.md#atomic-ordered-audit-descriptors-a3b02).
`internal/core/model` implements `ValidateEvents`, `CloneEventDescriptors` and
`EventDescriptor.RequiredResources`; emission and persistence are separate work.
Persisted history decoding retains legacy nil slices and their JSON `null`
representation for `Changeset.chain`, `Changeset.changes` and
`ChangeRecord.changedPaths`. Null collection elements remain invalid. This
compatibility does not relax `ChangeSet` requests or event descriptor fields;
the canonical OpenRPC request schema is unchanged.

### Parent/child permissions and epic progress (post-stop contract)

`CreateOp.Parent` and `ReparentOp.Parent` are direct canonical parent
references; nil means that the child is top-level. The binding permission
rules are:

1. parented create and reparent require edit authorization on the epic/parent to attach;
2. hidden parents reveal existence only, without content;
3. epic progress shows visible-child counts plus “N more you can’t see”;
4. the child remains in its own project;
5. moving a child needs edit authorization on the child and on the new epic/parent.

A parented create additionally requires ordinary create authorization for the
new child. A reparent checks edit authorization on the child and the new
parent; the old parent is not an authorization target. The parent link does
not rewrite `CreateOp.Project` or `ReparentOp.Project`, and child
creation/reparenting do not grant cross-project access. The child continues to
use its own project's permissions. A caller that can see the child but not the
parent may learn only that a parent exists; parent content is withheld, so
non-disclosure is preserved.

The parent-aware epic progress path reuses the existing `view.run` request and
`ViewResult`/`ViewGroup` envelope. Its producer selects direct children whose
stored `Parent` equals the requested epic, binds content, ontology and current
authority once, and evaluates each child with that binding. Only visible
children contribute status/readiness counts. Hidden direct children contribute
only an authorization-elided total rendered as “N more you can’t see”; no
status is exposed for them. Grandchildren, unrelated same-type beads and
denied children are excluded from the visible denominator. This contract adds
no new RPC, cursor, `groupBy` value or result member; exact producer wiring,
implementation and qualification remain deferred.

The historical #18 fixed-authz denial of parented create and reparent remains
the current runtime boundary at this head. This later contract/design step
supersedes that boundary only as a conditional target for a future separately
admitted implementation. It does not install a permit or claim runtime
support; project moves remain outside this slice.

### EP1 hierarchy write contract (conditional, implementation-deferred)

This subsection fixes the smallest write contract needed by the post-stop EP1
handoff. It describes a future, separately admitted hierarchy writer; it does
not remove the #18 runtime refusal or claim implementation, qualification, or
an authorization permit.

**References and ordered batches.** `CreateOp.Parent` and `ReparentOp.Parent`
are optional direct canonical `Ref` values. Absent or JSON `null` means a
top-level child. A `Ref` resolves to a live stored ID or to the key of an
earlier `create` operation in the same ordered `ChangeSet`; malformed, unknown,
or forward aliases refuse with `invalid_params`. Each operation is evaluated
against the immutable stored pre-image plus the effects staged by earlier
operations. Therefore a parent create followed by a child create (or reparent)
in one `tx.apply` is valid only when the parent operation comes first, and the
whole batch publishes at one revision or publishes nothing. A later operation
may refer to an earlier staged record, but no operation may observe a later
create or a partially published parent.

**Existence, cycles, and child guards.** A missing or deleted parent refuses
with `not_found` after the normal disclosure check; an unreadable parent may
produce the same `not_found` or the existing authorization denial, without
distinguishing hidden from absent content. A self-parent or descendant-parent
cycle, malformed closure, or final graph that violates the parent's ontology
child/blocker/no-open-children guard refuses with `ontology_guard_failed`.
The guard evaluates the final staged graph and real status/blocker facts; it is
not the current constant `NoOpenChildren=true` shortcut. No implicit detach,
cascade, or dangling `Parent`/closure row is allowed. Unsupported delete,
project-change, or other child-invariant cases remain refused until a separate
contract supplies their atomic behavior.

**Project boundaries.** `CreateOp.Project` selects the new child's project
under ordinary project rules; the parent project's identity is never inherited.
A reparent leaves the target child's project unchanged. A non-empty
`ReparentOp.Project` that differs from that project is an unsupported project
move and refuses; the parent link may cross projects, but it grants no access
to either project. The child continues to use its own project's permissions,
and the EP2 edit checks above remain required for the parent and, on reparent,
the child.

**Transactional ancestor closure.** Let `C(d,a)` be the live ancestor closure
with a self row `C(d,d)=0`. A root create adds only its self row. A parented
create adds the child self row and one row for every ancestor of the parent,
with depth one greater than the parent's depth. A reparent computes the full
subtree `S` from the staged graph: it keeps pairs internal to `S`, removes old
external ancestor pairs, and adds new external pairs (or removes all external
pairs when moving to root). Depth changes are replacements keyed by both pair
and value, never ignored updates. The persisted direct `Parent` and all
derived rows are published together and survive strict decode, projection,
history, and replay.

Before locking, the writer must enumerate the complete added/removed/replaced
closure and graph delta for the entire ordered batch, then take the existing
deterministic closure/derived locks and bump the existing graph/blocking and
closure sentinels. More than 5,000 affected closure rows, counting removals,
additions, and depth replacements across the batch, refuses with
`closure_too_large`; splitting or deferring one logical rewrite cannot bypass
the bound. Migration-class rewrites and populated-store backfill are outside
EP1. Any refusal rolls back the record, parent, closure, history, event, and
receipt state as one unit.

Required future acceptance cases are root and same-batch parent creation,
forward/missing/hidden parent refs, self and descendant cycles, the real child
guard, move-to-root and whole-subtree depth replacement, rollback after each
staged write or sentinel bump, and the 5,000/5,001 closure boundary. These
cases are implementation gates, not evidence that the current runtime supports
parented writes.

## `pluginapi` (public)

```go
// package pluginapi
package pluginapi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"reflect"
	"strconv"
	"strings"
)

// These wire-safe types live in pluginapi itself. A public SDK cannot import
// Plug's internal core/model package; the host converts these values at the
// boundary.
// Natural is the lossless nonnegative JSON-integer wire domain. A zero Go
// value represents zero. It deliberately has no uint32/int64/uint64 ceiling:
// the accepted OpenRPC schemas specify no such maximum. Drivers must check
// conversion to engine-sized integers and may never silently narrow a value.
// Direct casts are checked again by Validate and MarshalJSON.
type Natural string

func Natural64(value uint64) Natural { return Natural(strconv.FormatUint(value, 10)) }

func (value Natural) Canonical() (string, error) {
	text := string(value)
	if text == "" {
		text = "0"
	}
	canonical, err := canonicalInteger([]byte(text))
	if err != nil {
		return "", err
	}
	if strings.HasPrefix(canonical, "-") {
		return "", fmt.Errorf("expected nonnegative integer")
	}
	return canonical, nil
}

func (value Natural) Validate() error {
	_, err := value.Canonical()
	return err
}

func (value Natural) IsZero() bool {
	canonical, err := value.Canonical()
	return err == nil && canonical == "0"
}

func (value Natural) String() string {
	canonical, err := value.Canonical()
	if err != nil {
		return "<invalid integer>"
	}
	return canonical
}

func (value Natural) Uint64() (uint64, error) {
	canonical, err := value.Canonical()
	if err != nil {
		return 0, err
	}
	// Canonical values of at most 20 digits are expanded. An exponent here
	// therefore cannot fit uint64; ParseUint rejects it without allocation.
	return strconv.ParseUint(canonical, 10, 64)
}

func (value Natural) MarshalJSON() ([]byte, error) {
	canonical, err := value.Canonical()
	return []byte(canonical), err
}

func (value *Natural) UnmarshalJSON(data []byte) error {
	canonical, err := canonicalInteger(data)
	if err != nil {
		return err
	}
	decoded := Natural(canonical)
	if err := decoded.Validate(); err != nil {
		return err
	}
	*value = decoded
	return nil
}

// canonicalInteger recognizes mathematical JSON integers, including 1.0,
// 1e0, and large integral exponents, without float64 rounding. It normalizes
// equivalent spellings. Exponents stay symbolic so a short input such as
// 1e1000000000 does not allocate a billion-digit integer.
func canonicalInteger(data []byte) (string, error) {
	text := strings.TrimSpace(string(data))
	if text == "" || !json.Valid([]byte(text)) || (text[0] != '-' && (text[0] < '0' || text[0] > '9')) {
		return "", fmt.Errorf("expected JSON integer")
	}
	negative := strings.HasPrefix(text, "-")
	if negative {
		text = text[1:]
	}
	exponent := new(big.Int)
	if index := strings.IndexAny(text, "eE"); index >= 0 {
		if _, ok := exponent.SetString(text[index+1:], 10); !ok {
			return "", fmt.Errorf("invalid integer exponent")
		}
		text = text[:index]
	}
	fractionDigits := 0
	if index := strings.IndexByte(text, '.'); index >= 0 {
		fractionDigits = len(text) - index - 1
	}
	digits := strings.TrimLeft(strings.ReplaceAll(text, ".", ""), "0")
	if digits == "" {
		return "0", nil
	}
	coefficient := strings.TrimRight(digits, "0")
	trailingZeros := len(digits) - len(coefficient)
	exponent.Sub(exponent, big.NewInt(int64(fractionDigits-trailingZeros)))
	if exponent.Sign() < 0 {
		return "", fmt.Errorf("JSON number is not integral")
	}
	canonical := coefficient
	if exponent.Sign() > 0 {
		if exponent.IsInt64() && exponent.Int64() <= int64(20-len(coefficient)) {
			canonical += strings.Repeat("0", int(exponent.Int64()))
		} else {
			canonical += "e" + exponent.String()
		}
	}
	if negative {
		canonical = "-" + canonical
	}
	return canonical, nil
}

type Revision = Natural

func decodeStrict(data []byte, dst any, required ...string) error {
	var fields map[string]json.RawMessage
	if err := decodeJSON(data, &fields); err != nil {
		return err
	}
	if fields == nil {
		return fmt.Errorf("expected JSON object")
	}
	for _, name := range required {
		if _, ok := fields[name]; !ok {
			return fmt.Errorf("missing required JSON member %q", name)
		}
	}
	if err := checkWireFields(fields, reflect.TypeOf(dst).Elem()); err != nil {
		return err
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	dec.UseNumber()
	if err := dec.Decode(dst); err != nil {
		return err
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		if err == nil {
			return fmt.Errorf("trailing JSON value")
		}
		return err
	}
	return nil
}

// encoding/json matches field names without regard to case and treats null
// as a no-op for scalar fields. Neither behavior matches JSON Schema. Check
// the exact member names and typed collection elements before decoding. Only
// sponsor is nullable; RawMessage and any deliberately retain arbitrary JSON.
func checkWireFields(fields map[string]json.RawMessage, typ reflect.Type) error {
	allowed := map[string]reflect.StructField{}
	var collect func(reflect.Type)
	collect = func(t reflect.Type) {
		for i := 0; i < t.NumField(); i++ {
			field := t.Field(i)
			if field.Anonymous {
				collect(field.Type)
				continue
			}
			name := strings.Split(field.Tag.Get("json"), ",")[0]
			if name != "-" && name != "" {
				allowed[name] = field
			}
		}
	}
	collect(typ)
	for name, raw := range fields {
		field, ok := allowed[name]
		if !ok {
			return fmt.Errorf("unknown JSON member %q", name)
		}
		if err := checkWireValue(raw, field.Type, field.Tag.Get("wire") == "nullable"); err != nil {
			return fmt.Errorf("JSON member %q: %w", name, err)
		}
	}
	return nil
}

func checkWireValue(raw json.RawMessage, typ reflect.Type, nullable bool) error {
	if typ == reflect.TypeFor[json.RawMessage]() || typ.Kind() == reflect.Interface {
		return nil
	}
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		if nullable {
			return nil
		}
		return fmt.Errorf("null is not allowed")
	}
	if typ.Kind() == reflect.Pointer {
		return checkWireValue(raw, typ.Elem(), false)
	}
	if reflect.PointerTo(typ).Implements(reflect.TypeFor[json.Unmarshaler]()) {
		// The nested custom codec checks its own tagged schema.
		return nil
	}
	switch typ.Kind() {
	case reflect.Slice:
		var values []json.RawMessage
		if err := decodeJSON(raw, &values); err != nil {
			return err
		}
		for _, value := range values {
			if err := checkWireValue(value, typ.Elem(), false); err != nil {
				return err
			}
		}
	case reflect.Map:
		var values map[string]json.RawMessage
		if err := decodeJSON(raw, &values); err != nil {
			return err
		}
		for _, value := range values {
			if err := checkWireValue(value, typ.Elem(), false); err != nil {
				return err
			}
		}
	case reflect.Struct:
		var fields map[string]json.RawMessage
		if err := decodeJSON(raw, &fields); err != nil {
			return err
		}
		return checkWireFields(fields, typ)
	}
	return nil
}

func decodeJSON(data []byte, dst any) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	if err := dec.Decode(dst); err != nil {
		return err
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		if err == nil {
			return fmt.Errorf("trailing JSON value")
		}
		return err
	}
	return nil
}

type Ref struct {
	// Presence selects the oneOf arm, including an explicit empty id or key.
	// Use the constructors to select an empty arm in native Go code.
	ID       string       `json:"id,omitempty"`
	Branch   Natural      `json:"branch,omitempty"`
	Key      string       `json:"key,omitempty"`
	selected refSelection `json:"-"`
}

type refSelection uint8

const (
	refUnselected refSelection = iota
	refID
	refKey
)

func RefByID(id string, branch Natural) Ref { return Ref{ID: id, Branch: branch, selected: refID} }
func RefByKey(key string) Ref               { return Ref{Key: key, selected: refKey} }

func (r Ref) selection() refSelection {
	if r.selected != refUnselected {
		return r.selected
	}
	if r.ID != "" || !r.Branch.IsZero() {
		return refID
	}
	if r.Key != "" {
		return refKey
	}
	return refUnselected
}

// IsID preserves the selected form even when the selected string is empty.
// Callers validate the reference before using it as an identity or lock key.
func (r Ref) IsID() bool { return r.selection() == refID }

func (r Ref) Validate() error {
	if err := r.Branch.Validate(); err != nil {
		return fmt.Errorf("ref branch: %w", err)
	}
	switch r.selection() {
	case refID:
		if r.Key != "" {
			return fmt.Errorf("ref cannot contain both id and key")
		}
	case refKey:
		if r.ID != "" || !r.Branch.IsZero() {
			return fmt.Errorf("ref key cannot carry an id or branch")
		}
	default:
		return fmt.Errorf("ref must contain exactly one of id or key")
	}
	return nil
}

func (r Ref) MarshalJSON() ([]byte, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}
	if r.IsID() {
		return json.Marshal(struct {
			ID     string  `json:"id"`
			Branch Natural `json:"branch,omitempty"`
		}{r.ID, r.Branch})
	}
	return json.Marshal(struct {
		Key string `json:"key"`
	}{r.Key})
}

func (r *Ref) UnmarshalJSON(data []byte) error {
	type wireRef Ref
	var value wireRef
	if err := decodeStrict(data, &value); err != nil {
		return err
	}
	var fields map[string]json.RawMessage
	if err := decodeJSON(data, &fields); err != nil {
		return err
	}
	_, hasID := fields["id"]
	_, hasKey := fields["key"]
	if hasID == hasKey {
		return fmt.Errorf("ref must contain exactly one of id or key")
	}
	if hasKey {
		if _, hasBranch := fields["branch"]; hasBranch {
			return fmt.Errorf("ref key cannot carry a branch")
		}
	}
	decoded := Ref(value)
	if hasID {
		decoded.selected = refID
	} else {
		decoded.selected = refKey
	}
	if err := decoded.Validate(); err != nil {
		return err
	}
	*r = decoded
	return nil
}

type ResourceRef struct {
	Kind string `json:"kind"`
	Ref  Ref    `json:"ref"`
}
type PrincipalRef struct {
	Kind    string         `json:"kind"`
	ID      string         `json:"id"`
	Sponsor *PrincipalRef  `json:"sponsor,omitempty" wire:"nullable"`
	Caveats map[string]any `json:"caveats,omitempty"`
}

func validPrincipalKind(kind string) bool {
	switch kind {
	case "anonymous", "user", "agent", "service", "plugin", "system":
		return true
	default:
		return false
	}
}

func validatePrincipal(value PrincipalRef) error {
	if !validPrincipalKind(value.Kind) {
		return fmt.Errorf("unknown principal kind %q", value.Kind)
	}
	if value.Sponsor != nil {
		return validatePrincipal(*value.Sponsor)
	}
	return nil
}

func (p *PrincipalRef) UnmarshalJSON(data []byte) error {
	type wirePrincipal PrincipalRef
	var value wirePrincipal
	if err := decodeStrict(data, &value, "kind", "id"); err != nil {
		return err
	}
	if err := validatePrincipal(PrincipalRef(value)); err != nil {
		return err
	}
	*p = PrincipalRef(value)
	return nil
}

func (p PrincipalRef) MarshalJSON() ([]byte, error) {
	if err := validatePrincipal(p); err != nil {
		return nil, err
	}
	type wirePrincipal PrincipalRef
	return json.Marshal(wirePrincipal(p))
}

type Violation struct {
	Guard   string         `json:"guard"`
	Message string         `json:"message"`
	Details map[string]any `json:"details,omitempty"`
}
type Op struct {
	Kind string `json:"kind"`
	// Payload is the selected flat operation object (including its kind), not
	// a nested {kind, create: {...}} wrapper. The host validates it with the
	// model.Op/OpenRPC union before dispatch.
	Payload json.RawMessage `json:"-"`
}
type Record struct {
	Ref      Ref             `json:"ref"`
	Kind     string          `json:"kind"`
	Revision Revision        `json:"revision"`
	Body     json.RawMessage `json:"body"`
}

type ConfigDoc = map[string]any
type ConfigProblem struct {
	Path, Code, Message string
	Details             map[string]any
}
type IntegrityParams struct {
	Revision Revision
	Project  string
}
type IntegrityProblem struct {
	Path, Code, Message string
	Details             map[string]any
}

type Configurable interface {
	ValidateConfig(context.Context, ConfigDoc) ([]ConfigProblem, error)
	ApplyConfig(context.Context, ConfigDoc) error
	CheckIntegrity(context.Context, IntegrityParams) ([]IntegrityProblem, error)
}

type Host interface {
	// Call is the plugin's only Plug API. The host validates the method,
	// manifest prefix, owned-field and bridge/egress grants before dispatch.
	Call(ctx context.Context, method string, params, result any) error
	State() StateStore
	HTTP() *http.Client                                  // host egress proxy; unavailable in hot calls
	Secret(context.Context, string) (SecretValue, error) // audited injection
}

type StateStore interface {
	Get(ctx context.Context, key string, dst any) (found bool, err error)
	Put(ctx context.Context, key string, value any, expect Revision) (Revision, error)
	Delete(ctx context.Context, key string, expect Revision) error
	Revision(ctx context.Context, key string) (Revision, error)
}
type SecretValue struct {
	Name     string
	Redacted bool
}
type RuntimeState string

const (
	RuntimeHealthy  RuntimeState = "healthy"
	RuntimeDegraded RuntimeState = "degraded"
)

type ProposedChange struct {
	RequestID    string       `json:"requestId"`
	Principal    PrincipalRef `json:"principal"`
	BaseRevision Revision     `json:"baseRevision"`
	DryRun       bool         `json:"dryRun"`
	Changes      []Change     `json:"changes"`
}
type CommittedChange struct {
	RequestID string       `json:"requestId"`
	Principal PrincipalRef `json:"principal"`
	Revision  Revision     `json:"revision"`
	Changes   []Change     `json:"changes"`
}
type Change struct {
	Resource ResourceRef       `json:"resource"`
	Op       Op                `json:"op"`
	Before   json.RawMessage   `json:"before,omitempty"`
	After    json.RawMessage   `json:"after,omitempty"`
	SetBy    map[string]string `json:"setBy,omitempty"`
}
type Patch struct {
	Resource ResourceRef
	Paths    map[string]json.RawMessage
}
type Verdict struct {
	Allow      bool
	Violations []Violation
}

type SnapshotReader interface {
	// Reads are pinned to ProposedChange.BaseRevision and join the commit
	// guard's read set. A hot call cannot write or use egress.
	Get(context.Context, []Ref) ([]Record, error)
	Query(context.Context, Query) (Page[Record], error)
	Beads(context.Context, []Ref) ([]View, error)
	Closure(context.Context, Ref, bool) ([]Ref, error)
	Grants(context.Context, []SubjectRef, []Ref) ([]Grant, error)
}
type Mutator interface {
	Mutate(context.Context, ProposedChange, SnapshotReader) ([]Patch, error)
}
type Validator interface {
	Validate(context.Context, ProposedChange, SnapshotReader) (Verdict, error)
}
type PostSubscriber interface {
	// Runs after commit, before the response, at the committed revision. It
	// cannot change the committed ChangeSet; failures are warnings and DLQ.
	After(context.Context, CommittedChange, SnapshotReader) error
}

type Query struct {
	Filter map[string]any
	Cursor *Cursor
	Limit  int
}
type Cursor struct {
	// The JSON-RPC adapter encodes this bound value as the opaque cursor string;
	// plugins never choose or reinterpret the token.
	Incarnation      string   `json:"incarnation"`
	Revision         Revision `json:"revision"`
	Index            Natural  `json:"index"`
	Method           string   `json:"method"`
	FilterDigest     string   `json:"filterDigest"`
	PrincipalDigest  string   `json:"principalDigest"`
	OntologyRevision Revision `json:"ontologyRevision"`
}
type Page[T any] struct {
	Items      []T
	NextCursor *Cursor
	AsOf       Revision
}
type View struct {
	ID     string
	Type   string
	Fields map[string]any
}
type SubjectRef struct{ Kind, ID string }
type Grant struct {
	Subject SubjectRef
	Scope   Ref
	Role    string
}

// A native or WASM plugin registers only methods under x.<alias>.*. Authz
// links and synchronous subscribers are compiled in for v1; mode-2 authz is
// not permitted.
type Method struct {
	Name       string
	Params     any
	Result     any
	Permission string
	Resource   func(params any) []Ref
}
type Registry interface{ Register(Method) error }
```

Hot-path calls (`authn.authenticate`, `authz.*`, `mutate`, `validate`) receive
only a revision-pinned `SnapshotReader`; `Host.Call` writes fail with
`plugin_host_write_in_sync_call`, and HTTP egress fails with
`plugin_egress_in_hot_path`. Post subscribers may use their granted egress and
state, but are capped by the one-second host deadline. A guest trap or pool
exhaustion is mapped by the host to the public error catalog; plugin-internal
`plugin_*` errors never cross the client boundary.

## Storage driver

```go
// package driver (internal/storage/driver)
package driver

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

type SchemaVersion uint64
type Hash string
type StoreID string

type Config struct {
	DSN     string
	Options map[string]any
}
type Info struct {
	Name         string
	Version      string
	Capabilities []string
}
type Meta struct {
	StoreID       StoreID
	Incarnation   string
	SchemaVersion SchemaVersion
	State         string
	Head          model.Revision
	HistoryFloor  model.Revision
}
type View struct {
	Branch      model.Branch
	At          *model.Revision
	Incarnation string // required; must match Meta.Incarnation
}
type Ref = model.Ref
type Record struct {
	Ref      Ref
	Kind     string
	Revision model.Revision
	Body     []byte
}
type Query struct {
	Filter map[string]any
	Cursor *Cursor
	Limit  int
	AsOf   *model.Revision
}
type PageReq struct {
	Cursor *Cursor
	Limit  int
}
type Page[T any] struct {
	Items      []T
	NextCursor *Cursor
	AsOf       model.Revision
}
type Version struct {
	Ref       Ref
	Revision  model.Revision
	Base      bool
	Patch     []byte
	Body      []byte
	CreatedAt time.Time
	Principal model.PrincipalRef
}
type DerivedTable string
type DerivedRow struct {
	Key   string
	Value []byte
}
type LockKeyKind string
type ReadFact string
type Sentinel string

const (
	LockRecord   LockKeyKind = "record"
	LockReadFact LockKeyKind = "read_fact"
	LockSentinel LockKeyKind = "sentinel"
	LockDerived  LockKeyKind = "derived"
)

// LockKey is the engine-neutral name for every item in the transaction lock
// set. Ref is used for records, ReadFact for recorded predicate/read facts,
// Sentinel for predicates, and DerivedRowKey for derived rows. Exactly one
// key form matches Kind. Drivers compare the tuple (kind-rank, encoded-key)
// bytewise.
type LockKey struct {
	Kind     LockKeyKind
	Ref      *Ref
	Fact     ReadFact
	Sentinel Sentinel
	Derived  *DerivedRowKey
}
type DerivedRowKey struct {
	Table DerivedTable
	Key   string
}
type LockRequest struct {
	Key  LockKey
	Mode LockMode
}
type LockState struct {
	Key      LockKey
	Revision model.Revision
	Record   *Record
	Derived  *DerivedRow
}
type LockSet []LockRequest

const (
	SentinelGraphBlocking Sentinel = "graph/blocking"
	SentinelOntology      Sentinel = "ontology"
	SentinelPlugins       Sentinel = "plugins"
)

const (
	DerivedReadyState      DerivedTable = "ready_state"
	DerivedAncestorClosure DerivedTable = "ancestor_closure"
	DerivedCutClosure      DerivedTable = "cut_closure"
	DerivedAttribute       DerivedTable = "derived_attribute"
	DerivedSortKey         DerivedTable = "sort_key"
	DerivedReferenceIndex  DerivedTable = "reference_index"
	DerivedGrant           DerivedTable = "grant"
	DerivedOntology        DerivedTable = "ontology"
	DerivedPlugins         DerivedTable = "plugins"
)

var (
	GraphBlockingSentinelLock = LockKey{Kind: LockSentinel, Sentinel: SentinelGraphBlocking}
	OntologySentinelLock      = LockKey{Kind: LockSentinel, Sentinel: SentinelOntology}
	PluginsSentinelLock       = LockKey{Kind: LockSentinel, Sentinel: SentinelPlugins}
)

func validateSentinelPart(name, value string) error {
	if value == "" || strings.TrimSpace(value) != value || strings.ContainsAny(value, "/\x00") {
		return fmt.Errorf("sentinel %s component is not canonical", name)
	}
	return nil
}

// NewSentinel is the sole boundary for values entering LockKey.Sentinel. It
// accepts the complete design census, including its parameterized forms, and
// refuses old aliases such as ready-set, closure, grant, and plugin.
func NewSentinel(raw string) (Sentinel, error) {
	s := Sentinel(raw)
	if err := ValidateSentinel(s); err != nil {
		return "", err
	}
	return s, nil
}

func NewBeadSentinel(id string) (Sentinel, error) {
	if err := validateSentinelPart("id", id); err != nil {
		return "", err
	}
	return NewSentinel("bead/" + id)
}

func NewReadySentinel(project, typ string) (Sentinel, error) {
	if err := validateSentinelPart("project", project); err != nil {
		return "", err
	}
	if err := validateSentinelPart("type", typ); err != nil {
		return "", err
	}
	return NewSentinel("ready/" + project + "/" + typ)
}

func NewClosureSentinel(id string) (Sentinel, error) {
	if err := validateSentinelPart("id", id); err != nil {
		return "", err
	}
	return NewSentinel("closure/" + id)
}

func NewGrantSentinel(scope string) (Sentinel, error) {
	if err := validateSentinelPart("scope", scope); err != nil {
		return "", err
	}
	return NewSentinel("grant/" + scope)
}

func ValidateSentinel(s Sentinel) error {
	raw := string(s)
	switch raw {
	case string(SentinelGraphBlocking), string(SentinelOntology), string(SentinelPlugins):
		return nil
	}
	if strings.HasPrefix(raw, "bead/") {
		return validateSentinelPart("id", strings.TrimPrefix(raw, "bead/"))
	}
	if strings.HasPrefix(raw, "ready/") {
		parts := strings.Split(raw, "/")
		if len(parts) != 3 {
			return fmt.Errorf("ready sentinel must be ready/<project>/<type>")
		}
		if err := validateSentinelPart("project", parts[1]); err != nil {
			return err
		}
		return validateSentinelPart("type", parts[2])
	}
	if strings.HasPrefix(raw, "closure/") {
		return validateSentinelPart("id", strings.TrimPrefix(raw, "closure/"))
	}
	if strings.HasPrefix(raw, "grant/") {
		return validateSentinelPart("scope", strings.TrimPrefix(raw, "grant/"))
	}
	return fmt.Errorf("unknown sentinel %q", raw)
}

func NewReadFact(raw string) (ReadFact, error) {
	fact := ReadFact(raw)
	if err := fact.Validate(); err != nil {
		return "", err
	}
	return fact, nil
}

func (fact ReadFact) Validate() error {
	if string(fact) == "" || strings.TrimSpace(string(fact)) != string(fact) || strings.ContainsRune(string(fact), '\x00') {
		return fmt.Errorf("read fact is not canonical")
	}
	return nil
}

func (key DerivedRowKey) Validate() error {
	switch key.Table {
	case DerivedReadyState, DerivedAncestorClosure, DerivedCutClosure, DerivedAttribute,
		DerivedSortKey, DerivedReferenceIndex, DerivedGrant, DerivedOntology, DerivedPlugins:
	default:
		return fmt.Errorf("unknown derived table %q", key.Table)
	}
	if key.Key == "" || strings.TrimSpace(key.Key) != key.Key || strings.ContainsRune(key.Key, '\x00') {
		return fmt.Errorf("derived row key is not canonical")
	}
	return nil
}

func (k LockKey) Validate() error {
	switch k.Kind {
	case LockRecord:
		if k.Ref == nil || k.Fact != "" || k.Sentinel != "" || k.Derived != nil {
			return fmt.Errorf("record lock key has multiple or missing forms")
		}
		return k.Ref.Validate()
	case LockReadFact:
		if k.Ref != nil || k.Fact == "" || k.Sentinel != "" || k.Derived != nil {
			return fmt.Errorf("read-fact lock key has multiple or missing forms")
		}
		return k.Fact.Validate()
	case LockSentinel:
		if k.Ref != nil || k.Fact != "" || k.Sentinel == "" || k.Derived != nil {
			return fmt.Errorf("sentinel lock key has multiple or missing forms")
		}
		return ValidateSentinel(k.Sentinel)
	case LockDerived:
		if k.Ref != nil || k.Fact != "" || k.Sentinel != "" || k.Derived == nil {
			return fmt.Errorf("derived lock key has multiple or missing forms")
		}
		return k.Derived.Validate()
	default:
		return fmt.Errorf("unknown lock key kind %q", k.Kind)
	}
}

func ValidateLockSet(keys LockSet) error {
	previous := ""
	for _, request := range keys {
		if err := request.Key.Validate(); err != nil {
			return err
		}
		if err := validateLockMode(request.Mode); err != nil {
			return err
		}
		current := lockKeySortKey(request.Key)
		if previous != "" && current <= previous {
			return fmt.Errorf("lock set is unsorted or contains a duplicate key")
		}
		previous = current
	}
	return nil
}

func validateLockMode(mode LockMode) error {
	switch mode {
	case LockShare, LockUpdate, LockSkipLocked:
		return nil
	default:
		return fmt.Errorf("unknown lock mode %q", mode)
	}
}

func lockKeySortKey(key LockKey) string {
	var rank string
	var value string
	switch key.Kind {
	case LockRecord:
		rank = "0"
		if key.Ref.IsID() {
			value = fmt.Sprintf("id:%s:%s", key.Ref.ID, key.Ref.Branch.String())
		} else {
			value = "key:" + key.Ref.Key
		}
	case LockReadFact:
		rank, value = "1", string(key.Fact)
	case LockSentinel:
		rank, value = "2", string(key.Sentinel)
	case LockDerived:
		rank, value = "3", string(key.Derived.Table)+":"+key.Derived.Key
	}
	return rank + ":" + value
}

// Cursor is the decoded driver form of the opaque RPC cursor. The binding
// fields prevent a cursor issued for another method, filter, principal, or
// ontology from being replayed against this reader.
type Cursor struct {
	Incarnation      string
	Revision         model.Revision
	Index            model.Natural
	Method           string
	FilterDigest     string
	PrincipalDigest  string
	OntologyRevision model.Revision
}

type ErrFenced struct {
	ExpectedEpoch uint64
	ObservedEpoch uint64
}

func (e ErrFenced) Error() string { return "driver lease fenced" }

type ErrTruncated struct {
	Floor model.Revision
}

func (e ErrTruncated) Error() string { return "history is truncated" }

type ErrIndeterminate struct {
	IdempotencyKey string
}

func (e ErrIndeterminate) Error() string { return "commit outcome indeterminate" }

type ErrIncarnationChanged struct {
	From string
	To   string
}

func (e ErrIncarnationChanged) Error() string { return "store incarnation changed" }

type LockMode string

const (
	LockShare      LockMode = "share"
	LockUpdate     LockMode = "update"
	LockSkipLocked LockMode = "skipLocked"
)

type ChangesetHeader struct {
	Branch          model.Branch
	Incarnation     string
	Kind            model.ChangesetKind
	Time            time.Time
	Principal       model.PrincipalRef
	Chain           []string
	IdempotencyKey  string
	Method          string // part of the full G4 scope
	AgentLabel      string // scoped only for anonymous principals
	RequestHash     string
	Response        model.Commit // top revision assigned at commit; row revisions preserved
	ResponsePending bool // keyed graph write with posts; false is immutable final
	Mutators        []string
	Changes         []model.ChangeRecord
	Events          []model.EventDescriptor // complete atomic plan; nil retains legacy indexes
	LockKeys        []LockKey // complete, sorted set acquired before any write
}

// Internal receipt metadata; ResponsePending never enters model.Commit or RPC.
// For no-op receipts Changeset is zero (Kind == ""); use Commit.Revision.
type IdempotencyRecord struct {
	RequestHash     string
	Commit          model.Commit
	Changeset       model.Changeset
	ResponsePending bool
}

// Optional WriteTx capability; absent capabilities return ErrUnsupported.
// No graph/log/head mutation; pending header is invalid and an existing
// pending receipt returns ErrConflict instead of an unfinished success.
type NoopCommitter interface {
	CommitNoop(context.Context, ChangesetHeader) (model.Commit, error)
}

// Optional store capability. Atomically lease/scope/revision-fenced: append
// the same post outcome once and clear pending, or leave the receipt intact.
// Nil and empty warning lists are equivalent. See receipt lifecycle below.
type ResponseFinalizer interface {
	FinalizeResponse(ctx context.Context, lease ActiveLease, principal, agent, method, key string, revision model.Revision, warnings []model.Warning) error
}
type StoreHealth struct {
	Driver               string
	Role                 string // active, standby, unknown
	ReplicationLag       time.Duration
	LastBackupCheckpoint time.Time
	LeaseEpoch           uint64
	Head                 model.Revision
	Incarnation          string
	Ontology             string
	PluginSet            string
	AuthorizationChain   []string
}
type MigrationReport struct {
	From, To SchemaVersion
	Applied  []string
	Warnings []string
}
type DumpOptions struct {
	History  string
	PageSize int
}

type Driver interface {
	Info() Info
	Open(context.Context, Config) (Store, error)
}
type Store interface {
	Meta(context.Context) (Meta, error)
	Migrate(context.Context, SchemaVersion) (MigrationReport, error)

	// Lease blocks until the holder is active or ctx ends. The active holder
	// is fenced by epoch in every write, heartbeat and checkpoint commit.
	Lease(ctx context.Context, holder string, ttl time.Duration) (ActiveLease, error)
	Health(context.Context) (StoreHealth, error)

	// Read never waits for writers. The returned Reader is bound to the
	// requested branch/as-of view.
	Read(context.Context, View) (Reader, error)
	Write(context.Context, ActiveLease) (WriteTx, error)
	Close() error
}
type ActiveLease interface {
	Epoch() uint64
	Renew(context.Context) error
	Lost() <-chan struct{}
	Release(context.Context) error
}
type Reader interface {
	Get(context.Context, []Ref) ([]Record, error)
	Query(context.Context, Query) (Page[Record], error)
	Versions(context.Context, Ref, PageReq) (Page[Version], error)
	Changesets(context.Context, *Cursor, int) ([]model.Changeset, error)
	ChangesetByKey(context.Context, string, string, string, string) (*model.Changeset, error)
	IdempotencyByKey(context.Context, string, string, string, string) (*IdempotencyRecord, error)
	Close() error
}
type WriteTx interface {
	Reader
	// keys must be globally sorted and duplicate-free. The returned state is
	// the revision observed while holding each lock.
	Lock(context.Context, []LockRequest) ([]LockState, error)
	Head(context.Context) (model.Revision, error)
	Put(context.Context, Record) error
	PutBlob(context.Context, []byte) (Hash, error)
	PutDerived(context.Context, DerivedTable, []DerivedRow, []DerivedRow) error
	Commit(context.Context, ChangesetHeader) (model.Changeset, error)
	Rollback(context.Context) error
}
```

The driver conformance contract is:

- **Lifetime:** callers close every reader and roll back or commit every
  transaction before closing the store. `Reader.Close` releases its snapshot
  resources; `Store.Close` releases the store's handles and active lease.
  Both are idempotent. `WriteTx.Close` is inherited from `Reader` and rolls
  back an unfinished transaction; after commit or rollback it is a no-op.
  No close operation commits staged writes.
- **G1:** revisions are gapless and commit ordered; the head row is taken last
  in Postgres and `BEGIN IMMEDIATE` provides the SQLite equivalent.
- **G2:** `Lock` accepts the complete sorted lock set: written refs, recorded
  read facts, predicate sentinels, and derived rows. The global order is
  `record < read_fact < sentinel < derived`, then the canonical encoded key;
  an unsorted or duplicate set is rejected. It returns the latest committed
  state and revision (`FOR SHARE`/`FOR UPDATE` on Postgres; the driver's
  transaction lock on SQLite; per-key locks in memory).
- **G3:** records, versions, blobs, derived rows, and the changeset commit
  atomically. Every `PutDerived` call must have the corresponding
  `LockDerived` rows in the lock set; sentinel revisions are bumped in the
  same transaction as the operation or derived-row write. The commit header
  repeats the exact lock-key set so conformance can verify the declaration.
- **G4:** `(principal, anonymous-agent label when applicable, method, key)` is
  unique for 24 hours and the key is retained on the changeset.
- **G5:** every write, heartbeat, and checkpoint commits only under the active
  lease epoch. A deposed holder receives `ErrFenced`; after `Lost()` closes,
  the server stops admitting reads and writes until a new lease is acquired.
  A cursor or `View.Incarnation` that differs from `Meta.Incarnation` is
  rejected before any read with `ErrIncarnationChanged`, never interpreted as
  a revision in the new incarnation. `ErrTruncated` reports the history floor.

**Receipt lifecycle.** Done = core can distinguish a durable committed pending
response from an immutable final response, proven here by the compiling API
and signature/response-shape checks; memory and pipeline must separately prove
overlap, failed finalization and lease/restart behavior against real storage.

- A keyed graph write that will run post subscribers publishes
  `ResponsePending: true` atomically with its initial response, graph and log.
  A pending header without an idempotency key is invalid. False means final
  immediately. Core no-ops never run posts: `CommitNoop` rejects a pending
  header, writes a final receipt, and returns `ErrConflict` if the matching
  existing receipt is pending. Ordinary writes never replace a live receipt.
- Pending lookup exposes the initial response, request hash and committed
  revision for digest checks and authorization, but core must never return it
  as success or repeat callbacks. A duplicate polls fresh pinned readers
  within a bounded, cancellable context; old snapshots remain unchanged.
  A lookup miss after observing pending never permits a second graph write.
- `FinalizeResponse` atomically checks the active lease, full G4 scope and
  `Commit.Revision`, appends post warnings once, and clears pending, including
  for an empty outcome. Nil and empty post-warning lists are equivalent.
  Initial warnings/results, per-result revisions, hash, receipt age and
  changeset are preserved. Inputs and returned receipts are isolated copies;
  graph rows, versions, log and head never change.
- A final response is immutable. Identical finalization succeeds without
  changing it; a different outcome or revision is `ErrConflict`. An initially
  final receipt accepts only an empty post outcome. Missing/expired scope is
  `ErrNotFound`; a stale/foreign lease is `ErrFenced`. All validation,
  cancellation and finalization errors publish nothing and preserve the
  receipt, including its pending state. Missing optional capabilities fail
  explicitly with `ErrUnsupported`.
- The original writer retries only its same known post outcome in a bounded
  detached finalization window, then reloads the final durable receipt for
  every success. Exhaustion, cancellation, fencing or restart without that
  outcome returns retryable `busy` with committed revision, idempotency key
  and `responsePending` details. It never returns unpersisted warnings as
  success, reruns posts, or revises the graph/log to resolve the response.
- Durable unresolved pending is an accepted recoverable committed outcome.
  Lease takeover preserves it; an active holder with the known outcome can
  retry finalization. Without that outcome it remains pending. This does not
  extend or reset G4's 24-hour window, alter digest/scope matching, or convert
  a committed graph write into an indeterminate graph outcome.

The sentinel catalog and bump rules are fixed for this contract:

| Sentinel | Read facts that record it | Operations or derived tables that bump it |
| --- | --- | --- |
| `bead/<id>` | point reads and `Slice.Beads` | every op targeting that bead: patch, slot verbs, comment, attach, setMeta, reparent, delete |
| `graph/blocking` | cycle and blocking-edge predicates, `no_open_blockers` | `link`/`unlink` of a blocking edge; delete of a bead with blocking edges |
| `ready/<project>/<type>` | a type's ready-set query; `claimNext` does not guard its selection query | status-category changes, blocking-edge changes, and `ready_state` rows in that scope |
| `closure/<id>` | `Slice.Closure(id, cut)`, for both ancestor and cut closures | create with a parent; reparent or restricted-toggle of the bead or an ancestor; delete; ancestor-closure, cut-closure, and derived-attribute rows touching that bead |
| `grant/<scope>` | `Slice.Grants`, including misses | `x.<alias>.grant.*` writes in that scope and their indexed grant rows |
| `ontology` | ontology revision and guard reads | ontology apply and `ontology` rows |
| `plugins` | plugin-set, lock, and binding reads | plugin apply and `plugins` rows |

The `DerivedTable` constants name logical row families for the lock boundary,
not physical SQL tables. Ancestor/cut closures and derived attributes bump
`closure/<id>`; `ready_state` rows bump `ready/<project>/<type>`. Sort keys and
the reference index still require derived-row locks and atomic `PutDerived`,
but bump no predicate sentinel because no guarded read depends on them.

The constructors `NewBeadSentinel`, `NewReadySentinel`, `NewClosureSentinel`,
and `NewGrantSentinel` are the only way to create parameterized keys. Every
sentinel passes `ValidateSentinel` before entering a `LockKey`, and
`ValidateLockSet` checks that each lock has exactly one typed key form. Empty,
slash-containing, whitespace-padded values and the old unparameterized aliases
are rejected before a driver sees them. This is the construction and
validation boundary for the complete census.

Each lock returns the current committed revision for its key. A bump writes the
new key revision in the same transaction and changeset as the operation or
`PutDerived` rows. Deferred fan-out acquires the same typed derived-row keys in
the follow-up `derived` changeset; it may lag ordering, but it cannot bypass
the lock set or revise a sentinel without recording that changeset.

The lease policy selected by P71 is a ten-second TTL and three-second renewal.
`ErrFenced`, `ErrTruncated`, `ErrIndeterminate`, and
`ErrIncarnationChanged` are driver errors with typed wire mappings in
[errors.md](errors.md); they are not additional JSON-RPC methods. A driver
must preserve the `ErrIndeterminate` idempotency key so the service can resolve
the outcome by key before retrying.

### Optional direct blocker explanation

`view.explain` may include the optional `directBlockers` result when a host
configures a protected ontology source locator. An absent member means the
capability is unavailable; an included result with `edges: []` and
`hiddenCount: 0` means the bounded evaluation completed and found no visible
edges. The consumer evaluates the producer verdict against the one reader
already pinned for `view.explain`, and applies the current read authorizer to
each resolved target using that target's stored project. A denied existing
target contributes only to `hiddenCount`; its identity, project, status and
reason are omitted. Missing, unresolved, unavailable-ontology and unknown
status links remain `unknown` and are never authorized as targets. The
protected locator is opt-in and must not guess an ontology ID or use a live
resolver. The result is capped at 100 edges; overflow and cancellation refuse
atomically with the existing `closure_too_large` and `busy` errors, without a
partial result.

The existing edge kind vocabulary includes `waits-for`. Its verdict evaluates
the stored optional all/any and `also_blocks` metadata against the complete
direct child fanout from that same pinned reader, using each child's own
project ontology. Absent metadata keeps the terminal-spawner/all-child legacy
fallback. An unavailable child ontology or unknown child status stays unknown;
child identities, projects and statuses are not added to the result. Target
redaction and the existing 100 direct-edge bound are unchanged.

## Deliberate boundaries

The sketches leave JSON-RPC wire types in `contracts/openrpc.json` and keep
storage implementation details behind `driver`. `pluginapi` may depend on
wire-safe model aliases, but plugin code must not receive a driver transaction,
SQL handle, filesystem path, process environment, or unmediated network
client.
