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
	Path    string         `json:"path"`
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Details map[string]any `json:"details,omitempty"`
}
type IntegrityParams struct {
	Revision Revision `json:"revision"`
	Project  string   `json:"project"`
}
type IntegrityProblem struct {
	Path    string         `json:"path"`
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Details map[string]any `json:"details,omitempty"`
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
	// DurableState is the plugin-owned unversioned persistence plane. It is
	// backed by the host's durable checkpoint capability; plugins never receive
	// a driver, transaction, SQL handle, or filesystem path.
	DurableState() DurableState
	HTTP() *http.Client                                  // host egress proxy; unavailable in hot calls
	Secret(context.Context, string) (SecretValue, error) // audited injection
}

// DurableState is a narrow JSON capability for plugin-owned operational state.
// Values are addressed by opaque keys and protected by per-key revisions. The
// host adapter persists the complete namespace with one checkpoint CAS, so a
// restart observes either the old map or the new map. List is bounded and
// returns detached values.
type DurableState interface {
	// Read returns one value and its namespace revision from the same
	// checkpoint snapshot. Providers must use this atomic read before a CAS;
	// separate Get and Revision calls can pair a stale value with a newer
	// generation during restart or takeover.
	Read(ctx context.Context, key string, dst any) (found bool, revision Revision, err error)
	Get(ctx context.Context, key string, dst any) (found bool, err error)
	Put(ctx context.Context, key string, value any, expect Revision) (Revision, error)
	Delete(ctx context.Context, key string, expect Revision) error
	Revision(ctx context.Context, key string) (Revision, error)
	List(ctx context.Context, prefix string, limit int) ([]DurableEntry, error)
}

type DurableEntry struct {
	Key      string          `json:"key"`
	Revision Revision        `json:"revision"`
	Value    json.RawMessage `json:"value"`
}

type StateStore interface {
	Get(ctx context.Context, key string, dst any) (found bool, err error)
	Put(ctx context.Context, key string, value any, expect Revision) (Revision, error)
	Delete(ctx context.Context, key string, expect Revision) error
	Revision(ctx context.Context, key string) (Revision, error)
}
type SecretValue struct {
	Name     string `json:"name"`
	Redacted bool   `json:"redacted"`
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
	Resource ResourceRef                `json:"resource"`
	Paths    map[string]json.RawMessage `json:"paths"`
}
type Verdict struct {
	Allow      bool        `json:"allow"`
	Violations []Violation `json:"violations,omitempty"`
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
	Filter map[string]any `json:"filter,omitempty"`
	Cursor *Cursor        `json:"cursor,omitempty"`
	Limit  int            `json:"limit,omitempty"`
}
type Cursor struct {
	// The JSON-RPC adapter encodes this bound value as the opaque cursor string;
	// plugins never choose or reinterpret the token.
	Incarnation      string   `json:"incarnation"`
	Revision         Revision `json:"revision"`
	Index            Natural  `json:"index"`
	After            string   `json:"after,omitempty"`
	Method           string   `json:"method"`
	FilterDigest     string   `json:"filterDigest"`
	PrincipalDigest  string   `json:"principalDigest"`
	OntologyRevision Revision `json:"ontologyRevision"`
}
type Page[T any] struct {
	Items      []T      `json:"items"`
	NextCursor *Cursor  `json:"nextCursor,omitempty"`
	AsOf       Revision `json:"asOf"`
}
type View struct {
	ID     string         `json:"id"`
	Type   string         `json:"type"`
	Fields map[string]any `json:"fields"`
}
type SubjectRef struct {
	Kind string `json:"kind"`
	ID   string `json:"id"`
}
type Grant struct {
	Subject SubjectRef `json:"subject"`
	Scope   Ref        `json:"scope"`
	Role    string     `json:"role"`
}

// A native or WASM plugin registers only methods under x.<alias>.*. Authz
// links and synchronous subscribers are compiled in for v1; mode-2 authz is
// not permitted.
type Method struct {
	Name       string                 `json:"name"`
	Params     any                    `json:"params"`
	Result     any                    `json:"result"`
	Permission string                 `json:"permission"`
	Resource   func(params any) []Ref `json:"-"`
}
type Registry interface{ Register(Method) error }
