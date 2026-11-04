package model

import (
	"encoding/json"
	"fmt"
	"unicode/utf8"
)

type Precondition struct {
	Ref          Ref       `json:"ref"`
	Revision     *Revision `json:"revision,omitempty"`
	Field        string    `json:"field,omitempty"`
	Hash         string    `json:"hash,omitempty"`
	hashSelected bool
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

// CloseOp enters the canonical close slot at this position in tx.apply.
// Expect observes the ordered in-request image, unlike ChangeSet.Expect,
// which observes the pinned committed snapshot.
type CloseOp struct {
	Kind   OpKind         `json:"kind"`
	Target Ref            `json:"target"`
	Reason string         `json:"reason,omitempty"`
	Expect []Precondition `json:"expect,omitempty"`
}
type ReparentOp struct {
	Kind    OpKind `json:"kind"`
	Target  Ref    `json:"target"`
	Parent  *Ref   `json:"parent,omitempty"`
	Project string `json:"project,omitempty"`
}

// LinkMetadata is the optional waits-for fanout predicate. The edge's To
// remains the spawner; metadata never introduces another endpoint.
type LinkMetadata struct {
	Gate       string `json:"gate,omitempty"`
	AlsoBlocks bool   `json:"also_blocks,omitempty"`
}

// Normalized returns a detached all-children/false default for explicit {}.
func (m LinkMetadata) Normalized() (LinkMetadata, error) {
	if m.Gate == "" {
		m.Gate = "all-children"
	}
	if m.Gate != "all-children" && m.Gate != "any-children" {
		return LinkMetadata{}, fmt.Errorf("unsupported waits-for gate %q", m.Gate)
	}
	return m, nil
}

func (m *LinkMetadata) UnmarshalJSON(data []byte) error {
	var value struct {
		Gate       *string `json:"gate,omitempty"`
		AlsoBlocks bool    `json:"also_blocks,omitempty"`
	}
	if err := decodeStrict(data, &value); err != nil {
		return err
	}
	next := LinkMetadata{Gate: "all-children", AlsoBlocks: value.AlsoBlocks}
	if value.Gate != nil {
		next.Gate = *value.Gate
		if next.Gate == "" {
			return fmt.Errorf("waits-for gate must not be empty")
		}
	}
	normalized, err := next.Normalized()
	if err != nil {
		return err
	}
	*m = normalized
	return nil
}

// LinkTargetKind selects the target identity domain. The zero value is local.
// External identities are opaque and are never resolved against local beads.
type LinkTargetKind string

const (
	LinkTargetLocal    LinkTargetKind = "local"
	LinkTargetExternal LinkTargetKind = "external"
)

// Normalized canonicalizes the local default to omission without changing IDs.
func (k LinkTargetKind) Normalized() (LinkTargetKind, error) {
	switch k {
	case "", LinkTargetLocal:
		return "", nil
	case LinkTargetExternal:
		return k, nil
	default:
		return "", fmt.Errorf("unsupported link target kind %q", k)
	}
}

func (k *LinkTargetKind) UnmarshalJSON(data []byte) error {
	var value string
	if err := decodeJSON(data, &value); err != nil {
		return err
	}
	if value != string(LinkTargetLocal) && value != string(LinkTargetExternal) {
		return fmt.Errorf("link target kind must be local or external")
	}
	*k = LinkTargetKind(value)
	return nil
}

// ValidateLinkTarget validates the explicit external arm without interpreting
// its ID. Local references retain the producer's ordinary resolution rules.
func ValidateLinkTarget(kind LinkTargetKind, target Ref) error {
	normalized, err := kind.Normalized()
	if err != nil {
		return err
	}
	if normalized == LinkTargetExternal {
		if err := target.Validate(); err != nil || !target.IsID() || target.ID == "" || !utf8.ValidString(target.ID) || !target.Branch.IsZero() {
			return fmt.Errorf("external link target requires a nonempty ID on branch zero")
		}
	}
	return nil
}

type LinkOp struct {
	TargetKind LinkTargetKind `json:"targetKind,omitempty"`
	Kind       OpKind         `json:"kind"`
	From       Ref            `json:"from"`
	To         Ref            `json:"to"`
	LinkKind   string         `json:"linkKind"`
	Metadata   *LinkMetadata  `json:"metadata,omitempty"`
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
	if err := ValidateLinkTarget(value.TargetKind, value.To); err != nil {
		return err
	}
	if value.Kind != OpLink && value.Kind != OpUnlink {
		return fmt.Errorf("link operation has kind %q", value.Kind)
	}
	if value.Metadata != nil {
		if value.Kind != OpLink || value.LinkKind != "waits-for" {
			return fmt.Errorf("metadata is only valid on a waits-for link")
		}
		_, err := value.Metadata.Normalized()
		return err
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
	var payloadKind OpKind
	switch {
	case o.Create != nil:
		payloadKind = o.Create.Kind
	case o.Patch != nil:
		payloadKind = o.Patch.Kind
	case o.Close != nil:
		payloadKind = o.Close.Kind
	case o.Reparent != nil:
		payloadKind = o.Reparent.Kind
	case o.Link != nil:
		payloadKind = o.Link.Kind
	case o.Comment != nil:
		payloadKind = o.Comment.Kind
	case o.Attach != nil:
		payloadKind = o.Attach.Kind
	case o.Delete != nil:
		payloadKind = o.Delete.Kind
	case o.SetMeta != nil:
		payloadKind = o.SetMeta.Kind
	}
	if payloadKind != "" && payloadKind != o.Kind {
		return nil, fmt.Errorf("operation kind %q does not match payload kind %q", o.Kind, payloadKind)
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
		var err error
		value.TargetKind, err = value.TargetKind.Normalized()
		if err != nil {
			return nil, err
		}
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
	decoded := Op{Kind: kind}
	switch kind {
	case OpCreate:
		var value CreateOp
		if err := decodeStrict(data, &value, "kind", "type", "fields"); err != nil {
			return err
		}
		if err := validateCreate(value); err != nil {
			return err
		}
		decoded.Create = &value
	case OpPatch:
		var value PatchOp
		if err := decodeStrict(data, &value, "kind", "target", "fields"); err != nil {
			return err
		}
		if err := validatePatch(value); err != nil {
			return err
		}
		decoded.Patch = &value
	case OpClose:
		var value CloseOp
		if err := decodeStrict(data, &value, "kind", "target"); err != nil {
			return err
		}
		if err := validateClose(value); err != nil {
			return err
		}
		decoded.Close = &value
	case OpReparent:
		var value ReparentOp
		if err := decodeStrict(data, &value, "kind", "target"); err != nil {
			return err
		}
		if err := validateReparent(value); err != nil {
			return err
		}
		decoded.Reparent = &value
	case OpLink, OpUnlink:
		var value LinkOp
		if err := decodeStrict(data, &value, "kind", "from", "to", "linkKind"); err != nil {
			return err
		}
		if err := validateLink(value); err != nil {
			return err
		}
		decoded.Link = &value
	case OpComment:
		var value CommentOp
		if err := decodeStrict(data, &value, "kind", "target", "body"); err != nil {
			return err
		}
		if err := validateComment(value); err != nil {
			return err
		}
		decoded.Comment = &value
	case OpAttach:
		var value AttachOp
		if err := decodeStrict(data, &value, "kind", "target", "attachment"); err != nil {
			return err
		}
		if err := validateAttach(value); err != nil {
			return err
		}
		decoded.Attach = &value
	case OpDelete:
		var value DeleteOp
		if err := decodeStrict(data, &value, "kind", "target"); err != nil {
			return err
		}
		if err := validateDelete(value); err != nil {
			return err
		}
		decoded.Delete = &value
	case OpSetMeta:
		var value SetMetaOp
		if err := decodeStrict(data, &value, "kind", "target", "key", "value"); err != nil {
			return err
		}
		if err := validateSetMeta(value); err != nil {
			return err
		}
		decoded.SetMeta = &value
	default:
		return fmt.Errorf("unknown operation kind %q", kind)
	}
	*o = decoded
	return nil
}
