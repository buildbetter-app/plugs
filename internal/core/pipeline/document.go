package pipeline

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// Document is the stored bead body. Fields are ontology data; Metadata is
// separate plugin data. Parent, lease, comments, links and attachments are core owned.
type Document struct {
	ID      string     `json:"id"`
	Type    string     `json:"type"`
	Project string     `json:"project,omitempty"`
	Parent  *model.Ref `json:"parent,omitempty"`
	// Imported hierarchy has one parent edge, without a duplicate generic link.
	ParentMetadata *model.LinkMetadata        `json:"parentMetadata,omitempty"`
	Fields         map[string]any             `json:"fields"`
	Metadata       map[string]json.RawMessage `json:"metadata,omitempty"`
	Labels         []string                   `json:"labels,omitempty"`
	Comments       []Comment                  `json:"comments,omitempty"`
	Links          []Link                     `json:"links,omitempty"`
	Attachments    []model.Attachment         `json:"attachments,omitempty"`
	Lease          *ClaimLease                `json:"lease,omitempty"`
	// Core timestamps are outside ontology fields and cannot be patched by a
	// request or subscriber. Missing values preserve legacy unknown instants.
	CreatedAt *time.Time `json:"createdAt,omitempty"`
	StartedAt *time.Time `json:"startedAt,omitempty"`
	ClosedAt  *time.Time `json:"closedAt,omitempty"`
	// CloseReason records the first typed close, including an explicit empty
	// reason. Comments can carry later settlement text without replacing it.
	CloseReason *string `json:"closeReason,omitempty"`
}

// MemoryDocument is the canonical body for a core memory record. Memories
// share the Writer and driver record envelope with beads, but do not enter the
// bead ontology or graph. Keeping their body separate prevents memory fields
// from being mistaken for bead ontology data.
type MemoryDocument struct {
	ID       string         `json:"id"`
	Key      string         `json:"key"`
	Content  string         `json:"content"`
	Metadata map[string]any `json:"metadata,omitempty"`
	Overlay  *MemoryOverlay `json:"sourceOverlay,omitempty"`
}

func DecodeMemoryDocument(body []byte) (MemoryDocument, error) {
	var d MemoryDocument
	if err := decode(body, &d); err != nil {
		return MemoryDocument{}, err
	}
	if d.ID == "" || d.Key == "" {
		return MemoryDocument{}, fmt.Errorf("memory body requires id and key")
	}
	return d, nil
}

// MemorySource describes a validated retained source envelope. It is read-only:
// serializing it as a MemoryDocument would discard its source and classification.
type MemorySource struct {
	Legacy  bool
	Visible bool
	Project string
	Fields  map[string]any
	Record  driver.Record
	Archive string
}

// DecodeMemoryRecord shares the exact imported-memory boundary between public
// reads and writes. Only validated secret/non-string mirrors may be hidden;
// malformed envelopes remain errors, including when scanning an unrelated key.
func DecodeMemoryRecord(record driver.Record) (MemoryDocument, *MemorySource, error) {
	var raw map[string]any
	if err := decode(record.Body, &raw); err != nil {
		return MemoryDocument{}, nil, err
	}
	metadata, envelope := raw["metadata"].(map[string]any)
	_, hasID := raw["id"]
	_, hasContent := raw["content"]
	_, hasMetadata := raw["metadata"]
	_, hasSource := raw["source_id"]
	_, hasTable := raw["source_table"]
	_, hasProject := raw["source_project"]
	_, hasValue := raw["value"]
	_, hasClass := raw["classification"]
	if !(hasSource || hasTable || hasProject || hasValue || hasClass) || !(hasID || hasContent || hasMetadata) {
		if importedIneligibleMemory(record, raw) {
			return MemoryDocument{ID: record.Ref.ID, Key: raw["key"].(string)}, &MemorySource{}, nil
		}
		document, err := DecodeMemoryDocument(record.Body)
		if err != nil {
			return MemoryDocument{}, nil, err
		}
		if record.Kind != "memory" || document.ID != record.Ref.ID {
			return MemoryDocument{}, nil, fmt.Errorf("invalid persisted memory")
		}
		if err := validateMemoryOverlay(record, raw, document); err != nil {
			return MemoryDocument{}, nil, err
		}
		if source, imported, err := legacyMemorySource(record, document); imported || err != nil {
			if err == nil {
				// Old canonical mirrors stored a trimmed key. Their validated
				// provenance retains the exact source identity; expose that key
				// without rewriting immutable imported bytes or versions.
				document.Key = strings.TrimPrefix(record.Ref.ID, "source/memory/")
			}
			return document, source, err
		}
		return document, nil, nil
	}
	invalid := func() (MemoryDocument, *MemorySource, error) {
		return MemoryDocument{}, nil, fmt.Errorf("invalid joint source memory")
	}
	id, idOK := raw["source_id"].(string)
	project, projectOK := raw["source_project"].(string)
	source, sourceOK := raw["source"].(map[string]any)
	classification, classOK := raw["classification"].(string)
	metadataClass, metadataClassOK := metadata["beads.classification"].(string)
	if record.Kind != "memory" || !record.Ref.Branch.IsZero() || !idOK || id == "" || !projectOK || !sourceOK || !envelope || !classOK || !metadataClassOK || classification != metadataClass || raw["source_table"] != "config" || len(source) != 2 || len(raw) != 10 || len(metadata) != 2 || raw["key"] != id || source["key"] != "kv.memory."+id {
		return invalid()
	}
	wantID := driver.SourcePlaneMirrorIDV1("memory", project, "config", id)
	if record.Ref.ID != wantID || raw["id"] != wantID {
		return invalid()
	}
	value, present := raw["value"]
	sourceValue, sourcePresent := source["value"]
	a, errA := json.Marshal(value)
	b, errB := json.Marshal(sourceValue)
	if !present || !sourcePresent || errA != nil || errB != nil || !bytes.Equal(a, b) {
		return invalid()
	}
	content, text := value.(string)
	encoding := "text"
	if !text {
		content, encoding = string(a), "json"
	}
	if raw["content"] != content || metadata["beads.memory.value_encoding"] != encoding {
		return invalid()
	}
	if classification == "secret" {
		secret, ok := value.(map[string]any)
		if !ok || len(secret) != 1 || secret["$secret"] != "config/kv.memory."+id {
			return invalid()
		}
	} else if classification != "public" {
		return invalid()
	}
	return MemoryDocument{ID: wantID, Key: id, Content: content, Metadata: metadata}, &MemorySource{Visible: classification == "public" && text, Project: project, Fields: source}, nil
}

func importedIneligibleMemory(record driver.Record, raw map[string]any) bool {
	if record.Kind != "memory" {
		return false
	}
	id, ok := raw["source_id"].(string)
	if !ok || id == "" || raw["key"] != id || raw["source_table"] != "config" || !record.Ref.Branch.IsZero() {
		return false
	}
	wantID := "source/memory/" + id
	if project, present := raw["source_project"]; present {
		name, ok := project.(string)
		if !ok || len(raw) != 7 {
			return false
		}
		wantID = driver.SourcePlaneMirrorIDV1("memory", name, "config", id)
	} else if len(raw) != 6 {
		return false
	}
	if record.Ref.ID != wantID {
		return false
	}
	source, ok := raw["source"].(map[string]any)
	if !ok || len(source) != 2 || source["key"] != "kv.memory."+id {
		return false
	}
	value, present := raw["value"]
	sourceValue, sourcePresent := source["value"]
	a, errA := json.Marshal(value)
	b, errB := json.Marshal(sourceValue)
	if !present || !sourcePresent || errA != nil || errB != nil || !bytes.Equal(a, b) {
		return false
	}
	if raw["classification"] == "secret" {
		ref, ok := value.(map[string]any)
		return ok && len(ref) == 1 && ref["$secret"] == "config/kv.memory."+id
	}
	_, text := value.(string)
	return raw["classification"] == "public" && !text
}

type Comment struct {
	Body         string             `json:"body"`
	Principal    model.PrincipalRef `json:"principal"`
	Time         time.Time          `json:"time"`
	Origin       string             `json:"origin,omitempty"`
	SourceAuthor string             `json:"sourceAuthor,omitempty"`
	SourceTime   *time.Time         `json:"sourceTime,omitempty"`
}

type Link struct {
	TargetKind model.LinkTargetKind `json:"targetKind,omitempty"`
	To         model.Ref            `json:"to"`
	Kind       string               `json:"kind"`
	Metadata   *model.LinkMetadata  `json:"metadata,omitempty"`
}

type ClaimLease struct {
	Principal model.PrincipalRef `json:"principal"`
	Agent     string             `json:"agent,omitempty"`
	ExpiresAt time.Time          `json:"expiresAt"`
	TtlMs     model.Natural      `json:"ttlMs"`
	Epoch     model.Natural      `json:"epoch"`
}

// DecodeDocument preserves JSON numbers and resolves hierarchy in memory without
// rewriting the stored body.
func DecodeDocument(body []byte) (Document, error) {
	var d Document
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	if err := dec.Decode(&d); err != nil {
		return d, err
	}
	if d.ID == "" || d.Type == "" || d.Fields == nil {
		return d, fmt.Errorf("invalid bead document")
	}
	if err := d.validateLifecycleTimes(); err != nil {
		return d, err
	}
	if err := validateLifecycleTimeMembers(body); err != nil {
		return d, err
	}
	if err := validateParent(d.Parent); err != nil {
		return d, failure("invalid_params", err.Error())
	}
	for i := range d.Links {
		link := &d.Links[i]
		if err := model.ValidateLinkTarget(link.TargetKind, link.To); err != nil {
			return d, err
		}
		link.TargetKind, _ = link.TargetKind.Normalized()
	}
	parent, err := HierarchyParent(d)
	if err != nil {
		return d, err
	}
	d.Parent = parent
	for _, comment := range d.Comments {
		if err := comment.Validate(); err != nil {
			return d, err
		}
	}
	return d, nil
}

func validateParent(parent *model.Ref) error {
	if parent == nil {
		return nil
	}
	if err := parent.Validate(); err != nil || !parent.IsID() || parent.ID == "" || !parent.Branch.IsZero() {
		return fmt.Errorf("stored parent must be a canonical ID on branch zero")
	}
	_, err := driver.NewClosureSentinel(parent.ID)
	return err
}

// NewImportedComment accepts only the binding of a previously admitted host
// restore. It does not authenticate a caller or accept ordinary RPC input.
func NewImportedComment(profile driver.RestoreImportProfile, body, sourceAuthor string, sourceTime, restoreTime time.Time) (Comment, error) {
	bound, err := profile.Detached()
	if err != nil {
		return Comment{}, err
	}
	author, err := NormalizeSourceAuthor(sourceAuthor)
	if err != nil {
		return Comment{}, err
	}
	if sourceTime.IsZero() || restoreTime.IsZero() {
		return Comment{}, fmt.Errorf("imported comment requires source and restore times")
	}
	return Comment{Body: body, Principal: bound.Admission.Actor, Time: restoreTime, Origin: "import", SourceAuthor: author, SourceTime: &sourceTime}, nil
}

func NormalizeSourceAuthor(author string) (string, error) {
	if !utf8.ValidString(author) {
		return "", fmt.Errorf("source author must be valid Unicode")
	}
	author = strings.TrimSpace(author)
	if utf8.RuneCountInString(author) > 256 {
		return "", fmt.Errorf("source author exceeds 256 Unicode scalars")
	}
	for _, r := range author {
		if unicode.IsControl(r) || unicode.In(r, unicode.Cf) {
			return "", fmt.Errorf("source author contains a control or format character")
		}
	}
	return author, nil
}

func (c Comment) Validate() error {
	if c.Origin == "" {
		if c.SourceAuthor != "" || c.SourceTime != nil {
			return fmt.Errorf("legacy comment cannot carry import provenance")
		}
		return nil
	}
	author, err := NormalizeSourceAuthor(c.SourceAuthor)
	if err != nil {
		return err
	}
	if c.Origin != "import" || c.SourceTime == nil || c.SourceTime.IsZero() || c.Time.IsZero() || author != c.SourceAuthor {
		return fmt.Errorf("invalid imported comment provenance")
	}
	_, err = json.Marshal(c.Principal)
	return err
}
