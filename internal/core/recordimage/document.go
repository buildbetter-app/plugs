// Package recordimage holds the canonical stored wire shapes and pure helpers
// shared by the live pipeline and the deterministic receiving planner.
package recordimage

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

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
}

type MemoryDocument struct {
	ID       string         `json:"id"`
	Key      string         `json:"key"`
	Content  string         `json:"content"`
	Metadata map[string]any `json:"metadata,omitempty"`
	Overlay  *MemoryOverlay `json:"sourceOverlay,omitempty"`
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

type MemoryOverlay struct {
	Source         model.Ref          `json:"source"`
	SourceRevision model.Revision     `json:"sourceRevision"`
	SourceSHA256   string             `json:"sourceSha256"`
	Principal      model.PrincipalRef `json:"principal"`
	Time           time.Time          `json:"time"`
	Generation     uint64             `json:"generation"`
	Deleted        bool               `json:"deleted"`
}

// CreationTime returns the immutable host-supplied creation instant. A legacy
// body without createdAt has no known instant: neither its latest version time
// nor the time of a read or later edit is evidence of creation.
func (d Document) CreationTime() (time.Time, bool) {
	if d.CreatedAt == nil || d.CreatedAt.IsZero() {
		return time.Time{}, false
	}
	return d.CreatedAt.UTC(), true
}

func (d Document) ValidateLifecycleTimes() error {
	for _, value := range []struct {
		name string
		at   *time.Time
	}{{"createdAt", d.CreatedAt}, {"startedAt", d.StartedAt}, {"closedAt", d.ClosedAt}} {
		if value.at != nil && value.at.IsZero() {
			return fmt.Errorf("invalid bead %s", value.name)
		}
	}
	return nil
}
