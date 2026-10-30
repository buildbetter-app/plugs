package pipeline

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

// Document is the stored bead body. Fields are ontology data; Metadata is
// separate plugin data. Lease, comments, links and attachments are core owned.
type Document struct {
	ID          string                     `json:"id"`
	Type        string                     `json:"type"`
	Project     string                     `json:"project,omitempty"`
	Fields      map[string]any             `json:"fields"`
	Metadata    map[string]json.RawMessage `json:"metadata,omitempty"`
	Labels      []string                   `json:"labels,omitempty"`
	Comments    []Comment                  `json:"comments,omitempty"`
	Links       []Link                     `json:"links,omitempty"`
	Attachments []model.Attachment         `json:"attachments,omitempty"`
	Lease       *ClaimLease                `json:"lease,omitempty"`
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

type Comment struct {
	Body      string             `json:"body"`
	Principal model.PrincipalRef `json:"principal"`
	Time      time.Time          `json:"time"`
}

type Link struct {
	To   model.Ref `json:"to"`
	Kind string    `json:"kind"`
}

type ClaimLease struct {
	Principal model.PrincipalRef `json:"principal"`
	Agent     string             `json:"agent,omitempty"`
	ExpiresAt time.Time          `json:"expiresAt"`
	TtlMs     model.Natural      `json:"ttlMs"`
	Epoch     model.Natural      `json:"epoch"`
}

// DecodeDocument preserves JSON numbers instead of rounding fields to float64.
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
	return d, nil
}
