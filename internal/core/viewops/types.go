// Package viewops executes the bounded, read-only A7 view surface.
//
// The package owns no storage transaction and no authorization policy. The
// composition root supplies a read-only store, the active lease, the current
// read authorizer, and (optionally) a saved-view reader.
package viewops

import (
	"context"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

const (
	// OrdinaryCandidateCap is the maximum number of candidates that remain
	// after current read authorization for an ordinary view.
	OrdinaryCandidateCap = 20_000
	// TypeSummaryCandidateCap is the narrow #20 exception. It applies only to
	// an explicitly present fields=[] type-count summary.
	TypeSummaryCandidateCap = 100_000
	MaxGroups               = 100
	DefaultOutputBudget     = 100
)

// ReadStore is deliberately narrower than driver.Store. View execution never
// receives a write transaction.
type ReadStore interface {
	Meta(context.Context) (driver.Meta, error)
	Read(context.Context, driver.View) (driver.Reader, error)
}

// AuthorizeRead is the core read-authorization seam. A nil callback means the
// host has no authorization plugin and all authenticated reads are allowed.
// A forbidden/not_found model error denies only that candidate; all other
// errors fail closed and abort the view. The project argument is the exact
// stored body.project value, never an inferred parent or caller default.
type AuthorizeRead func(context.Context, model.PrincipalRef, model.ResourceRef, string) error

// ViewReader resolves saved definitions for read-only execution. Save/delete
// are intentionally not part of this package until their protected write path
// is implemented.
type ViewReader interface {
	GetView(context.Context, model.PrincipalRef, string) (Definition, error)
}

type Config struct {
	AuthorizeRead    AuthorizeRead
	Views            ViewReader
	OntologyRevision model.Revision
}

type Service struct {
	store ReadStore
	lease driver.ActiveLease
	cfg   Config
}

// Filter is the canonical shared filter shape. The query member uses the
// admitted equality grammar implemented by this slice; unsupported grammar is
// refused explicitly.
type Filter struct {
	Query     string   `json:"query,omitempty"`
	Resources []string `json:"resources,omitempty"`
	Types     []string `json:"types,omitempty"`
	Events    []string `json:"events,omitempty"`
	ViewID    string   `json:"viewId,omitempty"`
}

// Definition preserves presence for fields and budgetTokens through nil
// slices/pointers. In particular, non-nil empty Fields is the explicit
// fields=[] required by decision #20.
type Definition struct {
	Name         string         `json:"name,omitempty"`
	Filter       *Filter        `json:"filter"`
	Fields       []string       `json:"fields,omitempty"`
	GroupBy      []string       `json:"groupBy,omitempty"`
	Sort         []string       `json:"sort,omitempty"`
	BudgetTokens *model.Natural `json:"budgetTokens,omitempty"`
}

type AsOf struct {
	Revision    model.Revision `json:"revision"`
	Incarnation string         `json:"incarnation,omitempty"`
}

// Cursor is trusted only after viewrpc verifies its signature. Core still
// checks every binding field before using the position.
type Cursor struct {
	Version          int            `json:"version"`
	Method           string         `json:"method"`
	PrincipalDigest  string         `json:"principal"`
	DefinitionDigest string         `json:"definition"`
	ViewID           string         `json:"viewId,omitempty"`
	OntologyRevision model.Revision `json:"ontology"`
	Incarnation      string         `json:"incarnation"`
	Revision         model.Revision `json:"revision"`
	Index            uint64         `json:"index"`
}

type RunRequest struct {
	ViewID     string
	Definition *Definition
	Cursor     *Cursor
	AsOf       *AsOf
	Project    string
	Agent      string
}

type Bead struct {
	ID               string         `json:"id"`
	Type             string         `json:"type"`
	Project          string         `json:"project,omitempty"`
	Revision         model.Revision `json:"revision"`
	Status           string         `json:"status,omitempty"`
	Fields           map[string]any `json:"fields,omitempty"`
	Metadata         map[string]any `json:"metadata,omitempty"`
	Parent           string         `json:"parent,omitempty"`
	Labels           []string       `json:"labels,omitempty"`
	Assignee         string         `json:"assignee,omitempty"`
	HiddenBlockers   int            `json:"hiddenBlockers,omitempty"`
	ReadinessPending bool           `json:"readinessPending,omitempty"`
}

type Elision struct {
	Count         int
	Reason        string
	ContinueIndex uint64
}

type Group struct {
	Key    string
	Count  int
	Items  []Bead
	Elided *Elision
}

type Result struct {
	Items      []Bead
	NextCursor *Cursor
	AsOf       AsOf
	Groups     []Group
}

type ExplainRequest struct {
	ViewID  string
	ItemID  string
	Project string
	Agent   string
}

type ExplainResult struct {
	ViewID         string   `json:"viewId"`
	ItemID         string   `json:"itemId"`
	Included       bool     `json:"included"`
	Reasons        []string `json:"reasons"`
	HiddenBlockers int      `json:"hiddenBlockers,omitempty"`
}
