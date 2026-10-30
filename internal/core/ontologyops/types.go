// Package ontologyops connects protected stored ontology sources to the pure
// compiler and migration planner. It has no write or apply capability.
package ontologyops

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/ontologymigration"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// ReadStore deliberately excludes Store.Write. The composition root owns the
// active lease, protected source locator, authorizer and actual pre inventory.
type ReadStore interface {
	Meta(context.Context) (driver.Meta, error)
	Read(context.Context, driver.View) (driver.Reader, error)
}

// AuthorizeRead is optional only when no authorization plugin is installed.
// Planning requires access to every row, rather than an incomplete filtered
// inventory. Historical reads use this same current authorization callback.
type AuthorizeRead func(context.Context, model.PrincipalRef, model.ResourceRef, string) error

type Config struct {
	Source        service.OntologySourceLocator
	AuthorizeRead AuthorizeRead
	ValidatingPre ontologymigration.ValidatingPre
	// MaxPlans bounds retained immutable reports. Evicted plans are stale.
	MaxPlans int
}

type Service struct {
	store ReadStore
	lease driver.ActiveLease
	cfg   Config
	mu    sync.Mutex
	plans map[string]retainedPlan
	order []string
}

type Problem struct {
	Path    string         `json:"path"`
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Details map[string]any `json:"details,omitempty"`
}

type Result struct {
	Revision model.Revision     `json:"revision"`
	Document *ontology.Document `json:"document,omitempty"`
	Problems []Problem          `json:"problems"`
	Drifted  bool               `json:"drifted"`
}

type CheckResult struct {
	OK       bool      `json:"ok"`
	Problems []Problem `json:"problems"`
	Drifted  bool      `json:"drifted"`
}

type PlanRequest struct {
	Project  string
	Document json.RawMessage
	Expect   []model.Precondition
}

// SourceGuard binds exact stored bytes as well as the compiled interpretation.
type SourceGuard struct {
	Ref      model.Ref      `json:"ref"`
	Revision model.Revision `json:"revision"`
	BodyHash string         `json:"bodyHash"`
	Digest   string         `json:"digest"`
}

// PlannedRevision is detached when returned. Its digest also binds the caller,
// store identity, project, explicit CAS and real complete planner report.
type PlannedRevision struct {
	Digest       string                   `json:"planDigest"`
	StoreID      driver.StoreID           `json:"storeId"`
	Incarnation  string                   `json:"incarnation"`
	BaseRevision model.Revision           `json:"baseRevision"`
	Principal    model.PrincipalRef       `json:"principal"`
	Project      string                   `json:"project,omitempty"`
	Source       SourceGuard              `json:"source"`
	Document     service.OntologySource   `json:"document"`
	Expect       []model.Precondition     `json:"expect"`
	Migration    ontologymigration.Report `json:"migration"`
	Ready        bool                     `json:"ready"`
}

type PlanResult struct {
	PlanDigest    string            `json:"planDigest"`
	BaseRevision  model.Revision    `json:"baseRevision"`
	Changes       PlannedRevision   `json:"changes"`
	Violations    []model.Violation `json:"violations"`
	Warnings      []model.Warning   `json:"warnings"`
	RequiresHuman bool              `json:"requiresHuman"`
}

type HistoryResult struct {
	Revisions []model.Changeset `json:"revisions"`
}

type DiffResult struct {
	FromRevision model.Revision `json:"fromRevision"`
	ToRevision   model.Revision `json:"toRevision"`
	Patch        []Patch        `json:"patch"`
}

// Patch is an RFC 6902 operation over the effective ontology document.
type Patch struct {
	Op    string             `json:"op"`
	Path  string             `json:"path"`
	Value *ontology.Document `json:"value,omitempty"`
}

type retainedPlan struct {
	bytes []byte
}
