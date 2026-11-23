package viewops

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"strings"

	coreevents "github.com/buildbetter-app/plugs/internal/core/events"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func New(store ReadStore, lease driver.ActiveLease, cfg Config) (*Service, error) {
	if store == nil || lease == nil {
		return nil, fmt.Errorf("view operations require a read store and active lease")
	}
	if cfg.AuthorizeReadBatch != nil && cfg.AuthorizeRead == nil {
		return nil, fmt.Errorf("batch read authorization requires the ordinary read authorizer")
	}
	if err := cfg.OntologyRevision.Validate(); err != nil {
		return nil, fmt.Errorf("invalid ontology revision: %w", err)
	}
	if cfg.EpicProgress != nil {
		if cfg.EpicProgress.AnonymousOnly && (cfg.AuthorizeRead != nil || cfg.ParentProjection != nil || cfg.EpicProgress.ParentProjection != nil) {
			return nil, fmt.Errorf("anonymous epic progress cannot be combined with read authorization")
		}
		if cfg.EpicProgress.ValidateAuthority == nil || cfg.EpicProgress.AuthorizeScope == nil || cfg.EpicProgress.OntologySource == nil {
			return nil, fmt.Errorf("epic progress requires current authority, scope authorization and durable ontology source")
		}
		copy := *cfg.EpicProgress
		cfg.EpicProgress = &copy
		if cfg.ParentProjection == nil {
			cfg.ParentProjection = copy.ParentProjection
		}
	}
	if cfg.ParentProjection != nil {
		if cfg.ParentProjection.Project == nil || cfg.ParentProjection.ValidateAuthority == nil {
			return nil, fmt.Errorf("parent projection requires current authority and a final fence")
		}
		copy := *cfg.ParentProjection
		cfg.ParentProjection = &copy
	}
	return &Service{store: store, lease: lease, cfg: cfg}, nil
}

func failure(code model.ErrorCode, hint string) error {
	return &model.Error{Data: model.ErrorData{Code: code, Hint: hint, Retryable: code == "busy"}}
}

func mapped(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		// A cancellation match in one branch must not erase another cause.
		// Preserve the original tree when its leaves disagree or include a
		// separate failure; callers still need that failure's identity.
		if cancellationCause(err) == nil {
			return err
		}
		return failure("busy", "view read was canceled or exceeded its deadline")
	}
	if code, details, ok := driver.ErrorMapping(err); ok {
		return &model.Error{Data: model.ErrorData{Code: code, Details: details}}
	}
	return err
}

// cancellationCause retains a linear wrapper's context classification, which
// can normalize its underlying network timeout. Every branch of a joined error
// must independently carry that same cause; wrapping cannot hide another error.
func cancellationCause(err error) error {
	canceled, deadline := errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded)
	if canceled == deadline {
		return nil
	}
	kind := context.DeadlineExceeded
	if canceled {
		kind = context.Canceled
	}
	for cause := err; cause != nil; cause = errors.Unwrap(cause) {
		if joined, ok := cause.(interface{ Unwrap() []error }); ok {
			branches := joined.Unwrap()
			if len(branches) == 0 {
				return nil
			}
			for _, branch := range branches {
				if cancellationCause(branch) != kind {
					return nil
				}
			}
			return kind
		}
	}
	return kind
}

func (s *Service) admit(ctx context.Context, by model.PrincipalRef) error {
	if err := ctx.Err(); err != nil {
		return mapped(err)
	}
	if _, err := driver.PrincipalKey(by); err != nil {
		return failure("invalid_params", "invalid principal")
	}
	select {
	case <-s.lease.Lost():
		return failure("busy", "active storage lease was lost")
	default:
		return nil
	}
}

func (s *Service) reader(ctx context.Context, by model.PrincipalRef, at *AsOf) (driver.Meta, driver.Reader, error) {
	if err := s.admit(ctx, by); err != nil {
		return driver.Meta{}, nil, err
	}
	meta, err := s.store.Meta(ctx)
	if err != nil {
		return driver.Meta{}, nil, mapped(err)
	}
	view := driver.View{Incarnation: meta.Incarnation}
	if at != nil {
		if err := at.Revision.Validate(); err != nil {
			return driver.Meta{}, nil, failure("cursor_invalid", "view snapshot revision is invalid")
		}
		view.At = &at.Revision
		if at.Incarnation != "" {
			view.Incarnation = at.Incarnation
		}
	}
	r, err := s.store.Read(ctx, view)
	if err != nil {
		return driver.Meta{}, nil, mapped(err)
	}
	if err := s.admit(ctx, by); err != nil {
		_ = r.Close()
		return driver.Meta{}, nil, err
	}
	if r.Snapshot().At == nil {
		_ = r.Close()
		return driver.Meta{}, nil, fmt.Errorf("driver returned an unpinned view snapshot")
	}
	return meta, r, nil
}

type stored struct {
	record driver.Record
	doc    pipeline.Document
}

func scan(ctx context.Context, r driver.Reader) ([]stored, error) {
	var cursor *driver.Cursor
	rows := make([]stored, 0)
	for {
		page, err := r.Query(ctx, driver.Query{Filter: map[string]any{"kind": "bead"}, Cursor: cursor, Limit: 1000})
		if err != nil {
			return nil, mapped(err)
		}
		if page.AsOf.String() != r.Snapshot().At.String() {
			return nil, failure("stale_snapshot", "driver returned a page from another snapshot")
		}
		for _, record := range page.Items {
			if err := record.Ref.Validate(); err != nil || !record.Ref.IsID() || !record.Ref.Branch.IsZero() || record.Kind != "bead" {
				return nil, failure("internal_error", "invalid persisted bead identity")
			}
			doc, err := decodeDocument(record.Body)
			if err != nil || doc.ID != record.Ref.ID {
				return nil, failure("internal_error", "invalid persisted bead document")
			}
			rows = append(rows, stored{record: record, doc: doc})
		}
		if page.NextCursor == nil {
			return rows, nil
		}
		if len(page.Items) == 0 {
			return nil, failure("internal_error", "driver returned a non-advancing view page")
		}
		cursor = page.NextCursor
	}
}

func decodeDocument(raw []byte) (pipeline.Document, error) {
	var doc pipeline.Document
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.UseNumber()
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&doc); err != nil || doc.ID == "" || doc.Type == "" || doc.Fields == nil {
		return pipeline.Document{}, fmt.Errorf("unsupported bead document shape")
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return pipeline.Document{}, fmt.Errorf("bead document has trailing data")
	}
	return doc, nil
}

func isDenied(err error) bool {
	var typed *model.Error
	if !errors.As(err, &typed) {
		return false
	}
	return typed.Data.Code == "forbidden" || typed.Data.Code == "not_found"
}

func (s *Service) authorize(ctx context.Context, by model.PrincipalRef, row stored) (bool, error) {
	return s.authorizeRef(ctx, by, row.record.Ref, row.doc.Project)
}

func (s *Service) authorizeRef(ctx context.Context, by model.PrincipalRef, ref model.Ref, project string) (bool, error) {
	if s.cfg.AuthorizeRead == nil {
		return true, nil
	}
	err := s.cfg.AuthorizeRead(ctx, by, model.ResourceRef{Kind: "bead", Ref: ref}, project)
	if err == nil {
		return true, nil
	}
	if isDenied(err) {
		return false, nil
	}
	return false, err
}

func (s *Service) resolve(ctx context.Context, by model.PrincipalRef, id string, definition *Definition) (Definition, error) {
	if (id == "") == (definition == nil) {
		return Definition{}, failure("invalid_params", "supply exactly one of viewId or definition")
	}
	if id != "" {
		if s.cfg.Views == nil {
			return Definition{}, failure("not_found", "saved view reads are not configured")
		}
		resolved, err := s.cfg.Views.GetView(ctx, by, id)
		if err != nil {
			return Definition{}, err
		}
		definition = &resolved
	}
	if definition == nil {
		return Definition{}, failure("invalid_params", "view definition is required")
	}
	copy := cloneDefinition(*definition)
	if err := validateDefinition(copy); err != nil {
		return Definition{}, err
	}
	return copy, nil
}

// WatchBinding compiles a saved definition into the exact filter vocabulary
// understood by the event reader. A view watch is an event subscription with
// view membership; it does not reimplement view projection or grouping in the
// watch manager. Shapes that cannot be represented without changing event
// membership are refused explicitly instead of being approximated.
func (s *Service) WatchBinding(ctx context.Context, by model.PrincipalRef, id string) (coreevents.ViewBinding, error) {
	if err := s.admit(ctx, by); err != nil {
		return coreevents.ViewBinding{}, err
	}
	if strings.TrimSpace(id) == "" || id != strings.TrimSpace(id) {
		return coreevents.ViewBinding{}, failure("invalid_params", "viewId is required")
	}
	definition, err := s.resolve(ctx, by, id, nil)
	if err != nil {
		return coreevents.ViewBinding{}, err
	}
	if len(definition.GroupBy) != 0 {
		return coreevents.ViewBinding{}, failure("invalid_params", "view.watch does not support grouped views")
	}
	filter := definition.Filter
	if filter == nil {
		return coreevents.ViewBinding{}, failure("invalid_params", "view definition filter is required")
	}
	if len(filter.Events) != 0 || filter.ViewID != "" {
		return coreevents.ViewBinding{}, failure("invalid_params", "view.watch filter shape is unsupported")
	}
	resources := append([]string(nil), filter.Resources...)
	beadTypes := append([]string(nil), filter.Types...)
	terms, err := parseQuery(filter.Query)
	if err != nil {
		return coreevents.ViewBinding{}, err
	}
	var queryID string
	for _, term := range terms {
		switch term.key {
		case "id":
			if queryID != "" {
				return coreevents.ViewBinding{}, failure("invalid_params", "view.watch cannot compile multiple id clauses")
			}
			queryID = term.value
		case "type":
			if len(beadTypes) != 0 {
				return coreevents.ViewBinding{}, failure("invalid_params", "view.watch cannot compile multiple type clauses")
			}
			beadTypes = []string{term.value}
		default:
			return coreevents.ViewBinding{}, failure("invalid_params", "view.watch supports only id and type equality in query filters")
		}
	}
	if queryID != "" {
		if len(resources) != 0 {
			return coreevents.ViewBinding{}, failure("invalid_params", "view.watch cannot combine resources with an id query")
		}
		resources = []string{queryID}
	}
	eventResources := make([]string, 0, len(resources))
	for _, id := range resources {
		if strings.TrimSpace(id) == "" || id != strings.TrimSpace(id) || strings.HasPrefix(id, "bead/") {
			return coreevents.ViewBinding{}, failure("invalid_params", "view.watch resource IDs must be bare bead IDs")
		}
		eventResources = append(eventResources, "bead/"+id)
	}
	sort.Strings(eventResources)
	eventResources = compactStrings(eventResources)
	sort.Strings(beadTypes)
	beadTypes = compactStrings(beadTypes)
	if len(eventResources) > OrdinaryCandidateCap {
		return coreevents.ViewBinding{}, failure("scope_too_large", "view.watch resource membership exceeds the ordinary candidate cap")
	}
	meta, err := s.store.Meta(ctx)
	if err != nil {
		return coreevents.ViewBinding{}, mapped(err)
	}
	if meta.Incarnation == "" {
		return coreevents.ViewBinding{}, failure("internal_error", "view checkpoint has no store incarnation")
	}
	projection, err := s.projectionStatus(ctx)
	if err != nil {
		return coreevents.ViewBinding{}, err
	}
	return coreevents.ViewBinding{Filter: coreevents.Filter{Resources: eventResources, BeadTypes: beadTypes}, Ontology: s.cfg.OntologyRevision, PolicyVersion: s.cfg.PolicyVersion, Incarnation: meta.Incarnation, Projection: projection}, nil
}

func (s *Service) projectionStatus(ctx context.Context) (*coreevents.ProjectionStatus, error) {
	if s.cfg.ProjectionStatus == nil {
		return nil, nil
	}
	binding := s.cfg.ProjectionBinding
	if binding == "" {
		binding = "core"
	}
	status, err := s.cfg.ProjectionStatus.Status(ctx, binding)
	if err != nil {
		return nil, mapped(err)
	}
	status = coreevents.NormalizeProjectionStatus(status, binding)
	if status.Binding != binding {
		return nil, failure("internal_error", "projection status binding does not match the view checkpoint")
	}
	if err := validateProjectionPosition(status.Position); err != nil {
		return nil, failure("internal_error", "projection status has an invalid checkpoint")
	}
	if status.Resume != nil {
		if err := validateProjectionPosition(status.Resume.Position); err != nil {
			return nil, failure("internal_error", "projection status has an invalid resume checkpoint")
		}
	}
	return &status, nil
}

func validateProjectionPosition(position coreevents.Position) error {
	// A never-run projection has an all-zero checkpoint. Any partially filled
	// position must satisfy the normal signed cursor tuple invariants.
	if position.Incarnation == "" && position.Revision.IsZero() && position.Index.IsZero() {
		return nil
	}
	return position.Validate()
}

func compactStrings(values []string) []string {
	if len(values) < 2 {
		return values
	}
	out := values[:1]
	for _, value := range values[1:] {
		if value != out[len(out)-1] {
			out = append(out, value)
		}
	}
	return out
}

func cloneDefinition(in Definition) Definition {
	out := in
	if in.Filter != nil {
		filter := *in.Filter
		filter.Resources = append([]string(nil), in.Filter.Resources...)
		filter.Types = append([]string(nil), in.Filter.Types...)
		filter.Events = append([]string(nil), in.Filter.Events...)
		out.Filter = &filter
	}
	if in.Fields != nil {
		out.Fields = append([]string{}, in.Fields...)
	}
	out.GroupBy = append([]string(nil), in.GroupBy...)
	out.Sort = append([]string(nil), in.Sort...)
	if in.BudgetTokens != nil {
		budget := *in.BudgetTokens
		out.BudgetTokens = &budget
	}
	return out
}

func validateDefinition(def Definition) error {
	if def.Filter == nil {
		return failure("invalid_params", "view definition filter is required")
	}
	if err := validateFilter(*def.Filter); err != nil {
		return err
	}
	if isEpicProgress(def) {
		return validateEpicProgress(def)
	}
	for _, field := range def.Fields {
		if !supportedField(field) {
			return &model.Error{Data: model.ErrorData{Code: "unknown_field", Hint: "view field is not supported: " + field}}
		}
	}
	for _, group := range def.GroupBy {
		if group != "type" {
			return failure("invalid_params", "only groupBy type is supported by this view slice")
		}
	}
	if len(def.GroupBy) > 1 {
		return failure("invalid_params", "view grouping accepts one type key")
	}
	if len(def.GroupBy) == 1 && def.Fields == nil {
		return failure("invalid_params", "type summaries require explicitly present fields=[]")
	}
	for _, key := range def.Sort {
		name := strings.TrimPrefix(key, "-")
		if name == "" || !supportedSort(name) || strings.Count(key, "-") > 1 {
			return failure("invalid_params", "unsupported view sort key")
		}
	}
	if def.BudgetTokens != nil {
		if err := def.BudgetTokens.Validate(); err != nil {
			return failure("invalid_params", "budgetTokens must be a nonnegative integer")
		}
		if _, err := def.BudgetTokens.Uint64(); err != nil {
			return failure("invalid_params", "budgetTokens exceeds the supported range")
		}
	}
	return nil
}

func validateFilter(filter Filter) error {
	if filter.ViewID != "" {
		return failure("invalid_params", "filter.viewId is only valid for watches")
	}
	if len(filter.Events) != 0 {
		return failure("invalid_params", "event filters are not supported by bead views")
	}
	for _, id := range filter.Resources {
		if strings.TrimSpace(id) == "" || id != strings.TrimSpace(id) {
			return failure("invalid_params", "view resource IDs must be nonempty and trimmed")
		}
	}
	for _, typ := range filter.Types {
		if strings.TrimSpace(typ) == "" || typ != strings.TrimSpace(typ) {
			return failure("invalid_params", "view types must be nonempty and trimmed")
		}
	}
	return nil
}

func supportedSort(key string) bool {
	switch key {
	case "id", "type", "project", "status", "title", "revision":
		return true
	default:
		return false
	}
}

func supportedField(field string) bool {
	if field == "id" || field == "type" || field == "project" || field == "status" || field == "revision" || field == "parent" || field == "labels" || field == "fields" || field == "metadata" {
		return true
	}
	if strings.HasPrefix(field, "fields.") {
		switch strings.TrimPrefix(field, "fields.") {
		case "title", "description", "status":
			return true
		default:
			return false
		}
	}
	return false
}

func (s *Service) effectiveDigest(def Definition, project, viewID string, stale *StaleFilter) (string, error) {
	var fields *[]string
	if def.Fields != nil {
		copy := append([]string{}, def.Fields...)
		fields = &copy
	}
	var filter Filter
	if def.Filter != nil {
		filter = *def.Filter
	}
	return digest(struct {
		Name         string         `json:"name,omitempty"`
		Filter       Filter         `json:"filter"`
		Fields       *[]string      `json:"fields,omitempty"`
		GroupBy      []string       `json:"groupBy,omitempty"`
		Sort         []string       `json:"sort,omitempty"`
		BudgetTokens *model.Natural `json:"budgetTokens,omitempty"`
		Project      string         `json:"project,omitempty"`
		ViewID       string         `json:"viewId,omitempty"`
		Stale        *StaleFilter   `json:"stale,omitempty"`
	}{def.Name, filter, fields, def.GroupBy, def.Sort, def.BudgetTokens, project, viewID, stale})
}

func principalDigest(by model.PrincipalRef, agent string) (string, error) {
	return digest(struct {
		Principal model.PrincipalRef `json:"principal"`
		Agent     string             `json:"agent,omitempty"`
	}{by, agent})
}

func digest(value any) (string, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

func (s *Service) validateCursor(cursor *Cursor, by model.PrincipalRef, agent, viewID, definitionDigest, incarnation string, stale *StaleFilter) error {
	if cursor == nil {
		return nil
	}
	if cursor.Version != 1 || cursor.Method != "view.run" || cursor.Incarnation == "" || cursor.Incarnation != incarnation || cursor.ViewID != viewID || cursor.DefinitionDigest != definitionDigest || cursor.Revision.Validate() != nil || cursor.Revision.String() == "<invalid integer>" {
		return failure("cursor_invalid", "cursor does not match the view request")
	}
	if (stale == nil) != (cursor.StaleCutoff == nil) || cursor.StaleCutoff != nil && cursor.StaleCutoff.IsZero() {
		return failure("cursor_invalid", "cursor does not match the age query")
	}
	if stale != nil {
		// Stale cursors are keyset cursors. A numeric index would reveal how
		// many denied rows preceded the page and would be unsafe to replay after
		// current authority changes.
		if cursor.Index != 0 || len(cursor.StaleResumeDigest) != sha256.Size*2 {
			return failure("cursor_invalid", "age-query cursor lacks a visible resume key")
		}
		decoded, err := hex.DecodeString(cursor.StaleResumeDigest)
		if err != nil || hex.EncodeToString(decoded) != cursor.StaleResumeDigest {
			return failure("cursor_invalid", "age-query cursor has a noncanonical resume key")
		}
	} else if cursor.StaleResumeDigest != "" {
		return failure("cursor_invalid", "non-age cursor carries a stale resume key")
	}
	wantPrincipal, err := principalDigest(by, agent)
	if err != nil || cursor.PrincipalDigest != wantPrincipal {
		return failure("cursor_invalid", "cursor does not match the caller")
	}
	if cursor.OntologyRevision.String() != s.cfg.OntologyRevision.String() {
		return failure("cursor_invalid", "cursor was issued for another ontology revision")
	}
	return nil
}

func (s *Service) outputBudget(def Definition) (int, error) {
	if def.BudgetTokens == nil {
		return DefaultOutputBudget, nil
	}
	value, err := def.BudgetTokens.Uint64()
	if err != nil || value > uint64(math.MaxInt) {
		return 0, failure("invalid_params", "budgetTokens exceeds the supported range")
	}
	if value == 0 {
		return DefaultOutputBudget, nil
	}
	return int(value), nil
}

func sameRevision(a, b model.Revision) bool {
	ca, ea := a.Canonical()
	cb, eb := b.Canonical()
	return ea == nil && eb == nil && ca == cb
}

func sortStored(rows []stored, sortKeys []string) {
	if len(sortKeys) == 0 {
		sort.SliceStable(rows, func(i, j int) bool { return rows[i].doc.ID < rows[j].doc.ID })
		return
	}
	sort.SliceStable(rows, func(i, j int) bool {
		for _, raw := range sortKeys {
			desc := strings.HasPrefix(raw, "-")
			key := strings.TrimPrefix(raw, "-")
			left, right := sortValue(rows[i], key), sortValue(rows[j], key)
			order := strings.Compare(left, right)
			if key == "revision" {
				order = compareRevisionValues(rows[i].record.Revision, rows[j].record.Revision)
			}
			if order == 0 {
				continue
			}
			if desc {
				return order > 0
			}
			return order < 0
		}
		return rows[i].doc.ID < rows[j].doc.ID
	})
}

func sortValue(row stored, key string) string {
	switch key {
	case "id":
		return row.doc.ID
	case "type":
		return row.doc.Type
	case "project":
		return row.doc.Project
	case "status":
		if value, ok := row.doc.Fields["status"].(string); ok {
			return value
		}
	case "title":
		if value, ok := row.doc.Fields["title"].(string); ok {
			return value
		}
	case "revision":
		return row.record.Revision.String()
	}
	return ""
}
