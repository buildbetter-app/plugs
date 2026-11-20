package viewops

import (
	"context"
	"errors"
	"sort"
	"strings"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

const EpicProgressGrouping = "epic-progress"

func isEpicProgress(def Definition) bool {
	return len(def.GroupBy) == 1 && def.GroupBy[0] == EpicProgressGrouping
}

func epicParent(def Definition) (model.Ref, error) {
	if def.Filter == nil {
		return model.Ref{}, failure("invalid_params", "epic progress requires a parent equality filter")
	}
	id, ok := strings.CutPrefix(def.Filter.Query, "parent=")
	if !ok || id == "" || strings.ContainsAny(id, "=/\x00 \t\r\n") {
		return model.Ref{}, failure("invalid_params", "epic progress requires exactly parent=<bare bead ID>")
	}
	ref := model.RefByID(id, model.Natural64(0))
	if ref.Validate() != nil {
		return model.Ref{}, failure("invalid_params", "epic progress requires a canonical parent ID")
	}
	return ref, nil
}

func validateEpicProgress(def Definition) error {
	if _, err := epicParent(def); err != nil {
		return err
	}
	if def.Fields == nil || len(def.Fields) != 0 || len(def.Sort) != 0 || len(def.Filter.Resources) != 0 || len(def.Filter.Types) != 0 {
		return failure("invalid_params", "epic progress is a complete direct-child count with explicit fields=[] and no additional filters or sort")
	}
	if def.BudgetTokens != nil {
		if err := def.BudgetTokens.Validate(); err != nil || !def.BudgetTokens.IsZero() {
			return failure("invalid_params", "epic progress does not support a partial token budget; omit budgetTokens or use zero")
		}
	}
	return nil
}

func (s *Service) runEpicProgress(ctx context.Context, by model.PrincipalRef, req RunRequest, def Definition) (Result, error) {
	cfg := s.cfg.EpicProgress
	if cfg == nil {
		return Result{}, &model.Error{Data: model.ErrorData{Code: "invalid_params", Hint: "epic progress is not enabled by this host", Details: map[string]any{"capability": EpicProgressGrouping, "available": false}}}
	}
	if req.Cursor != nil {
		return Result{}, failure("cursor_invalid", "epic progress is atomic and has no continuation cursor; refresh without cursor")
	}
	parent, err := epicParent(def)
	if err != nil {
		return Result{}, err
	}
	_, content, err := s.reader(ctx, by, req.AsOf)
	if err != nil {
		return Result{}, err
	}
	defer content.Close()
	rows, err := epicProgressRows(ctx, content, parent)
	if err != nil {
		return Result{}, err
	}
	parentRow, exists := findStored(rows, parent.ID)
	if !exists || (req.Project != "" && parentRow.doc.Project != req.Project) {
		return Result{}, failure("not_found", "epic was not found")
	}
	if !cfg.ScopeAuthorizesRead {
		allowed, err := s.authorize(ctx, by, parentRow)
		if err != nil {
			return Result{}, mapped(err)
		}
		if !allowed {
			return Result{}, failure("not_found", "epic was not found")
		}
	}
	children := make([]model.Ref, 0)
	direct := map[string]stored{}
	for _, row := range rows {
		p := row.doc.Parent
		if p == nil {
			continue
		}
		if p.Validate() != nil || !p.IsID() || p.ID == "" || !p.Branch.IsZero() {
			return Result{}, failure("internal_error", "invalid persisted parent reference")
		}
		if p.ID == parent.ID {
			if row.doc.ID == parent.ID {
				return Result{}, failure("internal_error", "invalid persisted direct-child graph")
			}
			direct[row.doc.ID] = row
			children = append(children, row.record.Ref)
		}
	}
	// Legacy point authorizers can still share one request-local child set. The
	// parent was checked above before any child-specific validation.
	if !cfg.ScopeAuthorizesRead && s.cfg.AuthorizeRead != nil {
		resources := make([]model.ResourceRef, 0, len(children)+1)
		for _, child := range children {
			resources = append(resources, model.ResourceRef{Kind: "bead", Ref: child})
		}
		ctx, _ = service.WithReadBatchSession(ctx, resources)
	}
	// Historical content never supplies historical authority. Every child is
	// authorized in its own project using one current authority snapshot.
	authority, err := cfg.AuthorizeScope(ctx, by, content, parent, append([]model.Ref(nil), children...))
	if err != nil {
		return Result{}, mapped(err)
	}
	if authority.PolicyBinding == "" {
		return Result{}, epicUnavailable("epic authority binding is unavailable")
	}
	visible := authority.Visible
	seen := make(map[string]bool, len(visible))
	for _, ref := range visible {
		if ref.Validate() != nil || !ref.IsID() || !ref.Branch.IsZero() || seen[ref.ID] {
			return Result{}, epicUnavailable("epic authorization returned an invalid child set")
		}
		if _, ok := direct[ref.ID]; !ok {
			return Result{}, epicUnavailable("epic authorization returned a child outside the pinned scope")
		}
		seen[ref.ID] = true
	}
	hidden, err := authority.Hidden.Uint64()
	if err != nil || hidden != uint64(len(children)-len(visible)) {
		return Result{}, epicUnavailable("epic authorization returned an incomplete child set")
	}
	// The scoped adapter cannot bypass an additional host read boundary. Each
	// visible child still uses its own stored project; denial adds only to the
	// authorization remainder, and an unavailable authorizer aborts the count.
	readable := make([]model.Ref, 0, len(visible))
	if cfg.ScopeAuthorizesRead {
		readable = append(readable, visible...)
	} else {
		for _, ref := range visible {
			allowed, err := s.authorize(ctx, by, direct[ref.ID])
			if err != nil {
				return Result{}, mapped(err)
			}
			if allowed {
				readable = append(readable, ref)
			}
		}
	}
	visible = readable
	if len(visible) > OrdinaryCandidateCap {
		return Result{}, &model.Error{Data: model.ErrorData{Code: "scope_too_large", Hint: "authorized epic input exceeds the ordinary cap", Details: map[string]any{"cap": OrdinaryCandidateCap}}}
	}
	// Load ontology sources from the same content reader, never a process-local
	// default or the current authority reader. Hidden child statuses are unread.
	effective := map[string]ontology.Effective{}
	parentOntology, err := epicOntology(ctx, content, cfg.OntologySource, parentRow.doc.Project)
	if err != nil {
		return Result{}, err
	}
	effective[parentRow.doc.Project] = parentOntology
	counts := map[string]int{}
	for _, ref := range visible {
		row := direct[ref.ID]
		e, ok := effective[row.doc.Project]
		if !ok {
			e, err = epicOntology(ctx, content, cfg.OntologySource, row.doc.Project)
			if err != nil {
				return Result{}, err
			}
			effective[row.doc.Project] = e
		}
		status, ok := row.doc.Fields["status"].(string)
		definition, known := e.Status(status)
		if !ok || status == "" || !known || definition.Archived {
			return Result{}, failure("invalid_params", "visible epic status is unavailable in the pinned ontology")
		}
		counts[status]++
	}
	if len(counts)+1 > MaxGroups {
		return Result{}, &model.Error{Data: model.ErrorData{Code: "scope_too_large", Hint: "epic progress exceeds the group bound", Details: map[string]any{"cap": MaxGroups}}}
	}
	// EP2 re-opens and fences current authority after all status bucketing.
	// No partial counts escape grants/verification/plugin generation changes.
	if err := cfg.ValidateAuthority(ctx, by, authority.PolicyBinding); err != nil {
		return Result{}, mapped(err)
	}
	if err := s.admit(ctx, by); err != nil {
		return Result{}, err
	}
	total := Group{Key: "total", Count: len(visible), Items: []Bead{}}
	if hidden := len(children) - len(visible); hidden != 0 {
		total.Elided = &Elision{Count: hidden, Reason: "authorization"}
	}
	result := Result{Items: []Bead{}, Groups: []Group{total}, AsOf: AsOf{Revision: *content.Snapshot().At, Incarnation: content.Snapshot().Incarnation}}
	keys := make([]string, 0, len(counts))
	for status := range counts {
		keys = append(keys, status)
	}
	sort.Strings(keys)
	for _, status := range keys {
		result.Groups = append(result.Groups, Group{Key: "status:" + status, Count: counts[status], Items: []Bead{}})
	}
	return result, nil
}

// epicProgressRows selects source images at the content reader's pinned
// revision, including historical asOf reads. Publishing the optional interface
// alone is not evidence of coverage: ErrUnsupported retains the existing scan
// for unbuilt stores and revisions outside the certified interval.
func epicProgressRows(ctx context.Context, reader driver.Reader, parent model.Ref) ([]stored, error) {
	indexed, ok := reader.(driver.HierarchyChildrenReader)
	if !ok {
		return scan(ctx, reader)
	}
	records, more, err := indexed.HierarchyChildren(ctx, []model.Ref{parent}, driver.HierarchyMaxLimit)
	if errors.Is(err, driver.ErrUnsupported) {
		return scan(ctx, reader)
	}
	if err != nil {
		return nil, mapped(err)
	}
	// The indexed port has one bounded page, not a continuation cursor. A
	// partial set cannot supply an exact authorization remainder, and the
	// ordinary cap is applied after authorization, not to all direct children.
	// Preserve the complete scan's semantics when that page is exhausted.
	if more {
		return scan(ctx, reader)
	}
	if len(records) > driver.HierarchyMaxLimit {
		return nil, failure("internal_error", "invalid persisted direct-child graph")
	}
	parents, err := reader.Get(ctx, []model.Ref{parent})
	if err != nil {
		return nil, mapped(err)
	}
	if len(parents) > 1 {
		return nil, failure("internal_error", "invalid persisted bead identity")
	}
	decode := func(record driver.Record) (stored, error) {
		if err := record.Ref.Validate(); err != nil || !record.Ref.IsID() || !record.Ref.Branch.IsZero() || record.Kind != "bead" {
			return stored{}, failure("internal_error", "invalid persisted bead identity")
		}
		doc, err := decodeDocument(record.Body)
		if err != nil || doc.ID != record.Ref.ID {
			return stored{}, failure("internal_error", "invalid persisted bead document")
		}
		return stored{record: record, doc: doc}, nil
	}
	rows := make([]stored, 0, len(records)+1)
	if len(parents) == 1 && parents[0].Kind == "bead" {
		row, err := decode(parents[0])
		if err != nil {
			return nil, err
		}
		if row.doc.ID != parent.ID {
			return nil, failure("internal_error", "invalid persisted bead identity")
		}
		rows = append(rows, row)
	}
	seen := make(map[string]bool, len(records))
	for _, record := range records {
		row, err := decode(record)
		if err != nil {
			return nil, err
		}
		p := row.doc.Parent
		if p != nil && (p.Validate() != nil || !p.IsID() || p.ID == "" || !p.Branch.IsZero()) {
			return nil, failure("internal_error", "invalid persisted parent reference")
		}
		if p == nil || p.ID != parent.ID || row.doc.ID == parent.ID || seen[row.doc.ID] {
			return nil, failure("internal_error", "invalid persisted direct-child graph")
		}
		seen[row.doc.ID] = true
		rows = append(rows, row)
	}
	return rows, nil
}

func epicOntology(ctx context.Context, reader driver.Reader, locate service.OntologySourceLocator, project string) (ontology.Effective, error) {
	ref, err := locate(project)
	if err != nil {
		return ontology.Effective{}, mapped(err)
	}
	if ref.Validate() != nil || !ref.IsID() || !ref.Branch.IsZero() || (ref.ID != "ontology" && !strings.HasPrefix(ref.ID, "ontology/")) {
		return ontology.Effective{}, failure("invalid_params", "epic ontology locator must select a protected branch-zero ID")
	}
	rows, err := reader.Get(ctx, []model.Ref{ref})
	if err != nil {
		return ontology.Effective{}, mapped(err)
	}
	if len(rows) != 1 || rows[0].Kind != "ontology" || rows[0].Ref.ID != ref.ID || !rows[0].Ref.Branch.IsZero() {
		return ontology.Effective{}, failure("invalid_params", "epic ontology source is unavailable in the content snapshot")
	}
	source, err := service.DecodeOntologySource(rows[0].Body)
	if err != nil {
		return ontology.Effective{}, failure("invalid_params", "invalid protected epic ontology source")
	}
	snapshot, problems := ontology.Compile(source.Base, source.Overlays, source.Plugins)
	if len(problems) != 0 {
		return ontology.Effective{}, failure("invalid_params", "epic ontology source does not compile")
	}
	return snapshot.Effective(project), nil
}

// Scope adapter failures are retryable refusals at the core boundary too.
func epicUnavailable(hint string) error {
	return &model.Error{Data: model.ErrorData{Code: "authz_unavailable", Hint: hint, Retryable: true}}
}
