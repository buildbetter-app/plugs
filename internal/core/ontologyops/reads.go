package ontologyops

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func (s *Service) Get(ctx context.Context, by model.PrincipalRef, project string) (Result, error) {
	return s.Describe(ctx, by, project, "")
}

func (s *Service) Describe(ctx context.Context, by model.PrincipalRef, project, typ string) (Result, error) {
	_, r, err := s.reader(ctx, by, nil)
	if err != nil {
		return Result{}, err
	}
	defer r.Close()
	src, err := s.source(ctx, by, r, project)
	if err != nil {
		return Result{}, err
	}
	result := Result{Revision: src.guard.Revision, Problems: src.problems, Drifted: len(src.problems) != 0}
	if len(src.problems) != 0 {
		return result, nil
	}
	doc := src.compiled.Effective(project).Document()
	if typ != "" {
		t, exists := doc.Types[typ]
		if !exists || t.Archived {
			return Result{}, failure("not_found", "ontology type is unknown or archived")
		}
		w := doc.Workflows[t.Workflow]
		statuses := map[string]ontology.Status{}
		for _, key := range w.Statuses {
			statuses[key] = doc.Statuses[key]
		}
		doc = ontology.Document{Types: map[string]ontology.Type{typ: t}, Workflows: map[string]ontology.Workflow{t.Workflow: w}, Statuses: statuses}
	}
	result.Document = &doc
	checked, err := s.integrity(ctx, by, r, src, project)
	if err != nil {
		return Result{}, err
	}
	result.Problems, result.Drifted = checked.Problems, checked.Drifted
	return result, nil
}

func (s *Service) Schema(ctx context.Context, by model.PrincipalRef, project, typ, status string) (map[string]any, error) {
	if project == "" {
		return nil, failure("project_required", "schema requires a project")
	}
	_, r, err := s.reader(ctx, by, nil)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	src, err := s.source(ctx, by, r, project)
	if err != nil {
		return nil, err
	}
	if err = src.valid(); err != nil {
		return nil, err
	}
	schema, violations := src.compiled.Effective(project).Schema(typ, status)
	if len(violations) != 0 {
		return nil, failure("not_found", "ontology type or status is unknown or archived")
	}
	return schema, nil
}

func (s *Service) Check(ctx context.Context, by model.PrincipalRef, project string) (CheckResult, error) {
	_, r, err := s.reader(ctx, by, nil)
	if err != nil {
		return CheckResult{}, err
	}
	defer r.Close()
	src, err := s.source(ctx, by, r, project)
	if err != nil {
		return CheckResult{}, err
	}
	return s.integrity(ctx, by, r, src, project)
}

func (s *Service) integrity(ctx context.Context, by model.PrincipalRef, r driver.Reader, src source, project string) (CheckResult, error) {
	result := CheckResult{Problems: src.problems}
	if len(src.problems) == 0 {
		rows, err := readRows(ctx, r)
		if err != nil {
			return CheckResult{}, err
		}
		for _, row := range rows {
			if project != "" && row.State.Project != project {
				continue
			}
			if err := s.authorize(ctx, by, model.Ref{ID: row.State.ID}, "bead", row.State.Project); err != nil {
				return CheckResult{}, err
			}
			ref, err := s.cfg.Source(row.State.Project)
			if err != nil || !sameRef(ref, src.guard.Ref) {
				return CheckResult{}, failure("invalid_params", "integrity check requires one authoritative ontology source")
			}
			checked := ontology.Check(src.compiled.Effective(row.State.Project), &row.State, &row.State, model.FieldSet{"type"}, "ontology.check", ontology.GuardFacts{})
			for _, violation := range checked.Violations {
				result.Problems = append(result.Problems, Problem{Path: fmt.Sprintf("beads.%s.%s", row.State.ID, violation.Field), Code: string(violation.Code), Message: violation.Message})
			}
		}
	}
	sort.Slice(result.Problems, func(i, j int) bool {
		if result.Problems[i].Path != result.Problems[j].Path {
			return result.Problems[i].Path < result.Problems[j].Path
		}
		return result.Problems[i].Message < result.Problems[j].Message
	})
	result.OK = len(result.Problems) == 0
	result.Drifted = !result.OK
	return result, nil
}

// Diff uses the driver's historical reconstruction and current permissions.
// One root replacement is a complete valid JSON Patch, including removals.
func (s *Service) Diff(ctx context.Context, by model.PrincipalRef, project string, from, to model.Revision) (DiffResult, error) {
	meta, a, err := s.reader(ctx, by, &from)
	if err != nil {
		return DiffResult{}, err
	}
	defer a.Close()
	_, b, err := s.reader(ctx, by, &to)
	if err != nil {
		return DiffResult{}, err
	}
	defer b.Close()
	if meta.Incarnation != b.Snapshot().Incarnation {
		return DiffResult{}, failure("not_found", "ontology revisions belong to different incarnations")
	}
	left, err := s.source(ctx, by, a, project)
	if err != nil {
		return DiffResult{}, err
	}
	right, err := s.source(ctx, by, b, project)
	if err != nil {
		return DiffResult{}, err
	}
	if left.valid() != nil || right.valid() != nil || !sameRef(left.guard.Ref, right.guard.Ref) {
		return DiffResult{}, failure("invalid_params", "historical sources are incompatible or do not compile")
	}
	x, y := left.compiled.Effective(project).Document(), right.compiled.Effective(project).Document()
	xraw, _ := json.Marshal(x)
	yraw, _ := json.Marshal(y)
	result := DiffResult{FromRevision: from, ToRevision: to, Patch: []Patch{}}
	if string(xraw) != string(yraw) {
		result.Patch = append(result.Patch, Patch{Op: "replace", Path: "", Value: &y})
	}
	return result, nil
}

// History projects real changesets onto the protected source changes. It does
// not expose unrelated row changes or internal event descriptors. Continuation
// uses the canonical integer `after`, equal to the last returned revision.
func (s *Service) History(ctx context.Context, by model.PrincipalRef, project string, after model.Revision, limit int) (HistoryResult, error) {
	if limit == 0 {
		limit = 100
	}
	if limit < 1 || limit > 1000 {
		return HistoryResult{}, failure("invalid_params", "history limit must be between 1 and 1000")
	}
	_, r, err := s.reader(ctx, by, nil)
	if err != nil {
		return HistoryResult{}, err
	}
	defer r.Close()
	src, err := s.source(ctx, by, r, project)
	if err != nil {
		return HistoryResult{}, err
	}
	a, aerr := after.Uint64()
	h, herr := r.Snapshot().At.Uint64()
	if aerr != nil || herr != nil || a > h {
		return HistoryResult{}, failure("cursor_invalid", "after is beyond the stored history")
	}
	result := HistoryResult{Revisions: []model.Changeset{}}
	cursor := &driver.Cursor{Incarnation: r.Snapshot().Incarnation, Revision: after, Method: "changesets"}
	for {
		entries, err := r.Changesets(ctx, cursor, 1000)
		if err != nil {
			return HistoryResult{}, mapped(err)
		}
		for _, entry := range entries {
			changes := []model.ChangeRecord{}
			for _, change := range entry.Changes {
				if sameRef(change.Ref, src.guard.Ref) {
					changes = append(changes, change)
				}
			}
			if len(changes) != 0 {
				entry.Changes, entry.Events = changes, nil
				if entry.Chain == nil {
					entry.Chain = []string{}
				}
				result.Revisions = append(result.Revisions, entry)
				if len(result.Revisions) == limit {
					return result, nil
				}
			}
			cursor.Revision = entry.Revision
		}
		if len(entries) < 1000 {
			return result, nil
		}
	}
}
