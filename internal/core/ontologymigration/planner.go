package ontologymigration

import (
	"context"
	"fmt"
	"sort"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
)

// Build validates real compiled ontologies and complete row images. Refused
// plans retain deterministic evidence; operational failures and cancellation
// return an error and no usable plan. Ready means this read-only planner found
// no undispositioned violation, never that a protected apply is authorized.
func Build(ctx context.Context, req Request, source RowSource, pre ValidatingPre) (Plan, error) {
	if err := ctx.Err(); err != nil {
		return Plan{}, err
	}
	report := Report{Format: "ontology-migration/1"}
	refuse := func(r Refusal) {
		if r.Code == "missing_facts" || r.Code == "incomplete_enumeration" {
			r.Reason, r.Code = string(r.Code), "invalid_params"
		}
		report.Refusals = append(report.Refusals, r)
	}
	for _, pair := range []struct {
		version Version
		ref     *Reference
		name    string
	}{{req.Current, &report.Current, "current"}, {req.Candidate, &report.Candidate, "candidate"}} {
		canonical, err := pair.version.Revision.Canonical()
		if err != nil || pair.version.Revision == "" || pair.version.Snapshot.Digest() == "" {
			refuse(Refusal{Code: "missing_facts", Message: pair.name + " requires an explicit revision and successfully compiled snapshot"})
			continue
		}
		*pair.ref = Reference{Revision: model.Revision(canonical), Digest: pair.version.Snapshot.Digest()}
	}
	if len(report.Refusals) > 0 {
		return finish(report), nil
	}
	if !successor(report.Current.Revision, report.Candidate.Revision) {
		refuse(Refusal{Code: "invalid_params", Message: "candidate ontology revision must be current revision plus one"})
	}
	compilation, err := ontology.PlanCompilation(report.Current.Revision, req.Candidate.Snapshot)
	if err != nil {
		return Plan{}, err
	}
	report.Compilation = compilation
	directives, err := detached(req.Directives)
	if err != nil {
		return Plan{}, fmt.Errorf("copy migration directives: %w", err)
	}
	sort.Slice(directives, func(i, j int) bool {
		if directives[i].ID != directives[j].ID {
			return directives[i].ID < directives[j].ID
		}
		return digest(directives[i]) < digest(directives[j])
	})
	report.Directives = directives
	valid := []Directive{}
	used := map[string]bool{}
	for _, d := range directives {
		if _, exists := used[d.ID]; exists {
			refuse(Refusal{Code: "invalid_params", Directive: d.ID, Message: "duplicate directive ID"})
			continue
		}
		used[d.ID] = false
		if err := directiveShape(d); err != nil {
			refuse(Refusal{Code: "invalid_params", Directive: d.ID, Message: err.Error()})
			continue
		}
		valid = append(valid, d)
	}
	if source == nil || pre == nil {
		refuse(Refusal{Code: "missing_facts", Message: "authoritative row and validating-pre ports are required"})
		return finish(report), nil
	}
	if len(report.Refusals) != 0 {
		return finish(report), nil
	}
	inventory, err := source.Inventory(ctx)
	if err != nil {
		return Plan{}, fmt.Errorf("read migration inventory: %w", err)
	}
	if inventory.DataRevision == "" || inventory.Ontology.Revision == "" || inventory.Count == "" || !validDigest(inventory.RowsDigest) {
		refuse(Refusal{Code: "missing_facts", Message: "inventory needs explicit snapshot revision, ontology, row count and digest"})
		return finish(report), nil
	}
	inventory, err = detached(inventory)
	if err != nil {
		return Plan{}, fmt.Errorf("copy migration inventory: %w", err)
	}
	report.Inventory = inventory
	if !equal(inventory.Ontology, report.Current) {
		refuse(Refusal{Code: "ontology_plan_stale", Message: "row inventory does not describe the current ontology revision and digest"})
		return finish(report), nil
	}
	rows := []Row{}
	var scanErr error
	err = source.Scan(ctx, inventory, func(row Row) error {
		if scanErr != nil {
			return scanErr
		}
		if err := ctx.Err(); err != nil {
			scanErr = err
			return err
		}
		if row.Revision == "" {
			scanErr = fmt.Errorf("row revision was not observed")
			return scanErr
		}
		copy, err := detached(row)
		if err != nil {
			scanErr = err
			return err
		}
		rows = append(rows, copy)
		return nil
	})
	if scanErr != nil {
		return Plan{}, fmt.Errorf("enumerate migration rows: %w", scanErr)
	}
	if err != nil {
		return Plan{}, fmt.Errorf("enumerate migration rows: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return Plan{}, err
	}
	sortRows(rows)
	actualDigest, rowErr := RowsDigest(rows)
	report.ObservedRowCount = len(rows)
	report.ObservedRowsDigest = actualDigest
	if rowErr != nil {
		report.ObservedRowsDigest = digest(rows)
	}
	actualCount := model.Natural64(uint64(len(rows)))
	if rowErr != nil || !equal(inventory.Count, actualCount) || actualDigest != inventory.RowsDigest {
		refuse(Refusal{Code: "incomplete_enumeration", Message: "row images are incomplete, duplicated or do not match the authoritative inventory"})
		return finish(report), nil
	}
	manifest, err := pre.Inventory(ctx, report.Candidate)
	if err != nil {
		return Plan{}, fmt.Errorf("read validating-pre manifest: %w", err)
	}
	manifestDigest, manifestErr := BindingsDigest(manifest.Candidate, manifest.Bindings)
	if manifestErr != nil {
		refuse(Refusal{Code: "missing_facts", Message: "validating-pre manifest is incomplete or malformed"})
		return finish(report), nil
	}
	manifest, err = detached(manifest)
	if err != nil {
		return Plan{}, err
	}
	sort.Slice(manifest.Bindings, func(i, j int) bool { return manifest.Bindings[i].ID < manifest.Bindings[j].ID })
	report.PreInventory = manifest
	if !equal(manifest.Candidate, report.Candidate) || manifestDigest != manifest.Digest {
		refuse(Refusal{Code: "missing_facts", Message: "validating-pre manifest does not bind the candidate and complete bindings"})
		return finish(report), nil
	}
	for _, binding := range manifest.Bindings {
		t, exists := req.Candidate.Snapshot.Effective(binding.Project).Document().Types[binding.Type]
		if !exists || t.Archived {
			refuse(Refusal{Code: "invalid_params", Message: "validating-pre binding targets an unknown or archived candidate type: " + binding.ID})
		}
	}
	if len(report.Refusals) > 0 {
		return finish(report), nil
	}
	for _, before := range rows {
		if err := ctx.Err(); err != nil {
			return Plan{}, err
		}
		after, _ := detached(before)
		row := RowPlan{Before: before, After: after}
		matching := matchingDirectives(valid, before)
		applyDirectives(req, &row, matching, used, refuse)
		effective := req.Candidate.Snapshot.Effective(before.State.Project)
		// Defaults come from actual compiled schemas, skip owned fields and
		// never replace explicit nulls or other invalid existing values.
		withDefaults, violations := ontology.Defaults(effective, row.After.State)
		if len(violations) == 0 {
			for field := range withDefaults.Fields {
				if _, exists := row.After.State.Fields[field]; !exists {
					row.Defaults = append(row.Defaults, field)
				}
			}
			sort.Strings(row.Defaults)
			row.After.State = withDefaults
		}
		// An unchanged image passed as both pre/post asks the existing checker
		// for strict state validation without pretending a migration is a slot
		// verb or bypassing runtime transition checks in the ontology package.
		checks := ontology.Check(effective, &row.After.State, &row.After.State, model.FieldSet{"type"}, "ontology.plan", ontology.GuardFacts{})
		violations = append(violations, checks.Violations...)
		for _, violation := range violations {
			addFinding(&row, report.Candidate, violation, "")
		}
		row.BeforeStatus, _ = req.Current.Snapshot.Effective(before.State.Project).Status(before.State.Status)
		row.AfterStatus, _ = effective.Status(row.After.State.Status)
		row.Affected = len(matching) != 0 || len(row.Findings) != 0 || !equal(before, row.After) || !equal(rowRules(req.Current.Snapshot, before), rowRules(req.Candidate.Snapshot, before))
		if row.Affected {
			report.Affected++
		}
		report.Rows = append(report.Rows, row)
	}
	if report.Affected > MaxAffectedRows {
		refuse(Refusal{Code: "ontology_migration_too_large", Message: "migration affects more than 5000 rows"})
		return finish(report), nil
	}
	for i := range report.Rows {
		row := &report.Rows[i]
		if !row.Affected {
			continue
		}
		if err := guardFindings(ctx, req, inventory, source, row, report.Candidate, refuse); err != nil {
			return Plan{}, err
		}
		for _, binding := range manifest.Bindings {
			if binding.Type != row.After.State.Type || (binding.Project != "" && binding.Project != row.After.State.Project) {
				continue
			}
			if err := ctx.Err(); err != nil {
				return Plan{}, err
			}
			change, _ := detached(Change{Candidate: report.Candidate, Before: row.Before, After: row.After})
			violations, err := pre.Validate(ctx, binding, change)
			if err != nil {
				return Plan{}, fmt.Errorf("validating pre %s: %w", binding.ID, err)
			}
			checked := []ontology.Violation{}
			for _, violation := range violations {
				if violation.Code.Validate() != nil || violation.Message == "" {
					refuse(Refusal{Code: "missing_facts", Row: rowKey(row.Before), Message: "validating pre returned a malformed violation"})
					continue
				}
				checked = append(checked, violation)
				addFinding(row, report.Candidate, violation, binding.ID)
			}
			sort.Slice(checked, func(i, j int) bool { return digest(checked[i]) < digest(checked[j]) })
			row.PreRuns = append(row.PreRuns, PreRun{Binding: binding.ID, Violations: checked})
		}
		report.Grandfathered += dispose(row, matchingDirectives(valid, row.Before), used, refuse)
	}
	for _, d := range valid {
		if !used[d.ID] {
			refuse(Refusal{Code: "invalid_params", Directive: d.ID, Message: "directive is unused or its target/value is invalid"})
		}
	}
	if err := ctx.Err(); err != nil {
		return Plan{}, err
	}
	return finish(report), nil
}

func matchingDirectives(directives []Directive, row Row) []Directive {
	matching := []Directive{}
	for _, d := range directives {
		if matches(d, row) {
			matching = append(matching, d)
		}
	}
	return matching
}

// Only rules applicable to this row's type count toward affected-row sizing;
// an unrelated type addition does not turn every bead into a migration write.
func rowRules(snapshot ontology.Snapshot, row Row) any {
	doc := snapshot.Effective(row.State.Project).Document()
	t := doc.Types[row.State.Type]
	w := doc.Workflows[t.Workflow]
	statuses := map[string]ontology.Status{}
	for _, key := range w.Statuses {
		statuses[key] = doc.Statuses[key]
	}
	return struct {
		Type     ontology.Type              `json:"type"`
		Workflow ontology.Workflow          `json:"workflow"`
		Statuses map[string]ontology.Status `json:"statuses"`
	}{t, w, statuses}
}
