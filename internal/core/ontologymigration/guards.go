package ontologymigration

import (
	"context"
	"fmt"
	"sort"

	"github.com/buildbetter-app/plugs/internal/core/ontology"
)

func targetGuards(e ontology.Effective, row Row) []ontology.Guard {
	doc := e.Document()
	t := doc.Types[row.State.Type]
	var guards []ontology.Guard
	// A migration is state validation, not a lifecycle verb. Conjoin every rule
	// for the resulting status, including newly introduced state requirements.
	for _, rule := range doc.Workflows[t.Workflow].Transitions {
		if rule.To == row.State.Status {
			guards = append(guards, rule.Guards...)
		}
	}
	return guards
}

func externalGuard(g ontology.Guard) bool {
	return g.Kind != ontology.FieldsRequired && g.Kind != ontology.NotTemplate
}

func guardFindings(ctx context.Context, req Request, inventory Inventory, source RowSource, row *RowPlan, candidate Reference, refuse func(Refusal)) error {
	guards := targetGuards(req.Candidate.Snapshot.Effective(row.After.State.Project), row.After)
	var facts ontology.GuardFacts
	for _, guard := range guards {
		if !externalGuard(guard) {
			continue
		}
		observer, ok := source.(GuardSource)
		if !ok {
			refuse(Refusal{Code: "missing_facts", Row: rowKey(row.Before), Message: "target guards require an authoritative read-only GuardSource"})
			return nil
		}
		change, _ := detached(Change{Candidate: candidate, Before: row.Before, After: row.After})
		observed, err := observer.Observe(ctx, inventory, change)
		if err != nil {
			return fmt.Errorf("observe migration guards: %w", err)
		}
		observed, err = detached(observed)
		if err != nil {
			return fmt.Errorf("copy migration observations: %w", err)
		}
		row.Observation = &observed
		expected, _ := RowDigest(row.After)
		if !equal(observed.Candidate, candidate) || observed.RowDigest != expected {
			refuse(Refusal{Code: "missing_facts", Row: rowKey(row.Before), Message: "guard observation does not bind the candidate and final row image"})
			return nil
		}
		facts = observed.Facts
		break
	}
	for _, guard := range guards {
		if err := ctx.Err(); err != nil {
			return err
		}
		observed, passes := evaluateGuard(guard, row.After, facts)
		if !observed {
			refuse(Refusal{Code: "missing_facts", Row: rowKey(row.Before), Message: "required target guard observation missing: " + string(guard.Kind)})
		} else if !passes {
			addFinding(row, candidate, ontology.Violation{Code: "ontology_guard_failed", Guard: guard.Kind, Message: "migration target guard failed"}, "")
		}
	}
	return nil
}

func evaluateGuard(guard ontology.Guard, row Row, facts ontology.GuardFacts) (observed, passes bool) {
	observed, passes = true, true
	switch guard.Kind {
	case ontology.FieldsRequired:
		for _, name := range guard.Fields {
			value, exists := row.State.Fields[name]
			passes = passes && exists && value != nil && value != ""
		}
	case ontology.NotTemplate:
		flag, violations := ontology.IsTemplate(row.State)
		passes = !flag && len(violations) == 0
	case ontology.NoOpenChildren:
		observed = facts.NoOpenChildren != nil
		passes = observed && *facts.NoOpenChildren
	case ontology.NoOpenBlockers:
		observed = facts.NoOpenBlockers != nil
		passes = observed && *facts.NoOpenBlockers
	case ontology.EvidenceRequired, ontology.GatesResolved:
		values := facts.Evidence
		if guard.Kind == ontology.GatesResolved {
			values = facts.Gates
		}
		for _, kind := range guard.Kinds {
			value, exists := values[kind]
			observed = observed && exists
			passes = passes && value
		}
	case ontology.Permission:
		observed = !facts.AuthzInstalled || facts.Permission != nil
		passes = !facts.AuthzInstalled || (observed && *facts.Permission)
	default:
		return false, false
	}
	return observed, passes
}

func addFinding(row *RowPlan, candidate Reference, violation ontology.Violation, subscriber string) {
	finding := Finding{Violation: violation, Subscriber: subscriber}
	finding.ID = digest(struct {
		Candidate Reference `json:"candidate"`
		After     Row       `json:"after"`
		Finding   Finding   `json:"finding"`
	}{candidate, row.After, finding})
	for _, existing := range row.Findings {
		if existing.ID == finding.ID {
			return
		}
	}
	row.Findings = append(row.Findings, finding)
}

func dispose(row *RowPlan, directives []Directive, used map[string]bool, refuse func(Refusal)) int {
	count := 0
	for _, d := range directives {
		if d.Kind != Grandfather {
			continue
		}
		for i := range row.Findings {
			finding := &row.Findings[i]
			if finding.ID != d.Violation {
				continue
			}
			v := finding.Violation
			if v.Code == "field_owned" || v.Code == "subscriber_unavailable" || v.Field == "fields.priority" || v.Field == "fields.template" || v.Field == "id" {
				refuse(Refusal{Code: "invalid_params", Row: rowKey(row.Before), Directive: d.ID, Message: "fixed rules and missing/owned plugin behavior cannot be grandfathered"})
				continue
			}
			if finding.Disposition != "" {
				refuse(Refusal{Code: "invalid_params", Row: rowKey(row.Before), Directive: d.ID, Message: "conflicting grandfather directives"})
				continue
			}
			finding.Disposition = d.ID
			used[d.ID] = true
			row.Directives = append(row.Directives, d.ID)
			count++
		}
	}
	sort.Strings(row.Directives)
	sort.Slice(row.Findings, func(i, j int) bool { return row.Findings[i].ID < row.Findings[j].ID })
	return count
}
