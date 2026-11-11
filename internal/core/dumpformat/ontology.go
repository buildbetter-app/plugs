package dumpformat

import (
	"fmt"
	"strings"
)

const explicitOntologyVerification = "built-in status mapping; explicit map_type directives recorded in manifest"

// OntologyDirective retains the closed exporter directive shape. Legacy
// archives admit unknown source types through map_type. Explicit strict-union
// imports also resolve status categories through map_status; the disclosure
// selects their typed migration semantics without altering the raw source.
type OntologyDirective struct {
	ID     string         `json:"id"`
	Kind   string         `json:"kind"`
	Target OntologyTarget `json:"target"`
	From   string         `json:"from"`
	To     string         `json:"to"`
}

type OntologyTarget struct {
	Project string `json:"project"`
	Type    string `json:"type"`
	ID      string `json:"id,omitempty"`
}

func builtInIssueType(value string) bool {
	switch value {
	case "task", "bug", "feature", "epic", "chore":
		return true
	}
	return false
}

func validateOntology(directives []OntologyDirective, projects []Project) error {
	ids := map[string]bool{}
	for i, d := range directives {
		if strings.TrimSpace(d.ID) == "" || ids[d.ID] {
			return fmt.Errorf("missing or duplicate ontology directive ID")
		}
		ids[d.ID] = true
		if d.Kind != "map_type" || !projectKnown(d.Target.Project, projects) ||
			strings.TrimSpace(d.Target.Type) == "" || builtInIssueType(d.Target.Type) ||
			d.From != d.Target.Type || d.To != "issue" {
			return fmt.Errorf("unsupported source type directive")
		}
		for _, prior := range directives[:i] {
			if prior.Target.Project == d.Target.Project && prior.Target.Type == d.Target.Type &&
				(prior.Target.ID == "" || d.Target.ID == "" || prior.Target.ID == d.Target.ID) {
				return fmt.Errorf("overlapping source type directives")
			}
		}
	}
	return nil
}

// Structural record validation is shared with source-history frames. Type
// authorization is checked separately so a frame may use a subset of the
// head's directives, while admission of the head requires every declaration
// to be accounted for.
func validateIssueOntology(directives []OntologyDirective, projects []Project, records []Record, requireAll bool) error {
	used := map[string]bool{}
	for _, r := range records {
		sourceType := stringValue(r.Source.Fields["issue_type"])
		if builtInIssueType(sourceType) {
			continue
		}
		matches := 0
		for _, d := range directives {
			if d.Target.Project == r.Project && d.Target.Type == sourceType &&
				(d.Target.ID == "" || d.Target.ID == r.ID) {
				matches++
				used[d.ID] = true
				if sourceType == "gate" && stringValue(r.Body["type"]) != d.To {
					return fmt.Errorf("gate projection must honor its explicit ontology directive")
				}
			}
		}
		// Modern native catalogs carry the built-in gate node literally. A
		// matching historical gate->issue directive still takes precedence and
		// is accounted above. The validated per-project status catalog is the
		// admission context; legacy record validation has no such context.
		project := findProject(r.Project, projects)
		if matches == 0 && len(project.Statuses) != 0 && sourceType == "gate" && stringValue(r.Body["type"]) == "gate" {
			continue
		}
		if matches != 1 {
			return fmt.Errorf("unsupported source type; one explicit ontology directive required")
		}
	}
	if requireAll && len(used) != len(directives) {
		return fmt.Errorf("unaccounted ontology directive")
	}
	return nil
}
