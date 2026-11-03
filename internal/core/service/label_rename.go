package service

import (
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

// LabelRenamePlan is a bounded data migration, applied by the caller through
// ordinary tx.apply. Renamed includes every changed bead; Merged is its subset
// that already carried the destination. These are planned counts, reportable
// as successful only after this exact ChangeSet commits (or replays).
//
// An empty plan is an honest read-only no-op. Do not submit its zero operations
// to tx.apply, whose existing contract requires at least one operation.
type LabelRenamePlan struct {
	ChangeSet model.ChangeSet
	Renamed   int
	Merged    int
}

// PlanLabelRename folds B16 into typed fields.labels replacements, as adopted
// in commands.md. The input is an explicit, complete caller-selected scope of
// current public bead projections, never an assertion about unseen store rows.
// Each changed bead carries its observed revision as a mandatory CAS. A racing
// addition therefore refuses the entire transaction: reread and replan rather
// than dropping its value or reporting a stale merge count. Additions outside
// this selected scope are untouched.
//
// This function neither reads nor writes storage, normalizes label identity,
// nor divides an oversized migration into independently committed batches.
// Ephemeral planes require the separately adopted B26 provider contract.
func PlanLabelRename(project, oldLabel, newLabel string, beads []Bead) (LabelRenamePlan, error) {
	if strings.TrimSpace(project) == "" || project != strings.TrimSpace(project) {
		return LabelRenamePlan{}, failure("invalid_params", "label migration requires an exact nonempty project")
	}
	if !validRenameLabel(oldLabel) || !validRenameLabel(newLabel) {
		return LabelRenamePlan{}, failure("invalid_params", "rename labels must contain 1 to 255 Unicode scalars")
	}
	if strings.TrimSpace(oldLabel) == strings.TrimSpace(newLabel) {
		return LabelRenamePlan{}, failure("invalid_params", "rename labels must differ after trimming")
	}
	if len(beads) > 1000 {
		return LabelRenamePlan{}, failure("scope_too_large", "label migration requires at most 1000 selected beads")
	}
	// Sort a copy so request bytes and operation ordering are deterministic,
	// without modifying the caller's projections or their label slices/maps.
	ordered := append([]Bead(nil), beads...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].ID < ordered[j].ID })
	plan := LabelRenamePlan{ChangeSet: model.ChangeSet{Ops: []model.Op{}}}
	for i, bead := range ordered {
		ref := model.RefByID(bead.ID, model.Natural64(0))
		if err := namedRef(ref); err != nil {
			return LabelRenamePlan{}, err
		}
		if bead.Project != project || bead.Type == "" || bead.Revision.Validate() != nil || bead.Revision.IsZero() {
			return LabelRenamePlan{}, failure("invalid_params", "label migration requires current bead identity, project, type and revision")
		}
		if i > 0 && ordered[i-1].ID == bead.ID {
			return LabelRenamePlan{}, failure("invalid_params", "label migration scope contains a duplicate bead")
		}
		labels, err := renameProjectionLabels(bead)
		if err != nil {
			return LabelRenamePlan{}, err
		}
		oldIndex := sort.SearchStrings(labels, oldLabel)
		if oldIndex == len(labels) || labels[oldIndex] != oldLabel {
			continue
		}
		newIndex := sort.SearchStrings(labels, newLabel)
		merged := newIndex < len(labels) && labels[newIndex] == newLabel
		next := make([]string, 0, len(labels))
		for _, label := range labels {
			if label != oldLabel {
				next = append(next, label)
			}
		}
		if !merged {
			next = append(next, newLabel)
			sort.Strings(next)
		}
		revision := bead.Revision
		plan.ChangeSet.Expect = append(plan.ChangeSet.Expect, model.Precondition{Ref: ref, Revision: &revision})
		plan.ChangeSet.Ops = append(plan.ChangeSet.Ops, model.Op{Kind: model.OpPatch, Patch: &model.PatchOp{
			Kind: model.OpPatch, Target: ref, Fields: map[string]any{"labels": next},
		}})
		plan.Renamed++
		if merged {
			plan.Merged++
		}
	}
	return plan, nil
}

func validRenameLabel(label string) bool {
	return label != "" && utf8.ValidString(label) && utf8.RuneCountInString(label) <= 255
}

// Public projections carry the exact sorted set both in fields.labels and in
// labels. Refuse missing fields and contradictory/malformed images before
// returning any usable operations or counts. The caller supplies complete
// bead.get projections; absence of both label members denotes an empty set.
func renameProjectionLabels(bead Bead) ([]string, error) {
	if bead.Fields == nil {
		return nil, failure("invalid_params", "label migration requires a complete fields projection")
	}
	labels := []string{}
	if raw, present := bead.Fields["labels"]; present {
		switch values := raw.(type) {
		case []string:
			labels = append(labels, values...)
		case []any:
			for _, value := range values {
				label, ok := value.(string)
				if !ok {
					return nil, failure("invalid_params", "projected labels must be an array of strings")
				}
				labels = append(labels, label)
			}
		default:
			return nil, failure("invalid_params", "projected labels must be an array of strings")
		}
	}
	if len(labels) != len(bead.Labels) {
		return nil, failure("invalid_params", "projected label sets disagree")
	}
	for i, label := range labels {
		if !validRenameLabel(label) || label != bead.Labels[i] || i > 0 && labels[i-1] >= label {
			return nil, failure("invalid_params", "projected labels must be identical sorted exact sets")
		}
	}
	return labels, nil
}
