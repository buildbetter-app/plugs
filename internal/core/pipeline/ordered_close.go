package pipeline

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/port"
)

// The trusted ontology selects status keys without evaluating a guard or
// invoking a subscriber during reference preparation. Actual schema/permission
// and graph guards stay in the ontology phase, after field mutators.
type closeSlots interface {
	CloseSlots(project, beadType string) (initial, target string, ok bool)
}

// Explicit create targets are selected before later in-batch lifecycle ops.
// The ontology phase validates both the initial prefix and the final image.
type exactCloseStatuses interface {
	CloseStatuses(project, beadType string, requested *string) (initial, target string, ok bool)
}

type createStatuses interface {
	CreateStatus(project, beadType string, requested *string) (string, bool)
}

func (w *Writer) closedDocument(doc Document) bool {
	// Ask the same trusted selector whether the current project/type status
	// is a successful close target. A prior failure/cancellation timestamp
	// does not establish a first successful close. No guard is evaluated here.
	statuses, ok := w.cfg.ontology.(exactCloseStatuses)
	if !ok {
		return doc.CloseReason != nil || doc.ClosedAt != nil
	}
	status, _ := doc.Fields["status"].(string)
	_, selected, closed := statuses.CloseStatuses(doc.Project, doc.Type, &status)
	return closed && selected == status
}

type orderedCloseOntology interface {
	ValidateOrdered(context.Context, []port.Change, port.ProposedChange, port.SnapshotReader) error
}

func (w *Writer) prepareClose(ctx context.Context, p *prepared, current *pending, op *model.CloseOp, aliases map[string]model.Ref) (json.RawMessage, error) {
	initial, target, ok := "", "", false
	if slots, supported := w.cfg.ontology.(exactCloseStatuses); supported {
		initial, target, ok = slots.CloseStatuses(current.doc.Project, current.doc.Type, op.TargetStatus)
	} else if slots, supported := w.cfg.ontology.(closeSlots); supported && op.TargetStatus == nil {
		initial, target, ok = slots.CloseSlots(current.doc.Project, current.doc.Type)
	}
	if !ok && p.refusal == nil {
		p.refusal = failure("invalid_params", "ontology does not support the close slot")
	}
	if len(current.before) == 0 && current.doc.Fields["status"] == nil && initial != "" {
		current.doc.Fields["status"] = initial
	}
	before, err := json.Marshal(current.doc)
	if err != nil {
		return nil, err
	}
	for i, expect := range op.Expect {
		ref, err := p.resolve(expect.Ref, aliases)
		if err != nil {
			return nil, err
		}
		expect.Ref = ref
		op.Expect[i] = expect
		p.proposal.Expect = append(p.proposal.Expect, expect)
		var doc Document
		var revision model.Revision
		exists, written := false, false
		for _, prior := range p.refs {
			if prior.ID == ref.ID {
				written = true
				break
			}
		}
		if value := p.writes[ref.ID]; value != nil {
			if !value.deleted {
				doc, revision, exists = value.doc, value.revision, true
			}
		} else {
			rows, err := p.snapshot.Get(ctx, []model.Ref{ref})
			if err != nil {
				return nil, err
			}
			if len(rows) == 1 {
				doc, err = DecodeDocument(rows[0].Body)
				if err != nil {
					return nil, err
				}
				revision, exists = rows[0].Revision, true
			}
		}
		var refusal error
		if expect.Revision != nil {
			if !exists || revision.IsZero() {
				refusal = preconditionFailure("revision", "expected record is absent", ref)
			} else if written || expect.Revision.String() != revision.String() {
				refusal = preconditionRevisionFailure(revision, ref)
			}
		} else if !exists {
			refusal = preconditionFailure("hash", "expected record is absent", ref)
		} else {
			hash, err := expectationHash(doc, expect.Field, ref)
			if err != nil {
				refusal = err
			} else if hash != expect.Hash {
				refusal = preconditionFailure("hash", "field hash changed", ref)
			}
		}
		if p.refusal == nil {
			p.refusal = refusal
		}
	}
	if op.Reason != "" && strings.TrimSpace(op.Reason) == "" {
		return nil, failure("invalid_params", "comment body must not be empty")
	}
	alreadyClosed := w.closedDocument(current.doc) || ok && current.doc.Fields["status"] == target
	identicalReason := alreadyClosed && current.doc.CloseReason != nil && *current.doc.CloseReason == op.Reason
	if ok && !alreadyClosed {
		reason := op.Reason
		current.doc.CloseReason = &reason
		current.setBy["closeReason"] = "request"
	}
	if ok && current.doc.Fields["status"] != target {
		current.doc.Fields["status"] = target
		current.setBy["fields.status"] = "request"
	}
	if op.Reason != "" && !identicalReason {
		stamp := w.cfg.now().UTC()
		for _, comment := range current.doc.Comments {
			if !stamp.After(comment.Time) {
				stamp = comment.Time.Add(time.Second)
			}
		}
		current.doc.Comments = append(current.doc.Comments, Comment{Body: op.Reason, Principal: p.proposal.Principal, Time: stamp})
		current.setBy["comments"] = "request"
	}
	if current.doc.Lease != nil {
		current.doc.Lease = nil
		current.setBy["lease"] = "request"
	}
	p.closes[current.ref.ID] = true
	return before, nil
}

// Audit and claim handling derive the slot from this record's typed operation,
// never from the tx.apply method shared by unrelated records in the batch.
func (p *prepared) lifecycleMethod(value *pending) string {
	if value != nil && !value.deleted && p.closes[value.ref.ID] {
		return "bead.close"
	}
	return gateLifecycleMethod(p.proposal.Meta.Method, value)
}
