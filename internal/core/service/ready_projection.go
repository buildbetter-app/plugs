package service

import (
	"encoding/json"

	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// ReadyProjectionBinding binds the effective compilation, including runtime
// Refresh. A stored ready bit is never an authority for a different ontology.
func (o *Ontology) ReadyProjectionBinding(project string) (string, error) {
	encoded, err := json.Marshal(o.effective(project).Document())
	if err != nil {
		return "", err
	}
	return driver.SelectionDigest(encoded), nil
}
func (o *Ontology) ReadyProjectionState(doc pipeline.Document) (pipeline.ReadyProjectionState, error) {
	e := o.effective(doc.Project)
	b, err := json.Marshal(e.Document())
	if err != nil {
		return pipeline.ReadyProjectionState{}, err
	}
	st := state(doc)
	template, violations := ontology.IsTemplate(st)
	if err = violationsError(violations); err != nil {
		return pipeline.ReadyProjectionState{}, err
	}
	category, known := e.Category(st.Status)
	return pipeline.ReadyProjectionState{
		Eligible: !template && e.ReadyEligible(st.Type, st.Status),
		Local:    known && category != ontology.Backlog && doc.Lease == nil && len(doc.Links) == 0,
		Binding:  driver.SelectionDigest(b),
	}, nil
}

var _ pipeline.ReadyProjectionEvaluator = (*Ontology)(nil)
