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
	return readyProjectionBinding(o.effective(project))
}

func readyProjectionBinding(e ontology.Effective) (string, error) {
	doc := e.Document()
	// Effective readiness also includes compiled degradation from plugin
	// availability. The public document alone omits that compiled state.
	eligible := make(map[string]map[string]bool, len(doc.Types))
	for typ := range doc.Types {
		eligible[typ] = make(map[string]bool, len(doc.Statuses))
		for status := range doc.Statuses {
			eligible[typ][status] = e.ReadyEligible(typ, status)
		}
	}
	encoded, err := json.Marshal(struct {
		Document ontology.Document
		Eligible map[string]map[string]bool
	}{doc, eligible})
	if err != nil {
		return "", err
	}
	return driver.SelectionDigest(encoded), nil
}
func (o *Ontology) ReadyProjectionState(doc pipeline.Document) (pipeline.ReadyProjectionState, error) {
	e := o.effective(doc.Project)
	binding, err := readyProjectionBinding(e)
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
		Binding:  binding,
	}, nil
}

var _ pipeline.ReadyProjectionEvaluator = (*Ontology)(nil)
