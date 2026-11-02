package pipeline

import (
	"context"
	"sort"
	"unicode/utf8"
)

// labelSet keeps exact nonempty values. The limit matches the source-pinned
// VARCHAR contract in Unicode scalars, not UTF-8 bytes. The input is never
// modified: callers and earlier pending parent images may share its slices.
func labelSet(value any) ([]string, error) {
	labels := []string{}
	switch values := value.(type) {
	case []string:
		labels = append(labels, values...)
	case []any:
		for _, value := range values {
			label, ok := value.(string)
			if !ok {
				return nil, failure("invalid_params", "labels must be an array of strings")
			}
			labels = append(labels, label)
		}
	default:
		return nil, failure("invalid_params", "labels must be an array of strings")
	}
	set := make(map[string]bool, len(labels))
	for _, label := range labels {
		if !utf8.ValidString(label) || utf8.RuneCountInString(label) > 255 {
			return nil, failure("invalid_params", "a label must contain at most 255 Unicode scalars")
		}
		if label != "" {
			set[label] = true
		}
	}
	result := make([]string, 0, len(set))
	for label := range set {
		result = append(result, label)
	}
	sort.Strings(result)
	return result, nil
}

// setLabels lowers the existing typed field into both the ontology value and
// the existing core permission/projection attribute. In particular a caller
// cannot change labels through fields while leaving authorization's image
// unchanged. Nil retains the ordinary field-deletion contract.
func setLabels(p *pending, value any, setter string) error {
	p.setBy["fields.labels"], p.setBy["labels"] = setter, setter
	if value == nil {
		delete(p.doc.Fields, "labels")
		p.doc.Labels = nil
		return nil
	}
	labels, err := labelSet(value)
	if err != nil {
		return err
	}
	_, present := p.doc.Fields["labels"]
	if len(labels) != 0 || present || len(p.doc.Labels) != 0 {
		p.doc.Fields["labels"] = labels
	}
	p.doc.Labels = append([]string(nil), labels...)
	return nil
}

// inheritLabels reads the immediate parent's ordered pending image through
// the tracked snapshot. Inheritance is a create-time union, not propagation
// on later parent updates or reparenting. Parent reads gain no write/version.
func (p *prepared) inheritLabels(ctx context.Context, child *pending) error {
	if child.doc.Parent == nil {
		return nil
	}
	parent, err := p.load(ctx, *child.doc.Parent)
	if err != nil {
		return err
	}
	var source any = parent.doc.Labels
	if fields, present := parent.doc.Fields["labels"]; present {
		source = fields
	}
	labels, err := labelSet(source)
	if err != nil {
		return err
	}
	if len(labels) == 0 {
		return nil
	}
	labels = append(labels, child.doc.Labels...)
	return setLabels(child, labels, "request")
}
