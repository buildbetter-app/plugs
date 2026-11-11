package service

import (
	"slices"
	"strings"

	workflowplugin "github.com/buildbetter-app/plugs/plugins/workflow"
)

// Selection labels are case-sensitive. Native flag normalization trims empty
// entries; a normalized empty set adds no constraint. All/any/exclude compose.
func selectionLabels(values []string) []string {
	labels := make([]string, 0, len(values))
	for _, value := range values {
		if label := strings.TrimSpace(value); label != "" && !slices.Contains(labels, label) {
			labels = append(labels, label)
		}
	}
	return labels
}

func matchesSelectionFields(typ string, labels []string, assigned bool, filter *Filter) bool {
	if filter == nil {
		return true
	}
	if filter.Unassigned && assigned {
		return false
	}
	// An explicit type selection takes precedence over exclusions, matching
	// the native ReadyRequest contract. With no type selection, exclude types.
	if len(filter.Types) == 0 && slices.Contains(filter.ExcludeTypes, typ) {
		return false
	}
	for _, label := range selectionLabels(filter.Labels) {
		if !slices.Contains(labels, label) {
			return false
		}
	}
	anyLabels := selectionLabels(filter.LabelsAny)
	if len(anyLabels) != 0 {
		matched := false
		for _, label := range anyLabels {
			matched = matched || slices.Contains(labels, label)
		}
		if !matched {
			return false
		}
	}
	for _, label := range selectionLabels(filter.ExcludeLabels) {
		if slices.Contains(labels, label) {
			return false
		}
	}
	return true
}

func filterSelectionWorkflowItems(items []workflowplugin.ItemView, filter *Filter) []workflowplugin.ItemView {
	if filter == nil {
		return items
	}
	selected := make([]workflowplugin.ItemView, 0, len(items))
	for _, item := range items {
		if matchesSelectionFields(item.Type, item.Labels, item.Claim != nil, filter) {
			selected = append(selected, item)
		}
	}
	return selected
}
