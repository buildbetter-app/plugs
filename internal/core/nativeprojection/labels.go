package nativeprojection

import "unicode/utf8"

// ExactLabelSet validates a decoded archive/stored-field JSON label set.
// It never sorts, deduplicates, trims, case-folds or normalizes identities.
func ExactLabelSet(raw any) ([]string, bool) {
	values, ok := raw.([]any)
	if !ok {
		return nil, false
	}
	labels := make([]string, len(values))
	for i, value := range values {
		label, ok := value.(string)
		if !ok || label == "" || !utf8.ValidString(label) ||
			utf8.RuneCountInString(label) > 255 ||
			i > 0 && labels[i-1] >= label {
			return nil, false
		}
		labels[i] = label
	}
	return labels, true
}
