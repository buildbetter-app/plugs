package authz

import "context"

// ReplayReadPreparation preserves the exact per-resource reference demand.
// It delegates Match once, with no additional fact reads. The only exported
// bit is prepare's current-context result, never the Match permission result.
// The closed storage adapter independently evaluates all permission terms.
// This deliberately retains reference CPU work; it is a correctness stage.
func (t FilterTemplate) ReplayReadPreparation(ctx context.Context, s Slice, r Resource) (bool, error) {
	if s == nil || t.NoPlugins {
		// In this branch Match itself owns nil-slice/scoped-plugin behavior.
		return t.Match(ctx, s, r)
	}
	traced := &readPreparationSlice{Slice: s}
	_, err := t.Match(ctx, traced, r)
	if err != nil {
		return false, err
	}
	return traced.checked && traced.accepted, nil
}

type readPreparationSlice struct {
	Slice
	checked, accepted bool
}

func (s *readPreparationSlice) CheckContext(ctx context.Context, caller Caller) (bool, error) {
	ok, err := s.Slice.CheckContext(ctx, caller)
	s.checked, s.accepted = true, ok && err == nil
	return ok, err
}
