package service

import (
	"bytes"
	"encoding/json"
	"sort"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// QueryRewriteForPrincipal lowers only the deny-only refs caveat into a
// storage candidate rewrite. It deliberately ignores every other caveat and
// malformed input, leaving the ordinary authorization interpreter as the
// authority. Multiple sponsor layers are intersected exactly as caveatsAllow
// evaluates them.
func QueryRewriteForPrincipal(principal model.PrincipalRef) *driver.QueryRewrite {
	var selected map[string]struct{}
	saw := false
	for current := &principal; current != nil; current = current.Sponsor {
		value, ok := current.Caveats["refs"]
		if !ok {
			continue
		}
		raw, err := json.Marshal(value)
		if err != nil || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			return nil
		}
		var refs []model.Ref
		if err := json.Unmarshal(raw, &refs); err != nil {
			return nil
		}
		ids := make(map[string]struct{}, len(refs))
		for _, ref := range refs {
			if err := ref.Validate(); err != nil || !ref.IsID() || !ref.Branch.IsZero() {
				return nil
			}
			ids[ref.ID] = struct{}{}
		}
		if !saw {
			selected = ids
			saw = true
			continue
		}
		for id := range selected {
			if _, ok := ids[id]; !ok {
				delete(selected, id)
			}
		}
	}
	if !saw {
		return nil
	}
	ids := make([]string, 0, len(selected))
	for id := range selected {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return &driver.QueryRewrite{IDs: ids}
}
