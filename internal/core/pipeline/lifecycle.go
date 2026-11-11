package pipeline

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

// CreationTime returns the immutable host-supplied creation instant. A legacy
// body without createdAt has no known instant: neither its latest version time
// nor the time of a read or later edit is evidence of creation.
func (d Document) CreationTime() (time.Time, bool) {
	if d.CreatedAt == nil || d.CreatedAt.IsZero() {
		return time.Time{}, false
	}
	return d.CreatedAt.UTC(), true
}

func (d Document) validateLifecycleTimes() error {
	for _, value := range []struct {
		name string
		at   *time.Time
	}{{"createdAt", d.CreatedAt}, {"startedAt", d.StartedAt}, {"closedAt", d.ClosedAt}} {
		if value.at != nil && value.at.IsZero() {
			return fmt.Errorf("invalid bead %s", value.name)
		}
	}
	return nil
}

func validateLifecycleTimeMembers(body []byte) error {
	var members struct {
		CreatedAt json.RawMessage `json:"createdAt"`
		StartedAt json.RawMessage `json:"startedAt"`
		ClosedAt  json.RawMessage `json:"closedAt"`
	}
	if err := json.Unmarshal(body, &members); err != nil {
		return err
	}
	for _, raw := range []json.RawMessage{members.CreatedAt, members.StartedAt, members.ClosedAt} {
		if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			return fmt.Errorf("lifecycle time must be an instant or absent")
		}
	}
	return nil
}

// lifecycleTimes runs only after final ontology and graph validation. A3 slots
// select the category; concrete status names are never interpreted here. These
// are application-clock values in the same atomic body as the status and lease,
// not the storage driver's independently generated changeset/version time.
func (p *prepared) lifecycleTimes(stamp time.Time) error {
	for _, value := range p.writes {
		if value.kind == "memory" || value.deleted {
			continue
		}
		method := p.lifecycleMethod(value)
		if !isSlot(method) || isClaim(method) && p.proposal.SameClaim {
			continue
		}
		var beforeStatus string
		if len(value.before) != 0 {
			before, err := DecodeDocument(value.before)
			if err != nil {
				return err
			}
			beforeStatus, _ = before.Fields["status"].(string)
		}
		afterStatus, _ := value.doc.Fields["status"].(string)
		if beforeStatus == afterStatus {
			// Repeated close settlement retains the first close instant, even
			// when its optional reason adds a comment. Legacy absence stays absent.
			continue
		}
		switch method {
		case "bead.claim", "bead.claimNext":
			startedAt := stamp
			value.doc.StartedAt = &startedAt
			value.setBy["startedAt"] = "core"
			if value.doc.ClosedAt != nil {
				value.doc.ClosedAt = nil
				value.setBy["closedAt"] = "core"
			}
		case "bead.close":
			if value.doc.ClosedAt == nil {
				// The legacy named close still hashes and returns its original
				// patch plus optional comment. Record the first reason only on
				// its real transition; legacy settled rows remain identity-less.
				if value.doc.CloseReason == nil {
					reason := ""
					for _, change := range p.proposal.Changes {
						if change.Resource.Ref == value.ref && change.Op.Kind == model.OpComment {
							reason = change.Op.Comment.Body
							break
						}
					}
					value.doc.CloseReason = &reason
					value.setBy["closeReason"] = "request"
				}
				closedAt := stamp
				value.doc.ClosedAt = &closedAt
				value.setBy["closedAt"] = "core"
			}
		case "bead.fail", "bead.cancel":
			closedAt := stamp
			value.doc.ClosedAt = &closedAt
			value.setBy["closedAt"] = "core"
		case "bead.reopen", "bead.release", "bead.defer":
			if value.doc.CloseReason != nil {
				value.doc.CloseReason = nil
				value.setBy["closeReason"] = "core"
			}
			// Reopening clears terminal time but retains the last actual started
			// entry. A subsequent claim records its own started-category entry.
			if value.doc.ClosedAt != nil {
				value.doc.ClosedAt = nil
				value.setBy["closedAt"] = "core"
			}
		}
	}
	return nil
}
