package workflow

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
)

// SourceCommentOrigin is unverified attribution from the imported physical
// comment row. Author never becomes a runtime principal or authorization grant.
type SourceCommentOrigin struct {
	Project string `json:"project"`
	Table   string `json:"table"`
	ID      string `json:"id"`
	Author  string `json:"author"`
}

func cloneCommentOrigin(source *SourceCommentOrigin) *SourceCommentOrigin {
	if source == nil {
		return nil
	}
	copy := *source
	return &copy
}

type sourceCommentWire struct {
	Body      string               `json:"body"`
	Principal *model.PrincipalRef  `json:"principal,omitempty"`
	Time      time.Time            `json:"time"`
	Source    *SourceCommentOrigin `json:"source,omitempty"`
}

func marshalComment(body string, principal model.PrincipalRef, when time.Time, source *SourceCommentOrigin) ([]byte, error) {
	wire := sourceCommentWire{Body: body, Time: when, Source: source}
	if source == nil {
		wire.Principal = &principal
	}
	return json.Marshal(wire)
}

func unmarshalComment(data []byte) (sourceCommentWire, error) {
	var wire sourceCommentWire
	if err := decodeWorkflowJSON(data, &wire); err != nil {
		return wire, err
	}
	if (wire.Principal == nil) == (wire.Source == nil) {
		return wire, fmt.Errorf("workflow comment requires authenticated principal or unverified source origin")
	}
	if wire.Source != nil && (wire.Source.Project == "" || wire.Source.ID == "" || (wire.Source.Table != "comments" && wire.Source.Table != dumpformat.EphemeralCommentTable)) {
		return wire, sourceRefusal("invalid imported comment origin")
	}
	return wire, nil
}

func (c commentState) MarshalJSON() ([]byte, error) {
	return marshalComment(c.Body, c.Principal, c.Time, c.Source)
}
func (c *commentState) UnmarshalJSON(data []byte) error {
	wire, err := unmarshalComment(data)
	if err != nil {
		return err
	}
	*c = commentState{Body: wire.Body, Time: wire.Time, Source: wire.Source}
	if wire.Principal != nil {
		c.Principal = *wire.Principal
	}
	return nil
}
func (c CommentView) MarshalJSON() ([]byte, error) {
	return marshalComment(c.Body, c.Principal, c.Time, c.Source)
}
func (c *CommentView) UnmarshalJSON(data []byte) error {
	wire, err := unmarshalComment(data)
	if err != nil {
		return err
	}
	*c = CommentView{Body: wire.Body, Time: wire.Time, Source: wire.Source}
	if wire.Principal != nil {
		c.Principal = *wire.Principal
	}
	return nil
}
