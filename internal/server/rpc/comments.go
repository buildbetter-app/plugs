package rpc

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"strings"

	"github.com/buildbetter-app/plugs/internal/api/wire"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/service"
)

type commentCursorPayload struct {
	Version     int                     `json:"version"`
	Method      string                  `json:"method"`
	Principal   string                  `json:"principal"`
	Scope       string                  `json:"scope"`
	Ontology    model.Revision          `json:"ontology"`
	Incarnation string                  `json:"incarnation"`
	AsOf        asOf                    `json:"asOf"`
	After       service.CommentPosition `json:"after"`
}

// A comment cursor binds the selected bead as well as the authenticated
// principal. Ordinary list cursors do not select an individual bead.
func commentBinding(c Call, id model.Ref, requested *asOf) (string, string, error) {
	agent := ""
	if c.Principal.Kind == "anonymous" {
		agent = c.Meta.Agent
	}
	principal, err := hashJSON(struct {
		Principal model.PrincipalRef `json:"principal"`
		Agent     string             `json:"agent,omitempty"`
	}{c.Principal, agent})
	if err != nil {
		return "", "", err
	}
	scope, err := hashJSON(struct {
		ID      model.Ref `json:"id"`
		Project string    `json:"project"`
		AsOf    *asOf     `json:"asOf,omitempty"`
	}{id, c.Meta.Project, requested})
	return principal, scope, err
}

func decodeCommentCursor(c Call, p serviceParams, id model.Ref) (*commentCursorPayload, error) {
	if p.Cursor == "" {
		return nil, nil
	}
	if len(p.Cursor) > maxCursorBytes || c.paging == nil {
		return nil, cursorInvalid()
	}
	parts := strings.Split(p.Cursor, ".")
	if len(parts) != 2 {
		return nil, cursorInvalid()
	}
	body, err := base64.RawURLEncoding.Strict().DecodeString(parts[0])
	if err != nil {
		return nil, cursorInvalid()
	}
	signature, err := base64.RawURLEncoding.Strict().DecodeString(parts[1])
	if err != nil {
		return nil, cursorInvalid()
	}
	mac := hmac.New(sha256.New, c.paging.key)
	_, _ = mac.Write(body)
	if !hmac.Equal(signature, mac.Sum(nil)) {
		return nil, cursorInvalid()
	}
	var payload commentCursorPayload
	if wire.DecodeInto(body, &payload) != nil || payload.Version != 1 || payload.Method != "bead.comments" || payload.After.ID == 0 || payload.AsOf.Revision.Validate() != nil || payload.AsOf.Incarnation != payload.Incarnation {
		return nil, cursorInvalid()
	}
	principal, scope, err := commentBinding(c, id, p.AsOf)
	if err != nil || payload.Principal != principal || payload.Scope != scope || !sameRevision(payload.Ontology, c.paging.ontology) {
		return nil, cursorInvalid()
	}
	if payload.Incarnation != c.paging.incarnation {
		return nil, incarnationChanged(payload.Incarnation, c.paging.incarnation)
	}
	return &payload, nil
}

func encodeCommentCursor(c Call, p serviceParams, id model.Ref, at asOf, after service.CommentPosition) (string, error) {
	principal, scope, err := commentBinding(c, id, p.AsOf)
	if err != nil {
		return "", err
	}
	payload := commentCursorPayload{
		Version: 1, Method: "bead.comments", Principal: principal, Scope: scope,
		Ontology: c.paging.ontology, Incarnation: c.paging.incarnation,
		AsOf: at, After: after,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	mac := hmac.New(sha256.New, c.paging.key)
	_, _ = mac.Write(body)
	token := base64.RawURLEncoding.EncodeToString(body) + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	if len(token) > maxCursorBytes {
		return "", cursorInvalid()
	}
	return token, nil
}

// dispatchComments obtains the thread through the existing bead.get read
// authorization and current-policy fence, then pages that pinned thread.
func dispatchComments(ctx context.Context, svc Service, c Call, p serviceParams, limit int) (any, error) {
	if limit > 500 {
		return nil, &model.Error{Data: model.ErrorData{Code: "invalid_params", Hint: "comment page limit must be between 1 and 500"}}
	}
	id, err := p.refID()
	if err != nil {
		return nil, &model.Error{Data: model.ErrorData{Code: "invalid_params", Hint: err.Error()}}
	}
	position, err := decodeCommentCursor(c, p, id)
	if err != nil {
		return nil, err
	}
	selected := p.AsOf.domain()
	var after *service.CommentPosition
	if position != nil {
		selected = &service.AsOf{Revision: position.AsOf.Revision, Incarnation: position.AsOf.Incarnation}
		after = &position.After
	}
	thread, err := svc.Get(ctx, c.Principal, service.GetRequest{ID: id, AsOf: selected})
	if err != nil {
		return nil, err
	}
	if selected == nil {
		selected = &service.AsOf{Revision: thread.Bead.Revision, Incarnation: c.paging.incarnation}
	}
	if selected.Incarnation == "" {
		selected.Incarnation = c.paging.incarnation
	}
	if selected.Incarnation != c.paging.incarnation {
		return nil, incarnationChanged(selected.Incarnation, c.paging.incarnation)
	}
	page, err := service.PageComments(thread.Comments, limit, after)
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(page.Items))
	for _, item := range page.Items {
		comment := item.Comment
		value := map[string]any{"body": comment.Body, "principal": comment.Principal, "time": timestamp(comment.Time)}
		if comment.Origin != "" {
			value["origin"] = comment.Origin
			if comment.SourceTime != nil {
				value["sourceTime"] = timestamp(*comment.SourceTime)
			}
			if comment.SourceAuthor != "" {
				value["sourceAuthor"] = comment.SourceAuthor
			}
		}
		items = append(items, map[string]any{"id": item.ID, "comment": value})
	}
	at := asOf{Revision: selected.Revision, Incarnation: selected.Incarnation}
	result := map[string]any{"comments": items, "asOf": at}
	if page.Next != nil {
		token, err := encodeCommentCursor(c, p, id, at, *page.Next)
		if err != nil {
			return nil, err
		}
		result["nextCursor"] = token
	}
	return result, nil
}
