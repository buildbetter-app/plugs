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

const commentPageCursorMethod = "bead.get.commentPage"

type commentPageSelection struct {
	Limit  model.Natural `json:"limit"`
	Cursor string        `json:"cursor"`
}

func selectsCommentPage(params json.RawMessage) bool {
	var object map[string]json.RawMessage
	if json.Unmarshal(params, &object) != nil {
		return false
	}
	_, selected := object["commentPage"]
	return selected
}

// A comment cursor binds the selected bead as well as the authenticated
// principal. Ordinary list cursors do not select an individual bead.
func commentBinding(c Call, id model.Ref, requested *asOfSelector) (string, string, error) {
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
	if requested != nil && requested.Instant != nil {
		copy := *requested
		instant := copy.Instant.UTC()
		copy.Instant = &instant
		requested = &copy
	}
	scope, err := hashJSON(struct {
		ID      model.Ref     `json:"id"`
		Project string        `json:"project"`
		AsOf    *asOfSelector `json:"asOf,omitempty"`
	}{id, c.Meta.Project, requested})
	return principal, scope, err
}

func decodeCommentCursor(c Call, p serviceParams, id model.Ref) (*commentCursorPayload, error) {
	if p.CommentPage.Cursor == "" {
		return nil, nil
	}
	if len(p.CommentPage.Cursor) > maxCursorBytes || c.paging == nil {
		return nil, cursorInvalid()
	}
	parts := strings.Split(p.CommentPage.Cursor, ".")
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
	if wire.DecodeInto(body, &payload) != nil || payload.Version != 1 || payload.Method != commentPageCursorMethod || payload.After.ID == 0 || payload.AsOf.Revision.Validate() != nil || payload.AsOf.Incarnation != payload.Incarnation {
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
		Version: 1, Method: commentPageCursorMethod, Principal: principal, Scope: scope,
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

// dispatchCommentPage uses the same bead.get authorization and current-policy
// fence while replacing the unbounded legacy comment array with one page.
func dispatchCommentPage(ctx context.Context, svc Service, c Call, p serviceParams, id model.Ref) (any, error) {
	limit, err := p.CommentPage.Limit.Uint64()
	if err != nil || limit > 500 {
		return nil, &model.Error{Data: model.ErrorData{Code: "invalid_params", Hint: "comment page limit must be between 1 and 500"}}
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
	thread, err := svc.Get(ctx, c.Principal, service.GetRequest{ID: id, AsOf: selected, Project: c.Meta.Project, Agent: c.Meta.Agent})
	if err != nil {
		return nil, err
	}
	if selected == nil || selected.Instant != nil {
		// Comments are stored in this bead version. Pin that exact returned
		// version, as for a current read, rather than resolving the input
		// instant again after a concurrent eligible append.
		selected = &service.AsOf{Revision: thread.Bead.Revision, Incarnation: c.paging.incarnation}
	}
	if selected.Incarnation == "" {
		selected.Incarnation = c.paging.incarnation
	}
	if selected.Incarnation != c.paging.incarnation {
		return nil, incarnationChanged(selected.Incarnation, c.paging.incarnation)
	}
	page, err := service.PageComments(thread.Comments, int(limit), after)
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
	out := beadGetJSON(thread, false)
	out["commentPage"] = result
	return out, nil
}
