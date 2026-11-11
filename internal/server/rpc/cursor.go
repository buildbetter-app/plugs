package rpc

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/buildbetter-app/plugs/internal/api/wire"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/service"
)

const maxCursorBytes = 16384

type pageCodec struct {
	key         []byte
	ontology    model.Revision
	incarnation string
}
type cursorPayload struct {
	Version               int                           `json:"version"`
	Method                string                        `json:"method"`
	Principal             string                        `json:"principal"`
	Scope                 string                        `json:"scope"`
	Ontology              model.Revision                `json:"ontology"`
	Incarnation           string                        `json:"incarnation"`
	Offset                int                           `json:"offset"`
	AsOf                  asOf                          `json:"asOf"`
	AuthorizationRevision model.Revision                `json:"authorizationRevision,omitempty"`
	AuthorizationBound    bool                          `json:"authorizationBound,omitempty"`
	Workflow              bool                          `json:"workflow,omitempty"`
	WorkflowSnapshot      *service.WorkflowPageSnapshot `json:"workflowSnapshot,omitempty"`
	ReadyMembership       string                        `json:"readyMembership,omitempty"`
}

func newPageCodec(config Config) (*pageCodec, error) {
	key := append([]byte(nil), config.CursorKey...)
	if len(key) == 0 {
		key = make([]byte, 32)
		if _, err := rand.Read(key); err != nil {
			return nil, fmt.Errorf("cursor signing key generation failed: %w", err)
		}
	}
	if len(key) < 32 {
		return nil, fmt.Errorf("cursor signing key must contain at least 32 bytes")
	}
	return &pageCodec{key: key, ontology: config.OntologyRevision, incarnation: config.Incarnation}, nil
}

func hashJSON(value any) (string, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}
func (c *pageCodec) binding(call Call, p serviceParams) (string, string, error) {
	// Caveats and sponsor are part of the effective principal binding. An
	// anonymous client's asserted agent label is a scope, never an identity.
	agent := ""
	if call.Principal.Kind == "anonymous" {
		agent = call.Meta.Agent
	}
	principal, err := hashJSON(struct {
		Principal model.PrincipalRef `json:"principal"`
		Agent     string             `json:"agent,omitempty"`
	}{call.Principal, agent})
	if err != nil {
		return "", "", err
	}
	queryFilter := p.Filter
	if queryFilter == nil {
		queryFilter = &filter{}
	}
	var at *asOf
	if call.Method == "history.query" {
		if p.AsOf == nil {
			return "", "", fmt.Errorf("history requires asOf")
		}
		copy := *p.AsOf
		if copy.Incarnation == "" {
			copy.Incarnation = c.incarnation
		}
		at = &copy
	}
	scope, err := hashJSON(struct {
		Project string  `json:"project"`
		ViewID  string  `json:"viewId,omitempty"`
		Filter  *filter `json:"filter"`
		Query   string  `json:"query,omitempty"`
		AsOf    *asOf   `json:"asOf,omitempty"`
	}{call.Meta.Project, p.ViewID, queryFilter, p.Query, at})
	return principal, scope, err
}

func cursorInvalid() error {
	return &model.Error{Data: model.ErrorData{Code: "cursor_invalid", Hint: "cursor does not match the request; restart paging"}}
}
func incarnationChanged(from, to string) error {
	return &model.Error{Data: model.ErrorData{Code: "incarnation_changed", Details: map[string]any{"fromIncarnation": from, "toIncarnation": to}}}
}
func sameRevision(a, b model.Revision) bool {
	x, ex := a.Canonical()
	y, ey := b.Canonical()
	return ex == nil && ey == nil && x == y
}

func validReadyMembership(value string) bool {
	if value == "" {
		return true
	}
	value = strings.TrimPrefix(value, "v2:")
	if len(value) != sha256.Size*2 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func (c *pageCodec) decode(call Call, p serviceParams) (*service.PagePosition, error) {
	if p.Cursor == "" {
		c.defaultHistoryIncarnation(call, p)
		return nil, nil
	}
	if len(p.Cursor) > maxCursorBytes {
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
	mac := hmac.New(sha256.New, c.key)
	_, _ = mac.Write(body)
	if !hmac.Equal(signature, mac.Sum(nil)) {
		return nil, cursorInvalid()
	}
	var payload cursorPayload
	if err = wire.DecodeInto(body, &payload); err != nil || (payload.Version != 1 && payload.Version != 2) || payload.Offset < 0 {
		return nil, cursorInvalid()
	}
	// Version two is reserved for the domain-separated compiled Ready
	// commitment. Version-one tokens retain the original digest verifier so
	// existing signed cursors continue to work until normal expiry.
	if (payload.Version == 2) != strings.HasPrefix(payload.ReadyMembership, "v2:") {
		return nil, cursorInvalid()
	}
	// Check scope under the authenticated token's own generation before
	// reporting an incarnation change. In history's omitted-incarnation
	// form, that is the generation used when this token was issued.
	issued := *c
	issued.incarnation = payload.Incarnation
	principal, scope, err := issued.binding(call, p)
	if err != nil {
		return nil, cursorInvalid()
	}
	if payload.Method != call.Method || payload.Principal != principal || payload.Scope != scope || !sameRevision(payload.Ontology, c.ontology) || payload.AsOf.Incarnation != payload.Incarnation {
		return nil, cursorInvalid()
	}
	if payload.AuthorizationBound && payload.AuthorizationRevision.Validate() != nil {
		return nil, cursorInvalid()
	}
	if !payload.WorkflowSnapshot.Valid() || (payload.Workflow && payload.WorkflowSnapshot == nil) ||
		(call.Method != "bead.ready" && call.Method != "bead.blocked" && (payload.Workflow || payload.WorkflowSnapshot != nil)) {
		return nil, cursorInvalid()
	}
	if !validReadyMembership(payload.ReadyMembership) || (call.Method != "bead.ready" && call.Method != "bead.blocked" && payload.ReadyMembership != "") {
		return nil, cursorInvalid()
	}
	if payload.Incarnation != c.incarnation {
		return nil, incarnationChanged(payload.Incarnation, c.incarnation)
	}
	c.defaultHistoryIncarnation(call, p)
	return &service.PagePosition{
		Offset:                payload.Offset,
		AsOf:                  service.AsOf{Revision: payload.AsOf.Revision, Incarnation: payload.AsOf.Incarnation},
		AuthorizationRevision: payload.AuthorizationRevision,
		AuthorizationBound:    payload.AuthorizationBound,
		Workflow:              payload.Workflow,
		WorkflowSnapshot:      payload.WorkflowSnapshot,
		ReadyMembership:       payload.ReadyMembership,
	}, nil
}

func (c *pageCodec) defaultHistoryIncarnation(call Call, p serviceParams) {
	if call.Method == "history.query" && p.AsOf != nil && p.AsOf.Incarnation == "" {
		// p.AsOf belongs to this call's decoded parameters. The service must
		// receive the same generation as cursor binding and page validation.
		p.AsOf.Incarnation = c.incarnation
	}
}

func (c *pageCodec) encode(call Call, p serviceParams, position service.PagePosition) (string, error) {
	if position.Offset < 0 {
		return "", fmt.Errorf("service returned negative page offset")
	}
	if position.AsOf.Incarnation == "" {
		position.AsOf.Incarnation = c.incarnation
	}
	if position.AsOf.Incarnation != c.incarnation {
		return "", fmt.Errorf("service returned another incarnation")
	}
	if position.AuthorizationBound && position.AuthorizationRevision.Validate() != nil {
		return "", fmt.Errorf("service returned an invalid authorization revision")
	}
	if !position.WorkflowSnapshot.Valid() || (position.Workflow && position.WorkflowSnapshot == nil) ||
		(call.Method != "bead.ready" && call.Method != "bead.blocked" && (position.Workflow || position.WorkflowSnapshot != nil)) {
		return "", fmt.Errorf("service returned invalid workflow cursor state")
	}
	if !validReadyMembership(position.ReadyMembership) || (call.Method != "bead.ready" && call.Method != "bead.blocked" && position.ReadyMembership != "") {
		return "", fmt.Errorf("service returned invalid ready membership binding")
	}
	principal, scope, err := c.binding(call, p)
	if err != nil {
		return "", err
	}
	payload := cursorPayload{
		Version: 1, Method: call.Method, Principal: principal, Scope: scope,
		Ontology: c.ontology, Incarnation: c.incarnation, Offset: position.Offset,
		AsOf:                  asOf{position.AsOf.Revision, position.AsOf.Incarnation},
		AuthorizationRevision: position.AuthorizationRevision,
		AuthorizationBound:    position.AuthorizationBound,
		Workflow:              position.Workflow,
		WorkflowSnapshot:      position.WorkflowSnapshot,
		ReadyMembership:       position.ReadyMembership,
	}
	if strings.HasPrefix(position.ReadyMembership, "v2:") {
		payload.Version = 2
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	mac := hmac.New(sha256.New, c.key)
	_, _ = mac.Write(body)
	token := base64.RawURLEncoding.EncodeToString(body) + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	if len(token) > maxCursorBytes {
		return "", fmt.Errorf("cursor exceeds size bound")
	}
	return token, nil
}

func (c *pageCodec) page(call Call, p serviceParams, previous *service.PagePosition, value service.BeadPage) (map[string]any, error) {
	if value.NextCursor != "" {
		return nil, fmt.Errorf("service returned an unsigned legacy cursor")
	}
	pageIncarnation := value.AsOf.Incarnation
	if pageIncarnation == "" {
		pageIncarnation = c.incarnation
	}
	if pageIncarnation != c.incarnation {
		return nil, fmt.Errorf("service returned another page incarnation")
	}
	// Ready is a live list: the signed offset continues in current data and
	// each page reports its own revision. Only history pins the prior snapshot.
	if previous != nil && call.Method == "history.query" && !sameRevision(value.AsOf.Revision, previous.AsOf.Revision) {
		return nil, fmt.Errorf("service changed the pinned history snapshot")
	}
	out := pageJSON(value)
	if value.Next == nil {
		return out, nil
	}
	position := *value.Next
	if !position.WorkflowSnapshot.Equal(value.WorkflowSnapshot) {
		return nil, fmt.Errorf("service next position does not match workflow snapshot")
	}
	if position.ReadyMembership != value.ReadyMembership {
		return nil, fmt.Errorf("service next position does not match ready membership")
	}
	if position.AsOf.Incarnation == "" {
		position.AsOf.Incarnation = c.incarnation
	}
	if position.AsOf.Incarnation != pageIncarnation || !sameRevision(position.AsOf.Revision, value.AsOf.Revision) {
		return nil, fmt.Errorf("service next position does not match page asOf")
	}
	before := 0
	if previous != nil {
		before = previous.Offset
	}
	if position.Offset <= before && !(position.Workflow && (previous == nil || !previous.Workflow)) {
		return nil, fmt.Errorf("service page does not advance")
	}
	token, err := c.encode(call, p, position)
	if err != nil {
		return nil, err
	}
	out["nextCursor"] = token
	return out, nil
}

func (c *pageCodec) memoryPage(call Call, p serviceParams, previous *service.PagePosition, value service.MemoryPage) (map[string]any, error) {
	pageIncarnation := value.AsOf.Incarnation
	if pageIncarnation == "" {
		pageIncarnation = c.incarnation
	}
	if pageIncarnation != c.incarnation {
		return nil, fmt.Errorf("service returned another page incarnation")
	}
	// Memory lists are live. The previous revision is an observation, while
	// the signed incarnation and selector binding still identify the cursor.
	out := memoryPageJSON(value)
	if value.Next == nil {
		return out, nil
	}
	position := *value.Next
	if position.AsOf.Incarnation == "" {
		position.AsOf.Incarnation = c.incarnation
	}
	if position.AsOf.Incarnation != pageIncarnation || !sameRevision(position.AsOf.Revision, value.AsOf.Revision) {
		return nil, fmt.Errorf("service next position does not match memory page asOf")
	}
	before := 0
	if previous != nil {
		before = previous.Offset
	}
	if position.Offset <= before {
		return nil, fmt.Errorf("service memory page does not advance")
	}
	token, err := c.encode(call, p, position)
	if err != nil {
		return nil, err
	}
	out["nextCursor"] = token
	return out, nil
}
