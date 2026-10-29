package events

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	core "github.com/buildbetter-app/plugs/internal/core/events"
	"github.com/buildbetter-app/plugs/internal/core/model"
)

type CursorBinding struct {
	Method           string         `json:"method"`
	FilterDigest     string         `json:"filterDigest"`
	PrincipalDigest  string         `json:"principalDigest"`
	OntologyRevision model.Revision `json:"ontologyRevision"`
}
type cursorPayload struct {
	Version  int           `json:"version"`
	Position core.Position `json:"position"`
	Binding  CursorBinding `json:"binding"`
}

type CursorCodec struct{ key []byte }

func NewCursorCodec(key []byte) (*CursorCodec, error) {
	if len(key) < 32 {
		return nil, fmt.Errorf("event cursor signing key must have at least 32 bytes")
	}
	return &CursorCodec{key: append([]byte(nil), key...)}, nil
}
func validBinding(b CursorBinding) bool {
	return b.Method != "" && b.FilterDigest != "" && b.PrincipalDigest != "" && b.OntologyRevision.Validate() == nil
}
func (c *CursorCodec) Encode(position core.Position, binding CursorBinding) (string, error) {
	if err := position.Validate(); err != nil {
		return "", err
	}
	if !validBinding(binding) {
		return "", core.ErrCursorInvalid
	}
	data, err := json.Marshal(cursorPayload{Version: 1, Position: position, Binding: binding})
	if err != nil {
		return "", err
	}
	mac := hmac.New(sha256.New, c.key)
	mac.Write(data)
	return base64.RawURLEncoding.EncodeToString(data) + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil)), nil
}
func (c *CursorCodec) Decode(token string, binding CursorBinding) (core.Position, error) {
	var empty core.Position
	if len(token) > 8192 || !validBinding(binding) {
		return empty, core.ErrCursorInvalid
	}
	payload, signature, ok := strings.Cut(token, ".")
	if !ok {
		return empty, core.ErrCursorInvalid
	}
	data, err := base64.RawURLEncoding.DecodeString(payload)
	if err != nil {
		return empty, core.ErrCursorInvalid
	}
	sig, err := base64.RawURLEncoding.DecodeString(signature)
	if err != nil {
		return empty, core.ErrCursorInvalid
	}
	mac := hmac.New(sha256.New, c.key)
	mac.Write(data)
	if !hmac.Equal(sig, mac.Sum(nil)) {
		return empty, core.ErrCursorInvalid
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var decoded cursorPayload
	if err = decoder.Decode(&decoded); err != nil {
		return empty, core.ErrCursorInvalid
	}
	if err = decoder.Decode(new(any)); err != io.EOF {
		return empty, core.ErrCursorInvalid
	}
	if decoded.Version != 1 || decoded.Position.Validate() != nil || decoded.Binding.Method != binding.Method || decoded.Binding.FilterDigest != binding.FilterDigest || decoded.Binding.PrincipalDigest != binding.PrincipalDigest {
		return empty, core.ErrCursorInvalid
	}
	cmp, err := core.CompareNatural(decoded.Binding.OntologyRevision, binding.OntologyRevision)
	if err != nil || cmp != 0 {
		return empty, core.ErrCursorInvalid
	}
	return decoded.Position, nil
}

// BindingFor binds a cursor to the effective caller (including sponsor and
// caveats), filter, method, ontology and current authorization generation.
// The host supplies policyVersion from its authoritative policy state.
func BindingFor(method string, filter core.Filter, principal model.PrincipalRef, ontology model.Revision, policyVersion string) (CursorBinding, error) {
	digest := func(value any) (string, error) {
		data, err := json.Marshal(value)
		if err != nil {
			return "", err
		}
		sum := sha256.Sum256(data)
		return hex.EncodeToString(sum[:]), nil
	}
	filterDigest, err := digest(filter)
	if err != nil {
		return CursorBinding{}, err
	}
	principalDigest, err := digest(struct {
		Principal     model.PrincipalRef
		PolicyVersion string
	}{principal, policyVersion})
	if err != nil {
		return CursorBinding{}, err
	}
	return CursorBinding{Method: method, FilterDigest: filterDigest, PrincipalDigest: principalDigest, OntologyRevision: ontology}, nil
}
