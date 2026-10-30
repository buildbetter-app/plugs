package viewrpc

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/viewops"
)

const maxCursorBytes = 16 * 1024

// CursorCodec signs the core cursor payload. The payload is opaque on the
// wire; core still validates all request bindings after this MAC check.
type CursorCodec struct {
	key []byte
}

func NewCursorCodec(key []byte) (*CursorCodec, error) {
	key = append([]byte(nil), key...)
	if len(key) == 0 {
		key = make([]byte, 32)
		if _, err := rand.Read(key); err != nil {
			return nil, fmt.Errorf("view cursor signing key generation failed: %w", err)
		}
	}
	if len(key) < 32 {
		return nil, fmt.Errorf("view cursor signing key must contain at least 32 bytes")
	}
	return &CursorCodec{key: key}, nil
}

func (c *CursorCodec) Encode(cursor *viewops.Cursor) (string, error) {
	if c == nil || cursor == nil || cursor.Version != 1 || cursor.Method != "view.run" {
		return "", fmt.Errorf("invalid view cursor")
	}
	body, err := json.Marshal(cursor)
	if err != nil {
		return "", err
	}
	mac := hmac.New(sha256.New, c.key)
	_, _ = mac.Write(body)
	token := "v1." + base64.RawURLEncoding.EncodeToString(body) + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	if len(token) > maxCursorBytes {
		return "", fmt.Errorf("view cursor exceeds size bound")
	}
	return token, nil
}

func (c *CursorCodec) Decode(token string) (*viewops.Cursor, error) {
	if c == nil || len(token) == 0 || len(token) > maxCursorBytes {
		return nil, cursorInvalid()
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 || parts[0] != "v1" {
		return nil, cursorInvalid()
	}
	body, err := base64.RawURLEncoding.Strict().DecodeString(parts[1])
	if err != nil {
		return nil, cursorInvalid()
	}
	signature, err := base64.RawURLEncoding.Strict().DecodeString(parts[2])
	if err != nil {
		return nil, cursorInvalid()
	}
	mac := hmac.New(sha256.New, c.key)
	_, _ = mac.Write(body)
	if !hmac.Equal(signature, mac.Sum(nil)) {
		return nil, cursorInvalid()
	}
	var cursor viewops.Cursor
	if err := json.Unmarshal(body, &cursor); err != nil || cursor.Version != 1 || cursor.Method != "view.run" || cursor.Incarnation == "" || cursor.Revision.Validate() != nil {
		return nil, cursorInvalid()
	}
	return &cursor, nil
}

func cursorInvalid() error {
	return &model.Error{Data: model.ErrorData{Code: "cursor_invalid", Hint: "cursor does not match the view request; restart paging"}}
}
