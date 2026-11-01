package rehearsal

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"sync/atomic"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/server/authn"
)

// fixtureAdmin is a synthetic trusted-host session source, private to an owned
// loopback fixture. It cannot accept real credentials or perform a login. It
// exercises the admin transport boundary without claiming actual D32 proof.
// Ordinary service keys remain noninteractive and must fail admin admission.
type fixtureAdmin struct {
	token   string
	expires time.Time
	revoked atomic.Bool
}

func newFixtureAdmin() (*fixtureAdmin, error) {
	var secret [32]byte
	if _, err := rand.Read(secret[:]); err != nil {
		return nil, err
	}
	return &fixtureAdmin{token: authn.CoreSessionPrefix + hex.EncodeToString(secret[:]), expires: time.Now().Add(time.Hour)}, nil
}

func (s *fixtureAdmin) VerifyCoreSession(ctx context.Context, token string) (authn.CoreSession, error) {
	if ctx.Err() != nil {
		return nil, authn.ErrUnavailable
	}
	if subtle.ConstantTimeCompare([]byte(token), []byte(s.token)) != 1 {
		return nil, authn.ErrUnauthenticated
	}
	if err := s.RequireInteractiveUser(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *fixtureAdmin) Principal() model.PrincipalRef {
	return model.PrincipalRef{Kind: "user", ID: "user:rehearsal-admin"}
}
func (s *fixtureAdmin) RequireInteractiveUser() error {
	if s.revoked.Load() {
		return authn.ErrUnauthenticated
	}
	if !time.Now().Before(s.expires) {
		return authn.ErrTokenExpired
	}
	return nil
}
