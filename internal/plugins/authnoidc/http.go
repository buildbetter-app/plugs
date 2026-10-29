package authnoidc

import (
	"fmt"
	"net/http"

	"github.com/buildbetter-app/plugs/internal/server/authn"
)

// CarryIdentity wraps the admitted HTTP RPC handler to preserve the opaque
// identity for D32 consumers through authn.RequireInteractiveUser(ctx, by).
// The handler still performs its ordinary admission and all framing/posture
// checks. This adds no login routes, cookies, CORS or transport exemptions.
func CarryIdentity(a *authn.Authenticator, admitted http.Handler) (http.Handler, error) {
	if a == nil || admitted == nil {
		return nil, fmt.Errorf("authnoidc: authenticator and admitted HTTP handler are required")
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		identity, err := a.AdmitHTTP(r)
		if err == nil {
			r = r.WithContext(authn.WithIdentity(r.Context(), identity))
		}
		admitted.ServeHTTP(w, r)
	}), nil
}
