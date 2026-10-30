package composition

import (
	"errors"
	"net/http"

	"github.com/buildbetter-app/plugs/internal/server/authn"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/server/transport/mcp"
)

// newMCPHandler adapts the already-composed RPC and authentication ports.
// The MCP transport owns framing only: it receives the same canonical server,
// admitted principal and request lifetime as /rpc/v1. The composition root
// owns the listener and shuts both routes down through one http.Server.
//
// Keep this helper narrow so the continuous notification stream can provide a
// sink here without adding a second registry, authenticator or session store.
func newMCPHandler(server *rpc.Server, authenticator *authn.Authenticator) (http.Handler, error) {
	if server == nil || authenticator == nil {
		return nil, errors.New("composition: MCP requires the composed RPC server and authenticator")
	}
	return mcp.New(server, authenticator, mcp.Config{})
}
