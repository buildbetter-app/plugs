package composition

import (
	"io"
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
func newMCPHandler(server *rpc.Server, authenticator *authn.Authenticator, key [32]byte) (http.Handler, error) {
	return mcp.New(server, authenticator, mcp.Config{SessionKey: key})
}

// closeMCPHandler consumes the notification adapter's optional Close seam.
// The finite adapter has no resources to close. The streaming adapter closes
// its hub before HTTP drain so a connected GET cannot hold shutdown open.
func closeMCPHandler(handler http.Handler) error {
	if closer, ok := handler.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}
