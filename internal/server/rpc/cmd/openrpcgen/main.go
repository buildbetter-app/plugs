// openrpcgen generates the bootstrap server's OpenRPC document from its actual
// Go registry. Composed servers generate discovery from their bound handlers.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/buildbetter-app/plugs/internal/api/wire"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
)

func main() {
	output := flag.String("out", "", "output path (default stdout)")
	catalogPath := flag.String("catalog", "", "explicit reconciled catalog for schema generation")
	flag.Parse()
	registry, err := rpc.NewRegistry()
	if err != nil {
		fail(err)
	}
	server, err := rpc.New(registry, rpc.Config{Incarnation: "openrpc-generator"})
	if err != nil {
		fail(err)
	}
	document, err := server.OpenRPC(context.Background(), model.PrincipalRef{Kind: "anonymous", ID: "anonymous"})
	if *catalogPath != "" {
		raw, readErr := os.ReadFile(*catalogPath)
		if readErr != nil {
			fail(readErr)
		}
		catalog, parseErr := wire.ParseCatalog(raw)
		if parseErr != nil {
			fail(parseErr)
		}
		document, err = catalog.GenerateOpenRPC(registry.Descriptors())
	}
	if err != nil {
		fail(err)
	}
	if *output == "" {
		_, err = os.Stdout.Write(document)
	} else {
		err = os.WriteFile(*output, document, 0644)
	}
	if err != nil {
		fail(err)
	}
}
func fail(err error) { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
