// openrpcgen generates the bootstrap server's OpenRPC document from its actual
// Go registry. Composed servers generate discovery from their bound handlers.
package main

import (
	"context"
	"encoding/json"
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
	if err != nil {
		fail(err)
	}
	if *catalogPath != "" {
		raw, readErr := os.ReadFile(*catalogPath)
		if readErr != nil {
			fail(readErr)
		}
		catalog, parseErr := wire.ParseCatalog(raw)
		if parseErr != nil {
			fail(parseErr)
		}
		var bootstrap struct {
			Methods []struct {
				Name string `json:"name"`
			} `json:"methods"`
		}
		if decodeErr := json.Unmarshal(document, &bootstrap); decodeErr != nil {
			fail(decodeErr)
		}
		descriptors := make([]wire.MethodDesc, 0, len(bootstrap.Methods))
		for _, method := range bootstrap.Methods {
			descriptor, ok := catalog.Descriptor(method.Name)
			if !ok {
				fail(fmt.Errorf("bound bootstrap method missing from supplied catalog: %s", method.Name))
			}
			descriptors = append(descriptors, descriptor)
		}
		document, err = catalog.GenerateOpenRPC(descriptors)
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
