package main

import (
	"context"
	"fmt"
	"os"

	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/server/transport/stdio"
)

func main() {
	registry, err := rpc.NewRegistry()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	server, err := rpc.New(registry, rpc.Config{Incarnation: "stdio-fixture", DefaultProject: "fixture"})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	transport, err := stdio.New(server, stdio.Config{})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	if err := transport.Serve(context.Background(), os.Stdin, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
