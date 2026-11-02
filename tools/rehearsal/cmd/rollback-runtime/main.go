package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/buildbetter-app/plugs/tools/rehearsal"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if len(os.Args) != 1 || rehearsal.ServeRollbackRuntime(ctx) != nil {
		fmt.Fprintln(os.Stderr, "owned disposable runtime failed")
		os.Exit(1)
	}
}
