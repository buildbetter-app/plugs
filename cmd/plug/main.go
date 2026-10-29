package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/buildbetter-app/plugs/internal/cli"
)

func main() {
	os.Exit(run())
}

func run() int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	// Reopen standard input so pipes use Go's poller. Closing the inherited
	// os.Stdin can otherwise wait for its blocking read instead of canceling it.
	input, err := os.Open("/dev/stdin")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer input.Close()
	return cli.Execute(ctx, os.Args[1:], input, os.Stdout, os.Stderr)
}
