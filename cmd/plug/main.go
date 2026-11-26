package main

import (
	"context"
	"io"
	"os"
	"os/signal"
	"strings"
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
	// Detached invocations can lack a reopenable stdin. Treat that as EOF.
	var input io.Reader = strings.NewReader("")
	if stdin, err := os.Open("/dev/stdin"); err == nil {
		defer stdin.Close()
		input = stdin
	}
	return cli.Execute(ctx, os.Args[1:], input, os.Stdout, os.Stderr)
}
