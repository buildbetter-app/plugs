package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/buildbetter-app/plugs/internal/server/composition"
)

type serverOptions struct {
	local bool
}

func isServerCommand(args []string) bool {
	return len(args) > 0 && args[0] == "server"
}

func parseServerOptions(args []string) (serverOptions, error) {
	if len(args) == 0 || args[0] != "server" {
		return serverOptions{}, errors.New("server command is required")
	}
	var options serverOptions
	for _, arg := range args[1:] {
		switch arg {
		case "--local":
			if options.local {
				return serverOptions{}, errors.New("server: duplicate --local")
			}
			options.local = true
		case "-h", "--help":
			return serverOptions{}, ErrHelp
		default:
			if strings.HasPrefix(arg, "-") {
				return serverOptions{}, fmt.Errorf("server: unsupported option %s", arg)
			}
			return serverOptions{}, errors.New("server takes no positional arguments")
		}
	}
	return options, nil
}

func executeServer(ctx context.Context, args []string, out, errOut io.Writer) int {
	options, err := parseServerOptions(args)
	if errors.Is(err, ErrHelp) {
		fmt.Fprint(out, ServerUsage)
		return 0
	}
	if err != nil {
		fmt.Fprintln(errOut, err)
		return 2
	}
	var config composition.Config
	if options.local {
		config = composition.Config{
			Driver:  "memory",
			Project: "local",
			Address: composition.DefaultAddress,
			UI:      true,
		}
	} else {
		config, err = composition.LoadConfigFile(".plug/config.yaml")
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				fmt.Fprintln(errOut, "server: .plug/config.yaml is required; use `plug server --local` for disposable loopback mode")
			} else {
				fmt.Fprintln(errOut, err)
			}
			return 2
		}
	}
	runtime, err := composition.Open(ctx, config)
	if err != nil {
		fmt.Fprintf(errOut, "server: startup failed: %s\n", redactServerError(err, config.DSN))
		return 1
	}
	server, err := runtime.Listen(ctx, config.Address, nil)
	if err != nil {
		_ = runtime.Close(context.Background())
		fmt.Fprintf(errOut, "server: listen failed: %s\n", redactServerError(err, config.DSN))
		return 1
	}
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve() }()
	endpoint := "http://" + server.Addr().String()
	fmt.Fprintf(out, "plug server listening on %s\n", endpoint)
	if options.local || config.Driver == "memory" {
		fmt.Fprintln(errOut, "warning: memory storage is disposable; data is discarded when this server exits")
	}

	exitCode := 0
	select {
	case serveErr := <-serveDone:
		closeErr := runtime.Close(context.Background())
		if serveErr != nil && !errors.Is(serveErr, context.Canceled) && !errors.Is(serveErr, os.ErrClosed) && !errors.Is(serveErr, io.ErrClosedPipe) && !errors.Is(serveErr, http.ErrServerClosed) {
			fmt.Fprintf(errOut, "server: serve failed: %s\n", redactServerError(serveErr, config.DSN))
			return 1
		}
		if closeErr != nil {
			fmt.Fprintf(errOut, "server: shutdown failed: %s\n", redactServerError(closeErr, config.DSN))
			return 1
		}
		return 0
	case <-ctx.Done():
	case <-runtime.Lease.Lost():
		if ctx.Err() == nil {
			fmt.Fprintln(errOut, "server: active storage lease was lost")
			exitCode = 1
		}
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	closeErr := runtime.Close(shutdownCtx)
	cancel()
	serveErr := <-serveDone
	if closeErr != nil {
		fmt.Fprintf(errOut, "server: shutdown failed: %s\n", redactServerError(closeErr, config.DSN))
		return 1
	}
	if serveErr != nil && !errors.Is(serveErr, context.Canceled) && !errors.Is(serveErr, os.ErrClosed) && !errors.Is(serveErr, io.ErrClosedPipe) && !errors.Is(serveErr, http.ErrServerClosed) {
		fmt.Fprintf(errOut, "server: serve failed during shutdown: %s\n", redactServerError(serveErr, config.DSN))
		return 1
	}
	return exitCode
}

func redactServerError(err error, dsn string) string {
	if err == nil {
		return ""
	}
	message := err.Error()
	if dsn != "" {
		message = strings.ReplaceAll(message, dsn, "<redacted>")
	}
	return message
}

const ServerUsage = `Usage: plug server [--local]

Starts the shared HTTP server. The default reads .plug/config.yaml and may use
memory, sqlite, or postgres storage. --local always uses disposable memory
storage on a loopback ephemeral listener and serves the embedded UI.
`
