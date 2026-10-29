package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/buildbetter-app/plugs/internal/client"
)

// Execute selects an explicit server, then PLUG_SERVER, then the explicit
// in-process --memory mode. The shared server command owns config-file loading;
// client commands never open a configured store implicitly.
func Execute(ctx context.Context, args []string, in io.Reader, out, errOut io.Writer) int {
	return execute(ctx, args, in, out, errOut, nil)
}

func execute(ctx context.Context, args []string, in io.Reader, out, errOut io.Writer, factory StorageFactory) int {
	if isServerCommand(args) {
		return executeServer(ctx, args, out, errOut)
	}
	o, command, err := Parse(args)
	if errors.Is(err, ErrHelp) {
		fmt.Fprint(out, Usage)
		return 0
	}
	if err != nil {
		fmt.Fprintln(errOut, err)
		return 2
	}
	if command[0] == "session" {
		if len(command) != 1 {
			fmt.Fprintln(errOut, "session takes no arguments")
			return 2
		}
	} else if _, err := ParseCommand(command); err != nil {
		fmt.Fprintln(errOut, err)
		return 2
	}
	server, source := o.Server, "--server"
	if !o.serverSet {
		server, source = os.Getenv("PLUG_SERVER"), "PLUG_SERVER"
	}
	if server != "" {
		transport, err := client.NewHTTP(client.HTTPConfig{Server: server, Token: os.Getenv("PLUG_TOKEN")})
		if err != nil {
			fmt.Fprintf(errOut, "%s: %v\n", source, err)
			return 2
		}
		defer transport.Close()
		remote, err := client.New(transport, o.Project, o.Agent)
		if err == nil {
			err = runSession(ctx, &Session{Client: remote, Out: out, Err: errOut, JSON: o.JSON}, command, in)
		}
		if err != nil {
			fmt.Fprintf(errOut, "%s: %v\n", source, err)
			return 1
		}
		return 0
	}
	if _, err := os.Stat(".plug/config.yaml"); err == nil {
		fmt.Fprintln(errOut, ".plug/config.yaml loading is not installed; select --server explicitly")
		return 2
	} else if !errors.Is(err, os.ErrNotExist) {
		fmt.Fprintln(errOut, "unable to inspect .plug/config.yaml")
		return 2
	}
	if !o.Memory {
		fmt.Fprintln(errOut, "persistent storage is not installed; use --memory for an explicit disposable session")
		return 2
	}
	fmt.Fprintln(errOut, "warning: memory storage is disposable; all data is discarded when this process exits")
	l, err := OpenLocal(ctx, LocalConfig{Project: o.Project, Agent: o.Agent, Storage: StorageConfig{Driver: "memory"}, Factory: factory})
	if err != nil {
		fmt.Fprintln(errOut, err)
		return 1
	}
	s := &Session{Client: l.Client, Out: out, Err: errOut, JSON: o.JSON}
	err = runSession(ctx, s, command, in)
	closeErr := l.Close()
	if err = errors.Join(err, closeErr); err != nil {
		fmt.Fprintln(errOut, err)
		return 1
	}
	return 0
}

func runSession(ctx context.Context, s *Session, command []string, in io.Reader) error {
	if command[0] == "session" {
		return s.RunInput(ctx, in)
	}
	return s.Run(ctx, command)
}
