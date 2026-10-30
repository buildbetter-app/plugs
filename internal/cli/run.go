package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/buildbetter-app/plugs/internal/client"
)

// Execute selects an explicit server, then PLUG_SERVER, then workspace config,
// then persistent local SQLite. --memory is the explicit disposable local
// choice. The shared server command keeps its own composition lifecycle, while
// client-mode remote failures never open a local store.
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
	file, err := resolveFileConfig()
	if err != nil {
		fmt.Fprintln(errOut, err)
		return 2
	}
	if file.serverSet {
		if o.Memory {
			fmt.Fprintln(errOut, "--memory conflicts with server selected by .plug/config.yaml")
			return 2
		}
		return runRemote(ctx, file.server, ".plug/config.yaml", o, command, in, out, errOut)
	}
	storage := file.storage
	openFactory := factory
	if o.Memory {
		fmt.Fprintln(errOut, "warning: memory storage is disposable; all data is discarded when this process exits")
		storage = StorageConfig{Driver: "memory"}
		if openFactory == nil {
			openFactory = MemoryFactory
		}
	} else if openFactory == nil {
		openFactory = SQLiteFactory
	}
	l, err := OpenLocal(ctx, LocalConfig{Project: o.Project, Agent: o.Agent, Storage: storage, Factory: openFactory})
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

func runRemote(ctx context.Context, server, source string, o Options, command []string, in io.Reader, out, errOut io.Writer) int {
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

func runSession(ctx context.Context, s *Session, command []string, in io.Reader) error {
	if command[0] == "session" {
		return s.RunInput(ctx, in)
	}
	return s.Run(ctx, command)
}
