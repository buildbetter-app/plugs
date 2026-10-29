package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
)

// Execute opens exactly one local core for the invocation. Only explicit
// --memory selects disposable state; unimplemented remote or persistent modes
// return an error instead of silently losing data or falling back to local.
func Execute(ctx context.Context, args []string, in io.Reader, out, errOut io.Writer) int {
	return execute(ctx, args, in, out, errOut, nil)
}

func execute(ctx context.Context, args []string, in io.Reader, out, errOut io.Writer, factory StorageFactory) int {
	o, command, err := Parse(args)
	if errors.Is(err, ErrHelp) {
		fmt.Fprint(out, Usage)
		return 0
	}
	if err != nil {
		fmt.Fprintln(errOut, err)
		return 2
	}
	if os.Getenv("PLUG_SERVER") != "" {
		fmt.Fprintln(errOut, "remote transport is not implemented by this slice; PLUG_SERVER was not contacted")
		return 2
	}
	if !o.Memory {
		fmt.Fprintln(errOut, "persistent storage is not installed; use --memory for an explicit disposable session")
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
	fmt.Fprintln(errOut, "warning: memory storage is disposable; all data is discarded when this process exits")
	l, err := OpenLocal(ctx, LocalConfig{Project: o.Project, Agent: o.Agent, Storage: StorageConfig{Driver: "memory"}, Factory: factory})
	if err != nil {
		fmt.Fprintln(errOut, err)
		return 1
	}
	s := &Session{Client: l.Client, Out: out, Err: errOut, JSON: o.JSON}
	if command[0] == "session" {
		err = s.RunInput(ctx, in)
	} else {
		err = s.Run(ctx, command)
	}
	closeErr := l.Close()
	if err = errors.Join(err, closeErr); err != nil {
		fmt.Fprintln(errOut, err)
		return 1
	}
	return 0
}
