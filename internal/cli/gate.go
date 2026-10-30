package cli

import (
	"flag"
	"fmt"
	"io"
	"strings"

	"github.com/buildbetter-app/plugs/internal/api/wire"
)

// parseGateCommand is the CLI adapter for the two core gate verbs in the
// frozen RPC catalog. It deliberately keeps the catalog's named parameters;
// the server registry remains authoritative for the operation and result.
func parseGateCommand(args []string) (Command, error) {
	if len(args) == 0 {
		return Command{}, fmt.Errorf("gate requires approve or report")
	}
	action := args[0]
	if action != "approve" && action != "report" {
		return Command{}, fmt.Errorf("unsupported gate command %q", action)
	}
	var expect, key, state, details string
	fs := flag.NewFlagSet("gate "+action, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.StringVar(&expect, "expect", "", "compare-and-set revision")
	fs.StringVar(&key, "idempotency-key", "", "idempotency key")
	if action == "report" {
		fs.StringVar(&state, "state", "", "resolver state")
		fs.StringVar(&details, "details", "", "resolver details as a JSON object")
	}
	ordered, err := interspersed(fs, args[1:])
	if err != nil {
		return Command{}, err
	}
	if err := fs.Parse(ordered); err != nil {
		return Command{}, err
	}
	if fs.NArg() != 1 || strings.TrimSpace(fs.Arg(0)) == "" {
		return Command{}, fmt.Errorf("gate %s requires exactly one gate ID", action)
	}
	if action == "report" && strings.TrimSpace(state) == "" {
		return Command{}, fmt.Errorf("gate report requires --state")
	}
	seen := map[string]bool{}
	fs.Visit(func(f *flag.Flag) { seen[f.Name] = true })

	ref := map[string]any{"id": fs.Arg(0)}
	params := map[string]any{"id": ref}
	if action == "report" {
		params["state"] = state
		if seen["details"] {
			value, err := wire.DecodeJSON([]byte(details))
			object, ok := value.(map[string]any)
			if err != nil || !ok {
				return Command{}, fmt.Errorf("--details must be a JSON object")
			}
			params["details"] = object
		}
	}
	if seen["expect"] {
		revision, err := revision(expect)
		if err != nil {
			return Command{}, err
		}
		params["expect"] = []any{map[string]any{"ref": ref, "revision": revision}}
	}
	return Command{Method: "gate." + action, Params: params, Key: key}, nil
}
