package cli

import (
	"flag"
	"fmt"
	"io"
	"strings"

	"github.com/buildbetter-app/plugs/internal/api/wire"
)

// parseAgentCommand rewrites agent-facing operations to the existing wire
// catalog. It adds no storage fallback, shell execution, or write retry.
func parseAgentCommand(args []string) (Command, error) {
	name, rest := args[0], args[1:]
	method := map[string]string{"brief": "agent.brief", "claim-next": "bead.claimNext"}[name]
	if name == "memory" {
		if len(rest) == 0 {
			return Command{}, fmt.Errorf("memory requires list, get, save or delete")
		}
		name, rest = "memory."+rest[0], rest[1:]
		if name != "memory.list" && name != "memory.get" && name != "memory.save" && name != "memory.delete" {
			return Command{}, fmt.Errorf("unsupported memory command")
		}
		method = name
	}
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var typ, cursor, query, memoryKey, content, metadata, key string
	var limit, budget int
	switch name {
	case "brief":
		fs.IntVar(&budget, "budget-tokens", 2048, "brief token budget")
	case "claim-next":
		fs.StringVar(&typ, "type", "", "record type")
	case "memory.list":
		fs.StringVar(&query, "query", "", "memory query")
		fs.StringVar(&cursor, "cursor", "", "continuation cursor")
		fs.IntVar(&limit, "limit", 1000, "page limit")
	case "memory.save":
		fs.StringVar(&memoryKey, "key", "", "memory key")
		fs.StringVar(&content, "content", "", "memory content")
		fs.StringVar(&metadata, "metadata", "", "metadata object")
	}
	write := name == "claim-next" || name == "memory.save" || name == "memory.delete"
	if write {
		fs.StringVar(&key, "idempotency-key", "", "explicit write retry key")
	}
	ordered, err := interspersed(fs, rest)
	if err != nil {
		return Command{}, err
	}
	if err := fs.Parse(ordered); err != nil {
		return Command{}, err
	}
	params := map[string]any{}
	if name == "memory.get" || name == "memory.delete" {
		if fs.NArg() != 1 || strings.TrimSpace(fs.Arg(0)) == "" {
			return Command{}, fmt.Errorf("%s requires exactly one memory ID", name)
		}
		params["id"] = fs.Arg(0)
	} else if fs.NArg() != 0 {
		return Command{}, fmt.Errorf("%s takes no positional arguments", name)
	}
	switch name {
	case "brief":
		if budget < 1 {
			return Command{}, fmt.Errorf("--budget-tokens must be positive")
		}
		params["budgetTokens"] = budget
	case "claim-next":
		if typ != "" {
			params["filter"] = map[string]any{"types": []string{typ}}
		}
	case "memory.list":
		if limit < 1 || limit > 1000 {
			return Command{}, fmt.Errorf("--limit must be between 1 and 1000")
		}
		params["limit"] = limit
		if query != "" {
			params["query"] = query
		}
		if cursor != "" {
			params["cursor"] = cursor
		}
	case "memory.save":
		seenContent := false
		fs.Visit(func(f *flag.Flag) { seenContent = seenContent || f.Name == "content" })
		if strings.TrimSpace(memoryKey) == "" || !seenContent {
			return Command{}, fmt.Errorf("memory save requires --key and --content")
		}
		params["key"], params["content"] = memoryKey, content
		if metadata != "" {
			value, err := wire.DecodeJSON([]byte(metadata))
			if err != nil {
				return Command{}, fmt.Errorf("--metadata must be a JSON object")
			}
			object, ok := value.(map[string]any)
			if !ok {
				return Command{}, fmt.Errorf("--metadata must be a JSON object")
			}
			params["metadata"] = object
		}
	}
	if key != "" {
		if err := wire.ValidateIdempotencyKey(key); err != nil {
			return Command{}, err
		}
	}
	return Command{Method: method, Params: params, Key: key}, nil
}
