// Package cli implements the local and remote Plug command line.
package cli

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/big"
	"strings"

	"github.com/buildbetter-app/plugs/internal/api/wire"
)

const Usage = `Usage: plug [--server URL | --memory] [--project PROJECT] [--agent LABEL] [--json] COMMAND

Commands:
  ready [--type TYPE] [--limit N] [--cursor TOKEN]
  get ID [--as-of REVISION]
  create --title TITLE [--type issue] [--fields JSON]
  claim ID [--expect REVISION]
  update ID [--title TITLE] [--fields JSON] [--expect REVISION]
  comment ID --body TEXT [--expect REVISION]
  close ID [--reason TEXT] [--expect REVISION]
  link FROM TO --kind KIND [--expect REVISION]
  unlink FROM TO --kind KIND [--expect REVISION]
  batch --changeset JSON
  history --as-of REVISION [--id ID] [--limit N] [--cursor TOKEN]
  session                 Read one JSON argv array per line from stdin.

Memory storage is disposable and exists only for this process. In a session,
@last is the ID returned by the last write; @revision is its revision.
Server selection: --server, then PLUG_SERVER; PLUG_TOKEN supplies the bearer.
An unreachable configured server is an error. Remote calls use POST /rpc/v1.
Config-file loading and persistent local storage are not installed in this slice.
Global options precede COMMAND. Command options may precede or follow ID.
Writes accept --idempotency-key KEY to retry a write whose response was lost.
`

var ErrHelp = errors.New("help requested")

type Options struct {
	Memory    bool
	Server    string
	Project   string
	Agent     string
	JSON      bool
	serverSet bool
}

type Command struct {
	Method string
	Params map[string]any
	Key    string
}

// Parse separates the connection/session configuration from one named command.
func Parse(args []string) (Options, []string, error) {
	var o Options
	fs := flag.NewFlagSet("plug", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.BoolVar(&o.Memory, "memory", false, "disposable memory storage")
	fs.StringVar(&o.Server, "server", "", "server origin or /rpc/v1 URL")
	fs.StringVar(&o.Project, "project", "local", "project")
	fs.StringVar(&o.Agent, "agent", "plug-cli", "unverified anonymous agent label")
	fs.BoolVar(&o.JSON, "json", false, "JSON output")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return o, nil, ErrHelp
		}
		return o, nil, err
	}
	if len(fs.Args()) == 0 {
		return o, nil, ErrHelp
	}
	fs.Visit(func(f *flag.Flag) {
		if f.Name == "server" {
			o.serverSet = true
		}
	})
	if o.serverSet && strings.TrimSpace(o.Server) == "" {
		return o, nil, fmt.Errorf("--server must not be empty")
	}
	if strings.TrimSpace(o.Project) == "" {
		return o, nil, fmt.Errorf("--project must not be empty")
	}
	return o, fs.Args(), nil
}

// ParseCommand builds canonical named parameters; validation at the registry
// remains authoritative. Both transports expose the same named command slice.
func ParseCommand(args []string) (Command, error) {
	if len(args) == 0 {
		return Command{}, fmt.Errorf("command is required")
	}
	name := args[0]
	allowed := map[string]bool{"ready": true, "get": true, "create": true, "claim": true, "update": true, "comment": true, "close": true, "link": true, "unlink": true, "batch": true, "history": true}
	if !allowed[name] {
		return Command{}, fmt.Errorf("unsupported command %q", name)
	}
	var title, typ, fields, body, reason, asOf, expect, id, key, cursor, from, to, kind, changeset string
	var limit int
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	add := func(p *string, key string) { fs.StringVar(p, key, "", key) }
	switch name {
	case "ready":
		add(&typ, "type")
		add(&cursor, "cursor")
		fs.IntVar(&limit, "limit", 1000, "limit")
	case "get":
		add(&asOf, "as-of")
	case "create":
		add(&title, "title")
		fs.StringVar(&typ, "type", "issue", "type")
		add(&fields, "fields")
	case "update":
		add(&title, "title")
		add(&fields, "fields")
		add(&expect, "expect")
	case "claim":
		add(&expect, "expect")
	case "comment":
		add(&body, "body")
		add(&expect, "expect")
	case "close":
		add(&reason, "reason")
		add(&expect, "expect")
	case "link", "unlink":
		add(&kind, "kind")
		add(&expect, "expect")
	case "batch":
		add(&changeset, "changeset")
	case "history":
		add(&asOf, "as-of")
		add(&id, "id")
		add(&cursor, "cursor")
		fs.IntVar(&limit, "limit", 1000, "limit")
	}
	write := name == "create" || name == "update" || name == "claim" || name == "comment" || name == "close" || name == "link" || name == "unlink" || name == "batch"
	if write {
		add(&key, "idempotency-key")
	}
	ordered, err := interspersed(fs, args[1:])
	if err != nil {
		return Command{}, err
	}
	if err := fs.Parse(ordered); err != nil {
		return Command{}, err
	}
	seen := map[string]bool{}
	fs.Visit(func(f *flag.Flag) { seen[f.Name] = true })
	p := map[string]any{}
	method := "bead." + name
	if name == "batch" {
		method = "tx.apply"
	}
	c := Command{Method: method, Params: p, Key: key}
	needID := name == "get" || name == "update" || name == "claim" || name == "comment" || name == "close"
	if needID {
		if fs.NArg() != 1 || strings.TrimSpace(fs.Arg(0)) == "" {
			return c, fmt.Errorf("%s requires exactly one bead ID", name)
		}
		p["id"] = map[string]any{"id": fs.Arg(0)}
	} else if name == "link" || name == "unlink" {
		if fs.NArg() != 2 || strings.TrimSpace(fs.Arg(0)) == "" || strings.TrimSpace(fs.Arg(1)) == "" {
			return c, fmt.Errorf("%s requires exactly two bead IDs", name)
		}
		from, to = fs.Arg(0), fs.Arg(1)
		p["from"] = map[string]any{"id": from}
		p["to"] = map[string]any{"id": to}
	} else if name == "batch" {
		if fs.NArg() > 1 || (fs.NArg() == 1 && seen["changeset"]) {
			return c, fmt.Errorf("batch accepts one changeset JSON value")
		}
	} else if fs.NArg() != 0 {
		return c, fmt.Errorf("%s takes no positional arguments", name)
	}
	if name == "create" || name == "update" {
		f := map[string]any{}
		if seen["fields"] {
			value, err := wire.DecodeJSON([]byte(fields))
			object, ok := value.(map[string]any)
			if err != nil || !ok {
				return c, fmt.Errorf("--fields must be a JSON object")
			}
			f = object
		}
		if seen["title"] {
			f["title"] = title
		}
		if len(f) == 0 {
			return c, fmt.Errorf("%s requires --title or nonempty --fields", name)
		}
		p["fields"] = f
		if name == "create" {
			if strings.TrimSpace(typ) == "" {
				return c, fmt.Errorf("--type must not be empty")
			}
			p["type"] = typ
		}
	}
	if name == "comment" {
		if strings.TrimSpace(body) == "" {
			return c, fmt.Errorf("comment requires --body")
		}
		p["body"] = body
	}
	if name == "close" && seen["reason"] {
		p["reason"] = reason
	}
	if name == "link" || name == "unlink" {
		if !seen["kind"] || strings.TrimSpace(kind) == "" {
			return c, fmt.Errorf("%s requires --kind", name)
		}
		p["kind"] = kind
	}
	if name == "batch" {
		if !seen["changeset"] {
			if fs.NArg() != 1 {
				return c, fmt.Errorf("batch requires --changeset JSON or one changeset JSON value")
			}
			changeset = fs.Arg(0)
		}
		value, err := wire.DecodeJSON([]byte(changeset))
		object, ok := value.(map[string]any)
		if err != nil || !ok {
			return c, fmt.Errorf("--changeset must be a JSON object")
		}
		p["changeSet"] = object
	}
	if seen["expect"] {
		n, err := revision(expect)
		if err != nil {
			return c, err
		}
		expectRef := p["id"]
		if name == "link" || name == "unlink" {
			expectRef = p["from"]
		}
		p["expect"] = []any{map[string]any{"ref": expectRef, "revision": n}}
	}
	if name == "history" {
		c.Method = "history.query"
		if !seen["as-of"] {
			return c, fmt.Errorf("history requires --as-of REVISION")
		}
	}
	if seen["as-of"] {
		n, err := revision(asOf)
		if err != nil {
			return c, err
		}
		p["asOf"] = map[string]any{"revision": n}
	}
	if name == "ready" || name == "history" {
		if seen["cursor"] {
			if cursor == "" {
				return c, fmt.Errorf("--cursor must not be empty")
			}
			p["cursor"] = cursor
		}
		if limit < 1 || limit > 1000 {
			return c, fmt.Errorf("--limit must be between 1 and 1000")
		}
		p["limit"] = limit
		filter := map[string]any{}
		if seen["type"] {
			if typ == "" {
				return c, fmt.Errorf("--type must not be empty")
			}
			filter["types"] = []string{typ}
		}
		if seen["id"] {
			if id == "" {
				return c, fmt.Errorf("--id must not be empty")
			}
			filter["resources"] = []string{id}
		}
		if len(filter) > 0 {
			p["filter"] = filter
		}
	}
	return c, nil
}

func revision(s string) (json.Number, error) {
	if s == "" || strings.IndexFunc(s, func(r rune) bool { return r < '0' || r > '9' }) >= 0 {
		return "", fmt.Errorf("revision must be a nonnegative integer")
	}
	n, ok := new(big.Int).SetString(s, 10)
	if !ok {
		return "", fmt.Errorf("invalid revision")
	}
	return json.Number(n.String()), nil
}

func interspersed(fs *flag.FlagSet, args []string) ([]string, error) {
	var flags, positionals []string
	for i := 0; i < len(args); i++ {
		a := args[i]
		if a == "--" {
			positionals = append(positionals, args[i+1:]...)
			break
		}
		if !strings.HasPrefix(a, "-") || a == "-" {
			positionals = append(positionals, a)
			continue
		}
		key := strings.TrimLeft(a, "-")
		key, _, hasValue := strings.Cut(key, "=")
		if fs.Lookup(key) == nil {
			return nil, fmt.Errorf("unknown option %s", a)
		}
		flags = append(flags, a)
		if !hasValue {
			if i+1 == len(args) {
				return nil, fmt.Errorf("%s requires a value", a)
			}
			i++
			flags = append(flags, args[i])
		}
	}
	return append(append(flags, "--"), positionals...), nil
}
