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

Shared server:
  server                 Start the configured shared HTTP server
  server --local         Start a disposable loopback server with embedded UI

Commands:
  brief [--budget-tokens N]
  claim-next [--type TYPE]
  memory list [--query TEXT] [--limit N] [--cursor TOKEN]
  memory get ID
  memory save --key KEY --content TEXT [--metadata JSON]
  memory delete ID
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
  reopen ID [--reason TEXT] [--expect REVISION]
  defer ID [--reason TEXT] [--expect REVISION]
  cancel ID [--reason TEXT] [--expect REVISION]
  fail ID [--reason TEXT] [--expect REVISION]
  history --as-of REVISION [--id ID] [--limit N] [--cursor TOKEN]
  gate approve ID [--expect REVISION]
  gate report ID --state STATE [--details JSON] [--expect REVISION]
  ontology get [--project PROJECT]
  ontology describe [--project PROJECT] [--type TYPE]
  ontology schema --project PROJECT --type TYPE [--status STATUS]
  ontology check [--project PROJECT]
  ontology plan --document JSON [--project PROJECT] [--expect JSON]
  ontology apply PLAN_DIGEST --expect JSON [--proof DIGEST] [--session-id ID]
  ontology history [--project PROJECT] [--after REVISION] [--limit N]
  ontology diff --from REVISION --to REVISION [--project PROJECT]
  history get ID --as-of REVISION
  history diff ID --from REVISION --to REVISION
  events [--event TYPE] [--resource REF] [--limit N|all] [--after TOKEN]
  events all [--page-size N] [--after TOKEN]
  events export [--page-size N] [--after TOKEN]  Export all events as JSONL
  health
  dump --output FILE [--history full|head] [--page-size N]
  restore FILE [--history full|head]
  session                 Read one JSON argv array per line from stdin.

Without a selected server, local storage is persistent SQLite at .plug/plug.db.
Memory storage is disposable and exists only for this process when --memory is
explicitly selected. In a session, @last is the ID returned by the last write;
@revision is its revision. Server selection is --server, then PLUG_SERVER, then
.plug/config.yaml; PLUG_TOKEN supplies the bearer. An unreachable configured
server is an error. Remote calls use POST /rpc/v1. A local config may select a
server or SQLite storage (driver: sqlite, path: PATH). The shared server also
reads .plug/config.yaml. In-process --memory storage remains disposable and
never starts a listener or UI.
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
	Method       string
	Params       map[string]any
	Key          string
	AllEvents    bool
	ExportEvents bool
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
	if name == "events" {
		return parseEventsCommand(args[1:])
	}
	if name == "brief" || name == "claim-next" || name == "memory" {
		return parseAgentCommand(args)
	}
	if name == "gate" {
		return parseGateCommand(args[1:])
	}
	if name == "ontology" {
		return parseOntologyCommand(args[1:])
	}
	commandArgs := args[1:]
	if name == "history" && len(commandArgs) > 0 && (commandArgs[0] == "get" || commandArgs[0] == "diff") {
		name = "history." + commandArgs[0]
		commandArgs = commandArgs[1:]
	} else if name == "history-get" {
		name = "history.get"
	} else if name == "history-diff" {
		name = "history.diff"
	}
	allowed := map[string]bool{"ready": true, "get": true, "create": true, "claim": true, "update": true, "comment": true, "close": true, "reopen": true, "defer": true, "cancel": true, "fail": true, "link": true, "unlink": true, "batch": true, "history": true, "history.get": true, "history.diff": true, "health": true, "dump": true, "restore": true}
	if !allowed[name] {
		return Command{}, fmt.Errorf("unsupported command %q", name)
	}
	var title, typ, fields, body, reason, asOf, expect, id, key, cursor, from, to, kind, changeset, output, history string
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
	case "reopen", "defer", "cancel", "fail":
		add(&reason, "reason")
		add(&expect, "expect")
	case "history":
		add(&asOf, "as-of")
		add(&id, "id")
		add(&cursor, "cursor")
		fs.IntVar(&limit, "limit", 1000, "limit")
	case "dump":
		fs.StringVar(&history, "history", "full", "history")
		fs.StringVar(&cursor, "cursor", "", "cursor")
		fs.StringVar(&output, "output", "", "output")
		fs.IntVar(&limit, "page-size", 1<<20, "page size")
	case "restore":
		fs.StringVar(&history, "history", "full", "history")
	case "history.get":
		add(&asOf, "as-of")
	case "history.diff":
		add(&from, "from")
		add(&to, "to")
		fs.StringVar(&from, "from-revision", "", "from revision")
		fs.StringVar(&to, "to-revision", "", "to revision")
	case "health":
	}
	write := name == "create" || name == "update" || name == "claim" || name == "comment" || name == "close" || name == "reopen" || name == "defer" || name == "cancel" || name == "fail" || name == "link" || name == "unlink" || name == "batch"
	if write {
		add(&key, "idempotency-key")
	}
	ordered, err := interspersed(fs, commandArgs)
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
	needID := name == "get" || name == "update" || name == "claim" || name == "comment" || name == "close" || name == "reopen" || name == "defer" || name == "cancel" || name == "fail" || name == "history.get" || name == "history.diff"
	if name == "restore" {
		if fs.NArg() != 1 || strings.TrimSpace(fs.Arg(0)) == "" {
			return c, fmt.Errorf("restore requires exactly one archive path")
		}
		if history != "full" && history != "head" {
			return c, fmt.Errorf("--history must be full or head")
		}
		c.Method = "admin.restore.begin"
		c.Params["path"] = fs.Arg(0)
		c.Params["history"] = history
		return c, nil
	}
	if name == "dump" {
		if fs.NArg() != 0 {
			return c, fmt.Errorf("dump takes no positional arguments; use --output")
		}
		if strings.TrimSpace(output) == "" {
			return c, fmt.Errorf("dump requires --output FILE")
		}
		if history != "full" && history != "head" {
			return c, fmt.Errorf("--history must be full or head")
		}
		if limit < 1 || limit > 67108864 {
			return c, fmt.Errorf("--page-size must be between 1 and 67108864")
		}
		c.Method = "admin.dump"
		c.Params["output"] = output
		c.Params["history"] = history
		c.Params["pageSize"] = limit
		return c, nil
	}
	if needID {
		if fs.NArg() != 1 || strings.TrimSpace(fs.Arg(0)) == "" {
			return c, fmt.Errorf("%s requires exactly one bead ID", name)
		}
		if name == "history.get" || name == "history.diff" {
			p["ref"] = map[string]any{"id": fs.Arg(0)}
		} else {
			p["id"] = map[string]any{"id": fs.Arg(0)}
		}
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
	if (name == "close" || name == "reopen" || name == "defer" || name == "cancel" || name == "fail") && seen["reason"] {
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
	if name == "history.get" {
		c.Method = "history.get"
		if !seen["as-of"] {
			return c, fmt.Errorf("history get requires --as-of REVISION")
		}
	}
	if name == "history.diff" {
		c.Method = "history.diff"
		if !seen["from"] && !seen["from-revision"] || !seen["to"] && !seen["to-revision"] {
			return c, fmt.Errorf("history diff requires --from REVISION and --to REVISION")
		}
		fromValue, err := revision(from)
		if err != nil {
			return c, fmt.Errorf("--from: %w", err)
		}
		toValue, err := revision(to)
		if err != nil {
			return c, fmt.Errorf("--to: %w", err)
		}
		p["fromRevision"], p["toRevision"] = fromValue, toValue
	}
	if name == "health" {
		c.Method = "admin.health"
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

func parseOntologyCommand(args []string) (Command, error) {
	if len(args) == 0 {
		return Command{}, fmt.Errorf("ontology subcommand is required")
	}
	subcommand := args[0]
	allowed := map[string]bool{"get": true, "describe": true, "schema": true, "check": true, "plan": true, "apply": true, "history": true, "diff": true}
	if !allowed[subcommand] {
		return Command{}, fmt.Errorf("unsupported ontology command %q", subcommand)
	}
	fs := flag.NewFlagSet("ontology "+subcommand, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var project, typ, status, document, expect, after, from, to, proof, sessionID, key string
	var limit int
	fs.StringVar(&project, "project", "", "project")
	fs.StringVar(&typ, "type", "", "type")
	fs.StringVar(&status, "status", "", "status")
	fs.StringVar(&document, "document", "", "ontology document JSON")
	fs.StringVar(&expect, "expect", "", "expect JSON array")
	fs.StringVar(&after, "after", "", "after revision")
	fs.StringVar(&from, "from", "", "from revision")
	fs.StringVar(&to, "to", "", "to revision")
	fs.StringVar(&proof, "proof", "", "human presence proof")
	fs.StringVar(&sessionID, "session-id", "", "interactive session ID")
	fs.IntVar(&limit, "limit", 100, "limit")
	fs.StringVar(&key, "idempotency-key", "", "idempotency key")
	ordered, err := interspersed(fs, args[1:])
	if err != nil {
		return Command{}, err
	}
	if err := fs.Parse(ordered); err != nil {
		return Command{}, err
	}
	cmd := Command{Method: "ontology." + subcommand, Params: map[string]any{}, Key: key}
	seen := map[string]bool{}
	fs.Visit(func(f *flag.Flag) { seen[f.Name] = true })
	if (subcommand != "apply" && fs.NArg() != 0) || (subcommand == "apply" && fs.NArg() == 0) || fs.NArg() > 1 {
		return cmd, fmt.Errorf("ontology %s takes the expected positional arguments only", subcommand)
	}
	if seen["project"] {
		cmd.Params["project"] = project
	}
	switch subcommand {
	case "describe":
		if typ != "" {
			cmd.Params["type"] = typ
		}
	case "schema":
		if project == "" || typ == "" {
			return cmd, fmt.Errorf("ontology schema requires --project and --type")
		}
		cmd.Params["project"], cmd.Params["type"] = project, typ
		if status != "" {
			cmd.Params["status"] = status
		}
	case "plan":
		if document == "" {
			return cmd, fmt.Errorf("ontology plan requires --document JSON")
		}
		value, err := wire.DecodeJSON([]byte(document))
		if err != nil {
			return cmd, fmt.Errorf("--document must be a JSON object: %w", err)
		}
		if _, ok := value.(map[string]any); !ok {
			return cmd, fmt.Errorf("--document must be a JSON object")
		}
		cmd.Params["document"] = value
		if seen["expect"] {
			parsed, err := parseJSONArray(expect, "--expect")
			if err != nil {
				return cmd, err
			}
			cmd.Params["expect"] = parsed
		}
	case "apply":
		if fs.NArg() != 1 || strings.TrimSpace(fs.Arg(0)) == "" {
			return cmd, fmt.Errorf("ontology apply requires one plan digest")
		}
		if !canonicalDigest(fs.Arg(0)) {
			return cmd, fmt.Errorf("ontology apply requires a 64-character lowercase SHA-256 plan digest")
		}
		cmd.Params["planDigest"] = fs.Arg(0)
		if !seen["expect"] {
			return cmd, fmt.Errorf("ontology apply requires --expect JSON")
		}
		parsed, err := parseJSONArray(expect, "--expect")
		if err != nil {
			return cmd, err
		}
		cmd.Params["expect"] = parsed
		if proof == "" {
			return cmd, fmt.Errorf("ontology apply requires --proof DIGEST")
		}
		if sessionID != "" {
			cmd.Params["humanPresence"] = map[string]any{"mode": "interactive_session", "sessionId": sessionID, "challenge": fs.Arg(0), "proof": proof}
		} else {
			cmd.Params["humanPresence"] = map[string]any{"mode": "presence_check", "challenge": fs.Arg(0), "proof": proof}
		}
	case "history":
		if seen["after"] {
			n, err := revision(after)
			if err != nil {
				return cmd, err
			}
			cmd.Params["after"] = n
		}
		if limit < 1 || limit > 1000 {
			return cmd, fmt.Errorf("--limit must be between 1 and 1000")
		}
		cmd.Params["limit"] = limit
	case "diff":
		if !seen["from"] || !seen["to"] {
			return cmd, fmt.Errorf("ontology diff requires --from and --to")
		}
		fromRevision, err := revision(from)
		if err != nil {
			return cmd, err
		}
		toRevision, err := revision(to)
		if err != nil {
			return cmd, err
		}
		cmd.Params["fromRevision"], cmd.Params["toRevision"] = fromRevision, toRevision
	}
	return cmd, nil
}

func canonicalDigest(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, r := range value {
		if !(r >= '0' && r <= '9' || r >= 'a' && r <= 'f') {
			return false
		}
	}
	return true
}

func parseJSONArray(raw, flagName string) ([]any, error) {
	value, err := wire.DecodeJSON([]byte(raw))
	if err != nil {
		return nil, fmt.Errorf("%s must be a JSON array: %w", flagName, err)
	}
	array, ok := value.([]any)
	if !ok {
		return nil, fmt.Errorf("%s must be a JSON array", flagName)
	}
	return array, nil
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
