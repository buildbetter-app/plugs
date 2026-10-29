package cli

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/buildbetter-app/plugs/internal/api/wire"
	"github.com/buildbetter-app/plugs/internal/client"
)

type Session struct {
	Client       *client.Client
	Out          io.Writer
	Err          io.Writer
	JSON         bool
	LastID       string
	LastRevision string
}

func (s *Session) Run(ctx context.Context, args []string) error {
	if s.Client == nil || s.Out == nil || s.Err == nil {
		return fmt.Errorf("session is not configured")
	}
	args = append([]string(nil), args...)
	for i, a := range args {
		switch a {
		case "@last":
			if s.LastID == "" {
				return fmt.Errorf("@last has no previous write ID")
			}
			args[i] = s.LastID
		case "@revision":
			if s.LastRevision == "" {
				return fmt.Errorf("@revision has no previous write revision")
			}
			args[i] = s.LastRevision
		}
	}
	cmd, err := ParseCommand(args)
	if err != nil {
		return err
	}
	if cmd.Method == "ontology.schema" {
		if _, exists := cmd.Params["project"]; !exists {
			cmd.Params["project"] = s.Client.Project()
		}
	}
	result, err := s.Client.Call(ctx, cmd.Method, cmd.Params, cmd.Key)
	if err != nil {
		if result.IdempotencyKey != "" {
			fmt.Fprintf(s.Err, "write idempotency key: %s\n", result.IdempotencyKey)
		}
		return err
	}
	decoded, err := wire.DecodeJSON(result.JSON)
	if err != nil {
		return err
	}
	object, _ := decoded.(map[string]any)
	if warnings, ok := object["warnings"].([]any); ok {
		for _, item := range warnings {
			warning, _ := item.(map[string]any)
			if _, err := fmt.Fprintf(s.Err, "warning [%v]: %v", warning["code"], warning["message"]); err != nil {
				return err
			}
			if details, ok := warning["details"]; ok {
				raw, _ := json.Marshal(details)
				if _, err := fmt.Fprintf(s.Err, " %s", raw); err != nil {
					return err
				}
			}
			if _, err := fmt.Fprintln(s.Err); err != nil {
				return err
			}
		}
	}
	if revision, ok := object["revision"].(json.Number); ok {
		s.LastRevision = revision.String()
	}
	if results, ok := object["results"].([]any); ok {
		for _, item := range results {
			row, _ := item.(map[string]any)
			ref, _ := row["ref"].(map[string]any)
			if id, ok := ref["id"].(string); ok && id != "" {
				s.LastID = id
				break
			}
		}
	}
	if s.JSON {
		_, err := fmt.Fprintln(s.Out, string(result.JSON))
		return err
	}
	return s.format(object)
}

func (s *Session) format(object map[string]any) error {
	if items, ok := object["items"].([]any); ok {
		if len(items) == 0 {
			_, err := fmt.Fprintln(s.Out, "No beads.")
			return err
		}
		for _, item := range items {
			row, _ := item.(map[string]any)
			if err := s.bead(row); err != nil {
				return err
			}
		}
		if next, ok := object["nextCursor"].(string); ok && next != "" {
			_, err := fmt.Fprintf(s.Out, "Next cursor: %s\n", next)
			return err
		}
		return nil
	}
	if bead, ok := object["bead"].(map[string]any); ok {
		return s.bead(bead)
	}
	if revision, ok := object["revision"]; ok {
		_, err := fmt.Fprintf(s.Out, "revision %v  %s\n", revision, s.LastID)
		return err
	}
	raw, err := json.Marshal(object)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(s.Out, string(raw))
	return err
}

func (s *Session) bead(row map[string]any) error {
	fields, _ := row["fields"].(map[string]any)
	title, _ := fields["title"].(string)
	status, _ := row["status"].(string)
	_, err := fmt.Fprintf(s.Out, "%v  %s  %s  (revision %v)\n", row["id"], status, title, row["revision"])
	return err
}

// RunInput executes one command array per line against the selected core.
// It stops on the first refusal; earlier committed writes stay committed.
// A blocking input must implement io.ReadCloser so cancellation can interrupt
// its pending read. Finite readers need no additional lifetime handling.
func (s *Session) RunInput(ctx context.Context, in io.Reader) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if input, ok := in.(io.ReadCloser); ok {
		stop := context.AfterFunc(ctx, func() { _ = input.Close() })
		defer stop()
	}
	scanner := bufio.NewScanner(in)
	scanner.Buffer(make([]byte, 4096), 1<<20)
	line := 0
	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return err
		}
		line++
		if strings.TrimSpace(scanner.Text()) == "" {
			continue
		}
		var args []string
		if err := wire.DecodeInto(scanner.Bytes(), &args); err != nil || len(args) == 0 {
			return fmt.Errorf("session line %d: expected a nonempty JSON argv array", line)
		}
		if err := s.Run(ctx, args); err != nil {
			return fmt.Errorf("session line %d: %w", line, err)
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return scanner.Err()
}
