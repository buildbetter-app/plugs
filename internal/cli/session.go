package cli

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/buildbetter-app/plugs/internal/api/wire"
	"github.com/buildbetter-app/plugs/internal/client"
	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
)

type Session struct {
	Client        *client.Client
	Out           io.Writer
	Err           io.Writer
	JSON          bool
	HumanPresence map[string]any
	LastID        string
	LastRevision  string
}

func (s *Session) Run(ctx context.Context, args []string) error {
	if s.Client == nil || s.Out == nil || s.Err == nil {
		return fmt.Errorf("session is not configured")
	}
	if len(args) > 0 {
		switch args[0] {
		case "dump":
			return s.dump(ctx, args[1:])
		case "restore":
			return s.restore(ctx, args[1:])
		}
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
	if cmd.AllEvents {
		return s.allEvents(ctx, cmd)
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
	commit := object
	if nested, ok := object["commit"].(map[string]any); ok {
		// Gate results wrap the canonical commit beside gate state and approval
		// count. Keep session aliases and warning handling tied to that commit as
		// with bead writes.
		commit = nested
	}
	if warnings, ok := commit["warnings"].([]any); ok {
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
	if revision, ok := commit["revision"].(json.Number); ok {
		s.LastRevision = revision.String()
	}
	if results, ok := commit["results"].([]any); ok {
		for _, item := range results {
			row, _ := item.(map[string]any)
			ref, _ := row["ref"].(map[string]any)
			if id, ok := ref["id"].(string); ok && id != "" {
				s.LastID = id
				break
			}
		}
	}
	if s.JSON || cmd.Method == "agent.brief" || strings.HasPrefix(cmd.Method, "memory.") {
		_, err := fmt.Fprintln(s.Out, string(result.JSON))
		return err
	}
	return s.format(object)
}

// HumanPresence is supplied by an interactive host when it has a trusted D32
// verifier. The CLI never manufactures a challenge or proof. The empty shape
// keeps the frozen RPC schema satisfied so the server can return its typed,
// fail-closed human_presence_required error when no verifier is configured.
func (s *Session) humanPresence() map[string]any {
	if len(s.HumanPresence) == 0 {
		return map[string]any{"mode": "presence_check", "challenge": "", "proof": ""}
	}
	return cloneAnyMap(s.HumanPresence)
}

func (s *Session) dump(ctx context.Context, args []string) error {
	cmd, err := ParseCommand(append([]string{"dump"}, args...))
	if err != nil {
		return err
	}
	path := cmd.Params["output"].(string)
	history := cmd.Params["history"].(string)
	pageSize := cmd.Params["pageSize"].(int)
	var archive bytes.Buffer
	cursor := ""
	dumpID := ""
	var page uint64
	for {
		params := map[string]any{"history": history, "pageSize": pageSize}
		if presence := s.humanPresence(); presence != nil {
			params["humanPresence"] = presence
		}
		if cursor != "" {
			params["cursor"] = cursor
		}
		result, err := s.Client.Call(ctx, "admin.dump", params, "")
		if err != nil {
			return err
		}
		value, err := wire.DecodeJSON(result.JSON)
		if err != nil {
			return err
		}
		object, ok := value.(map[string]any)
		if !ok {
			return fmt.Errorf("admin.dump returned a non-object")
		}
		gotID, _ := object["dumpId"].(string)
		if gotID == "" || (dumpID != "" && gotID != dumpID) {
			return fmt.Errorf("dump identity changed while paging")
		}
		dumpID = gotID
		gotPage, ok := object["page"].(json.Number)
		if !ok || gotPage.String() != strconv.FormatUint(page, 10) {
			return fmt.Errorf("dump page sequence changed")
		}
		encoded, _ := object["data"].(string)
		chunk, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil {
			return fmt.Errorf("dump page is not base64: %w", err)
		}
		if expected, _ := object["sha256"].(string); expected != "" {
			sum := sha256.Sum256(chunk)
			if expected != fmt.Sprintf("%x", sum[:]) {
				return fmt.Errorf("dump page checksum mismatch")
			}
		}
		archive.Write(chunk)
		done, _ := object["done"].(bool)
		if done {
			break
		}
		next, ok := object["nextPage"].(json.Number)
		if !ok {
			return fmt.Errorf("admin.dump omitted nextPage")
		}
		page, err = strconv.ParseUint(next.String(), 10, 64)
		if err != nil || page != uint64(archive.Len()/pageSize) {
			return fmt.Errorf("admin.dump returned an invalid nextPage")
		}
		cursor = dumpID + ":" + next.String()
	}
	if _, err := dumpformat.Read(bytes.NewReader(archive.Bytes())); err != nil {
		return fmt.Errorf("dump failed final validation: %w", err)
	}
	if err := os.WriteFile(path, archive.Bytes(), 0o600); err != nil {
		return err
	}
	_, err = fmt.Fprintf(s.Out, "wrote %s\n", path)
	return err
}

func (s *Session) restore(ctx context.Context, args []string) error {
	cmd, err := ParseCommand(append([]string{"restore"}, args...))
	if err != nil {
		return err
	}
	path := cmd.Params["path"].(string)
	history := cmd.Params["history"].(string)
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	archive, err := dumpformat.Read(bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("restore archive validation failed: %w", err)
	}
	manifestBytes, ok := archive.Member("manifest.json")
	if !ok {
		return fmt.Errorf("restore archive has no manifest")
	}
	manifestValue, err := wire.DecodeJSON(manifestBytes)
	if err != nil {
		return err
	}
	manifest, ok := manifestValue.(map[string]any)
	if !ok {
		return fmt.Errorf("restore manifest is not an object")
	}
	beginParams := map[string]any{"manifest": manifest, "history": history}
	if presence := s.humanPresence(); presence != nil {
		beginParams["humanPresence"] = presence
	}
	begin, err := s.Client.Call(ctx, "admin.restore.begin", beginParams, "")
	if err != nil {
		return err
	}
	statusValue, err := wire.DecodeJSON(begin.JSON)
	if err != nil {
		return err
	}
	status, ok := statusValue.(map[string]any)
	if !ok {
		return fmt.Errorf("restore begin returned a non-object")
	}
	restoreID, _ := status["restoreId"].(string)
	if restoreID == "" {
		return fmt.Errorf("restore begin omitted restoreId")
	}
	pageSize := 1 << 20
	for page, offset := uint64(0), 0; offset < len(data); page++ {
		end := offset + pageSize
		if end > len(data) {
			end = len(data)
		}
		chunk := data[offset:end]
		sum := sha256.Sum256(chunk)
		chunkParams := map[string]any{
			"restoreId": restoreID, "page": page, "data": base64.StdEncoding.EncodeToString(chunk), "sha256": fmt.Sprintf("%x", sum[:]),
		}
		if presence := s.humanPresence(); presence != nil {
			chunkParams["humanPresence"] = presence
		}
		_, err := s.Client.Call(ctx, "admin.restore.chunk", chunkParams, "")
		if err != nil {
			return err
		}
		offset = end
	}
	canonical, err := json.Marshal(archive.Manifest())
	if err != nil {
		return err
	}
	sum := sha256.Sum256(canonical)
	finishParams := map[string]any{"restoreId": restoreID, "manifestSha256": fmt.Sprintf("%x", sum[:])}
	if presence := s.humanPresence(); presence != nil {
		finishParams["humanPresence"] = presence
	}
	finish, err := s.Client.Call(ctx, "admin.restore.finish", finishParams, "")
	if err != nil {
		return err
	}
	if s.JSON {
		_, err = fmt.Fprintln(s.Out, string(finish.JSON))
		return err
	}
	_, err = fmt.Fprintf(s.Out, "restored %s\n", path)
	return err
}

func cloneAnyMap(value map[string]any) map[string]any {
	result := make(map[string]any, len(value))
	for key, item := range value {
		result[key] = item
	}
	return result
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
