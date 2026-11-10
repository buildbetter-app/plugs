package cardb20

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/cli"
	"github.com/buildbetter-app/plugs/internal/client"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
)

// This transport only records real bytes and dispatches the production RPC
// server. It supplies no pages, cursors, rows or authorization decisions.
type callerTransport struct {
	r                   *runtime
	Requests, Responses []json.RawMessage
}

func (c *callerTransport) RoundTrip(ctx context.Context, raw []byte) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.Requests = append(c.Requests, append(json.RawMessage(nil), raw...))
	response := c.r.server.Dispatch(rpc.WithPrincipal(ctx, c.r.principal), raw)
	c.Responses = append(c.Responses, append(json.RawMessage(nil), response...))
	return response, nil
}

func runCaller(t *testing.T, r *runtime, args []string) ([]model.Event, *callerTransport, string) {
	t.Helper()
	transport := &callerTransport{r: r}
	c, err := client.New(transport, "cardb20", "b20-real-cli")
	if err != nil {
		t.Fatal(err)
	}
	var out, stderr bytes.Buffer
	s := cli.Session{Client: c, Out: &out, Err: &stderr, JSON: true}
	if err := s.Run(r.ctx, args); err != nil {
		t.Fatalf("real CLI %q: %v; stderr=%s", args, err, stderr.String())
	}
	var events []model.Event
	if len(args) > 1 && args[1] == "export" {
		decoder := json.NewDecoder(bytes.NewReader(out.Bytes()))
		for decoder.More() {
			var event model.Event
			if err := decoder.Decode(&event); err != nil {
				t.Fatal(err)
			}
			events = append(events, event)
		}
	} else {
		var page eventPage
		if err := json.Unmarshal(out.Bytes(), &page); err != nil {
			t.Fatalf("CLI output=%s: %v", out.Bytes(), err)
		}
		events = page.Events
	}
	for _, raw := range transport.Requests {
		var request struct {
			Method string
			Params struct{ Limit int }
		}
		if err := json.Unmarshal(raw, &request); err != nil {
			t.Fatal(err)
		}
		if request.Method != "event.list" || request.Params.Limit < 1 || request.Params.Limit > 1000 {
			t.Fatalf("unbounded/non-event wire request: %s", raw)
		}
	}
	return events, transport, out.String()
}

func observeCurrentCallers(t *testing.T, r *runtime, engine string, want []stableEvent) {
	t.Helper()
	first := list(t, r, "", 1)
	for i, args := range [][]string{
		{"events", "--limit", "0", "--page-size", "2"},
		{"events", "--limit", "all", "--page-size", "1"},
		{"events", "all", "--page-size", "2"},
		{"events", "export", "--page-size", "2"},
		{"events", "--limit", "0", "--page-size", "1", "--after", first.Cursor},
		{"events", "export", "--page-size", "1", "--after", first.Cursor},
	} {
		got, transport, output := runCaller(t, r, args)
		expected := want
		if i >= 4 {
			expected = want[1:]
		}
		if !reflect.DeepEqual(projectEvents(t, got), expected) {
			t.Fatalf("real CLI %q projection differs: %+v", args, projectEvents(t, got))
		}
		if len(transport.Requests) < 3 {
			t.Fatalf("caller did not cross bounded pages and confirm caught-up: %q", args)
		}
		retain(t, fmt.Sprintf("plug-%s-caller-%d.json", engine, i), map[string]any{"argv": args, "wire_requests": transport.Requests, "wire_responses": transport.Responses, "stdout": output, "projection": projectEvents(t, got), "path": "internal/cli.Session.Run -> internal/client.Client.WalkEvents -> Client.Call -> production byte RPC"})
	}
	for _, limit := range []int{0, -1} {
		refused := r.call(t, "", "event.list", map[string]any{"filter": map[string]any{}, "limit": limit}, false)
		if refused.Error == nil || refused.Error.Data.Code != "invalid_params" {
			t.Fatalf("wire limit%d refusal=%+v", limit, refused)
		}
	}
}

func TestB20RealCallerPassesEmptyAdvancingFilteredPages(t *testing.T) {
	for _, engine := range engines {
		t.Run(engine, func(t *testing.T) {
			f := openStore(t, engine)
			r := newRuntime(t, f.store, false, "b20-1")
			create(t, r, "journal anchor")
			for i := 0; i < 70; i++ {
				update(t, r, "b20-1", fmt.Sprintf("filtered update%d", i))
			}
			closeBead(t, r, "b20-1")
			// The real reader scans only 64 changesets on its first request.
			// Its first page has no matching event, but advances the cursor.
			for i, args := range [][]string{{"events", "--limit", "0", "--event", "bead.closed", "--page-size", "2"}, {"events", "export", "--event", "bead.closed", "--page-size", "2"}} {
				got, transport, output := runCaller(t, r, args)
				if len(got) != 1 || got[0].Durable == nil || got[0].Durable.Revision.String() != "72" {
					t.Fatalf("matching event beyond filtered prefix=%+v", got)
				}
				var page eventPage
				var envelope struct{ Result json.RawMessage }
				if err := json.Unmarshal(transport.Responses[0], &envelope); err != nil {
					t.Fatal(err)
				}
				if err := json.Unmarshal(envelope.Result, &page); err != nil {
					t.Fatal(err)
				}
				if len(page.Events) != 0 || page.Cursor == "" || len(transport.Requests) < 3 {
					t.Fatal("fixture did not exercise empty advancing page and completion")
				}
				retain(t, fmt.Sprintf("plug-%s-filtered-caller-%d.json", engine, i), map[string]any{"argv": args, "wire_requests": transport.Requests, "wire_responses": transport.Responses, "stdout": output, "matching_projection": projectEvents(t, got)})
			}
		})
	}
}
