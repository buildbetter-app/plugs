package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/api/wire"
)

func TestWalkEventsEmptyAdvancingPagesPreserveFilterAndAfter(t *testing.T) {
	var requests []map[string]any
	c, err := New(roundTripFunc(func(_ context.Context, raw []byte) ([]byte, error) {
		var request struct {
			ID     string
			Method string
			Params map[string]any
		}
		if err := json.Unmarshal(raw, &request); err != nil {
			return nil, err
		}
		if request.Method != "event.list" {
			t.Fatal(request.Method)
		}
		requests = append(requests, request.Params)
		cursor := "advanced"
		if len(requests) == 1 {
			cursor = "filtered"
		}
		return json.Marshal(map[string]any{"jsonrpc": "2.0", "id": request.ID, "result": EventPage{Events: []json.RawMessage{}, Cursor: cursor}})
	}), "project", "caller")
	if err != nil {
		t.Fatal(err)
	}
	filter := map[string]any{"events": []string{"bead.closed"}, "resources": []string{"bead/pl-1"}}
	var visits []string
	cursor, err := c.WalkEvents(context.Background(), filter, "start", 2, func(page EventPage) error { visits = append(visits, page.Cursor); return nil })
	if err != nil || cursor != "advanced" || !reflect.DeepEqual(visits, []string{"filtered", "advanced"}) || len(requests) != 3 {
		t.Fatalf("cursor=%s visits=%v requests=%v err=%v", cursor, visits, requests, err)
	}
	for i, request := range requests {
		if request["after"] != []string{"start", "filtered", "advanced"}[i] || request["limit"] != float64(2) {
			t.Fatal(request)
		}
		got, _ := json.Marshal(request["filter"])
		want, _ := json.Marshal(filter)
		if string(got) != string(want) {
			t.Fatalf("filter changed: %s", got)
		}
	}
}

func TestWalkEventsPreservesFailuresAndStopsCycles(t *testing.T) {
	for _, mode := range []string{"remote", "transport", "cancel", "cycle", "visitor"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			sentinel := errors.New("caller stopped")
			calls := 0
			c, err := New(roundTripFunc(func(_ context.Context, raw []byte) ([]byte, error) {
				calls++
				var request struct{ ID string }
				if err := json.Unmarshal(raw, &request); err != nil {
					return nil, err
				}
				if calls == 2 {
					switch mode {
					case "transport":
						return nil, sentinel
					case "remote":
						catalog, _ := wire.DefaultCatalog()
						return json.Marshal(map[string]any{"jsonrpc": "2.0", "id": request.ID, "error": catalog.NewError("cursor_invalid", "", nil)})
					}
				}
				cursor := fmt.Sprint(calls)
				if mode == "cycle" && calls == 2 {
					cursor = "start"
				}
				if calls > 2 {
					t.Fatal("paging continued after failure")
				}
				return json.Marshal(map[string]any{"jsonrpc": "2.0", "id": request.ID, "result": EventPage{Events: []json.RawMessage{}, Cursor: cursor}})
			}), "project", "caller")
			if err != nil {
				t.Fatal(err)
			}
			_, err = c.WalkEvents(ctx, nil, "start", 2, func(EventPage) error {
				if mode == "visitor" {
					return sentinel
				}
				if mode == "cancel" {
					cancel()
				}
				return nil
			})
			switch mode {
			case "transport", "visitor":
				if !errors.Is(err, sentinel) {
					t.Fatal(err)
				}
			case "cancel":
				if !errors.Is(err, context.Canceled) || calls != 1 {
					t.Fatalf("calls=%d err=%v", calls, err)
				}
			case "remote":
				var remote *wire.Error
				if !errors.As(err, &remote) || remote.Data.Code != "cursor_invalid" {
					t.Fatal(err)
				}
			case "cycle":
				if err == nil || !strings.Contains(err.Error(), "repeated") {
					t.Fatal(err)
				}
			}
		})
	}
}
