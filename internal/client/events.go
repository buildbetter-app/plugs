package client

import (
	"context"
	"encoding/json"
	"fmt"
)

// EventPage preserves canonical event bytes, including arbitrary-precision
// positions and future event payloads. Cursor is exclusive on the next call.
type EventPage struct {
	Events []json.RawMessage `json:"events"`
	Cursor string            `json:"cursor"`
}

// WalkEvents drains event.list through positive bounded requests. A short or
// empty page can still advance past filtered changesets; only an empty page
// with the same cursor establishes that the caller is caught up. This is a
// sequence of reads, not a snapshot across concurrent writes.
func (c *Client) WalkEvents(ctx context.Context, filter map[string]any, after string, pageSize int, visit func(EventPage) error) (string, error) {
	if pageSize < 1 || pageSize > 1000 || visit == nil {
		return after, fmt.Errorf("event paging requires a visitor and page size 1..1000")
	}
	if filter == nil {
		filter = map[string]any{}
	}
	seen := map[string]bool{after: true}
	for {
		params := map[string]any{"filter": filter, "limit": pageSize}
		if after != "" {
			params["after"] = after
		}
		result, err := c.Call(ctx, "event.list", params, "")
		if err != nil {
			return after, err
		}
		var page EventPage
		if err := json.Unmarshal(result.JSON, &page); err != nil {
			return after, err
		}
		if page.Cursor == "" {
			return after, fmt.Errorf("event.list omitted its continuation cursor")
		}
		if page.Cursor == after {
			if len(page.Events) != 0 {
				return after, fmt.Errorf("event.list returned events without advancing its cursor")
			}
			return after, nil
		}
		if seen[page.Cursor] {
			return after, fmt.Errorf("event.list repeated a continuation cursor")
		}
		if err := visit(page); err != nil {
			return after, err
		}
		after = page.Cursor
		seen[after] = true
	}
}
