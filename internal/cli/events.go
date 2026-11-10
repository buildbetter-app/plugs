package cli

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"strconv"

	"github.com/buildbetter-app/plugs/internal/client"
)

func parseEventsCommand(args []string) (Command, error) {
	c := Command{Method: "event.list", Params: map[string]any{}}
	if len(args) > 0 {
		switch args[0] {
		case "tail":
			args = args[1:]
		case "all":
			c.AllEvents = true
			args = args[1:]
		case "export":
			c.AllEvents, c.ExportEvents = true, true
			args = args[1:]
		}
	}
	var eventType, resource, after, cursor, limit string
	var pageSize int
	fs := flag.NewFlagSet("events", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.StringVar(&eventType, "event", "", "event type")
	fs.StringVar(&resource, "resource", "", "resource")
	fs.StringVar(&after, "after", "", "exclusive event cursor")
	fs.StringVar(&cursor, "cursor", "", "exclusive event cursor")
	fs.StringVar(&limit, "limit", "1000", "row limit, or 0/all to page to completion")
	fs.IntVar(&pageSize, "page-size", 1000, "bounded batch size for all/export")
	ordered, err := interspersed(fs, args)
	if err != nil {
		return c, err
	}
	if err := fs.Parse(ordered); err != nil {
		return c, err
	}
	if fs.NArg() != 0 {
		return c, fmt.Errorf("events takes no positional arguments after tail/all/export")
	}
	n, err := strconv.Atoi(limit)
	if limit == "all" {
		n, err = 0, nil
	}
	if err != nil || n < 0 || n > 1000 || pageSize < 1 || pageSize > 1000 {
		return c, fmt.Errorf("--limit must be 0/all or 1..1000; --page-size must be 1..1000")
	}
	c.AllEvents = c.AllEvents || n == 0
	if c.AllEvents {
		n = pageSize
	}
	if after != "" && cursor != "" && after != cursor {
		return c, fmt.Errorf("--after and --cursor must agree")
	}
	if after == "" {
		after = cursor
	}
	if after != "" {
		c.Params["after"] = after
	}
	filter := map[string]any{}
	if eventType != "" {
		filter["events"] = []string{eventType}
	}
	if resource != "" {
		filter["resources"] = []string{resource}
	}
	c.Params["filter"], c.Params["limit"] = filter, n
	return c, nil
}

func (s *Session) allEvents(ctx context.Context, cmd Command) error {
	after, _ := cmd.Params["after"].(string)
	first := true
	if !cmd.ExportEvents {
		if _, err := io.WriteString(s.Out, `{"events":[`); err != nil {
			return err
		}
	}
	cursor, err := s.Client.WalkEvents(ctx, cmd.Params["filter"].(map[string]any), after, cmd.Params["limit"].(int), func(page client.EventPage) error {
		for _, event := range page.Events {
			if cmd.ExportEvents {
				if _, err := fmt.Fprintln(s.Out, string(event)); err != nil {
					return err
				}
				continue
			}
			if !first {
				if _, err := io.WriteString(s.Out, ","); err != nil {
					return err
				}
			}
			if _, err := s.Out.Write(event); err != nil {
				return err
			}
			first = false
		}
		return nil
	})
	if err != nil {
		return err
	}
	if cmd.ExportEvents {
		return nil
	}
	raw, err := json.Marshal(cursor)
	if err == nil {
		_, err = fmt.Fprintf(s.Out, `],"cursor":%s}`+"\n", raw)
	}
	return err
}
