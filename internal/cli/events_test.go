package cli

import "testing"

func TestEventsCompletionModesKeepPositiveWireLimits(t *testing.T) {
	for _, args := range [][]string{{"events", "--limit", "0"}, {"events", "--limit=all"}, {"events", "all"}, {"events", "export"}, {"events", "tail", "--limit", "0", "--page-size", "2"}} {
		cmd, err := ParseCommand(args)
		if err != nil || !cmd.AllEvents || cmd.Method != "event.list" || cmd.Params["limit"].(int) < 1 {
			t.Fatalf("%q => %+v %v", args, cmd, err)
		}
		if cmd.ExportEvents != (args[1] == "export") {
			t.Fatalf("export mode=%+v", cmd)
		}
	}
	cmd, err := ParseCommand([]string{"events", "tail", "--limit", "2", "--event", "bead.updated", "--after", "opaque"})
	if err != nil || cmd.AllEvents || cmd.Params["limit"] != 2 || cmd.Params["after"] != "opaque" {
		t.Fatalf("bounded mode=%+v err=%v", cmd, err)
	}
	for _, args := range [][]string{{"events", "--limit", "-1"}, {"events", "--limit", "1001"}, {"events", "--limit", "all", "--page-size", "0"}, {"events", "export", "extra"}, {"events", "--after", "first", "--cursor", "second"}, {"events", "prune"}} {
		if _, err := ParseCommand(args); err == nil {
			t.Fatalf("accepted %q", args)
		}
	}
}
