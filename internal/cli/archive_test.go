package cli

import (
	"bytes"
	"context"
	"path/filepath"
	"strings"
	"testing"
)

func TestDumpAndRestoreCommandsUsePagedRPC(t *testing.T) {
	ctx := context.Background()
	source, err := OpenLocal(ctx, LocalConfig{Project: "cli-fixture", Agent: "source", Storage: StorageConfig{Driver: "memory"}})
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	if _, err := source.Client.Call(ctx, "bead.create", map[string]any{"type": "issue", "fields": map[string]any{"title": "CLI archive"}}, ""); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "fixture.tar")
	var out, errOut bytes.Buffer
	if err := (&Session{Client: source.Client, Out: &out, Err: &errOut}).Run(ctx, []string{"dump", "--output", path, "--page-size", "512"}); err != nil {
		t.Fatalf("dump: %v; stderr=%s", err, errOut.String())
	}
	if !strings.Contains(out.String(), "wrote ") {
		t.Fatalf("dump output: %s", out.String())
	}
	target, err := OpenLocal(ctx, LocalConfig{Project: "cli-fixture", Agent: "target", Storage: StorageConfig{Driver: "memory"}})
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	out.Reset()
	if err := (&Session{Client: target.Client, Out: &out, Err: &errOut}).Run(ctx, []string{"restore", path}); err != nil {
		t.Fatalf("restore: %v; stderr=%s", err, errOut.String())
	}
	if !strings.Contains(out.String(), "restored ") {
		t.Fatalf("restore output: %s", out.String())
	}
	ready, err := target.Client.Call(ctx, "bead.ready", map[string]any{"limit": 10}, "")
	if err != nil || !bytes.Contains(ready.JSON, []byte("CLI archive")) {
		t.Fatalf("restored CLI fixture not readable: %s %v", ready.JSON, err)
	}
}
