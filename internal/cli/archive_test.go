package cli

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/restore"
)

var testPresence = map[string]any{"mode": "presence_check", "challenge": "cli-test", "proof": "cli-test"}

func testPresenceVerifier() restore.PresenceVerifier {
	return restore.PresenceVerifierFunc(func(_ context.Context, method, digest string, presence restore.Presence) error {
		if method == "" || digest == "" || presence["proof"] != "cli-test" {
			return fmt.Errorf("unbound test presence")
		}
		return nil
	})
}

func TestDumpAndRestoreCommandsUsePagedRPC(t *testing.T) {
	ctx := context.Background()
	source, err := OpenLocal(ctx, LocalConfig{Project: "cli-fixture", Agent: "source", Storage: StorageConfig{Driver: "memory"}, PresenceVerifier: testPresenceVerifier()})
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	if _, err := source.Client.Call(ctx, "bead.create", map[string]any{"type": "issue", "fields": map[string]any{"title": "CLI archive"}}, ""); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "fixture.tar")
	var out, errOut bytes.Buffer
	if err := (&Session{Client: source.Client, Out: &out, Err: &errOut, HumanPresence: testPresence}).Run(ctx, []string{"dump", "--history", "head", "--output", path, "--page-size", "512"}); err != nil {
		t.Fatalf("dump: %v; stderr=%s", err, errOut.String())
	}
	if !strings.Contains(out.String(), "wrote ") {
		t.Fatalf("dump output: %s", out.String())
	}
	target, err := OpenLocal(ctx, LocalConfig{Project: "cli-fixture", Agent: "target", Storage: StorageConfig{Driver: "memory"}, PresenceVerifier: testPresenceVerifier()})
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	out.Reset()
	if err := (&Session{Client: target.Client, Out: &out, Err: &errOut, HumanPresence: testPresence}).Run(ctx, []string{"restore", path, "--history", "head"}); err != nil {
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

func TestAdminCLIRefusesWithoutTrustedPresence(t *testing.T) {
	ctx := context.Background()
	local, err := OpenLocal(ctx, LocalConfig{Project: "cli-presence", Agent: "source", Storage: StorageConfig{Driver: "memory"}})
	if err != nil {
		t.Fatal(err)
	}
	defer local.Close()
	path := filepath.Join(t.TempDir(), "refused.tar")
	var out, errOut bytes.Buffer
	err = (&Session{Client: local.Client, Out: &out, Err: &errOut}).Run(ctx, []string{"dump", "--history", "head", "--output", path})
	if err == nil || !strings.Contains(err.Error(), "human presence") {
		t.Fatalf("dump without trusted presence was accepted: err=%v stderr=%q", err, errOut.String())
	}
	if _, statErr := os.Stat(path); !os.IsNotExist(statErr) {
		t.Fatalf("refused dump created %s: %v", path, statErr)
	}
}
