package plugdump

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/steveyegge/beads/internal/testutil"
	"github.com/steveyegge/beads/internal/testutil/recipehooks"
)

func TestSQLSourceHistoryFixtureHandoff(t *testing.T) {
	if os.Getenv("BEADS_TEST_SKIP") == "dolt" {
		t.Skip("requires the explicit pinned Dolt source-history lane")
	}
	if err := os.MkdirAll("/tmp/plug-swarm", 0o700); err != nil {
		t.Fatal(err)
	}
	parent, err := os.MkdirTemp("/tmp/plug-swarm", "public-partial-handoff-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(parent)
	retained := filepath.Join(parent, "source")
	var archive []byte
	if !t.Run("recipe", func(t *testing.T) {
		archive = runSQLSourceHistoryRecipe(t, recipehooks.Options{RetainStore: retained})
	}) {
		return
	}
	if len(archive) == 0 {
		t.Fatal("recipe did not export its clean source")
	}
	testutil.RequireDoltCLIOnly(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "dolt", "sql", "-r", "csv", "-q", "SELECT id, title FROM issues ORDER BY id")
	cmd.Dir = filepath.Join(retained, "beads_test")
	output, err := cmd.CombinedOutput()
	if err != nil || !strings.Contains(string(output), "demo-7,Dolt historical update") || strings.Contains(string(output), "uncommitted head") {
		t.Fatalf("handoff failed to retain the clean export point: error=%v output=%s", err, output)
	}
	// The original recipe's dirty-state refusal already passed inside the
	// child, and its normal cleanup has run; neither affects the retained copy.
	if _, err := os.Stat(filepath.Join(cmd.Dir, ".dolt")); err != nil {
		t.Fatal("handoff did not retain the physical source root")
	}
}
