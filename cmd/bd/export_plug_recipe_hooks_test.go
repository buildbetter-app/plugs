package main

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/steveyegge/beads/internal/testutil"
	"github.com/steveyegge/beads/internal/testutil/recipehooks"
)

func TestPlugDumpCompleteRootFixtureHandoff(t *testing.T) {
	if os.Getenv("BEADS_TEST_SKIP") == "dolt" {
		t.Skip("requires the explicit public source-history Dolt lane")
	}
	if err := os.MkdirAll("/tmp/plug-swarm", 0o700); err != nil {
		t.Fatal(err)
	}
	parent, err := os.MkdirTemp("/tmp/plug-swarm", "public-complete-handoff-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(parent)
	retained := filepath.Join(parent, "source")
	var archive []byte
	if !t.Run("recipe", func(t *testing.T) {
		archive = runPlugDumpCompleteRootRecipe(t, recipehooks.Options{RetainStore: retained})
	}) {
		return
	}
	// The child has run all of the original cleanup before the consumer opens
	// the handed-off store. Read the actual exported identity, never a golden ID.
	reader := tar.NewReader(bytes.NewReader(archive))
	issueID := ""
	for {
		header, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if header.Name == "records/bead.ndjson" {
			var record struct {
				ID string `json:"id"`
			}
			if err := json.NewDecoder(reader).Decode(&record); err != nil {
				t.Fatal(err)
			}
			issueID = record.ID
		}
	}
	if issueID == "" {
		t.Fatal("actual export omitted its source identity")
	}
	testutil.RequireDoltCLIOnly(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "dolt", "sql", "-r", "csv", "-q", "SELECT id, title FROM issues ORDER BY id")
	cmd.Dir = filepath.Join(retained, "beads_test")
	output, err := cmd.CombinedOutput()
	if err != nil || !strings.Contains(string(output), issueID+",Public source updated through Beads") {
		t.Fatalf("retained source differs from this run's export: error=%v output=%s", err, output)
	}
	if _, err := os.Stat(filepath.Join(cmd.Dir, ".dolt")); err != nil {
		t.Fatal("handoff did not retain the physical source root")
	}
}
