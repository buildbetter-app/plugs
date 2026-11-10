package main_test

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"

	"github.com/steveyegge/beads/internal/captureclock"
	"github.com/steveyegge/beads/internal/storage/physicalcapture"
	"github.com/steveyegge/beads/internal/testutil/bazeltest"
)

// Producer-owned helper copied unchanged from fd1af3c9 physical_capture_test.go.
// Keep the original 347 physical-capture assertion file byte-identical.
// capturePhysical uses the standalone supported route on a fingerprinted public
// disposable copy. Its second connection independently rereads every immutable
// image in the default full-width contract; current rows stay on the source SQL.
func (f *nativeFixture) capturePhysical(output string) (string, string, []byte) {
	f.t.Helper()
	dataDir := ""
	for _, entry := range f.env {
		if value, ok := strings.CutPrefix(entry, "BEADS_DOLT_DATA_DIR="); ok {
			dataDir = value
		}
	}
	if dataDir == "" {
		f.t.Fatal("public fixture data directory missing")
	}
	// The fixture is quiescent: no command mutates it after this point.
	// Source/copy/source fingerprints establish an exact public byte copy.
	copyDir := privateDir(f.t)
	before := fingerprintPublicCapture(f.t, filepath.Join(dataDir, "capture"), filepath.Join(copyDir, "capture"))
	after := fingerprintPublicCapture(f.t, filepath.Join(dataDir, "capture"), "")
	copied := fingerprintPublicCapture(f.t, filepath.Join(copyDir, "capture"), "")
	if !reflect.DeepEqual(before, after) || !reflect.DeepEqual(before, copied) {
		f.t.Fatal("public source bytes moved during capture copy")
	}
	workspace := privateDir(f.t)
	beadsDir := filepath.Join(workspace, ".beads")
	beadsSource := filepath.Join(f.work, ".beads")
	beadsBefore := fingerprintPublicCapture(f.t, beadsSource, beadsDir)
	if !reflect.DeepEqual(beadsBefore, fingerprintPublicCapture(f.t, beadsSource, "")) || !reflect.DeepEqual(beadsBefore, fingerprintPublicCapture(f.t, beadsDir, "")) {
		f.t.Fatal("public source metadata moved during capture copy")
	}
	binary := filepath.Join(bazeltest.OverrideRoot(), os.Getenv("BEADS_PHYSICAL_CAPTURE_BINARY"))
	binaryBytes, err := os.ReadFile(binary)
	if err != nil {
		f.t.Fatal(err)
	}
	toolFixture := *f
	toolFixture.work = workspace
	toolFixture.env = nil
	for _, entry := range f.env {
		key, _, _ := strings.Cut(entry, "=")
		if key != captureclock.FileEnv && key != captureclock.PublicEnv && key != "BEADS_DIR" {
			toolFixture.env = append(toolFixture.env, entry)
		}
	}
	toolFixture.env = append(toolFixture.env, "BEADS_DIR="+beadsDir)
	toolFixture.run(binary, "--disposable", "--data-dir", copyDir, "--database", "capture", "--beads-dir", beadsDir, "--output-dir", output)
	db, closeDriver, err := physicalcapture.Open(copyDir, "capture")
	if err != nil {
		f.t.Fatal(err)
	}
	f.physicalDB = db
	f.t.Cleanup(func() {
		if err := db.Close(); err != nil {
			f.t.Error(err)
		}
		if err := closeDriver(); err != nil {
			f.t.Error(err)
		}
	})
	return filepath.Join(output, "inventory.json"), filepath.Join(output, "capture.tar"), binaryBytes
}
