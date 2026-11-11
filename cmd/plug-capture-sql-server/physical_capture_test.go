package main_test

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/steveyegge/beads/internal/captureclock"
	"github.com/steveyegge/beads/internal/plugdump"
	"github.com/steveyegge/beads/internal/storage/physicalcapture"
	"github.com/steveyegge/beads/internal/testutil/bazeltest"
)

// A new bounded capture test; the original default/tracked fixtures and their
// assertions remain unchanged. No current family is force-staged into history.
func TestPublicNativePhysicalCapture(t *testing.T) {
	for _, prefix := range []string{"reprocody", "reprosignals"} {
		t.Run(prefix, func(t *testing.T) {
			f := newNativeFixtureForProject(t, prefix)
			types, roots := []string{"zzzzzzzz"}, []string{"a1b", "c2d"}
			statuses := []string{"todo", "done", "parked"}
			statusDeclarations := "todo:active,done:done,parked:frozen"
			expectedCommits := 87
			if prefix == "reprosignals" {
				types = []string{"aaaaa", "zzzzzzzz", "yyyyyyyy", "xxxxxxxxx"}
				roots = []string{"abc-4de", "abc-45d", "abc-456", "abc-d4e", "abc-de4", "abc-def"}
				statuses = []string{"todo", "review", "done", "parked"}
				statusDeclarations = "todo:wip,review:active,done:done,parked:frozen"
				expectedCommits = 91
			}
			f.run(f.bd, "config", "set", "types.custom", strings.Join(types, ","))
			f.run(f.bd, "config", "set", "status.custom", statusDeclarations)
			for i, root := range roots {
				id := prefix + "-" + root
				if f.create("synthetic shape control", "--id", id, "--type", types[i%len(types)], "--status", statuses[i%len(statuses)]) != id {
					t.Fatal("actual native identity changed")
				}
			}
			parent := prefix + "-" + roots[0]
			if f.create("synthetic child control", "--parent", parent, "--status", "parked") != parent+".1" {
				t.Fatal("actual native child identity changed")
			}
			ctx := context.Background()
			source := plugdump.SQLSource{DB: f.db}
			log, err := source.ReadSourceCommits(ctx)
			if err != nil || !log.Clean || len(log.Commits) != expectedCommits {
				t.Fatalf("actual native graph: commits=%d expected=%d error=%v", len(log.Commits), expectedCommits, err)
			}
			names, err := source.Tables(ctx)
			if err != nil || len(names) != 33 {
				t.Fatalf("current native families=%d error=%v", len(names), err)
			}
			current := map[string]plugdump.NativeTable{}
			for _, name := range names {
				table, err := source.ReadTable(ctx, name)
				if err != nil {
					t.Fatal(err)
				}
				current[name] = canonicalPhysicalCaptureTable(table)
			}
			ignore, err := source.ReadSourceIgnore(ctx)
			if err != nil {
				t.Fatal(err)
			}
			output := privateDir(t)
			_, archivePath, binaryBytes := f.capturePhysical(output)
			archive, err := os.ReadFile(archivePath)
			if err != nil {
				t.Fatal(err)
			}
			manifest, err := plugdump.Validate(bytes.NewReader(archive))
			if err != nil || manifest.SourceFamilyContract != plugdump.SourceFamilyContract {
				t.Fatalf("full producer archive validation: %v", err)
			}
			var native plugdump.NativeHistory
			var portable plugdump.SourceHistory
			reader := tar.NewReader(bytes.NewReader(archive))
			for {
				header, err := reader.Next()
				if err == io.EOF {
					break
				}
				if err != nil {
					t.Fatal(err)
				}
				decoder := json.NewDecoder(reader)
				decoder.UseNumber()
				if header.Name == plugdump.NativeHistoryMember {
					if err := decoder.Decode(&native); err != nil {
						t.Fatal(err)
					}
				} else if header.Name == plugdump.SourceHistoryMember {
					if err := decoder.Decode(&portable); err != nil {
						t.Fatal(err)
					}
				}
			}
			if native.Format != plugdump.NativeHistoryFormat || !native.Complete || len(native.Gaps) != 0 || len(native.Images) != expectedCommits || len(native.Commits) != expectedCommits || native.Head != log.Head {
				t.Fatalf("native physical history incomplete: commits=%d images=%d gaps=%d complete=%t", len(native.Commits), len(native.Images), len(native.Gaps), native.Complete)
			}
			originalCommits := append([]plugdump.SourceCommit{}, log.Commits...)
			capturedCommits := append([]plugdump.SourceCommit{}, native.Commits...)
			sort.Slice(originalCommits, func(i, j int) bool { return originalCommits[i].ID < originalCommits[j].ID })
			sort.Slice(capturedCommits, func(i, j int) bool { return capturedCommits[i].ID < capturedCommits[j].ID })
			if !equalPhysicalCaptureJSON(capturedCommits, originalCommits) || !equalPhysicalCaptureJSON(native.Current, current) || !equalPhysicalCaptureJSON(native.Ignore, canonicalPhysicalCaptureTable(ignore)) {
				t.Fatal("native graph metadata, exact current schema/cells or ignore rows changed")
			}
			unversioned := 0
			for _, disposition := range manifest.Sources {
				if table, ok := native.Current[disposition.Family]; ok {
					if disposition.Rows != len(table.Rows) {
						t.Fatal("family current row count changed")
					}
					if disposition.History == plugdump.FamilyUnversioned {
						unversioned++
					}
				}
			}
			if unversioned != 13 || len(native.Current) != 33 || len(native.Current["issues"].Rows) != len(roots)+1 || len(native.Current["events"].Rows) == 0 {
				t.Fatalf("native current/by-design accounting: current=%d unversioned=%d issues=%d events=%d", len(native.Current), unversioned, len(native.Current["issues"].Rows), len(native.Current["events"].Rows))
			}
			preEvents := 0
			for i, image := range native.Images {
				if image.Sequence != uint64(i+1) || image.CommitID != native.Commits[i].ID {
					t.Fatal("native image sequence/commit differs")
				}
				if _, present := image.Tables["events"]; present {
					preEvents++
				}
			}
			if preEvents == 0 {
				t.Fatal("actual pre0062 events table history omitted")
			}
			if portable.Range.Status != "unverified" || portable.Range.Complete || len(portable.Range.Frames) != 0 || portable.Range.Reason != "portable-image-not-supported; native history is separately retained" {
				t.Fatal("native completeness promoted portable replay")
			}
			finalLog, err := source.ReadSourceCommits(ctx)
			if err != nil || !reflect.DeepEqual(log, finalLog) {
				t.Fatal("original public graph changed")
			}
			artifacts := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR")
			if artifacts == "" {
				t.Fatal("Bazel artifact directory required")
			}
			for name, data := range map[string][]byte{prefix + "-physical.tar": archive, "native-physical-capture": binaryBytes} {
				if err := os.WriteFile(filepath.Join(artifacts, name), data, 0600); err != nil {
					t.Fatal(err)
				}
			}
			t.Logf("PUBLIC_NATIVE_PHYSICAL_CAPTURE fixture=%s source_rows=%d commits=%d images=%d gaps=%d current_families=%d unversioned=%d pre0062_events_images=%d portable=%s binary_sha256=%x archive_sha256=%x", prefix, len(roots)+1, len(native.Commits), len(native.Images), len(native.Gaps), len(native.Current), unversioned, preEvents, portable.Range.Status, sha256.Sum256(binaryBytes), sha256.Sum256(archive))
		})
	}
}

func equalPhysicalCaptureJSON(a, b any) bool {
	x, errA := json.Marshal(a)
	y, errB := json.Marshal(b)
	return errA == nil && errB == nil && bytes.Equal(x, y)
}

func canonicalPhysicalCaptureTable(table plugdump.Table) plugdump.NativeTable {
	columns := append([]string{}, table.Columns...)
	sort.Strings(columns)
	rows := append([]plugdump.Row{}, table.Rows...)
	sort.Slice(rows, func(i, j int) bool {
		a, _ := json.Marshal(rows[i])
		b, _ := json.Marshal(rows[j])
		return string(a) < string(b)
	})
	return plugdump.NativeTable{Columns: columns, Rows: rows}
}

func fingerprintPublicCapture(t *testing.T, root, destination string) map[string]string {
	t.Helper()
	files := map[string]string{}
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if destination != "" {
				return os.MkdirAll(filepath.Join(destination, rel), 0700)
			}
			return nil
		}
		if !entry.Type().IsRegular() {
			return fmt.Errorf("nonregular public fixture entry")
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		files[rel] = fmt.Sprintf("%x", sha256.Sum256(data))
		if destination != "" {
			return os.WriteFile(filepath.Join(destination, rel), data, 0600)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return files
}

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
