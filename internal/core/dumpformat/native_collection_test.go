package dumpformat

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const joinedPublicFixture = "testdata/native-joined-public-ca26683b"

func physicalCollectionBinding(t *testing.T) NativeJoinedBinding {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(joinedPublicFixture, "creation-record.json"))
	if err != nil {
		t.Fatal(err)
	}
	if digest(raw) != "2e0e02826753f83f9bf72c621a2863ea7312c16af8875bb685495fa7fdc9c0e6" {
		t.Fatal("physical public pair creation record differs")
	}
	var creation struct {
		Pair []struct {
			Database          string              `json:"database"`
			RehearsalProject  string              `json:"rehearsal_project"`
			OrderedFileLedger []NativeCaptureFile `json:"ordered_file_ledger"`
		}
	}
	if err := json.Unmarshal(raw, &creation); err != nil {
		t.Fatal(err)
	}
	binding := NativeJoinedBinding{Format: NativeJoinedInputsFormatV2, PolicySHA256: strings.Repeat("a", 64), ProfileSHA256: strings.Repeat("b", 64)}
	for _, source := range creation.Pair {
		rootBytes, err := os.ReadFile(filepath.Join(joinedPublicFixture, source.Database, "capture-set.json"))
		if err != nil {
			t.Fatal(err)
		}
		var root NativeCaptureSet
		if err = json.Unmarshal(rootBytes, &root); err != nil {
			t.Fatal(err)
		}
		files := make([]NativeCaptureFile, len(source.OrderedFileLedger))
		// Creation evidence uses sha256/name/bytes exactly as the wire ledger.
		copy(files, source.OrderedFileLedger)
		for _, file := range files {
			data, err := os.ReadFile(filepath.Join(joinedPublicFixture, source.Database, file.Name))
			if err != nil || uint64(len(data)) != file.Bytes || digest(data) != file.SHA256 {
				t.Fatal("exact physical original differs", file.Name, err)
			}
		}
		binding.Sources = append(binding.Sources, NativeJoinedInput{RehearsalProject: source.RehearsalProject, Database: source.Database, SourceStoreID: root.StoreID, SourceProject: root.Projects[0], ArchiveSHA256: digest(rootBytes), Files: files})
	}
	return binding
}

func appendPhysicalCollectionSource(t *testing.T, ctx context.Context, c *NativeCaptureCollection, database string) {
	t.Helper()
	source, err := ReadNativeCaptureSetDirectory(ctx, filepath.Join(joinedPublicFixture, database), NativeCaptureSetOptions{PrepareReceiving: true})
	if err != nil {
		t.Fatal(err)
	}
	defer source.DiscardNativeCaptureSet()
	if err := c.Append(ctx, source); err != nil {
		t.Fatal(err)
	}
	// The function boundary drops every source/prepared reference before the
	// next source is read. Only the collection's detached current leaf survives.
}

func TestNativeCollectionRetainsExactPhysicalCurrentAndReleasesEachSource(t *testing.T) {
	budget := NewNativeDeltaBudget(0)
	ctx := WithNativeDeltaBudget(context.Background(), budget)
	binding := physicalCollectionBinding(t)
	c, err := NewNativeCaptureCollection(ctx, binding)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	for _, database := range []string{"ops", "sv2"} {
		appendPhysicalCollectionSource(t, ctx, c, database)
		usage := budget.Usage()
		if usage.CurrentCategories["receiving_staged_changes"] != 0 || usage.CurrentCategories["receiving_staged_originals"] != 0 {
			t.Fatal("source history preparation escaped its lifetime", usage.CurrentCategories)
		}
	}
	archive, err := c.Finish()
	if err != nil {
		t.Fatal(err)
	}
	if !archive.HasNativeCaptureCollection() || archive.HasNativeDelta() || len(archive.nativeHistories) != 0 || len(archive.tarBytes) != 0 {
		t.Fatal("collection retained an aggregate source history/envelope")
	}
	manifest := archive.Manifest()
	if len(manifest.Projects) != 2 || manifest.Projects[0].Prefix != "ops" || manifest.Projects[1].Prefix != "sv2" {
		t.Fatal("source ordinal/project namespaces changed", manifest.Projects)
	}
	metadata, err := archive.NativeSourceHistoryMetadata()
	if err != nil {
		t.Fatal(err)
	}
	if len(metadata) != 2 {
		t.Fatal("lost source metadata")
	}
	for _, source := range metadata {
		if source.CommitCount != 97 || source.ImageCount != 97 || source.SchemaProfile == nil {
			t.Fatal("physical source census changed", source)
		}
	}
	files, err := archive.NativeCollectionFiles()
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 6 {
		t.Fatal("source-qualified original ledger incomplete", files)
	}
	for i, source := range binding.Sources {
		for _, file := range source.Files {
			if files[NativeCollectionFileName(i, file.Name)] != (File{SHA256: file.SHA256, Size: int64(file.Bytes)}) {
				t.Fatal("original commitment changed")
			}
		}
	}
	records, err := archive.SourcePlaneRecords()
	if err != nil {
		t.Fatal(err)
	}
	projects := map[string]bool{}
	for _, record := range records {
		var row Record
		if err = json.Unmarshal(record.Raw, &row); err != nil {
			t.Fatal(err)
		}
		if row.Source.Identity == nil {
			t.Fatal("lost exact source namespace")
		}
		projects[row.Source.Identity.Project] = true
	}
	if !projects["ops"] || !projects["sv2"] {
		t.Fatal("current source rows omitted project")
	}
	archive = nil
	records = nil
	metadata = nil
	files = nil
	c.Close()
	if usage := budget.Usage(); usage.CurrentAccountedDecodedBytes != 0 || usage.PeakAccountedDecodedBytes > NativeDeltaDefaultDecodedLimit {
		t.Fatal("collection owner failed release/cap", usage)
	}
}

func TestNativeCollectionRefusesChangedPhysicalLedgerBeforeCurrentPublication(t *testing.T) {
	budget := NewNativeDeltaBudget(0)
	ctx := WithNativeDeltaBudget(context.Background(), budget)
	binding := physicalCollectionBinding(t)
	binding.Sources[0].Files[1].SHA256 = strings.Repeat("c", 64)
	c, err := NewNativeCaptureCollection(ctx, binding)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	source, err := ReadNativeCaptureSetDirectory(ctx, filepath.Join(joinedPublicFixture, "ops"), NativeCaptureSetOptions{PrepareReceiving: true})
	if err != nil {
		t.Fatal(err)
	}
	defer source.DiscardNativeCaptureSet()
	if err = c.Append(ctx, source); err == nil {
		t.Fatal("substituted physical file ledger accepted")
	}
	if _, err = c.Finish(); err == nil {
		t.Fatal("failed source yielded current collection")
	}
}
