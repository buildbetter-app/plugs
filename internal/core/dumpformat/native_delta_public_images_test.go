package dumpformat

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
)

// Compare the incremental path against the original full-table file reader on
// the unchanged, independently captured public source. This is a complete EOF
// and canonical image-byte regression, separate from prefix speed observation.
func TestNativeDeltaRetainedPublicImagesMatchOriginalFileReader(t *testing.T) {
	raw, err := os.ReadFile("../../integration/converterroundtrip/testdata/public-native-delta-small-0e4d5da4.tar.gz")
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	if hex.EncodeToString(sum[:]) != "66e2a1410cb6de0532e8bd453f06353d3fde8159ed9d6ffe4b0ed17f20bf6ef8" {
		t.Fatal("existing public capture changed")
	}
	z, err := gzip.NewReader(bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	defer z.Close()
	files := map[string][]byte{}
	input := tar.NewReader(z)
	for {
		header, err := input.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if header.Typeflag != tar.TypeReg || !strings.HasPrefix(header.Name, "capture-set/") {
			continue
		}
		value, err := io.ReadAll(input)
		if err != nil {
			t.Fatal(err)
		}
		files[strings.TrimPrefix(header.Name, "capture-set/")] = value
	}
	if len(files) != 3 {
		t.Fatal("existing public capture file census changed")
	}
	open := func(_ context.Context, ordinal uint64) (io.ReadCloser, error) {
		value, ok := files[fmt.Sprintf("parts/%020d.part", ordinal)]
		if !ok {
			return nil, os.ErrNotExist
		}
		return io.NopCloser(bytes.NewReader(value)), nil
	}
	archive, err := ReadNativeCaptureSet(context.Background(), bytes.NewReader(files["capture-set.json"]), bytes.NewReader(files["head.tar"]), open, NativeCaptureSetOptions{})
	if err != nil {
		t.Fatal("authenticate complete public capture", err)
	}
	defer archive.ReleaseNativeDecoded()
	if archive.SHA256() != "fd910c93ebad8da226c7ff6cc84f49c46f87aafe2c3530a4a27bf48280e4c67d" {
		t.Fatal("existing public root changed")
	}
	var root NativeCaptureSet
	if err := json.Unmarshal(files["capture-set.json"], &root); err != nil {
		t.Fatal(err)
	}
	parts := func() *NativeDeltaPartReader {
		t.Helper()
		reader, err := NewNativeDeltaPartReader(context.Background(), root.PartCount, root.FinalPartSHA256, root.StreamSHA256, open)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = reader.Close() })
		return reader
	}
	originalParts, incrementalParts := parts(), parts()
	budget := NewNativeDeltaBudget(0)
	owner := budget.Scope()
	defer owner.Release()
	original := nativeDeltaReconstructor{ctx: context.Background(), input: newNativeDeltaRecordReader(context.Background(), originalParts, owner), budget: owner, dir: t.TempDir()}
	defer original.input.Close()
	originalSchemas := map[string]NativeDeltaSchema{}
	originalStates := map[string]deltaStagedState{}
	// Only the orchestration is local to this test. Every table takes the
	// unchanged pre-cache tableFromFile path: reread, decode, validate, hash,
	// normalize and sort the full first-parent table on every transaction.
	nextOriginal := func() (NativeHistoryImage, string) {
		t.Helper()
		var schemaID, commitID, kind string
		var sequence uint64
		base := deltaStagedState{tables: map[string]deltaStagedTable{}}
		for {
			raw, nextKind, err := original.next()
			if err != nil {
				t.Fatal("original public transaction missing", err)
			}
			kind = nextKind
			switch kind {
			case "header":
				if err := decodeJSON(raw, &original.header); err != nil {
					t.Fatal(err)
				}
				continue
			case "schema":
				var schema NativeDeltaSchema
				if err := decodeJSON(raw, &schema); err != nil {
					t.Fatal(err)
				}
				originalSchemas[schema.SchemaID] = schema
				continue
			case "commit":
				var commit NativeDeltaCommit
				if err := decodeJSON(raw, &commit); err != nil {
					t.Fatal(err)
				}
				schemaID, commitID, sequence = commit.SchemaID, commit.CommitID, commit.Sequence
				if commit.BaseCommit != nil {
					base = originalStates[*commit.BaseCommit]
				}
			case "head-checkpoint", "current":
				var checkpoint NativeDeltaCheckpoint
				if err := decodeJSON(raw, &checkpoint); err != nil {
					t.Fatal(err)
				}
				schemaID, commitID = checkpoint.SchemaID, checkpoint.CommitID
			default:
				t.Fatal("unexpected original public transaction", kind)
			}
			break
		}
		schema, ok := originalSchemas[schemaID]
		if !ok {
			t.Fatal("original public schema missing")
		}
		state := deltaStagedState{tables: map[string]deltaStagedTable{}}
		image := NativeHistoryImage{Sequence: sequence, CommitID: commitID, SchemaProfile: &schema.Profile, Tables: map[string]NativeTable{}}
		for {
			raw, recordKind, err := original.next()
			if err != nil {
				t.Fatal(err)
			}
			if strings.HasPrefix(recordKind, "end_") {
				if recordKind != "end_"+kind {
					t.Fatal("original public terminal differs", recordKind, kind)
				}
				break
			}
			if recordKind == "drop_table" {
				continue
			}
			if recordKind != "table" {
				t.Fatal("original public table expected", recordKind)
			}
			var open NativeDeltaTable
			if err := decodeJSON(raw, &open); err != nil {
				t.Fatal(err)
			}
			var tableSchema NativeDeltaTableSchema
			for _, table := range schema.Tables {
				if table.Name == open.Name {
					tableSchema = table
					break
				}
			}
			staged, normalized, err := original.tableFromFile(open, tableSchema, base.tables[open.Name], open.Mode == "replace")
			if err != nil {
				t.Fatal("original full-table reader refused public source", err)
			}
			state.tables[open.Name], image.Tables[open.Name] = staged, normalized
		}
		if kind == "commit" {
			originalStates[commitID] = state
		}
		return image, kind
	}
	equalBytes := func(actual, expected any) {
		t.Helper()
		a, err := json.Marshal(actual)
		if err != nil {
			t.Fatal(err)
		}
		b, err := json.Marshal(expected)
		if err != nil || !bytes.Equal(a, b) {
			t.Fatal("public image bytes changed", err)
		}
	}
	seen := 0
	history, proof, err := reconstructNativeDeltaVisit(context.Background(), incrementalParts, root, NewNativeDeltaBudget(0), func(image NativeHistoryImage, _ []string) error {
		expected, kind := nextOriginal()
		if kind != "commit" {
			t.Fatal("public image callback order changed", kind)
		}
		equalBytes(image, expected)
		seen++
		return nil
	}, true)
	if err != nil || seen != 32 || proof.Totals != root.Totals || !incrementalParts.Verified() || incrementalParts.StreamBytes() != root.StreamBytes {
		t.Fatal("complete incremental public stream not authenticated", seen, err)
	}
	head, kind := nextOriginal()
	if kind != "head-checkpoint" || len(history.Images) != 1 {
		t.Fatal("public HEAD checkpoint missing")
	}
	head.Sequence = history.Images[0].Sequence
	equalBytes(history.Images[0], head)
	current, kind := nextOriginal()
	if kind != "current" {
		t.Fatal("public current checkpoint missing")
	}
	equalBytes(history.Current, current.Tables)
	if _, kind, err := original.next(); err != nil || kind != "end" {
		t.Fatal("original public end missing", kind, err)
	}
	if _, _, err := original.next(); err != io.EOF || !originalParts.Verified() || originalParts.StreamBytes() != root.StreamBytes {
		t.Fatal("original public source not exhausted", err)
	}
	t.Logf("public_image_bytes_equal commits=%d checkpoints=2 authenticated_eof=true fixture_sha256=%s root_sha256=%s", seen, hex.EncodeToString(sum[:]), archive.SHA256())
}
