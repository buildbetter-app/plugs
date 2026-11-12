// Package graphcertfixture builds explicitly authored scale inputs for the real
// native capture-set reader. These are not physical producer recordings.
package graphcertfixture

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
)

func Hash(raw []byte) string { h := sha256.Sum256(raw); return hex.EncodeToString(h[:]) }
func JSON(t *testing.T, value any) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return append(raw, '\n')
}
func Decode(t *testing.T, raw []byte, out any) {
	t.Helper()
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	if err := d.Decode(out); err != nil {
		t.Fatal(err)
	}
}
func members(t *testing.T, raw []byte) map[string][]byte {
	t.Helper()
	out := map[string][]byte{}
	r := tar.NewReader(bytes.NewReader(raw))
	for {
		h, err := r.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if h.Typeflag != tar.TypeReg {
			continue
		}
		b, err := io.ReadAll(r)
		if err != nil {
			t.Fatal(err)
		}
		out[h.Name] = b
	}
	return out
}
func pack(t *testing.T, files map[string][]byte) []byte {
	t.Helper()
	var b bytes.Buffer
	w := tar.NewWriter(&b)
	names := make([]string, 0, len(files))
	for name := range files {
		if name != "manifest.json" {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	names = append([]string{"manifest.json"}, names...)
	for _, name := range names {
		raw := files[name]
		if err := w.WriteHeader(&tar.Header{Name: name, Mode: 0600, Size: int64(len(raw)), Typeflag: tar.TypeReg, Format: tar.FormatUSTAR}); err != nil {
			t.Fatal(err)
		}
		if _, err := w.Write(raw); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return b.Bytes()
}

// Capture retains all original current non-issue rows and adds isolated issues
// to exactly count. It authors a one-commit COMPLETE history whose issue rows
// agree at current and head, preserving the source's separate schemas. H is
// still later than its source revision, distinguishing [H,H] from history.
func Capture(t *testing.T, original string, count int) (directory, parent string) {
	t.Helper()
	raw, err := os.ReadFile(original)
	if err != nil {
		t.Fatal(err)
	}
	if Hash(raw) != "66e2a1410cb6de0532e8bd453f06353d3fde8159ed9d6ffe4b0ed17f20bf6ef8" {
		t.Fatal("original fixture changed")
	}
	gz, err := gzip.NewReader(bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	expanded, err := io.ReadAll(gz)
	if err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	container := members(t, expanded)
	files := members(t, container["capture-set/head.tar"])
	var manifest dumpformat.Manifest
	Decode(t, files["manifest.json"], &manifest)
	var root dumpformat.NativeCaptureSet
	Decode(t, container["capture-set/capture-set.json"], &root)
	part := container["capture-set/parts/00000000000000000001.part"]
	_, payload, ok := bytes.Cut(part, []byte{'\n'})
	if !ok {
		t.Fatal("missing original part header")
	}
	schemas := map[string]dumpformat.NativeDeltaSchema{}
	tables := map[string][]dumpformat.NativeDeltaRow{}
	current := false
	table := ""
	var schema, headSchema dumpformat.NativeDeltaSchema
	var header dumpformat.NativeDeltaHeader
	for _, line := range bytes.Split(bytes.TrimSpace(payload), []byte{'\n'}) {
		var tag struct {
			Type string `json:"type"`
		}
		Decode(t, line, &tag)
		switch tag.Type {
		case "header":
			Decode(t, line, &header)
		case "schema":
			var s dumpformat.NativeDeltaSchema
			Decode(t, line, &s)
			schemas[s.SchemaID] = s
		case "head-checkpoint":
			var c dumpformat.NativeDeltaCheckpoint
			Decode(t, line, &c)
			headSchema = schemas[c.SchemaID]
		case "current":
			var c dumpformat.NativeDeltaCheckpoint
			Decode(t, line, &c)
			schema = schemas[c.SchemaID]
			current = true
		case "table":
			if current {
				var v dumpformat.NativeDeltaTable
				Decode(t, line, &v)
				table = v.Name
				tables[table] = []dumpformat.NativeDeltaRow{}
			}
		case "row":
			if current {
				var v dumpformat.NativeDeltaRow
				Decode(t, line, &v)
				tables[table] = append(tables[table], v)
			}
		case "end_current":
			current = false
		}
	}
	if len(tables["issues"]) != 2 || count < 3 || len(schema.Tables) == 0 || len(headSchema.Tables) == 0 {
		t.Fatal("unexpected current fixture")
	}
	// The source head excludes its unversioned working-set families. Preserve
	// that schema boundary while authoring the same changed issue rows at both.
	headTables := map[string]dumpformat.NativeDeltaTableSchema{}
	for _, table := range headSchema.Tables {
		headTables[table.Name] = table
	}
	var records []dumpformat.Record
	for _, line := range bytes.Split(bytes.TrimSpace(files["records/bead.ndjson"]), []byte{'\n'}) {
		var r dumpformat.Record
		Decode(t, line, &r)
		records = append(records, r)
	}
	prototype := records[1]
	physical := tables["issues"][0]
	var issueSchema dumpformat.NativeDeltaTableSchema
	idColumn := -1
	for _, s := range schema.Tables {
		if s.Name == "issues" {
			issueSchema = s
			for i, c := range s.Columns {
				if c.Name == "id" {
					idColumn = i
				}
			}
		}
	}
	if idColumn < 0 {
		t.Fatal("missing issue identity column")
	}
	// Match the physical prototype to the exact portable source image.
	for _, row := range tables["issues"] {
		var fields dumpformat.Row
		b, err := base64.StdEncoding.DecodeString(row.NormalizedJSON)
		if err != nil {
			t.Fatal(err)
		}
		Decode(t, b, &fields)
		if fields["id"] == prototype.ID {
			physical = row
		}
	}
	for len(records) < count {
		id := fmt.Sprintf("%s-cert%06d", manifest.Projects[0].Prefix, len(records))
		parent = id
		// Keep scaled authored rows deliberately small. The source fixture's
		// second bead is a valid prototype, but its 3 KiB description would be
		// multiplied across every current row by the native decoded accounting
		// and verifier's per-record validation charge. This fixture reduction
		// exercises the unchanged admission and import paths without consuming
		// that budget on repeated prose.
		description := "x"
		var record dumpformat.Record
		Decode(t, JSON(t, prototype), &record)
		record.ID = id
		record.Source.Fields["id"] = id
		record.Source.Fields["description"] = description
		record.Body["description"] = description
		records = append(records, record)
		row := physical
		row.Cells = append([]dumpformat.NativeDeltaCell(nil), physical.Cells...)
		row.Cells[idColumn].Value = id
		for j, c := range issueSchema.Columns {
			if c.Name == "description" {
				row.Cells[j].Value = description
			}
		}
		key, err := dumpformat.NativeDeltaPrimaryKey(issueSchema, row.Cells)
		if err != nil {
			t.Fatal(err)
		}
		row.Key = base64.StdEncoding.EncodeToString(key)
		b, err := base64.StdEncoding.DecodeString(physical.NormalizedJSON)
		if err != nil {
			t.Fatal(err)
		}
		var fields dumpformat.Row
		Decode(t, b, &fields)
		fields["id"] = id
		fields["description"] = description
		// The native row contract includes the canonical JSON line terminator.
		row.NormalizedJSON = base64.StdEncoding.EncodeToString(JSON(t, fields))
		tables["issues"] = append(tables["issues"], row)
	}
	sort.Slice(records, func(i, j int) bool { return records[i].ID < records[j].ID })
	var beads bytes.Buffer
	for _, r := range records {
		beads.Write(JSON(t, r))
	}
	files["records/bead.ndjson"] = beads.Bytes()
	commitID := strings.Repeat("a", 32)
	metadata := dumpformat.Row{"fixture": "authored current-head graph certification scale input"}
	var history dumpformat.SourceHistory
	Decode(t, files[dumpformat.SourceHistoryMember], &history)
	history.Commits = []dumpformat.SourceCommit{{ID: commitID, Parents: []string{}, Metadata: metadata}}
	files[dumpformat.SourceHistoryMember] = JSON(t, history)
	for i := range manifest.Kinds {
		if manifest.Kinds[i].Name == "bead" {
			manifest.Kinds[i].Count = count
		}
	}
	for i := range manifest.Sources {
		switch manifest.Sources[i].Family {
		case "issues", "issues.row_lock", "issues.is_blocked":
			manifest.Sources[i].Rows = count
		}
	}
	for name, b := range files {
		if name != "manifest.json" {
			manifest.Files[name] = dumpformat.File{Size: int64(len(b)), SHA256: Hash(b)}
		}
	}
	files["manifest.json"] = JSON(t, manifest)
	head := pack(t, files)
	digests := map[string]dumpformat.NativeDeltaTableDigest{}
	image := dumpformat.NativeHistoryImage{Sequence: 1, CommitID: commitID, SchemaProfile: &headSchema.Profile, Tables: map[string]dumpformat.NativeTable{}}
	headDigests := map[string]dumpformat.NativeDeltaTableDigest{}
	for _, s := range schema.Tables {
		rows := tables[s.Name]
		sum, err := dumpformat.NativeDeltaTableSHA256(s, rows)
		if err != nil {
			t.Fatal(err)
		}
		digests[s.Name] = dumpformat.NativeDeltaTableDigest{Rows: uint64(len(rows)), SHA256: sum}
		headTable, versioned := headTables[s.Name]
		if !versioned {
			continue
		}
		if dumpformat.NativeDeltaTableSchemaID(headTable) != dumpformat.NativeDeltaTableSchemaID(s) {
			t.Fatal("fixture head/current table schema differs", s.Name)
		}
		headDigests[s.Name] = digests[s.Name]
		names := []string{}
		for _, c := range s.Columns {
			names = append(names, c.Name)
		}
		sort.Strings(names)
		tab := dumpformat.NativeTable{Columns: names, Rows: []dumpformat.Row{}}
		for _, r := range rows {
			b, err := base64.StdEncoding.DecodeString(r.NormalizedJSON)
			if err != nil {
				t.Fatal(err)
			}
			var fields dumpformat.Row
			Decode(t, b, &fields)
			tab.Rows = append(tab.Rows, fields)
		}
		sort.Slice(tab.Rows, func(i, j int) bool { return bytes.Compare(JSON(t, tab.Rows[i]), JSON(t, tab.Rows[j])) < 0 })
		image.Tables[s.Name] = tab
	}
	if len(headDigests) != len(headTables) {
		t.Fatal("fixture current image lacks a source head table")
	}
	graphRoot, err := dumpformat.NativeDeltaRootSHA256(digests)
	if err != nil {
		t.Fatal(err)
	}
	headRoot, err := dumpformat.NativeDeltaRootSHA256(headDigests)
	if err != nil {
		t.Fatal(err)
	}
	var stream bytes.Buffer
	write := func(v any) { stream.Write(JSON(t, v)) }
	header.Head = commitID
	header.CommitCount = 1
	header.HistoryCoverage = nil
	write(header)
	write(headSchema)
	totals := dumpformat.NativeDeltaTotals{Schemas: 1, Commits: 1, FullImageBytes: uint64(len(JSON(t, image)))}
	for _, kind := range []string{"commit", "head-checkpoint", "current"} {
		stateSchema, stateRoot := headSchema, headRoot
		if kind == "current" {
			stateSchema, stateRoot = schema, graphRoot
			if schema.SchemaID != headSchema.SchemaID {
				write(schema)
				totals.Schemas++
			}
		}
		if kind == "commit" {
			write(dumpformat.NativeDeltaCommit{Type: kind, Sequence: 1, CommitID: commitID, Parents: []string{}, Metadata: metadata, SchemaID: stateSchema.SchemaID, Mode: "snapshot", ExpectedRootSHA256: stateRoot})
		} else {
			write(dumpformat.NativeDeltaCheckpoint{Type: kind, CommitID: commitID, SchemaID: stateSchema.SchemaID, ExpectedRootSHA256: stateRoot})
		}
		for _, s := range stateSchema.Tables {
			d := digests[s.Name]
			write(dumpformat.NativeDeltaTable{Type: "table", Name: s.Name, TableSchemaID: dumpformat.NativeDeltaTableSchemaID(s), Mode: "replace", ExpectedRows: d.Rows, ExpectedSHA256: d.SHA256})
			for _, r := range tables[s.Name] {
				write(r)
			}
			write(dumpformat.NativeDeltaEndTable{Type: "end_table", RowOperations: d.Rows, Rows: d.Rows, SHA256: d.SHA256})
			totals.TableTransactions++
			totals.RowOperations += d.Rows
			totals.ReconstructedRows += d.Rows
			totals.ReconstructedCells += d.Rows * uint64(len(s.Columns))
		}
		if kind == "commit" {
			write(dumpformat.NativeDeltaEndCommit{Type: "end_commit", Sequence: 1, CommitID: commitID, TableCount: uint64(len(stateSchema.Tables)), RootSHA256: stateRoot})
		} else {
			write(dumpformat.NativeDeltaEndCheckpoint{Type: "end_" + kind, TableCount: uint64(len(stateSchema.Tables)), RootSHA256: stateRoot})
		}
	}
	write(dumpformat.NativeDeltaEnd{Type: "end", Totals: totals, HeadRootSHA256: headRoot, CurrentRootSHA256: graphRoot})
	directory = t.TempDir()
	if err := os.Mkdir(filepath.Join(directory, "parts"), 0700); err != nil {
		t.Fatal(err)
	}
	root.PartCount = 0
	var previous *string
	for offset := 0; offset < stream.Len(); offset += dumpformat.NativeDeltaPartPayloadLimit {
		piece := stream.Bytes()[offset:min(offset+dumpformat.NativeDeltaPartPayloadLimit, stream.Len())]
		root.PartCount = uint64(offset/dumpformat.NativeDeltaPartPayloadLimit + 1)
		part = append(JSON(t, dumpformat.NativeDeltaPartHeader{Format: dumpformat.NativeDeltaPartFormat, Sequence: root.PartCount, PreviousPartSHA256: previous, PayloadBytes: uint64(len(piece)), PayloadSHA256: Hash(piece)}), piece...)
		digest := Hash(part)
		previous = &digest
		if err := os.WriteFile(filepath.Join(directory, fmt.Sprintf("parts/%020d.part", root.PartCount)), part, 0600); err != nil {
			t.Fatal(err)
		}
	}
	root.Head = commitID
	root.HeadArchive = dumpformat.File{Size: int64(len(head)), SHA256: Hash(head)}
	root.FinalPartSHA256 = *previous
	root.StreamSHA256 = Hash(stream.Bytes())
	root.StreamBytes = uint64(stream.Len())
	root.Totals = totals
	for name, b := range map[string][]byte{"head.tar": head, "capture-set.json": JSON(t, root)} {
		if err := os.WriteFile(filepath.Join(directory, name), b, 0600); err != nil {
			t.Fatal(err)
		}
	}
	return directory, parent
}
