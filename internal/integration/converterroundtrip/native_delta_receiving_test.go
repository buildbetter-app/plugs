package converterroundtrip

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
)

// This adapter fixture deliberately synthesizes JSON SQL types and keyless
// keys from an existing public v1 archive. It is ordinary receiver integration
// evidence, never a native producer capture, physical type or 1200-commit proof.
type syntheticDeltaState struct {
	schema dumpformat.NativeDeltaSchema
	rows   map[string][]dumpformat.NativeDeltaRow
	hashes map[string]dumpformat.NativeDeltaTableDigest
	root   string
}

func syntheticDeltaStateFor(t *testing.T, image dumpformat.NativeHistoryImage) syntheticDeltaState {
	t.Helper()
	s := syntheticDeltaState{schema: dumpformat.NativeDeltaSchema{Type: "schema", Profile: *image.SchemaProfile, Tables: []dumpformat.NativeDeltaTableSchema{}}, rows: map[string][]dumpformat.NativeDeltaRow{}, hashes: map[string]dumpformat.NativeDeltaTableDigest{}}
	names := []string{}
	for name := range image.Tables {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		table := image.Tables[name]
		definition := dumpformat.NativeDeltaTableSchema{Name: name, Columns: []dumpformat.NativeDeltaColumn{}}
		for _, column := range table.Columns {
			definition.Columns = append(definition.Columns, dumpformat.NativeDeltaColumn{Name: column, SQLType: "JSON"})
		}
		rows := []dumpformat.NativeDeltaRow{}
		occurrences := map[string]uint64{}
		for _, fields := range table.Rows {
			cells := []dumpformat.NativeDeltaCell{}
			for _, column := range table.Columns {
				cell := dumpformat.NativeDeltaCell{Kind: "string", Value: string(bytes.TrimSpace(encode(t, fields[column])))}
				if fields[column] == nil {
					cell = dumpformat.NativeDeltaCell{Kind: "sql-null"}
				}
				cells = append(cells, cell)
			}
			normalized := encode(t, fields)
			payload := dumpformat.NativeDeltaRowPayload(cells, normalized)
			group := string(dumpformat.NativeDeltaBagKey(payload, 0))
			key := dumpformat.NativeDeltaBagKey(payload, occurrences[group])
			occurrences[group]++
			rows = append(rows, dumpformat.NativeDeltaRow{Type: "row", Op: "insert", Key: base64.StdEncoding.EncodeToString(key), Cells: cells, NormalizedJSON: base64.StdEncoding.EncodeToString(normalized)})
		}
		sort.Slice(rows, func(i, j int) bool {
			a, _ := base64.StdEncoding.DecodeString(rows[i].Key)
			b, _ := base64.StdEncoding.DecodeString(rows[j].Key)
			return bytes.Compare(a, b) < 0
		})
		hash, err := dumpformat.NativeDeltaTableSHA256(definition, rows)
		if err != nil {
			t.Fatal(err)
		}
		s.schema.Tables = append(s.schema.Tables, definition)
		s.rows[name], s.hashes[name] = rows, dumpformat.NativeDeltaTableDigest{Rows: uint64(len(rows)), SHA256: hash}
	}
	s.schema.SchemaID = dumpformat.NativeDeltaSchemaID(s.schema)
	var err error
	s.root, err = dumpformat.NativeDeltaRootSHA256(s.hashes)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func syntheticDeltaCurrentProfile(t *testing.T, history dumpformat.NativeHistory, manifest dumpformat.Manifest) dumpformat.NativeCommitSchemaProfile {
	t.Helper()
	// Recover the schema surfaces explicitly witnessed by this fixture's
	// images/absence facts. This is test data construction, not product inference.
	surfaces := map[string]map[string]bool{}
	add := func(family, column string) {
		if surfaces[family] == nil {
			surfaces[family] = map[string]bool{}
		}
		if column != "" {
			surfaces[family][column] = true
		}
	}
	for _, image := range append(append([]dumpformat.NativeHistoryImage{}, history.Images...), dumpformat.NativeHistoryImage{Tables: history.Current}) {
		for family, table := range image.Tables {
			for _, column := range table.Columns {
				add(family, column)
			}
		}
		if image.SchemaProfile != nil {
			for _, absence := range image.SchemaProfile.Absences {
				add(absence.Family, absence.Column)
			}
		}
	}
	main, ignored := uint64(manifest.MainVersion), uint64(manifest.IgnoredVersion)
	p := dumpformat.NativeCommitSchemaProfile{Format: dumpformat.NativeCommitSchemaFormat, Scope: dumpformat.NativeCommitSchemaScope, MainVersion: &main, IgnoredVersion: &ignored, Absences: []dumpformat.NativeCommitSchemaAbsence{}}
	for family, columns := range surfaces {
		table, present := history.Current[family]
		if !present {
			p.Absences = append(p.Absences, dumpformat.NativeCommitSchemaAbsence{Family: family, Reason: dumpformat.AbsentBySourceSchema})
			continue
		}
		for column := range columns {
			found := false
			for _, actual := range table.Columns {
				found = found || column == actual
			}
			if !found {
				p.Absences = append(p.Absences, dumpformat.NativeCommitSchemaAbsence{Family: family, Column: column, Reason: dumpformat.AbsentBySourceSchema})
			}
		}
	}
	sort.Slice(p.Absences, func(i, j int) bool {
		a, b := p.Absences[i], p.Absences[j]
		if a.Family != b.Family {
			return a.Family < b.Family
		}
		return a.Column < b.Column
	})
	return p
}

func syntheticNativeDeltaCaptureSet(t *testing.T) (string, *dumpformat.Archive, []byte) {
	t.Helper()
	original, input := actualE25MultiVersionArchive(t)
	history := original.NativeSourceHistories()[0]
	// These are deliberately synthetic inert physical observations. The
	// portable archive has no corresponding business records or dispositions.
	view := func(sequence uint64) dumpformat.NativeTable {
		return dumpformat.NativeTable{Columns: []string{"fragment", "name", "type"}, Rows: []dumpformat.Row{{"fragment": fmt.Sprintf("SELECT %d AS public_value", sequence), "name": "synthetic_public_view", "type": "view"}}}
	}
	for i := range history.Images {
		image := &history.Images[i]
		image.Tables["dolt_ignore"] = history.Ignore
		if image.SchemaProfile != nil && image.SchemaProfile.MainVersion != nil {
			image.Tables["dolt_schemas"] = view(image.Sequence)
		}
		if image.CommitID == history.Head {
			history.Current["dolt_schemas"] = view(image.Sequence)
		}
	}
	history.Current["dolt_ignore"] = history.Ignore
	history.Current["dolt_nonlocal_tables"] = dumpformat.NativeTable{Columns: []string{"name"}, Rows: []dumpformat.Row{{"name": "synthetic_nonlocal"}}}
	members := tarMembers(t, input)
	manifest := original.Manifest()
	delete(members, dumpformat.NativeHistoryMember)
	delete(manifest.Files, dumpformat.NativeHistoryMember)
	manifest.NativeHistoryContract = dumpformat.NativeDeltaFormat
	members["manifest.json"] = encode(t, manifest)
	head := writeTarMembers(t, members)
	var stream bytes.Buffer
	write := func(value any) { stream.Write(encode(t, value)) }
	policy := map[string]string{}
	for _, family := range []string{"config", "metadata"} {
		for _, row := range history.Current[family].Rows {
			key, ok := row["key"].(string)
			if !ok {
				t.Fatal("public fixture config key missing")
			}
			policy[family+"/"+key] = "public"
		}
	}
	write(dumpformat.NativeDeltaHeader{Type: "header", Format: dumpformat.NativeDeltaFormat, StoreID: history.StoreID, Projects: history.Projects, Head: history.Head, CommitCount: uint64(len(history.Commits)), Ignore: history.Ignore, HistoryCoverage: history.HistoryCoverage, ConfigPolicy: policy})
	totals := dumpformat.NativeDeltaTotals{}
	schemas := map[string]bool{}
	states := map[string]syntheticDeltaState{}
	transaction := func(kind string, image dumpformat.NativeHistoryImage, commit *dumpformat.SourceCommit) syntheticDeltaState {
		s := syntheticDeltaStateFor(t, image)
		if !schemas[s.schema.SchemaID] {
			write(s.schema)
			schemas[s.schema.SchemaID] = true
			totals.Schemas++
		}
		var base syntheticDeltaState
		var first, baseID *string
		mode := "snapshot"
		if commit != nil {
			if len(commit.Parents) != 0 {
				first = &commit.Parents[0]
				if retained, ok := states[*first]; ok {
					base, baseID, mode = retained, first, "delta"
				}
			}
			write(dumpformat.NativeDeltaCommit{Type: "commit", Sequence: image.Sequence, CommitID: image.CommitID, Parents: commit.Parents, FirstParent: first, Metadata: commit.Metadata, SchemaID: s.schema.SchemaID, Mode: mode, BaseCommit: baseID, ExpectedRootSHA256: s.root})
		} else {
			write(dumpformat.NativeDeltaCheckpoint{Type: kind, CommitID: history.Head, SchemaID: s.schema.SchemaID, ExpectedRootSHA256: s.root})
		}
		definitions := map[string]dumpformat.NativeDeltaTableSchema{}
		for _, definition := range s.schema.Tables {
			definitions[definition.Name] = definition
		}
		previous := map[string]dumpformat.NativeDeltaTableSchema{}
		for _, definition := range base.schema.Tables {
			previous[definition.Name] = definition
		}
		names := []string{}
		for name := range definitions {
			names = append(names, name)
		}
		for name := range previous {
			if _, ok := definitions[name]; !ok {
				names = append(names, name)
			}
		}
		sort.Strings(names)
		for _, name := range names {
			definition, present := definitions[name]
			if !present {
				write(dumpformat.NativeDeltaDropTable{Type: "drop_table", Name: name, PreviousTableSchemaID: dumpformat.NativeDeltaTableSchemaID(previous[name])})
				continue
			}
			tableMode := "replace"
			before := map[string]dumpformat.NativeDeltaRow{}
			if old, ok := previous[name]; ok && dumpformat.NativeDeltaTableSchemaID(old) == dumpformat.NativeDeltaTableSchemaID(definition) {
				tableMode = "delta"
				for _, row := range base.rows[name] {
					before[row.Key] = row
				}
			}
			digest := s.hashes[name]
			write(dumpformat.NativeDeltaTable{Type: "table", Name: name, TableSchemaID: dumpformat.NativeDeltaTableSchemaID(definition), Mode: tableMode, ExpectedRows: digest.Rows, ExpectedSHA256: digest.SHA256})
			operations := map[string]any{}
			for _, row := range s.rows[name] {
				if _, ok := before[row.Key]; !ok {
					operations[row.Key] = row
				}
				delete(before, row.Key)
			}
			for key := range before {
				operations[key] = dumpformat.NativeDeltaDelete{Type: "row", Op: "delete", Key: key}
			}
			keys := []string{}
			for key := range operations {
				keys = append(keys, key)
			}
			sort.Slice(keys, func(i, j int) bool {
				a, _ := base64.StdEncoding.DecodeString(keys[i])
				b, _ := base64.StdEncoding.DecodeString(keys[j])
				return bytes.Compare(a, b) < 0
			})
			for _, key := range keys {
				write(operations[key])
			}
			write(dumpformat.NativeDeltaEndTable{Type: "end_table", RowOperations: uint64(len(keys)), Rows: digest.Rows, SHA256: digest.SHA256})
			totals.TableTransactions++
			totals.RowOperations += uint64(len(keys))
			totals.ReconstructedRows += digest.Rows
			totals.ReconstructedCells += digest.Rows * uint64(len(definition.Columns))
		}
		if commit != nil {
			write(dumpformat.NativeDeltaEndCommit{Type: "end_commit", Sequence: image.Sequence, CommitID: image.CommitID, TableCount: uint64(len(s.schema.Tables)), RootSHA256: s.root})
			totals.Commits++
			totals.FullImageBytes += uint64(len(encode(t, image)))
		} else {
			write(dumpformat.NativeDeltaEndCheckpoint{Type: "end_" + kind, TableCount: uint64(len(s.schema.Tables)), RootSHA256: s.root})
		}
		return s
	}
	for i, image := range history.Images {
		states[image.CommitID] = transaction("commit", image, &history.Commits[i])
	}
	var headImage dumpformat.NativeHistoryImage
	for _, image := range history.Images {
		if image.CommitID == history.Head {
			headImage = image
		}
	}
	headImage.Sequence = 0
	headState := transaction("head-checkpoint", headImage, nil)
	currentProfile := syntheticDeltaCurrentProfile(t, history, manifest)
	currentState := transaction("current", dumpformat.NativeHistoryImage{CommitID: history.Head, Tables: history.Current, SchemaProfile: &currentProfile}, nil)
	write(dumpformat.NativeDeltaEnd{Type: "end", Totals: totals, HeadRootSHA256: headState.root, CurrentRootSHA256: currentState.root})
	if stream.Len() > dumpformat.NativeDeltaPartBytes {
		t.Fatal("ordinary adapter fixture unexpectedly needs multiple parts")
	}
	header := dumpformat.NativeDeltaPartHeader{Format: dumpformat.NativeDeltaPartFormat, Sequence: 1, PayloadBytes: uint64(stream.Len()), PayloadSHA256: hashBytes(stream.Bytes())}
	part := append(encode(t, header), stream.Bytes()...)
	root := dumpformat.NativeCaptureSet{Format: dumpformat.NativeCaptureSetFormat, Contract: dumpformat.NativeDeltaFormat, StoreID: history.StoreID, Projects: history.Projects, Head: history.Head, HeadArchive: dumpformat.File{SHA256: hashBytes(head), Size: int64(len(head))}, PartCount: 1, FinalPartSHA256: hashBytes(part), StreamSHA256: hashBytes(stream.Bytes()), StreamBytes: uint64(stream.Len()), Totals: totals}
	dir := filepath.Join(t.TempDir(), "capture-set")
	if err := os.MkdirAll(filepath.Join(dir, "parts"), 0o700); err != nil {
		t.Fatal(err)
	}
	for name, data := range map[string][]byte{"head.tar": head, "parts/00000000000000000001.part": part, "capture-set.json": encode(t, root)} {
		if err := os.WriteFile(filepath.Join(dir, name), data, 0o600); err != nil {
			t.Fatal(err)
		}
		if out := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR"); out != "" {
			path := filepath.Join(out, "synthetic-delta-capture-set", name)
			if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, data, 0o600); err != nil {
				t.Fatal(err)
			}
		}
	}
	archive, err := dumpformat.ReadNativeCaptureSetDirectory(context.Background(), dir, dumpformat.NativeCaptureSetOptions{})
	if err != nil {
		t.Fatal("declared synthetic v2 adapter fixture", err)
	}
	return dir, archive, input
}

func TestNativeDeltaCaptureSetConstructorImportAndPersistentReopen(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	dir, archive, legacy := syntheticNativeDeltaCaptureSet(t)
	binary, binaryHash := receivingBinary(t)
	for _, engine := range []string{"memory", "sqlite"} {
		t.Run(engine, func(t *testing.T) {
			var policy map[string]json.RawMessage
			decode(t, nativeCommitReceivingPolicy(t, legacy, engine), &policy)
			policy["archive_sha256"] = encode(t, archive.SHA256())
			work := t.TempDir()
			policyPath, requestPath := filepath.Join(work, "policy.json"), filepath.Join(work, "request.json")
			if err := os.WriteFile(policyPath, encode(t, policy), 0o600); err != nil {
				t.Fatal(err)
			}
			if out := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR"); out != "" {
				if err := os.WriteFile(filepath.Join(out, "synthetic-delta-"+engine+"-policy.json"), encode(t, policy), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			base := []string{"--capture-set", dir, "--policy", policyPath, "--driver", engine, "--expected-binary-sha256", binaryHash}
			invoke := func(args []string, name string) ([]byte, []byte) {
				command := exec.CommandContext(ctx, binary, args...)
				var stdout, stderr bytes.Buffer
				command.Stdout, command.Stderr = &stdout, &stderr
				err := command.Run()
				if out := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR"); out != "" {
					for suffix, data := range map[string][]byte{"stdout": stdout.Bytes(), "stderr": stderr.Bytes(), "argv.json": encode(t, append([]string{binary}, args...))} {
						if err := os.WriteFile(filepath.Join(out, "synthetic-delta-"+engine+"-"+name+"."+suffix), data, 0o600); err != nil {
							t.Fatal(err)
						}
					}
				}
				if err != nil {
					t.Fatalf("declared synthetic capture-set %s: %v %s", name, err, stderr.Bytes())
				}
				return stdout.Bytes(), stderr.Bytes()
			}
			requestRaw, _ := invoke(append([]string{"prepare-request"}, base...), "constructor")
			if err := os.WriteFile(requestPath, requestRaw, 0o600); err != nil {
				t.Fatal(err)
			}
			target := ""
			if engine == "sqlite" {
				target = filepath.Join(work, "store.sqlite")
			}
			stdout, _ := invoke(append(append([]string{}, base...), "--request", requestPath, "--target", target), "receive")
			var receipt struct {
				ArchiveSHA256 string `json:"archive_sha256"`
				NativeDelta   struct {
					Verification dumpformat.NativeDeltaVerification `json:"verification"`
					RSS          struct {
						Status    string `json:"status"`
						PeakBytes uint64 `json:"peak_bytes"`
					} `json:"rss"`
				} `json:"native_delta"`
				Verification struct {
					Reopen        string `json:"reopen"`
					HistoryImages int    `json:"history_images"`
				} `json:"verification"`
				Report struct {
					NativeFamilies []NativeFamilyReport `json:"NativeFamilies"`
				} `json:"committed_report"`
			}
			if err := json.Unmarshal(stdout, &receipt); err != nil {
				t.Fatal(err)
			}
			want, _ := archive.NativeDeltaVerification()
			if receipt.ArchiveSHA256 != archive.SHA256() || !reflect.DeepEqual(receipt.NativeDelta.Verification.Commits, want.Commits) || receipt.NativeDelta.Verification.Totals != want.Totals || receipt.NativeDelta.RSS.Status != "MEASURED" || receipt.NativeDelta.RSS.PeakBytes == 0 {
				t.Fatal("current reconstructed proofs or actual receiver RSS missing")
			}
			wantReopen := "same-process-memory"
			if engine == "sqlite" {
				wantReopen = "driver-reopened"
			}
			if receipt.Verification.Reopen != wantReopen || receipt.Verification.HistoryImages != len(want.Commits) {
				t.Fatal("full-history receive/reopen verification missing")
			}
			current := archive.NativeSourceHistories()[0]
			opaque := map[string]bool{}
			for _, family := range receipt.Report.NativeFamilies {
				if family.History != dumpformat.FamilyOpaqueNative {
					continue
				}
				if opaque[family.Family] || !current.OpaqueNativeCurrentFamily(family.Family) || family.SourceRows != len(current.Current[family.Family].Rows) || family.ImportedRows != family.SourceRows || family.ExpiredRows != 0 {
					t.Fatal("opaque native current report differs from exact source rows")
				}
				opaque[family.Family] = true
			}
			if len(opaque) != 3 || !opaque["dolt_schemas"] || !opaque["dolt_ignore"] || !opaque["dolt_nonlocal_tables"] {
				t.Fatal("opaque physical system family counts missing")
			}
			usage := receipt.NativeDelta.Verification.Usage
			var total uint64
			for phase, charge := range usage.Categories {
				total += charge
				if usage.Charges[phase] == 0 {
					t.Fatal("missing category count")
				}
			}
			if total != usage.PeakAccountedDecodedBytes || total > usage.LimitBytes {
				t.Fatal("decoded accounting sum differs")
			}
		})
	}
}
