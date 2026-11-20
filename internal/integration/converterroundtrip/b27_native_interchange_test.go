package converterroundtrip

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// These inputs are the rich producer's original public sources. Expected SQL
// rows are recorded before either export format; receiver output is never an
// expectation. The two original StoreIDs remain distinct after offline merge.
type b27NativeInput struct {
	Project       string               `json:"project"`
	StoreID       string               `json:"store_id"`
	Head          string               `json:"source_head"`
	Projects      []dumpformat.Project `json:"projects"`
	CaptureSHA256 string               `json:"capture_set_sha256"`
	dir           string
	producerHead  string
	current       map[string]dumpformat.NativeTable
	// physicalCurrent is the complete authenticated physical source image.
	// current remains the independent SQL-row oracle and intentionally omits
	// Dolt's three physical-only tables.
	physicalCurrent map[string]dumpformat.NativeTable
}

// This consumer manifest binds selected byte-exact original payloads. It is
// admission provenance, not a replacement producer identity or row oracle.
type b27NativeBundle struct {
	Format                       string `json:"format"`
	ProducerHead                 string `json:"producer_head"`
	NativeOriginalManifestSHA256 string `json:"native_original_manifest_sha256"`
	Files                        map[string]struct {
		Bytes  int64  `json:"bytes"`
		SHA256 string `json:"sha256"`
	} `json:"files"`
}

func b27NativeHex(t *testing.T, name, value string, length int) {
	t.Helper()
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded)*2 != length || value != strings.ToLower(value) {
		t.Fatal("invalid native provenance", name)
	}
}

func b27NativeJSON(t *testing.T, raw []byte, out any) {
	t.Helper()
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	if err := d.Decode(out); err != nil {
		t.Fatal(err)
	}
	if err := d.Decode(new(any)); err != io.EOF {
		t.Fatal("unexpected trailing native oracle data", err)
	}
}

func b27NativeBytes(t *testing.T, path string) []byte {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func b27NativeInputs(t *testing.T) []b27NativeInput {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("testdata", "b27-native-rich"))
	if err != nil {
		t.Fatal(err)
	}
	var bundle b27NativeBundle
	b27NativeJSON(t, b27NativeBytes(t, filepath.Join(root, "SOURCE.json")), &bundle)
	if bundle.Format != "b27-public-native-inputs/1" || len(bundle.Files) == 0 {
		t.Fatal("actual admitted native payload manifest required")
	}
	b27NativeHex(t, "producer Git head", bundle.ProducerHead, 40)
	b27NativeHex(t, "original manifest", bundle.NativeOriginalManifestSHA256, 64)
	seenFiles := map[string]bool{}
	if err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		// Bazel's data runfiles are symlinks. Check their regular file
		// targets, then enforce the same exact membership/size/digest
		// contract as directly checked-out input files.
		if info.Mode()&os.ModeSymlink != 0 {
			info, err = os.Stat(path)
			if err != nil {
				return err
			}
		}
		rel, err := filepath.Rel(root, path)
		if err != nil || !info.Mode().IsRegular() {
			return fmt.Errorf("nonregular native payload")
		}
		rel = filepath.ToSlash(rel)
		if rel == "SOURCE.json" {
			return nil
		}
		declared, ok := bundle.Files[rel]
		if !ok || info.Size() != declared.Bytes || publicProofHashFile(t, path) != declared.SHA256 {
			return fmt.Errorf("native payload membership or digest differs: %s", rel)
		}
		seenFiles[rel] = true
		return nil
	}); err != nil || len(seenFiles) != len(bundle.Files) {
		t.Fatal("native input manifest incomplete", err)
	}
	// Capture-set admission deliberately refuses symlink inputs. Materialize
	// the already authenticated committed bytes as ordinary disposable files;
	// the source directory and every original digest remain unchanged.
	physical := t.TempDir()
	for rel, declared := range bundle.Files {
		path := filepath.Join(physical, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		raw := b27NativeBytes(t, filepath.Join(root, filepath.FromSlash(rel)))
		if int64(len(raw)) != declared.Bytes || hashBytes(raw) != declared.SHA256 {
			t.Fatal("native input changed during materialization", rel)
		}
		if err := os.WriteFile(path, raw, 0600); err != nil {
			t.Fatal(err)
		}
	}
	var result []b27NativeInput
	seenStores := map[string]bool{}
	for _, project := range []string{"review-project", "delivery-project"} {
		dir, err := filepath.Abs(filepath.Join(physical, project))
		if err != nil {
			t.Fatal(err)
		}
		var input b27NativeInput
		b27NativeJSON(t, b27NativeBytes(t, filepath.Join(dir, "identity.json")), &input)
		if input.Project != project || input.StoreID == "" || seenStores[input.StoreID] || input.Head == "" || len(input.Projects) != 1 || input.Projects[0].Prefix != project {
			t.Fatal("independent native source identity is incomplete or repeated", project)
		}
		seenStores[input.StoreID] = true
		input.dir = dir
		input.producerHead = bundle.ProducerHead
		if input.producerHead == input.Head {
			t.Fatal("producer Git commit conflated with native source commit")
		}
		b27NativeJSON(t, b27NativeBytes(t, filepath.Join(dir, "raw-current.json")), &input.current)
		if len(input.current) == 0 {
			t.Fatal("independent SQL census is empty")
		}
		result = append(result, input)
	}
	return result
}

// The generic full-history test helper leaves dependencies inert. B27 supplies
// checked relations for the original durable endpoints, including parent edges.
// Other source planes keep their explicit archived disposition.
func b27NativePlan(t *testing.T, archive *dumpformat.Archive, store driver.Store, lease driver.ActiveLease) driver.SourcePlanePlanV1 {
	t.Helper()
	plan := publicProofHostIntent(t, archive, nativeHistoryPlanFromArchive(t, archive, store, lease))
	return b27NativeIntent(t, archive, plan)
}

func b27NativeIntent(t *testing.T, archive *dumpformat.Archive, plan driver.SourcePlanePlanV1) driver.SourcePlanePlanV1 {
	t.Helper()
	entities := map[driver.SourceEntityKey]bool{}
	for _, entity := range plan.Entities {
		entities[entity.Source] = true
	}
	work, release := archive.NativeDeltaAccountingScope()
	defer release()
	raw, err := work.SourcePlaneRecords()
	if err != nil {
		t.Fatal(err)
	}
	mapped := map[driver.SourceEdgeKey]bool{}
	for _, record := range raw {
		var row dumpformat.Record
		b27NativeJSON(t, record.Raw, &row)
		if row.Kind != "edge" || row.Source.Table != dumpformat.DurableDependencyTable {
			continue
		}
		from := driver.SourceEntityKey{Project: row.Project, Plane: dumpformat.DurableEntityTable, ID: stringValue(row.Body["from"])}
		if !entities[from] {
			t.Fatal("durable native relation lacks its explicit source", from)
		}
		relation := driver.SourcePlaneRelationV1{Source: driver.SourceEdgeKey{Project: row.Project, Table: row.Source.Table, ID: row.ID}, From: from, Kind: stringValue(row.Body["type"]), PredicateContract: "beads-typed-metadata/1", Origin: jointCommitment(record.Raw, "object"), Fields: jointFields(t, record)}
		switch stringValue(row.Body["target_kind"]) {
		case "depends_on_issue_id":
			to := driver.SourceEntityKey{Project: jointSourceProject(t, stringValue(row.Body["to"]), archive.Manifest().Projects), Plane: dumpformat.DurableEntityTable, ID: stringValue(row.Body["to"])}
			if !entities[to] {
				t.Fatal("durable native relation lacks its explicit target", to)
			}
			relation.To = &to
		case "depends_on_external":
			external := stringValue(row.Body["to"])
			relation.External = &external
		default:
			t.Fatal("native relation requires an independently declared target disposition", row.ID, row.Body["target_kind"])
		}
		predicate, err := dumpformat.SourceEdgeMetadata(archive.Manifest().EdgeVersion(), row.ID, relation.Kind, row.Source.Fields["metadata"])
		if err != nil {
			t.Fatal(err)
		}
		relation.PredicateJSON, err = json.Marshal(predicate)
		if err != nil {
			t.Fatal(err)
		}
		if mapped[relation.Source] {
			t.Fatal("repeated native relation", relation.Source)
		}
		mapped[relation.Source] = true
		plan.Relations = append(plan.Relations, relation)
	}
	origins := plan.Origins[:0]
	for _, origin := range plan.Origins {
		if origin.Path == "" && mapped[driver.SourceEdgeKey{Project: origin.Project, Table: origin.Table, ID: origin.ID}] {
			continue
		}
		origins = append(origins, origin)
	}
	plan.Origins = origins
	sort.Slice(plan.Relations, func(i, j int) bool {
		a, b := plan.Relations[i].Source, plan.Relations[j].Source
		if a.Project != b.Project {
			return a.Project < b.Project
		}
		if a.Table != b.Table {
			return a.Table < b.Table
		}
		return a.ID < b.ID
	})
	return plan
}

// Compare decoded values, preserving json.Number spelling, null, absent keys,
// array order and duplicate source rows. Object member order is not a value.
func b27NativeEqual(t *testing.T, name string, got, want any) {
	t.Helper()
	decode := func(value any) any {
		raw, err := json.Marshal(value)
		if err != nil {
			t.Fatal(name, err)
		}
		var result any
		b27NativeJSON(t, raw, &result)
		return result
	}
	if !reflect.DeepEqual(decode(got), decode(want)) {
		t.Fatalf("native %s differs: got %#v want %#v", name, got, want)
	}
}

func b27NativeRows(t *testing.T, store driver.Store, at *model.Revision) map[string]driver.Record {
	t.Helper()
	ctx := context.Background()
	meta, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if at == nil {
		at = &meta.Head
	}
	r, err := store.Read(ctx, driver.View{Incarnation: meta.Incarnation, At: at})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	rows := map[string]driver.Record{}
	cursors := map[string]bool{}
	var cursor *driver.Cursor
	for {
		page, err := r.Query(ctx, driver.Query{AsOf: at, Limit: 97, Cursor: cursor})
		if err != nil || page.AsOf.String() != at.String() {
			t.Fatal("native readback snapshot", err)
		}
		for _, row := range page.Items {
			if !row.Ref.IsID() || !row.Ref.Branch.IsZero() || rows[row.Ref.ID].Kind != "" {
				t.Fatal("native readback Ref repeated or noncanonical", row.Ref)
			}
			rows[row.Ref.ID] = row
		}
		if page.NextCursor == nil {
			break
		}
		key, err := json.Marshal(page.NextCursor)
		if err != nil || len(page.Items) == 0 || cursors[string(key)] {
			t.Fatal("native readback cursor repeated", err)
		}
		cursors[string(key)] = true
		cursor = page.NextCursor
	}
	return rows
}

func b27NativeTableEqual(t *testing.T, name string, got, want dumpformat.NativeTable) {
	t.Helper()
	normalize := func(table dumpformat.NativeTable) dumpformat.NativeTable {
		table.Rows = append([]dumpformat.Row{}, table.Rows...)
		sort.Slice(table.Rows, func(i, j int) bool {
			a, _ := json.Marshal(table.Rows[i])
			b, _ := json.Marshal(table.Rows[j])
			return bytes.Compare(a, b) < 0
		})
		return table
	}
	b27NativeEqual(t, name, normalize(got), normalize(want))
}

func b27NativeDependencyTarget(t *testing.T, row dumpformat.Row) (string, string) {
	t.Helper()
	arm, target := "", ""
	for _, name := range []string{"depends_on_issue_id", dumpformat.EphemeralDependencyTargetField, "depends_on_external"} {
		if row[name] == nil {
			continue
		}
		value, ok := row[name].(string)
		if !ok || value == "" || arm != "" {
			t.Fatal("independent native edge target is ambiguous", row["id"])
		}
		arm, target = name, value
	}
	if arm == "" {
		t.Fatal("independent native edge target is absent", row["id"])
	}
	return arm, target
}

func b27NativeRequest(t *testing.T, input b27NativeInput, mode string) publicProofRequest {
	t.Helper()
	states := b27NativeBytes(t, filepath.Join(input.dir, "oracle", "states.ndjson"))
	decoder := json.NewDecoder(bytes.NewReader(states))
	commits := 0
	for {
		var state publicProofState
		if err := decoder.Decode(&state); err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		if state.Kind == "commit" {
			commits++
		}
	}
	if commits < 2 {
		t.Fatal("rich source requires genuine multiple native commits")
	}
	req := publicProofRequest{Format: "public-native-capture-proof/1", Capture: filepath.Join(input.dir, "capture", "capture-set"), CaptureSHA256: input.CaptureSHA256, Source: dumpformat.SourceIdentity{StoreID: input.StoreID, Project: input.Project}, ProducerHead: input.producerHead, Oracle: filepath.Join(input.dir, "oracle"), LedgerSHA256: hashBytes(b27NativeBytes(t, filepath.Join(input.dir, "oracle", "tables.ndjson"))), StatesSHA256: hashBytes(states), RestoreTime: time.Date(2026, 11, 11, 17, 0, 0, 0, time.UTC).Format(time.RFC3339Nano), RestoreMode: mode, JournalTable: dumpformat.SourceEventJournalTable, Commits: commits}
	if mode == "partial" {
		req.RestoreFirstSequence = json.RawMessage("2")
	}
	return req
}

// The portable merge's revision mapping is checked against the independent
// physical-state ledger, never against rows reconstructed by the converter.
// Legacy normalized history makes no D3 SQL-type/physical-key claim; the
// separate per-source delta matrix checks that stronger typed contract.
func b27NativeFullHistory(t *testing.T, store driver.Store, archive *dumpformat.Archive, inputs []b27NativeInput, report Report) {
	t.Helper()
	ctx := context.Background()
	rows := b27NativeRows(t, store, nil)
	var evidence driver.HistoryImportEvidence
	b27NativeJSON(t, rows[driver.OfflineHistoryEvidenceID].Body, &evidence)
	if evidence.Source != driver.NativeTableHistorySource || evidence.Mode != driver.RestoreHistoryModeFull || !evidence.Complete || evidence.First != 1 || !evidence.Floor.IsZero() || len(evidence.Gaps) != 0 {
		t.Fatal("two-project complete native history evidence differs")
	}
	seenMappings := map[driver.SourceEntityKey]bool{}
	expectedMappings := 0
	for _, input := range inputs {
		req := b27NativeRequest(t, input, "full")
		ledger := publicProofLedger(t, req)
		states := publicProofStates(t, req, ledger)
		if states.Head.CommitID != input.Head {
			t.Fatal("independent source HEAD differs", input.Project)
		}
		expectedMappings += req.Commits
		for seq := uint64(1); seq <= uint64(req.Commits); seq++ {
			var mapped driver.SourceImportRevision
			matches := 0
			for _, candidate := range evidence.Mapping {
				if candidate.SourceStoreID == input.StoreID && candidate.Project == input.Project && candidate.SourceSequence == seq {
					mapped, matches = candidate, matches+1
				}
			}
			key := driver.SourceEntityKey{Project: input.Project, Plane: input.StoreID, ID: fmt.Sprint(seq)}
			if matches != 1 || seenMappings[key] || mapped.Revision == "" || len(mapped.EventIDs) != 0 || !reflect.DeepEqual(mapped.CommitIDs, []string{states.Commits[seq].CommitID}) {
				t.Fatal("native source history mapping differs", input.Project, seq)
			}
			seenMappings[key] = true
			reader, err := store.Read(ctx, driver.View{Incarnation: report.Restored.Incarnation, At: &mapped.Revision})
			if err != nil {
				t.Fatal(err)
			}
			groups, err := queryNativeRowGroups(ctx, reader, report.Restored.Incarnation, mapped.Revision, archive)
			closeErr := reader.Close()
			if err != nil || closeErr != nil {
				t.Fatal("independent native history query", err, closeErr)
			}
			tables := 0
			for _, entry := range ledger {
				if entry.Kind != "commit" || entry.Sequence != seq {
					continue
				}
				group := nativeRowGroup{req.Source, entry.Family, entry.History}
				records, exists := groups[group]
				value, meta, err := dumpformat.ReconstructNativeRowTable(req.Source, entry.Family, entry.History, records, entry.ConfigPolicy)
				if !exists || err != nil || meta.RowCount != uint64(entry.ImportedRows) || len(records) != entry.ImportedRows+1 {
					t.Fatal("independent native history table census", input.Project, seq, entry.Family, err)
				}
				if entry.Family == "local_metadata" {
					// Machine-local bookkeeping is opaque input, excluded from
					// business equality by the 18:51Z frozen classification.
					tables++
					delete(groups, group)
					continue
				}
				actual, err := dumpformat.NativeTableBodyBytes(req.Source, entry.Family, entry.History, value)
				want := b27NativeBytes(t, filepath.Join(req.Oracle, entry.File))
				if err != nil || len(want) != entry.Bytes || hashBytes(want) != entry.SHA256 || !bytes.Equal(actual, want) {
					t.Fatal("independent native history bytes differ", input.Project, seq, entry.Family, err)
				}
				tables++
				delete(groups, group)
			}
			if tables != states.Commits[seq].TableCount {
				t.Fatal("independent native history complete table census differs")
			}
			for group := range groups {
				if group.Source == req.Source {
					t.Fatal("extra native history table", input.Project, seq, group.Table)
				}
			}
		}
		t.Logf("b27_independent_native_history project=%s source_store=%s source_commits=%d exact_tables=true", input.Project, input.StoreID, req.Commits)
	}
	if len(evidence.Mapping) != expectedMappings || evidence.Head != uint64(expectedMappings) {
		t.Fatal("complete two-source history mapping census differs")
	}
}

func b27NativeCurrent(t *testing.T, store driver.Store, archive *dumpformat.Archive, inputs []b27NativeInput, report Report) {
	t.Helper()
	ctx := context.Background()
	meta, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	r, err := store.Read(ctx, driver.View{Incarnation: meta.Incarnation, At: &meta.Head})
	if err != nil {
		t.Fatal(err)
	}
	groups, err := queryNativeRowGroups(ctx, r, meta.Incarnation, meta.Head, archive)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	bySource := map[dumpformat.SourceIdentity]b27NativeInput{}
	for _, input := range inputs {
		bySource[dumpformat.SourceIdentity{StoreID: input.StoreID, Project: input.Project}] = input
	}
	seen := map[string]bool{}
	for _, family := range report.NativeFamilies {
		input, ok := bySource[family.Source]
		if !ok {
			t.Fatal("unexpected native source family", family.Source)
		}
		key := input.Project + "/" + family.Family
		want, exists := input.current[family.Family]
		physicalOnly := false
		if !exists {
			want, exists = input.physicalCurrent[family.Family]
			physicalOnly = exists
		}
		if !exists || seen[key] || family.SourceRows != len(want.Rows) || family.ImportedRows != len(want.Rows) || family.ExpiredRows != 0 {
			t.Fatal("native current family census differs", key)
		}
		seen[key] = true
		records, exists := groups[nativeRowGroup{Source: family.Source, Table: family.Family, History: family.History}]
		if !exists {
			t.Fatal("native current table absent", key)
		}
		if family.Family == "local_metadata" {
			// Preserve original opaque commitments and family membership.
			// Business comparisons/reporting expose only key names/counts,
			// never bookkeeping values or a cause analysis.
			if len(records) != family.ImportedRows+1 {
				t.Fatal("machine-local metadata census differs", key)
			}
			keys := make([]string, 0, len(want.Rows))
			for _, row := range want.Rows {
				name, ok := row["key"].(string)
				if !ok {
					t.Fatal("machine-local metadata key is not text")
				}
				keys = append(keys, name)
			}
			sort.Strings(keys)
			t.Logf("b27_machine_local_metadata project=%s rows=%d key_names=%q business_comparison=excluded", input.Project, len(keys), keys)
			continue
		}
		got, _, err := dumpformat.ReconstructNativeRowTable(family.Source, family.Family, family.History, records, nil)
		if err != nil {
			t.Fatal("native current reconstruction", key, err)
		}
		b27NativeTableEqual(t, key, got, want)
		if physicalOnly {
			t.Logf("b27_physical_only_current project=%s family=%s rows=%d sql_oracle=excluded", input.Project, family.Family, len(want.Rows))
		}
	}
	for _, input := range inputs {
		for family := range input.current {
			if !seen[input.Project+"/"+family] {
				t.Fatal("native source family omitted", input.Project, family)
			}
		}
		for family := range input.physicalCurrent {
			if _, sqlFamily := input.current[family]; sqlFamily {
				continue
			}
			if !seen[input.Project+"/"+family] {
				t.Fatal("physical-only native source family omitted", input.Project, family)
			}
		}
	}
}

func b27NativeDocuments(t *testing.T, store driver.Store, archive *dumpformat.Archive, inputs []b27NativeInput) {
	t.Helper()
	rows := b27NativeRows(t, store, nil)
	b27NativeDocumentRows(t, rows, archive, inputs)
}

func b27NativeDocumentRows(t *testing.T, rows map[string]driver.Record, archive *dumpformat.Archive, inputs []b27NativeInput) {
	t.Helper()
	wantIDs := map[string]bool{}
	for _, input := range inputs {
		for _, source := range input.current["issues"].Rows {
			id, ok := source["id"].(string)
			if !ok || id == "" || wantIDs[id] {
				t.Fatal("independent native issue identity repeated")
			}
			wantIDs[id] = true
			row, exists := rows[id]
			if !exists || row.Kind != "bead" {
				t.Fatal("imported native bead missing", id)
			}
			doc, err := pipeline.DecodeDocument(row.Body)
			if err != nil {
				t.Fatal(id, err)
			}
			// An admitted map_type is an explicit target disposition. The
			// untouched raw native type is separately checked below in the
			// complete independently observed source_fields object.
			sourceType := fmt.Sprint(source["issue_type"])
			// Native task/bug/etc. are the admitted issue projection;
			// the modern native gate has its own installed target type.
			wantType := "issue"
			if sourceType == "gate" {
				wantType = "gate"
			}
			mapped := false
			for _, directive := range archive.Manifest().Ontology {
				if directive.Kind == "map_type" && directive.Target.Project == input.Project && directive.From == sourceType && (directive.Target.ID == "" || directive.Target.ID == id) {
					if mapped || directive.Target.Type != sourceType || directive.To == "" {
						t.Fatal("ambiguous admitted native type mapping", input.Project, wantType)
					}
					wantType, mapped = directive.To, true
				}
			}
			b27NativeEqual(t, id+"/identity", []string{doc.ID, doc.Project, doc.Type}, []string{id, input.Project, wantType})
			for _, field := range []string{"title", "description", "priority", "status"} {
				b27NativeEqual(t, id+"/"+field, doc.Fields[field], source[field])
			}
			var preserved map[string]any
			b27NativeJSON(t, doc.Metadata["beads.source_fields"], &preserved)
			// census.md: issues.row_lock is transient and is_blocked is
			// rebuilt. Their original physical cells remain checked by the
			// independent native-table oracle, outside the live bead body.
			admitted := make(map[string]any, len(source))
			for key, value := range source {
				if key != "row_lock" && key != "is_blocked" {
					admitted[key] = value
				}
			}
			b27NativeEqual(t, id+"/admitted-original-fields", preserved, admitted)
			b27NativeSpecificFields(t, input, source, doc, false, 0)
			if raw := source["metadata"]; raw != nil {
				var expected map[string]any
				if text, ok := raw.(string); ok {
					b27NativeJSON(t, []byte(text), &expected)
				} else {
					encoded, err := json.Marshal(raw)
					if err != nil {
						t.Fatal(err)
					}
					b27NativeJSON(t, encoded, &expected)
				}
				for key, value := range expected {
					var actual any
					b27NativeJSON(t, doc.Metadata[key], &actual)
					b27NativeEqual(t, id+"/metadata/"+key, actual, value)
				}
			}
			labels := []string{}
			for _, label := range input.current["labels"].Rows {
				if label["issue_id"] == id {
					labels = append(labels, label["label"].(string))
				}
			}
			sort.Strings(labels)
			b27NativeEqual(t, id+"/labels", append([]string{}, doc.Labels...), labels)
			var parent string
			links := map[string]string{}
			for _, edge := range input.current["dependencies"].Rows {
				if edge["issue_id"] != id {
					continue
				}
				kind := fmt.Sprint(edge["type"])
				_, target := b27NativeDependencyTarget(t, edge)
				if kind == "parent-child" {
					if parent != "" {
						t.Fatal("source has repeated parent", id)
					}
					parent = target
					continue
				}
				if kind == "related" {
					kind = "relates-to"
				}
				links[kind+"\x00"+target] = target
			}
			if parent == "" {
				if doc.Parent != nil {
					t.Fatal("unexpected native parent", id)
				}
			} else if doc.Parent == nil || !doc.Parent.IsID() || doc.Parent.ID != parent || !doc.Parent.Branch.IsZero() {
				t.Fatal("native parent differs", id, parent, doc.Parent)
			}
			if len(doc.Links) != len(links) {
				t.Fatal("native link census differs", id)
			}
			for _, link := range doc.Links {
				key := link.Kind + "\x00" + link.To.ID
				if _, ok := links[key]; !ok || !link.To.IsID() || !link.To.Branch.IsZero() {
					t.Fatal("native link target differs", id, link)
				}
				delete(links, key)
			}
		}
	}
	for id, row := range rows {
		if row.Kind == "bead" && !wantIDs[id] {
			t.Fatal("unexpected imported bead", id)
		}
	}
}

// Each delta source uses the installed single-source receiver. In particular,
// two delta directories are never relabeled as one multi-project input. The
// shared oracle checks exact independent SQL table/state bytes at import and
// reopen for every mode; the callback checks the live B27 projection as well.
func TestB27RichNativeDeltaModes(t *testing.T) {
	for _, input := range b27NativeInputs(t) {
		t.Run(input.Project, func(t *testing.T) {
			for _, mode := range []string{"full", "partial", "snapshot"} {
				t.Run(mode, func(t *testing.T) {
					for _, engine := range []string{"memory", "sqlite", "postgres"} {
						t.Run(engine, func(t *testing.T) {
							req := b27NativeRequest(t, input, mode)
							name := strings.NewReplacer("/", "_", ":", "_", " ", "_").Replace(t.Name())
							root := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR")
							if root == "" {
								t.Fatal("canonical Bazel output directory required")
							}
							raw, err := json.Marshal(req)
							if err != nil {
								t.Fatal(err)
							}
							request := filepath.Join(root, name+"-request.json")
							if err := os.WriteFile(request, raw, 0600); err != nil {
								t.Fatal(err)
							}
							self, err := os.Executable()
							if err != nil {
								t.Fatal(err)
							}
							t.Setenv("PLUG_PUBLIC_PROOF_DRIVER", engine)
							t.Setenv("PLUG_PUBLIC_PROOF_ELF_SHA256", publicProofHashFile(t, self))
							t.Setenv("PLUG_PUBLIC_PROOF_REQUEST", request)
							t.Setenv("PLUG_PUBLIC_PROOF_REQUEST_SHA256", hashBytes(raw))
							t.Setenv("PLUG_PUBLIC_PROOF_OUTPUT", filepath.Join(root, name))
							var conserved b27NativeConservation
							runPublicNativeExternalCaptureWithPlan(t, b27NativeIntent, func(t *testing.T, ctx context.Context, store driver.Store, lease driver.ActiveLease, profile driver.RestoreImportProfile, report Report) error {
								if profile.HistoryMode != driver.RestoreHistoryMode(mode) {
									t.Fatal("B27 selected mode changed")
								}
								b27NativeDocuments(t, store, report.Archive, []b27NativeInput{input})
								b27NativeAuxiliaryReadback(t, store, report.Archive, input, report)
								b27NativeCatalogReadback(t, store, report.Archive, input)
								conserved = b27NativeNonemptyRefusal(t, ctx, store, lease, profile, report.Archive, req.Capture)
								t.Logf("b27_existing_rows_conserved project=%s phase=after-nonempty-refusal rows=%d", input.Project, len(conserved.Rows))
								return nil
							}, func(t *testing.T, _ context.Context, store driver.Store, _ driver.ActiveLease, _ driver.RestoreImportProfile, report Report) error {
								if conserved.Rows == nil || !reflect.DeepEqual(conserved, b27NativeConservationAt(t, store)) {
									t.Fatal("reopen after nonempty refusal changed existing durable rows")
								}
								b27NativeDocuments(t, store, report.Archive, []b27NativeInput{input})
								b27NativeAuxiliaryReadback(t, store, report.Archive, input, report)
								b27NativeCatalogReadback(t, store, report.Archive, input)
								t.Logf("b27_existing_rows_conserved project=%s phase=after-shared-reopen rows=%d", input.Project, len(conserved.Rows))
								return nil
							})
						})
					}
				})
			}
		})
	}
}

func TestB27RichNativeTwoProjectFull(t *testing.T) {
	inputs := b27NativeInputs(t)
	archives := make([]*dumpformat.Archive, 0, len(inputs))
	for _, input := range inputs {
		a, err := dumpformat.Read(bytes.NewReader(b27NativeBytes(t, filepath.Join(input.dir, "legacy", "capture.tar"))))
		if err != nil {
			t.Fatal("actual full native source", input.Project, err)
		}
		defer a.ReleaseNativeDecoded()
		if a.Manifest().StoreID != input.StoreID || !a.HasNativeSourceHistories() {
			t.Fatal("portable native source identity/history differs", input.Project)
		}
		b27NativeEqual(t, "original explicit project/profile", a.Manifest().Projects, input.Projects)
		archives = append(archives, a)
	}
	data, err := dumpformat.MergeArchives(archives...)
	if err != nil {
		t.Fatal("actual two-project native merge", err)
	}
	archive, err := dumpformat.Read(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	defer archive.ReleaseNativeDecoded()
	if len(archive.Manifest().Projects) != 2 {
		t.Fatal("merged native project census differs")
	}
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			ctx := context.Background()
			store, lease, reopen := openIdentityTarget(t, engine)
			plan := b27NativePlan(t, archive, store, lease)
			profile := driver.RestoreImportProfile{Admission: driver.RestoreImportAdmission{Mode: "authenticated", Actor: model.PrincipalRef{Kind: "user", ID: "restore-admin"}, Authority: "restore-admin", AdmissionDigest: strings.Repeat("a", 64)}, SourceStoreID: driver.StoreID(archive.Manifest().StoreID), Method: "offline.restore.import", IdempotencyKey: strings.Repeat("b", 32), ArchiveDigest: archive.SHA256(), HistoryMode: driver.RestoreHistoryModeFull, HistoryTime: time.Date(2026, 11, 11, 17, 0, 0, 0, time.UTC).Format(time.RFC3339Nano)}
			profile, err := BindSourceImport(ctx, bytes.NewReader(data), profile, plan)
			if err != nil {
				t.Fatal("bind original native history", err)
			}
			report, err := RestoreImportWithLease(ctx, bytes.NewReader(data), store, lease, profile)
			if err != nil {
				t.Fatal("restore original native history", err)
			}
			if report.Archive != nil {
				defer report.Archive.ReleaseNativeDecoded()
			}
			b27NativeCurrent(t, store, archive, inputs, report)
			b27NativeDocuments(t, store, archive, inputs)
			b27NativeFullHistory(t, store, archive, inputs, report)
			store, lease = reopen()
			_ = lease
			_, recovered, found, err := RecoverBoundSourceImport(ctx, store)
			if err != nil || !found || !reflect.DeepEqual(recovered.Receipt, report.Receipt) || !reflect.DeepEqual(recovered.NativeFamilies, report.NativeFamilies) {
				t.Fatal("native reopened receipt/family binding", err)
			}
			if recovered.Archive != nil {
				defer recovered.Archive.ReleaseNativeDecoded()
			}
			b27NativeCurrent(t, store, archive, inputs, recovered)
			b27NativeDocuments(t, store, archive, inputs)
			b27NativeFullHistory(t, store, archive, inputs, recovered)
		})
	}
}
