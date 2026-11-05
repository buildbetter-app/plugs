package converterroundtrip

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// projectHistoryArchive is a detached, real plug-dump/1 fixture derived from
// the checked-in complete source export. It keeps the source-shaped history,
// events and archive rows, but gives each copy a distinct project namespace
// and removes the global config table so two different issue prefixes cannot
// alias one target record. No private store or producer archive is touched.
func projectHistoryArchive(t *testing.T, prefix string, ordinal int) []byte {
	t.Helper()
	base, _ := publicSourceFixture(t, true)
	members := tarMembers(t, base)

	var manifest map[string]any
	decode(t, members["manifest.json"], &manifest)
	manifest["store_id"] = "history-fixture-" + prefix
	manifest["machine"] = "history-fixture-" + prefix
	projects := manifest["projects"].([]any)
	projects[0].(map[string]any)["prefix"] = prefix
	manifest["projects"] = projects

	// Config rows are global in the legacy source table. Keeping two different
	// issue_prefix values under one config identity would be an unorderable
	// overlap, so the fixture intentionally carries the source config through
	// config/sources.json while excluding it from portable history images.
	filteredKinds := make([]any, 0, len(manifest["kind_registry"].([]any)))
	for _, raw := range manifest["kind_registry"].([]any) {
		kind := raw.(map[string]any)
		if kind["name"] == "config" {
			continue
		}
		filteredKinds = append(filteredKinds, kind)
	}
	manifest["kind_registry"] = filteredKinds
	files := manifest["files"].(map[string]any)
	delete(files, "records/config.ndjson")
	var externalEntries []map[string]any
	decode(t, members["config/sources.json"], &externalEntries)
	externalFamilies := map[string]bool{}
	for _, entry := range externalEntries {
		input, _ := entry["input"].(map[string]any)
		family, _ := input["family"].(string)
		externalFamilies[family] = true
	}
	for _, raw := range manifest["sources"].([]any) {
		source := raw.(map[string]any)
		family, _ := source["family"].(string)
		if !externalFamilies[family] {
			switch family {
			case "issues", "issues.row_lock", "issues.is_blocked":
				source["rows"] = 1
			default:
				source["rows"] = 0
			}
		}
	}

	// First rewrite the raw record/archive bytes. The complete fixture has one
	// bead; replacing the exact quoted values preserves every other source field
	// and relationship spelling.
	oldBead := []byte(`"demo-42v"`)
	newBead := []byte(`"` + prefix + `-42v"`)
	oldPrefix := []byte(`"demo"`)
	newPrefix := []byte(`"` + prefix + `"`)
	for name, raw := range members {
		if name == "manifest.json" || name == dumpformat.SourceHistoryMember {
			continue
		}
		raw = bytes.ReplaceAll(raw, oldBead, newBead)
		raw = bytes.ReplaceAll(raw, oldPrefix, newPrefix)
		members[name] = raw
	}
	delete(members, "records/config.ndjson")

	// The archive rows above are deliberately narrowed to the project bead.
	// Rebind every database disposition to the retained record census instead
	// of leaving the complete fixture's counts for the removed archive tables.
	// This keeps validation strict while making the fixture's lossless scope
	// explicit; excluded issue fields follow the retained issue count.
	retainedCounts := map[string]int{}
	for name, raw := range members {
		if !strings.HasPrefix(name, "records/") || name == "records/config.ndjson" {
			continue
		}
		for _, line := range bytes.Split(raw, []byte{'\n'}) {
			line = bytes.TrimSpace(line)
			if len(line) == 0 {
				continue
			}
			var record dumpformat.Record
			if err := json.Unmarshal(line, &record); err != nil {
				t.Fatalf("retained record census: %s: %v", name, err)
			}
			retainedCounts[record.Source.Table]++
			if record.Source.Table == "issues" {
				if labels, ok := record.Body["labels"].([]any); ok {
					retainedCounts["labels"] += len(labels)
				}
			}
		}
	}
	retainedCounts["issues.row_lock"] = retainedCounts["issues"]
	retainedCounts["issues.is_blocked"] = retainedCounts["issues"]
	for _, raw := range manifest["sources"].([]any) {
		source := raw.(map[string]any)
		family, _ := source["family"].(string)
		if !externalFamilies[family] {
			if rows, ok := retainedCounts[family]; ok {
				source["rows"] = rows
			} else {
				source["rows"] = 0
			}
		}
	}

	var history dumpformat.SourceHistory
	decode(t, members[dumpformat.SourceHistoryMember], &history)
	if len(history.ProjectHistories) != 0 || len(history.Commits) < 2 {
		t.Fatal("complete fixture no longer has a legacy verified history")
	}

	// Make commit and event identities disjoint without changing their shape.
	commitMap := map[string]string{}
	for i := range history.Commits {
		old := history.Commits[i].ID
		newID := strings.Repeat(string("abcdefghijklmnopqrstuv"[ordinal*7+i]), 32)
		commitMap[old] = newID
		history.Commits[i].ID = newID
	}
	for i := range history.Commits {
		if i == 0 {
			history.Commits[i].Parents = []string{}
		} else {
			history.Commits[i].Parents = []string{history.Commits[i-1].ID}
		}
	}

	eventMap := map[string]string{}
	eventIndex := 0
	collectEvent := func(row dumpformat.Row) {
		if old, ok := row["id"].(string); ok && old != "" {
			if _, exists := eventMap[old]; !exists {
				eventMap[old] = fmt.Sprintf("%s-event-%d", prefix, eventIndex)
				eventIndex++
			}
		}
	}
	for _, row := range history.Events {
		collectEvent(row)
	}
	for _, frame := range history.Range.Frames {
		for _, row := range frame.Events {
			collectEvent(row)
		}
	}
	for i := range history.Events {
		rewriteHistoryRow(history.Events[i], prefix, eventMap)
	}
	for i := range history.Range.Frames {
		oldCommit := history.Range.Frames[i].CommitID
		history.Range.Frames[i].CommitID = commitMap[oldCommit]
		for j := range history.Range.Frames[i].Events {
			rewriteHistoryRow(history.Range.Frames[i].Events[j], prefix, eventMap)
		}
		kept := history.Range.Frames[i].Records[:0]
		for _, record := range history.Range.Frames[i].Records {
			if record.Kind != "config" {
				rewriteHistoryRecord(t, &record, prefix)
				kept = append(kept, record)
			}
		}
		history.Range.Frames[i].Records = kept
	}
	for i := range history.Range.Baseline {
		if history.Range.Baseline[i].Kind == "config" {
			history.Range.Baseline = append(history.Range.Baseline[:i], history.Range.Baseline[i+1:]...)
		}
	}

	for name, raw := range members {
		for old, newID := range commitMap {
			raw = bytes.ReplaceAll(raw, []byte(`"`+old+`"`), []byte(`"`+newID+`"`))
		}
		for old, newID := range eventMap {
			raw = bytes.ReplaceAll(raw, []byte(`"`+old+`"`), []byte(`"`+newID+`"`))
		}
		members[name] = raw
	}
	// Archive rows are content-addressed by their source fields. Prefix and
	// event-ID rewrites above therefore require recomputing each changed row's
	// identity; unchanged migration rows retain their exact shared identity and
	// are deduplicated by MergeArchives.
	if raw, ok := members["records/beads-archive.ndjson"]; ok {
		rows := make([]dumpformat.Record, 0)
		for _, line := range bytes.Split(raw, []byte{'\n'}) {
			line = bytes.TrimSpace(line)
			if len(line) == 0 {
				continue
			}
			var record dumpformat.Record
			if err := json.Unmarshal(line, &record); err != nil {
				t.Fatalf("rewrite archive record: %v", err)
			}
			fields, err := json.Marshal(record.Source.Fields)
			if err != nil {
				t.Fatalf("marshal archive source fields: %v", err)
			}
			record.ID = record.Source.Table + "/" + hashBytes(append(fields, '\n'))
			rows = append(rows, record)
		}
		sort.Slice(rows, func(i, j int) bool {
			return rows[i].ID < rows[j].ID
		})
		var output bytes.Buffer
		for _, record := range rows {
			encoded, err := json.Marshal(record)
			if err != nil {
				t.Fatalf("marshal archive record: %v", err)
			}
			output.Write(encoded)
			output.WriteByte('\n')
		}
		members["records/beads-archive.ndjson"] = output.Bytes()
	}
	members[dumpformat.SourceHistoryMember] = encode(t, history)
	for name, raw := range members {
		if name == "manifest.json" {
			continue
		}
		if _, exists := files[name]; exists {
			files[name] = map[string]any{"sha256": hashBytes(raw), "size": len(raw)}
		}
	}
	manifest["files"] = files
	members["manifest.json"] = encode(t, manifest)
	return writeTarMembers(t, members)
}

func rewriteHistoryRecord(t *testing.T, record *dumpformat.Record, project string) {
	t.Helper()
	raw, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	raw = bytes.ReplaceAll(raw, []byte(`"demo-42v"`), []byte(`"`+project+`-42v"`))
	raw = bytes.ReplaceAll(raw, []byte(`"demo"`), []byte(`"`+project+`"`))
	if err := json.Unmarshal(raw, record); err != nil {
		t.Fatal(err)
	}
}

func rewriteHistoryRow(row dumpformat.Row, project string, eventMap map[string]string) {
	if id, ok := row["id"].(string); ok {
		if replacement, exists := eventMap[id]; exists {
			row["id"] = replacement
		}
	}
	if issueID, ok := row["issue_id"].(string); ok && strings.HasPrefix(issueID, "demo-") {
		row["issue_id"] = project + strings.TrimPrefix(issueID, "demo")
	}
}

func tarMembers(t *testing.T, data []byte) map[string][]byte {
	t.Helper()
	result := map[string][]byte{}
	reader := tar.NewReader(bytes.NewReader(data))
	for {
		header, err := reader.Next()
		if err == io.EOF {
			return result
		}
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(reader)
		if err != nil {
			t.Fatal(err)
		}
		result[header.Name] = body
	}
}

func writeTarMembers(t *testing.T, members map[string][]byte) []byte {
	t.Helper()
	var output bytes.Buffer
	writer := tar.NewWriter(&output)
	names := make([]string, 0, len(members))
	for name := range members {
		names = append(names, name)
	}
	sort.Strings(names)
	if index := sort.SearchStrings(names, "manifest.json"); index < len(names) && names[index] == "manifest.json" {
		names = append([]string{"manifest.json"}, append(names[:index], names[index+1:]...)...)
	}
	for _, name := range names {
		body := members[name]
		if err := writer.WriteHeader(&tar.Header{Name: name, Mode: 0600, Size: int64(len(body)), Typeflag: tar.TypeReg, Format: tar.FormatUSTAR}); err != nil {
			t.Fatal(err)
		}
		if _, err := writer.Write(body); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return output.Bytes()
}

func TestMergeAndRestorePreservesDisjointProjectHistoryAcrossDrivers(t *testing.T) {
	leftBytes := projectHistoryArchive(t, "pl", 0)
	rightBytes := projectHistoryArchive(t, "qa", 1)
	left, err := dumpformat.Read(bytes.NewReader(leftBytes))
	if err != nil {
		t.Fatalf("left fixture: %v", err)
	}
	right, err := dumpformat.Read(bytes.NewReader(rightBytes))
	if err != nil {
		t.Fatalf("right fixture: %v", err)
	}
	mergedBytes, err := dumpformat.Merge(bytes.NewReader(leftBytes), bytes.NewReader(rightBytes))
	if err != nil {
		t.Fatalf("merge: %v", err)
	}
	merged, err := dumpformat.Read(bytes.NewReader(mergedBytes))
	if err != nil {
		t.Fatalf("merged archive: %v", err)
	}
	history, ok := merged.SourceHistory()
	if !ok || len(history.ProjectHistories) != 2 || history.ProjectHistories[0].Project != "pl" || history.ProjectHistories[1].Project != "qa" {
		t.Fatalf("merged project histories: ok=%t histories=%+v", ok, history.ProjectHistories)
	}
	for _, project := range []string{"pl", "qa"} {
		image, imageErr := merged.HistoryImageForProject(project, 7)
		if imageErr != nil || len(image.Records("bead")) != 1 || image.Records("bead")[0].Project != project {
			t.Fatalf("%s merged image: %v %#v", project, imageErr, image)
		}
	}
	if _, err := dumpformat.Merge(bytes.NewReader(leftBytes), bytes.NewReader(leftBytes)); err == nil {
		t.Fatal("overlapping project history was accepted")
	}
	leftHistory, _ := left.SourceHistory()
	rightHistory, _ := right.SourceHistory()
	if leftHistory.Range.Head != 7 || rightHistory.Range.Head != 7 {
		t.Fatal("fixture history range changed")
	}

	profile, _ := admittedImportProfile(t, mergedBytes)
	profile.HistoryMode = driver.RestoreHistoryModeFull
	when := time.Date(2026, 11, 5, 17, 30, 0, 0, time.UTC)
	profile, err = BindHistoryImport(context.Background(), bytes.NewReader(mergedBytes), profile, when)
	if err != nil {
		t.Fatalf("bind merged history: %v", err)
	}
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			store, lease, cleanup := openIdentityTarget(t, engine)
			defer cleanup()
			report, restoreErr := RestoreImportWithLease(context.Background(), bytes.NewReader(mergedBytes), store, lease, profile)
			if restoreErr != nil {
				t.Fatalf("restore: %v", restoreErr)
			}
			if report.Restored.StoreID != driver.StoreID(merged.Manifest().StoreID) || !report.Restored.HistoryFloor.IsZero() {
				t.Fatalf("restored metadata: %+v", report.Restored)
			}
			reader, readErr := store.Read(context.Background(), driver.View{Incarnation: report.Restored.Incarnation})
			if readErr != nil {
				t.Fatal(readErr)
			}
			defer reader.Close()
			rows, getErr := reader.Get(context.Background(), []model.Ref{
				model.RefByID("pl-42v", model.Natural64(0)),
				model.RefByID("qa-42v", model.Natural64(0)),
			})
			if getErr != nil || len(rows) != 2 {
				t.Fatalf("per-project final rows=%d err=%v", len(rows), getErr)
			}
			seen := map[string]bool{}
			for _, row := range rows {
				seen[row.Ref.ID] = true
			}
			if !seen["pl-42v"] || !seen["qa-42v"] {
				t.Fatalf("project isolation rows=%v", seen)
			}
			var evidence driver.HistoryImportEvidence
			evidenceRows, evidenceErr := reader.Get(context.Background(), []model.Ref{model.RefByID(driver.OfflineHistoryEvidenceID, model.Natural64(0))})
			if evidenceErr != nil || len(evidenceRows) != 1 || json.Unmarshal(evidenceRows[0].Body, &evidence) != nil {
				t.Fatalf("history evidence: rows=%d err=%v", len(evidenceRows), evidenceErr)
			}
			if len(evidence.Mapping) != 14 {
				t.Fatalf("history mapping count=%d", len(evidence.Mapping))
			}
			counts := map[string]int{}
			for _, mapping := range evidence.Mapping {
				counts[mapping.Project]++
			}
			if !reflect.DeepEqual(counts, map[string]int{"pl": 7, "qa": 7}) {
				t.Fatalf("per-project history mapping=%v", counts)
			}
			t.Logf("HISTORY_MERGE_RESTORE engine=%s archive=%s projects=pl,qa mappings=%d head=%s", engine, merged.SHA256(), len(evidence.Mapping), report.Restored.Head)
		})
	}
}
