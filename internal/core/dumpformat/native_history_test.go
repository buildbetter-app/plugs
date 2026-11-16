package dumpformat

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"
)

// The complete fixture is declared synthetic, mechanically derived from the
// existing public synthetic head. It is not output from a native exporter.
func nativeSyntheticFixture(t *testing.T, project string) []byte {
	t.Helper()
	data, err := os.ReadFile("testdata/native-tables-synthetic.tar")
	if err != nil {
		t.Fatal(err)
	}
	members := sourcePlaneReaderMembers(t, data)
	var rename func(any) any
	rename = func(value any) any {
		switch value := value.(type) {
		case string:
			if value == "demo" {
				return project
			}
			return strings.ReplaceAll(value, "demo-", project+"-")
		case map[string]any:
			result := map[string]any{}
			for key, item := range value {
				result[rename(key).(string)] = rename(item)
			}
			return result
		case []any:
			for i := range value {
				value[i] = rename(value[i])
			}
		}
		return value
	}
	for name, raw := range members {
		if strings.HasPrefix(name, "records/") {
			records := []Record{}
			for _, line := range bytes.Split(bytes.TrimSpace(raw), []byte{'\n'}) {
				var object map[string]any
				if err := decodeJSON(line, &object); err != nil {
					t.Fatal(err)
				}
				var record Record
				if err := decodeJSON(fixtureJSON(t, rename(object)), &record); err != nil {
					t.Fatal(err)
				}
				if record.Kind == "beads-archive" {
					record.ID = record.Source.Table + "/" + digest(fixtureJSON(t, record.Source.Fields))
				}
				records = append(records, record)
			}
			sort.Slice(records, func(i, j int) bool {
				return compareSourceRecords(records[i], records[j], SourcePlaneAddressContract) < 0
			})
			members[name] = nil
			for _, record := range records {
				members[name] = append(members[name], fixtureJSON(t, record)...)
			}
		} else {
			var object any
			if err := decodeJSON(raw, &object); err != nil {
				t.Fatal(err)
			}
			members[name] = fixtureJSON(t, rename(object))
		}
	}
	return nativeFixtureMembers(t, members)
}

func TestNativeHistoryMergePreservesOriginalSourcesAndRawBytes(t *testing.T) {
	left, right := nativeSyntheticFixture(t, "pl"), nativeSyntheticFixture(t, "qa")
	// One source really retains this family at its head; the other deliberately
	// does not. Their per-source classifications must never be coalesced.
	members := sourcePlaneReaderMembers(t, right)
	var h NativeHistory
	if err := decodeJSON(members[NativeHistoryMember], &h); err != nil {
		t.Fatal(err)
	}
	h.Images[1].Tables["local_metadata"] = h.Current["local_metadata"]
	members[NativeHistoryMember] = fixtureJSON(t, h)
	var m Manifest
	if err := decodeJSON(members["manifest.json"], &m); err != nil {
		t.Fatal(err)
	}
	for i := range m.Sources {
		if m.Sources[i].Family == "local_metadata" {
			m.Sources[i].History = FamilyVersioned
		}
	}
	members["manifest.json"] = fixtureJSON(t, m)
	right = nativeFixtureMembers(t, members)
	inputs := []*Archive{readMergeArchive(t, left), readMergeArchive(t, right)}
	data, err := MergeArchives(inputs...)
	if err != nil {
		t.Fatal(err)
	}
	a := readMergeArchive(t, data)
	if a.Manifest().SourceIdentityContract != SourceIdentityContract || a.Manifest().SourceFamilyContract != SourceFamilyContract || a.Manifest().History != "available-unverified" {
		t.Fatal("native source namespace or portable state changed")
	}
	if _, present := a.Member(NativeHistoryMember); present {
		t.Fatal("merged native member fabricated an aggregate wire wrapper")
	}
	counts := map[SourceIdentity]int{}
	for _, kind := range knownKindNames() {
		for _, r := range a.Records(kind) {
			key, ok := r.SourceKey()
			if !ok || key.StoreID != "synthetic-store" {
				t.Fatal("original native record lacks its exact source tuple")
			}
			counts[*r.Source.Identity]++
		}
	}
	for _, input := range inputs {
		project := input.Manifest().Projects[0].Prefix
		raw, _ := input.Member(NativeHistoryMember)
		retained, _ := a.Member(nativeHistoryPath(project))
		if !bytes.Equal(raw, retained) {
			t.Fatal("native member was remarshaled or lost")
		}
		want := 0
		for _, kind := range knownKindNames() {
			want += len(input.Records(kind))
		}
		if counts[SourceIdentity{StoreID: "synthetic-store", Project: project}] != want {
			t.Fatal("native head/global rows deduplicated across sources")
		}
	}
	for _, h := range a.NativeSourceHistories() {
		if len(h.Images) != 2 || h.Images[0].CommitID != strings.Repeat("a", 32) || h.Images[1].CommitID != strings.Repeat("b", 32) {
			t.Fatal("colliding original revisions were rewritten or dropped")
		}
		for _, source := range h.Sources {
			if source.Identity == nil || source.Identity.StoreID != h.StoreID || source.Identity.Project != h.Projects[0] {
				t.Fatal("original per-source family census lost ownership")
			}
			if source.Family == "local_metadata" && (h.Projects[0] == "qa") != (source.History == FamilyVersioned) {
				t.Fatal("different family designations collapsed across sources")
			}
		}
	}
	if len(a.Records("comment")) != 2 || len(a.Records("edge")) != 2 || a.Records("edge")[0].ID != "1" || a.Records("comment")[0].ID != "1" {
		t.Fatal("equal raw IDs across source/kind changed")
	}
	for _, readers := range [][]io.Reader{{bytes.NewReader(right), bytes.NewReader(left)}} {
		again, err := Merge(readers...)
		if err != nil || !bytes.Equal(data, again) {
			t.Fatalf("native merge is not deterministic: %v", err)
		}
	}
}

func TestNativeHistoryNestedMergeNeverDeduplicatesSourceRows(t *testing.T) {
	first, err := Merge(bytes.NewReader(nativeSyntheticFixture(t, "pl")), bytes.NewReader(nativeSyntheticFixture(t, "qa")))
	if err != nil {
		t.Fatal(err)
	}
	data, err := Merge(bytes.NewReader(first), bytes.NewReader(nativeSyntheticFixture(t, "zz")))
	if err != nil {
		t.Fatal(err)
	}
	a := readMergeArchive(t, data)
	if len(a.NativeSourceHistories()) != 3 || len(a.SourceIdentities()) != 3 || len(a.Records("memory")) != 3 {
		t.Fatal("nested merge deduplicated original content")
	}
	for _, h := range a.NativeSourceHistories() {
		if h.StoreID != "synthetic-store" || len(h.Current["leases"].Rows) != 2 || len(h.Images[0].Tables["retired_family"].Rows) != 2 {
			t.Fatal("nested merge substituted aggregate store or lost SQL rows")
		}
		h.Sources[0].Identity.Project = "mutated"
	}
	for _, h := range a.NativeSourceHistories() {
		if h.Sources[0].Identity.Project != h.Projects[0] {
			t.Fatal("detached disposition identity aliases archive")
		}
	}
	portable, _ := a.SourceHistory()
	if len(portable.ProjectHistories) != 3 {
		t.Fatal("nested merge lost portable source evidence")
	}
	for _, p := range portable.ProjectHistories {
		if p.Range.Status != "unverified" || p.Range.Complete || len(p.Range.Frames) != 0 || p.SourceStoreID != "synthetic-store" {
			t.Fatal("native completeness promoted literal portable status")
		}
	}
}

func TestNativeHistoryRefusesExactNamedGapAndOverlappingProject(t *testing.T) {
	complete := nativeSyntheticFixture(t, "pl")
	gapData := editNativeFixture(t, complete, func(h *NativeHistory) {
		h.Complete = false
		h.Gaps = []NativeHistoryGap{{Sequence: 1, CommitID: h.Commits[0].ID, Family: "events", Reason: "retained-source-image-not-readable"}}
		h.Images = h.Images[1:]
	})
	a := readMergeArchive(t, gapData)
	if a.NativeSourceHistories()[0].Complete {
		t.Fatal("named native gap became complete")
	}
	data, err := MergeArchives(a)
	var gap IncompleteHistoryError
	if data != nil || !errors.As(err, &gap) || gap.StoreID != "synthetic-store" || gap.Project != "pl" || gap.First != 1 || gap.CommitID != strings.Repeat("a", 32) || gap.Family != "events" || gap.Reason != "retained-source-image-not-readable" {
		t.Fatalf("native gap refusal changed evidence: %v", err)
	}
	data, err = Merge(bytes.NewReader(complete), bytes.NewReader(complete))
	var overlap HistoryMergeRefusal
	if data != nil || !errors.As(err, &overlap) || overlap.Kind != "project" || overlap.Project != "pl" {
		t.Fatalf("native source overlap was not atomic and typed: %v", err)
	}
	legacy := newMergeFixture(t, "qa").bytes(t, nil)
	data, err = Merge(bytes.NewReader(complete), bytes.NewReader(legacy))
	if data != nil || !errors.As(err, &overlap) {
		t.Fatalf("mixed explicit native/legacy route accepted: %v", err)
	}
}

func TestNativeHistoryReaderRejectsMalformedEvidence(t *testing.T) {
	baseline := nativeSyntheticFixture(t, "pl")
	readMergeArchive(t, baseline)
	tests := []struct {
		name string
		edit func(*NativeHistory)
	}{
		{"source-store", func(h *NativeHistory) { h.StoreID = "other-store" }},
		{"source-project", func(h *NativeHistory) { h.Projects[0] = "qa" }},
		{"image-coverage", func(h *NativeHistory) { h.Images = h.Images[1:] }},
		{"image-sequence", func(h *NativeHistory) { h.Images[0].Sequence = 2 }},
		{"unknown-commit", func(h *NativeHistory) { h.Images[0].CommitID = strings.Repeat("c", 32) }},
		{"commit-cycle", func(h *NativeHistory) { h.Commits[0].Parents = []string{h.Head} }},
		{"missing-parent", func(h *NativeHistory) { h.Commits[0].Parents = []string{strings.Repeat("c", 32)} }},
		{"duplicate-commit", func(h *NativeHistory) { h.Commits[1].ID = h.Commits[0].ID }},
		{"current-census", func(h *NativeHistory) { delete(h.Current, "config") }},
		{"current-schema", func(h *NativeHistory) { h.Current["config"].Columns[0] = "not_key" }},
		{"current-exact-bind", func(h *NativeHistory) { h.Current["events"].Rows[0]["actor"] = "invented" }},
		{"versioned-head-missing", func(h *NativeHistory) { delete(h.Images[1].Tables, "issues") }},
		{"versioned-head-differs", func(h *NativeHistory) { h.Images[1].Tables["issues"].Rows[0]["title"] = "invented" }},
		{"old-row-schema", func(h *NativeHistory) { delete(h.Images[0].Tables["events"].Rows[0], "id") }},
		{"old-column-order", func(h *NativeHistory) { h.Images[0].Tables["events"].Columns[0] = "z" }},
		{"ignore-no-designation", func(h *NativeHistory) { h.Ignore.Rows = []Row{} }},
		{"false-complete", func(h *NativeHistory) { h.Complete = false }},
		{"census-metadata", func(h *NativeHistory) { h.Commits[0].Metadata["message"] = "contradicted" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := Read(bytes.NewReader(editNativeFixture(t, baseline, test.edit))); err == nil {
				t.Fatal("malformed native evidence accepted")
			}
		})
	}
	for _, test := range []struct {
		name string
		edit func(map[string]any)
	}{
		{"missing-complete", func(h map[string]any) { delete(h, "complete") }},
		{"null-images", func(h map[string]any) { h["images"] = nil }},
		{"case-field", func(h map[string]any) { h["Complete"] = true }},
		{"unknown-field", func(h map[string]any) { h["sources"] = []any{} }},
	} {
		t.Run(test.name, func(t *testing.T) {
			members := sourcePlaneReaderMembers(t, baseline)
			var object map[string]any
			if err := decodeJSON(members[NativeHistoryMember], &object); err != nil {
				t.Fatal(err)
			}
			test.edit(object)
			members[NativeHistoryMember] = fixtureJSON(t, object)
			if _, err := Read(bytes.NewReader(nativeFixtureMembers(t, members))); err == nil {
				t.Fatal("noncanonical native shape accepted")
			}
		})
	}
}

func TestNativeHistoryFamilyMarkerRequiresExactEvidence(t *testing.T) {
	baseline := nativeSyntheticFixture(t, "pl")
	for _, key := range []string{"source_family_contract", "SOURCE_FAMILY_CONTRACT", "Source_Family_Contract"} {
		for _, value := range []any{nil, "", "beads-source-families/2", false, SourceFamilyContract} {
			members := sourcePlaneReaderMembers(t, baseline)
			var manifest map[string]any
			if err := decodeJSON(members["manifest.json"], &manifest); err != nil {
				t.Fatal(err)
			}
			manifest[key] = value
			members["manifest.json"] = fixtureJSON(t, manifest)
			_, err := Read(bytes.NewReader(fixtureTar(t, members)))
			if key == "source_family_contract" && value == SourceFamilyContract {
				if err != nil {
					t.Fatal("exact native marker refused", err)
				}
			} else if err == nil {
				t.Fatal("noncanonical source family marker accepted")
			}
		}
	}
	for _, change := range []func(map[string][]byte){
		func(m map[string][]byte) { delete(m, NativeHistoryMember) },
		func(m map[string][]byte) { delete(m, SourceHistoryMember) },
	} {
		members := sourcePlaneReaderMembers(t, baseline)
		change(members)
		if _, err := Read(bytes.NewReader(nativeFixtureMembers(t, members))); err == nil {
			t.Fatal("native marker accepted without its source evidence")
		}
	}
}

func TestNativeHistoryCommitCensusMatchesEquivalentUnverifiedGraph(t *testing.T) {
	members := sourcePlaneReaderMembers(t, nativeSyntheticFixture(t, "pl"))
	var h NativeHistory
	if err := decodeJSON(members[NativeHistoryMember], &h); err != nil {
		t.Fatal(err)
	}
	root, branch, other, head := strings.Repeat("a", 32), strings.Repeat("b", 32), strings.Repeat("c", 32), strings.Repeat("d", 32)
	h.Commits = []SourceCommit{
		{ID: root, Parents: []string{}, Metadata: Row{"message": "root"}},
		{ID: branch, Parents: []string{root}, Metadata: Row{"message": "branch"}},
		{ID: other, Parents: []string{root}, Metadata: Row{"message": "other"}},
		{ID: head, Parents: []string{other, branch}, Metadata: Row{"message": "merge"}},
	}
	old, current := h.Images[0].Tables, h.Images[1].Tables
	h.Head, h.Images = head, []NativeHistoryImage{}
	for i, commit := range h.Commits {
		tables := old
		if commit.ID == head {
			tables = current
		}
		h.Images = append(h.Images, NativeHistoryImage{Sequence: uint64(i + 1), CommitID: commit.ID, Tables: tables})
	}
	members[NativeHistoryMember] = fixtureJSON(t, h)
	var portable SourceHistory
	if err := decodeJSON(members[SourceHistoryMember], &portable); err != nil {
		t.Fatal(err)
	}
	portable.Commits = []SourceCommit{h.Commits[3], h.Commits[2], h.Commits[0], h.Commits[1]}
	portable.Commits[0].Parents = []string{branch, other}
	members[SourceHistoryMember] = fixtureJSON(t, portable)
	a := readMergeArchive(t, nativeFixtureMembers(t, members))
	if len(a.NativeSourceHistories()[0].Images) != 4 {
		t.Fatal("merge ancestry images were lost")
	}
	// Keep a coherent but truncated native graph. The independently retained
	// portable census still contradicts it, so completeness must refuse.
	h.Commits, h.Head, h.Images = h.Commits[:1], root, []NativeHistoryImage{{Sequence: 1, CommitID: root, Tables: current}}
	members[NativeHistoryMember] = fixtureJSON(t, h)
	_, err := Read(bytes.NewReader(nativeFixtureMembers(t, members)))
	var gap IncompleteHistoryError
	if !errors.As(err, &gap) || gap.Reason != "native-portable-commit-census-mismatch" {
		t.Fatalf("native-only truncated graph accepted or misreported: %v", err)
	}
}

func TestNativeHistoryStoredSettingValidationIncludesHistoricalColumns(t *testing.T) {
	baseline := nativeSyntheticFixture(t, "pl")
	for _, test := range []struct {
		name  string
		key   string
		value any
		extra any
		pass  bool
	}{
		{"public-setting", "old.setting", "ordinary", nil, true},
		{"secret-reference", "old.api_key", map[string]any{"$secret": "old/config"}, nil, true},
		{"secret-plaintext", "old.api_key", "synthetic-plaintext", nil, false},
		{"nested-secret", "old.setting", map[string]any{"api_key": "synthetic-plaintext"}, nil, false},
		{"extra-historical-column", "old.setting", "ordinary", "synthetic-plaintext", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			data := editNativeFixture(t, baseline, func(h *NativeHistory) {
				table := h.Images[0].Tables["config"]
				table.Rows[0]["key"], table.Rows[0]["value"] = test.key, test.value
				if test.extra != nil {
					table.Columns = append(table.Columns, "secret")
					sort.Strings(table.Columns)
					table.Rows[0]["secret"] = test.extra
				}
				h.Images[0].Tables["config"] = table
			})
			_, err := Read(bytes.NewReader(data))
			if (err == nil) != test.pass {
				t.Fatalf("historical stored-setting check=%v, pass=%v", err, test.pass)
			}
		})
	}
}

func TestActualNativeFamilyProducerRetainsNamedIncompleteEvidence(t *testing.T) {
	// Public producer e6156c3b output, hash-bound in the source packet. This
	// diagnostic input is incomplete and cannot earn native/full-history credit.
	data, err := os.ReadFile("testdata/producer-e615-reprocody-gap.tar")
	if err != nil {
		t.Fatal(err)
	}
	a := readMergeArchive(t, data)
	h := a.NativeSourceHistories()
	if len(h) != 1 || h[0].Complete || len(h[0].Commits) != 87 || len(h[0].Images) != 82 || len(h[0].Gaps) != 5 || h[0].Gaps[0].Sequence != 42 || h[0].Gaps[0].CommitID != "rqqqfvrt2vjipj5sqd9fc23qcmium07c" {
		t.Fatal("actual producer gap evidence changed")
	}
	portable, _ := a.SourceHistory()
	if portable.Range.Status != "unverified" || portable.Range.Complete || len(portable.Range.Frames) != 0 {
		t.Fatal("actual producer portable state promoted")
	}
	// SQL observations are clean even though config dispositions count two keys.
	// The actual named history gap must remain the refusal.
	merged, err := MergeArchives(a)
	var gap IncompleteHistoryError
	if merged != nil || !errors.As(err, &gap) || gap.First != 42 || gap.Last != 42 || gap.CommitID != h[0].Gaps[0].CommitID || gap.Reason != h[0].Gaps[0].Reason || gap.Family != h[0].Gaps[0].Family {
		t.Fatalf("actual named incomplete-history refusal changed: %v", err)
	}
}

func nativeFixtureMembers(t *testing.T, members map[string][]byte) []byte {
	t.Helper()
	var manifest Manifest
	if err := decodeJSON(members["manifest.json"], &manifest); err != nil {
		t.Fatal(err)
	}
	manifest.Files = map[string]File{}
	for name, raw := range members {
		if name != "manifest.json" {
			manifest.Files[name] = File{Size: int64(len(raw)), SHA256: digest(raw)}
		}
	}
	members["manifest.json"] = fixtureJSON(t, manifest)
	return fixtureTar(t, members)
}

func editNativeFixture(t *testing.T, data []byte, edit func(*NativeHistory)) []byte {
	t.Helper()
	members := sourcePlaneReaderMembers(t, data)
	var h NativeHistory
	if err := decodeJSON(members[NativeHistoryMember], &h); err != nil {
		t.Fatal(err)
	}
	edit(&h)
	members[NativeHistoryMember] = fixtureJSON(t, h)
	return nativeFixtureMembers(t, members)
}

func TestNativeHistoryRetainsUnversionedCurrentAndHistoricalSchemas(t *testing.T) {
	a := readMergeArchive(t, nativeSyntheticFixture(t, "pl"))
	histories := a.NativeSourceHistories()
	if len(histories) != 1 {
		t.Fatal("original native source missing")
	}
	h := histories[0]
	if !h.Complete || len(h.Gaps) != 0 || len(h.Images) != 2 || len(h.Current) != 33 || len(h.Commits) != 2 || h.StoreID != "synthetic-store" || !reflect.DeepEqual(h.Projects, []string{"pl"}) {
		t.Fatal("native raw census changed")
	}
	if len(h.Current["events"].Rows) != 1 || len(h.Current["leases"].Rows) != 2 || len(h.Current["repo_mtimes"].Rows) != 1 || len(h.Current["local_metadata"].Rows) != 1 {
		t.Fatal("current audit/lease/local rows filtered or lost before import-time policy")
	}
	for _, source := range h.Sources {
		if nativeUnversioned[source.Family] && (source.History != FamilyUnversioned || !strings.Contains(source.Reason, "unversioned in Beads by design")) {
			t.Fatal("explicit unversioned designation lost")
		}
	}
	old := h.Images[0].Tables
	if !reflect.DeepEqual(old["events"].Columns, []string{"id", "legacy_payload"}) || old["events"].Rows[0]["legacy_payload"] != "pre-0062 event; old schema only" || old["config"].Rows[0]["value"] != "old-only-setting" {
		t.Fatal("historical schema/catalog was replaced with current fields")
	}
	if _, exists := h.Images[1].Tables["events"]; exists || len(old["retired_family"].Rows) != 2 {
		t.Fatal("unversioned current audit was grafted onto history or duplicate old SQL rows collapsed")
	}
	cell := old["retired_family"].Rows[0]
	if cell["amount"] != json.Number("9007199254740993.25") || cell["nullable"] != nil || cell["blob"].(map[string]any)["$binary"] != "AAEC" {
		t.Fatal("SQL decimal/null/binary cell changed")
	}
	portable, ok := a.SourceHistory()
	if !ok || portable.Range.Status != "unverified" || portable.Range.Complete || len(portable.Range.Frames) != 0 || a.Manifest().History != "available-unverified" {
		t.Fatal("raw completeness promoted portable replay")
	}
	if _, err := a.HistoryImage(1); err == nil {
		t.Fatal("raw old schema was reinterpreted as a portable body")
	}
	h.Current["events"].Rows[0]["actor"] = "mutated"
	h.Images[0].Tables["events"].Columns[0] = "mutated"
	h.Commits[0].Metadata["message"] = "mutated"
	h.Sources[0].Reason = "mutated"
	again := a.NativeSourceHistories()[0]
	if again.Current["events"].Rows[0]["actor"] == "mutated" || again.Images[0].Tables["events"].Columns[0] == "mutated" || again.Commits[0].Metadata["message"] == "mutated" || again.Sources[0].Reason == "mutated" {
		t.Fatal("detached native accessor aliases validated archive")
	}
}
