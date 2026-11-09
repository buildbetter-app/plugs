package dumpformat

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"
)

// These are declared synthetic, complete histories. The collisions represent
// independent stores, never recordings or native/rehearsal qualification.
func collidingProjectHistory(t *testing.T, project string) []byte {
	t.Helper()
	f := newMergeFixture(t, project)
	f.records["comment"][0].ID = "1"
	f.records["comment"][0].Source.Fields["id"] = "1"
	f.records["edge"][0].ID = "shared-edge"
	f.records["edge"][0].Source.Fields["id"] = "shared-edge"
	f.records["memory"][0].ID = "shared"
	f.records["memory"][0].Body["key"] = "shared"
	f.records["memory"][0].Body["value"] = project
	f.records["memory"][0].Source.Fields["key"] = "kv.memory.shared"
	f.records["memory"][0].Source.Fields["value"] = project
	for i := range f.config {
		f.config[i].Input.Path = "/synthetic/common/" + f.config[i].Input.Family
	}
	event := Row{}
	for _, column := range strings.Fields(tableColumns["events"]) {
		event[column] = nil
	}
	event["id"], event["issue_id"] = json.Number("1"), project+"-1"
	f.addArchive(t, "events", event)
	h := sourceHistoryForFixture(t, f, strings.Repeat("a", 32), strings.Repeat("b", 32), false)
	h.Events = []Row{event}
	for i := range h.Range.Frames {
		h.Range.Frames[i].Events = []Row{event}
	}
	return withSourceHistory(t, f, h)
}

func TestSourceIdentityMergePreservesCollidingProjectHistory(t *testing.T) {
	left, right := collidingProjectHistory(t, "pl"), collidingProjectHistory(t, "qa")
	merged, err := Merge(bytes.NewReader(left), bytes.NewReader(right))
	if err != nil {
		t.Fatal(err)
	}
	a := readMergeArchive(t, merged)
	if a.Manifest().SourceIdentityContract != SourceIdentityContract {
		t.Fatal("merge omitted the explicit source identity contract")
	}
	counts := map[SourceIdentity]int{}
	for _, kind := range knownKindNames() {
		for _, record := range a.Records(kind) {
			if record.Source.Identity == nil {
				t.Fatalf("unscoped %s/%s", kind, record.ID)
			}
			counts[*record.Source.Identity]++
		}
	}
	for _, original := range [][]byte{left, right} {
		input := readMergeArchive(t, original)
		identity := SourceIdentity{StoreID: input.Manifest().StoreID, Project: input.Manifest().Projects[0].Prefix}
		want := 0
		for _, kind := range knownKindNames() {
			want += len(input.Records(kind))
		}
		if counts[identity] != want {
			t.Fatalf("source %v retained %d records, want %d", identity, counts[identity], want)
		}
	}
	h, ok := a.SourceHistory()
	if !ok || len(h.ProjectHistories) != 2 {
		t.Fatal("project histories lost")
	}
	for _, scope := range h.ProjectHistories {
		if scope.SourceStoreID != "store-"+scope.Project || scope.Commits[0].ID != strings.Repeat("a", 32) || SourceEventID(scope.Events[0]) != "1" {
			t.Fatal("source revision/event ID was changed or lost its owning store")
		}
		image, err := a.HistoryImageForProject(scope.Project, 1)
		if err != nil {
			t.Fatal(err)
		}
		manifest := image.Manifest()
		if manifest.StoreID != scope.SourceStoreID || len(manifest.Projects) != 1 || manifest.Projects[0].Prefix != scope.Project {
			t.Fatal("historical image retained another project's manifest or store")
		}
		for _, kind := range knownKindNames() {
			for _, record := range image.Records(kind) {
				if record.Source.Identity == nil || record.Source.Identity.Project != scope.Project {
					t.Fatalf("%s image leaked another project's %s", scope.Project, kind)
				}
			}
		}
		memory := image.Records("memory")
		if len(memory) != 1 || memory[0].ID != "shared" || memory[0].Body["value"] != scope.Project {
			t.Fatal("colliding memory content aliased across projects")
		}
	}
	var entries []externalEntry
	config, _ := a.Member("config/sources.json")
	if err := decodeJSON(config, &entries); err != nil || len(entries) != 2*len(externalFamilies) {
		t.Fatalf("source config provenance collapsed: %v, entries=%d", err, len(entries))
	}
	for _, e := range entries {
		if e.Identity == nil || e.Identity.StoreID != "store-"+e.Identity.Project {
			t.Fatal("configuration path lacks its source namespace")
		}
	}
	// The exact same content-hash rows must survive twice, once per source.
	if len(a.Records("beads-archive")) != 2*len(readMergeArchive(t, left).Records("beads-archive")) {
		t.Fatal("shared migration hashes were deduplicated across projects")
	}
	// Source keys are suitable for legacy-ID maps without ambiguous delimiters.
	keys := map[SourceRecordKey]bool{}
	for _, r := range a.Records("comment") {
		key, ok := r.SourceKey()
		if !ok || key.ID != "1" || keys[key] {
			t.Fatal("legacy source key collapsed or rewrote a raw ID")
		}
		encoded, err := json.Marshal(key)
		var decoded SourceRecordKey
		if err != nil || json.Unmarshal(encoded, &decoded) != nil || decoded != key {
			t.Fatal("legacy source tuple did not round trip")
		}
		keys[key] = true
	}
	if len(keys) != 2 {
		t.Fatal("legacy source map lost a project")
	}
	for _, readers := range [][]io.Reader{{bytes.NewReader(right), bytes.NewReader(left)}} {
		again, err := Merge(readers...)
		if err != nil || !bytes.Equal(again, merged) {
			t.Fatalf("source identity merge is nondeterministic: %v", err)
		}
	}
}

func TestSourceIdentityNestedMergeRetainsOriginalStores(t *testing.T) {
	left, right, third := collidingProjectHistory(t, "pl"), collidingProjectHistory(t, "qa"), collidingProjectHistory(t, "zz")
	first, err := Merge(bytes.NewReader(left), bytes.NewReader(right))
	if err != nil {
		t.Fatal(err)
	}
	combined, err := Merge(bytes.NewReader(first), bytes.NewReader(third))
	if err != nil {
		t.Fatal(err)
	}
	a := readMergeArchive(t, combined)
	h, _ := a.SourceHistory()
	if len(h.ProjectHistories) != 3 {
		t.Fatal("nested merge lost history")
	}
	for _, scope := range h.ProjectHistories {
		if scope.SourceStoreID != "store-"+scope.Project {
			t.Fatal("nested merge replaced original source identity with merge identity")
		}
	}
}

func TestSourceIdentityHistoryPreservesEqualRawIDsAcrossKinds(t *testing.T) {
	inputs := []io.Reader{}
	for _, project := range []string{"pl", "qa"} {
		f := newMergeFixture(t, project)
		for _, kind := range []string{"edge", "comment"} {
			f.records[kind][0].ID = "1"
			f.records[kind][0].Source.Fields["id"] = "1"
		}
		h := sourceHistoryForFixture(t, f, strings.Repeat("a", 32), strings.Repeat("b", 32), false)
		for i := range h.Range.Frames {
			// SQL image order must not affect head parity when different kinds
			// share a raw ID. Neither record may be renamed or discarded.
			rows := append([]Record{}, h.Range.Frames[i].Records...)
			for j, k := 0, len(rows)-1; j < k; j, k = j+1, k-1 {
				rows[j], rows[k] = rows[k], rows[j]
			}
			h.Range.Frames[i].Records = rows
		}
		data := withSourceHistory(t, f, h)
		readMergeArchive(t, data)
		inputs = append(inputs, bytes.NewReader(data))
	}
	data, err := Merge(inputs...)
	if err != nil {
		t.Fatal(err)
	}
	a := readMergeArchive(t, data)
	h, _ := a.SourceHistory()
	keys := map[SourceRecordKey]bool{}
	for _, scope := range h.ProjectHistories {
		for _, frame := range scope.Range.Frames {
			image, err := a.HistoryImageForProject(scope.Project, frame.Sequence)
			if err != nil {
				t.Fatal(err)
			}
			for _, kind := range []string{"edge", "comment"} {
				rows := image.Records(kind)
				if len(rows) != 1 || rows[0].ID != "1" || rows[0].Source.Fields["id"] != "1" {
					t.Fatalf("%s/%s raw identity changed or was dropped", scope.Project, kind)
				}
				key, ok := rows[0].SourceKey()
				if !ok || key.StoreID != "store-"+scope.Project || key.Project != scope.Project || key.Kind != kind || key.Table != rows[0].Source.Table || key.ID != "1" {
					t.Fatal("historical source key lost a tuple component")
				}
				keys[key] = true
			}
		}
	}
	if len(keys) != 4 {
		t.Fatalf("equal raw IDs collapsed across source/kind/table: %d keys", len(keys))
	}
}

func TestSourceIdentityHistoryRejectsDuplicateTypedRecord(t *testing.T) {
	f, h := sourceHistoryFixture(t, false)
	h.Range.Frames[0].Records = append(h.Range.Frames[0].Records, f.records["edge"][0])
	if _, err := Read(bytes.NewReader(withSourceHistory(t, f, h))); err == nil {
		t.Fatal("duplicate source/project/kind/table/raw ID accepted in one image")
	}
}

func TestSourceIdentitySameProjectOverlapRefusesAtomically(t *testing.T) {
	input := collidingProjectHistory(t, "pl")
	a := readMergeArchive(t, input)
	before := a.TarBytes()
	data, err := MergeArchives(a, a)
	var refusal HistoryMergeRefusal
	if !errors.As(err, &refusal) || refusal.Project != "pl" || data != nil {
		t.Fatalf("ambiguous source overlap not refused atomically: %v", err)
	}
	if !bytes.Equal(a.TarBytes(), before) {
		t.Fatal("refused merge mutated its input")
	}
}

func TestSourceIdentityIncompleteHistoryNamesGap(t *testing.T) {
	f, h := sourceHistoryFixture(t, true)
	data, err := Merge(bytes.NewReader(withSourceHistory(t, f, h)))
	var gap IncompleteHistoryError
	if !errors.As(err, &gap) || data != nil || gap.StoreID != "store-pl" || gap.Project != "pl" || gap.First != 1 || gap.Last != 1 || gap.Reason != "not-retained" {
		t.Fatalf("incomplete source history did not name its gap: %v", err)
	}
}

func TestSourceIdentityUnverifiedHistoryNamesMissingFamilies(t *testing.T) {
	f, h := sourceHistoryFixture(t, false)
	h.Range = SourceHistoryRange{Status: "unverified", Reason: "head-image-missing-families:config,issues", Baseline: []Record{}, BaselineEvents: []Row{}, Frames: []SourceHistoryFrame{}, Gaps: []HistoryGap{}}
	a := readMergeArchive(t, withSourceHistory(t, f, h))
	before := a.TarBytes()
	data, err := MergeArchives(a)
	var gap IncompleteHistoryError
	if data != nil || !errors.As(err, &gap) || gap.StoreID != "store-pl" || gap.Project != "pl" || gap.Reason != "head-image-missing-families:config,issues" {
		t.Fatalf("unverified history lost its actual missing-family reason: %v", err)
	}
	if !bytes.Equal(before, a.TarBytes()) {
		t.Fatal("incomplete-history refusal altered retained source evidence")
	}
}

func TestSourceIdentityReaderRefusesMisboundHistory(t *testing.T) {
	input, err := Merge(bytes.NewReader(collidingProjectHistory(t, "pl")), bytes.NewReader(collidingProjectHistory(t, "qa")))
	if err != nil {
		t.Fatal(err)
	}
	a := readMergeArchive(t, input)
	before, _ := a.SourceHistory()
	detached, _ := a.SourceHistory()
	detached.ProjectHistories[0].Range.Frames[0].Records[0].Source.Identity.Project = "qa"
	fresh, _ := a.SourceHistory()
	if !reflect.DeepEqual(before, fresh) {
		t.Fatal("source identity accessor aliases validated state")
	}
	for _, change := range []func(*SourceHistory){
		func(h *SourceHistory) { h.ProjectHistories[0].SourceStoreID = "wrong-store" },
		func(h *SourceHistory) { h.ProjectHistories[0].Range.Frames[0].Records[0].Source.Identity = nil },
		func(h *SourceHistory) { h.ProjectHistories = h.ProjectHistories[:1] },
	} {
		history, _ := a.SourceHistory()
		change(&history)
		files := map[string][]byte{}
		for path := range a.Manifest().Files {
			files[path], _ = a.Member(path)
		}
		files[SourceHistoryMember] = fixtureJSON(t, history)
		m := a.Manifest()
		m.Files[SourceHistoryMember] = File{Size: int64(len(files[SourceHistoryMember])), SHA256: digest(files[SourceHistoryMember])}
		files["manifest.json"] = fixtureJSON(t, m)
		if _, err := Read(bytes.NewReader(fixtureTar(t, files))); err == nil {
			t.Fatal("misbound or missing project history accepted")
		}
	}
}

func TestSourceIdentityReaderRequiresExplicitCompleteContract(t *testing.T) {
	merged, err := Merge(bytes.NewReader(collidingProjectHistory(t, "pl")), bytes.NewReader(collidingProjectHistory(t, "qa")))
	if err != nil {
		t.Fatal(err)
	}
	a := readMergeArchive(t, merged)
	for _, policy := range []string{"missing-marker", "unknown-marker", "missing-record-identity", "wrong-record-project", "missing-config-identity"} {
		t.Run(policy, func(t *testing.T) {
			files := map[string][]byte{}
			for path := range a.Manifest().Files {
				files[path], _ = a.Member(path)
			}
			m := a.Manifest()
			switch policy {
			case "missing-marker":
				m.SourceIdentityContract = ""
			case "unknown-marker":
				m.SourceIdentityContract = "unknown/1"
			case "missing-record-identity", "wrong-record-project":
				rows := a.Records("comment")
				if policy == "missing-record-identity" {
					rows[0].Source.Identity = nil
				} else {
					rows[0].Source.Identity.Project = "qa"
				}
				files["records/comment.ndjson"] = nil
				for _, row := range rows {
					files["records/comment.ndjson"] = append(files["records/comment.ndjson"], fixtureJSON(t, row)...)
				}
			case "missing-config-identity":
				var entries []externalEntry
				if err := decodeJSON(files["config/sources.json"], &entries); err != nil {
					t.Fatal(err)
				}
				entries[0].Identity = nil
				files["config/sources.json"] = fixtureJSON(t, entries)
			}
			for path, data := range files {
				m.Files[path] = File{Size: int64(len(data)), SHA256: digest(data)}
			}
			files["manifest.json"] = fixtureJSON(t, m)
			if _, err := Read(bytes.NewReader(fixtureTar(t, files))); err == nil {
				t.Fatal("incomplete or misbound source identity contract accepted")
			}
		})
	}
}

func TestSourceIdentitySidecarsAndHistoryImagesKeepProjectScope(t *testing.T) {
	merged, err := Merge(bytes.NewReader(collidingProjectHistory(t, "pl")), bytes.NewReader(collidingProjectHistory(t, "qa")))
	if err != nil {
		t.Fatal(err)
	}
	// Declare a synthetic source-plane wire with no ephemeral entities. This
	// exercises sidecar addressing without claiming native producer history.
	a := readMergeArchive(t, merged)
	files := map[string][]byte{}
	for path := range a.Manifest().Files {
		files[path], _ = a.Member(path)
	}
	m := a.Manifest()
	m.SourcePlaneAddressContract = SourcePlaneAddressContract
	for i := range m.Kinds {
		if m.Kinds[i].Name == "edge" {
			m.Kinds[i].Version = EdgeVersionTypedDependencies
		}
	}
	for i := range m.Sources {
		if sourcePlaneArchiveTable(m.Sources[i].Family) || m.Sources[i].Family == EphemeralDependencyTable {
			m.Sources[i].Action = "archive"
		}
	}
	files["manifest.json"] = fixtureJSON(t, m)
	a = readMergeArchive(t, fixtureTar(t, files))
	origins, err := a.SourcePlaneSidecarOrigins()
	if err != nil || len(origins) != 2*len(externalFamilies) {
		t.Fatalf("source sidecar census: %v", err)
	}
	seen := map[string]bool{}
	previous := ""
	for _, origin := range origins {
		key := origin.Project + "\x00" + origin.ID + "\x00" + origin.Layer
		if origin.Project == "" || seen[key] || previous != "" && key <= previous {
			t.Fatal("source sidecar project identity aliased or unordered")
		}
		seen[key], previous = true, key
	}
	for _, project := range []string{"pl", "qa"} {
		image, err := a.HistoryImageForProject(project, 1)
		if err != nil {
			t.Fatal(err)
		}
		local, err := image.SourcePlaneSidecarOrigins()
		if err != nil || len(local) != len(externalFamilies) {
			t.Fatalf("historical source sidecars: %v", err)
		}
		for _, origin := range local {
			matched := false
			for _, original := range origins {
				if original.Project == project && original.ID == origin.ID && original.Layer == origin.Layer && reflect.DeepEqual(original, origin) {
					matched = true
				}
			}
			if origin.Project != project || !matched {
				t.Fatal("selected history sidecar borrowed another project or changed raw entry bytes")
			}
		}
	}
}
