package plugdump

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

type nativeHistoryFixture struct {
	*historyFixtureSource
	ignore Table
}

func (s nativeHistoryFixture) ReadNativeSourceImage(ctx context.Context, id string) (map[string]Table, error) {
	return s.ReadSourceImage(ctx, id)
}
func (s nativeHistoryFixture) ReadSourceIgnore(context.Context) (Table, error) { return s.ignore, nil }

func nativeHistoryFixtureForTest(t *testing.T) (nativeHistoryFixture, Inventory) {
	t.Helper()
	s, inventory := historyFixture(t)
	inventory.SourceFamilyContract = SourceFamilyContract
	inventory.SourcePlaneAddressContract = SourcePlaneAddressContract
	ignore := Table{Columns: []string{"pattern", "ignored"}, Rows: []Row{}}
	for name := range nativeUnversioned {
		ignore.Rows = append(ignore.Rows, Row{"pattern": name, "ignored": true})
	}
	for _, image := range s.images {
		for name := range nativeUnversioned {
			delete(image, name)
		}
	}
	return nativeHistoryFixture{s, ignore}, inventory
}

func nativeCapture(t *testing.T, data []byte) (Manifest, NativeHistory) {
	t.Helper()
	m, err := Validate(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	var h NativeHistory
	if err := decodeJSON(archiveMembers(t, data)[NativeHistoryMember], &h); err != nil {
		t.Fatal(err)
	}
	return m, h
}

func TestNativeHistoryRetainsActualImagesAndCurrentUnversionedRows(t *testing.T) {
	s, inventory := nativeHistoryFixtureForTest(t)
	// Older categories/config and an obsolete schema belong to their own image.
	// Raw retention does not pretend the current business mapper supports them.
	first := s.images[s.log.Commits[0].ID]
	first["old_native_table"] = Table{Columns: []string{"id", "value"}, Rows: []Row{{"id": "opaque", "value": json.Number("9007199254740993")}}}
	first["custom_statuses"] = Table{Columns: columnsFor("custom_statuses"), Rows: []Row{{"name": "oldnative", "category": "wip"}}}
	rootBefore := mustJSON(t, first)
	currentBefore := mustJSON(t, s.fixtureSource)
	data := buildFixture(t, s, inventory)
	m, h := nativeCapture(t, data)
	if !h.Complete || len(h.Images) != 4 || len(h.Gaps) != 0 || h.StoreID != inventory.StoreID || len(h.Current) != 33 {
		t.Fatalf("native census: complete=%t images=%d gaps=%v", h.Complete, len(h.Images), h.Gaps)
	}
	if len(h.Current["events"].Rows) != len(s.fixtureSource["events"].Rows) || len(h.Images[3].Tables["events"].Rows) != 0 {
		t.Fatal("current audit rows were lost or assigned to an immutable commit")
	}
	if text(h.Images[0].Tables["custom_statuses"].Rows[0], "name") != "oldnative" || len(h.Images[0].Tables["old_native_table"].Rows) != 1 {
		t.Fatal("historical catalog/schema was replaced by current state")
	}
	for _, source := range m.Sources {
		if _, table := tableColumns[source.Family]; !table {
			continue
		}
		want := FamilyVersioned
		if nativeUnversioned[source.Family] {
			want = FamilyUnversioned
		}
		if source.History != want || source.Rows != len(s.fixtureSource[source.Family].Rows) {
			t.Fatalf("classification/count: %+v", source)
		}
	}
	if !bytes.Equal(rootBefore, mustJSON(t, first)) || !bytes.Equal(currentBefore, mustJSON(t, s.fixtureSource)) {
		t.Fatal("native capture mutated source")
	}
	// Portable history retains its independent mapping failure.
	var portable SourceHistory
	if err := decodeJSON(archiveMembers(t, data)[SourceHistoryMember], &portable); err != nil {
		t.Fatal(err)
	}
	if portable.Range.Complete || m.History != "available-unverified" {
		t.Fatal("raw native retention promoted portable replay")
	}
}

func TestNativeHistoryRetainsPre0062VersionedEvents(t *testing.T) {
	s, inventory := nativeHistoryFixtureForTest(t)
	old := cloneRow(s.fixtureSource["events"].Rows[0])
	old["comment"] = "only in retained pre-flip history"
	s.images[s.log.Commits[1].ID]["events"] = Table{Columns: columnsFor("events"), Rows: []Row{old}}
	_, h := nativeCapture(t, buildFixture(t, s, inventory))
	if !h.Complete || len(h.Images[1].Tables["events"].Rows) != 1 || h.Images[1].Tables["events"].Rows[0]["comment"] != old["comment"] {
		t.Fatal("retained pre-flip event history dropped")
	}
	if bytes.Contains(mustJSON(t, h.Current["events"]), []byte("only in retained")) {
		t.Fatal("historical-only event was grafted onto current audit")
	}
}

func TestNativeHistoryMissingVersionIsNamedAndNeverBackfilled(t *testing.T) {
	s, inventory := nativeHistoryFixtureForTest(t)
	missing := s.log.Commits[1].ID
	delete(s.images, missing)
	_, h := nativeCapture(t, buildFixture(t, s, inventory))
	if h.Complete || len(h.Images) != 3 || len(h.Gaps) != 1 || h.Gaps[0].CommitID != missing || h.Gaps[0].Sequence != 2 || h.Gaps[0].Reason != "historical-image-unavailable" {
		t.Fatalf("missing source version: %+v", h.Gaps)
	}
}

func TestNativeHistoryRefusesMissingVersionedHeadAndChangedRows(t *testing.T) {
	for _, mode := range []string{"missing-issues", "changed-comments", "tracked-event-missing", "ignore-override"} {
		t.Run(mode, func(t *testing.T) {
			s, inventory := nativeHistoryFixtureForTest(t)
			head := s.images[s.log.Head]
			want := ""
			switch mode {
			case "missing-issues":
				delete(head, "issues")
				want = "head-image-missing-versioned-family:issues"
			case "changed-comments":
				row := cloneRow(s.fixtureSource["comments"].Rows[0])
				row["text"] = "actual committed text differs"
				head["comments"] = Table{Columns: columnsFor("comments"), Rows: []Row{row}}
				want = "head-image-differs-versioned-family:comments"
			case "tracked-event-missing":
				for i := range s.ignore.Rows {
					if s.ignore.Rows[i]["pattern"] == "events" {
						s.ignore.Rows[i]["ignored"] = false
					}
				}
				want = "head-image-missing-versioned-family:events"
			case "ignore-override":
				delete(head, "comments")
				s.ignore.Rows = append(s.ignore.Rows, Row{"pattern": "comments", "ignored": true})
				want = "head-image-missing-versioned-family:comments"
			}
			_, err := Build(context.Background(), s, inventory)
			if err == nil || !strings.Contains(err.Error(), want) {
				t.Fatalf("expected named versioned refusal %s, got %v", want, err)
			}
		})
	}
}

func TestNativeHistoryConservesLeasesAndMachineLocalRows(t *testing.T) {
	s, inventory := nativeHistoryFixtureForTest(t)
	s.fixtureSource["leases"] = Table{Columns: columnsFor("leases"), Rows: []Row{{"issue_id": "demo-7", "holder": "public holder", "granted_at": "2026-01-01T00:00:00Z", "lease_expires_at": "2030-01-01T00:00:00Z", "heartbeat_at": nil, "granted_node": "public node"}}}
	_, h := nativeCapture(t, buildFixture(t, s, inventory))
	if len(h.Current["leases"].Rows) != 1 || h.Current["leases"].Rows[0]["lease_expires_at"] != "2030-01-01T00:00:00Z" {
		t.Fatal("producer filtered or invented lease expiry")
	}
	legacy := inventory
	legacy.SourceFamilyContract = ""
	if _, err := Build(context.Background(), s, legacy); err == nil {
		t.Fatal("legacy live-lease refusal was weakened")
	}
}

func TestNativeRawKeyValueCredentialPolicyIsPreserved(t *testing.T) {
	for _, family := range []string{"config", "metadata", "local_metadata"} {
		t.Run(family, func(t *testing.T) {
			public := Row{"key": "public_setting", "value": "public value"}
			ref := Row{"key": "api_token", "value": map[string]any{"$secret": family + "/api_token"}}
			for _, row := range []Row{public, ref} {
				got, err := nativeTable(Table{Columns: []string{"key", "value"}, Rows: []Row{row}}, family, nil)
				if err != nil || !equalJSON(got.Rows, []Row{row}) {
					t.Fatalf("stored setting/reference changed: %v", err)
				}
			}
			// Reinterpreting the structural SQL column must not exempt nested
			// credential names or malformed explicit references from validation.
			for _, value := range []any{
				map[string]any{"api_token": "plaintext"},
				map[string]any{"$secret": "public/ref", "extra": "plaintext"},
			} {
				row := Row{"key": "public_setting", "value": value}
				if _, err := nativeTable(Table{Columns: []string{"key", "value"}, Rows: []Row{row}}, family, nil); err == nil {
					t.Fatal("raw SQL credential policy was bypassed")
				}
			}
			// Raw historical schemas may carry additional columns; they keep
			// their ordinary credential checks rather than inheriting an exemption.
			row := Row{"key": "public_setting", "value": "public value", "api_token": "plaintext"}
			if _, err := nativeTable(Table{Columns: []string{"key", "value", "api_token"}, Rows: []Row{row}}, family, nil); err == nil {
				t.Fatal("additional historical SQL column bypassed credential checks")
			}
		})
	}
}

func TestNativeHistoryReaderChecksDesignationAndRawImageIntegrity(t *testing.T) {
	s, inventory := nativeHistoryFixtureForTest(t)
	data := buildFixture(t, s, inventory)
	for _, mode := range []string{"classification", "count", "current-row", "image-identity", "drop-image", "invent-complete", "marker-omission", "marker-null"} {
		t.Run(mode, func(t *testing.T) {
			members := archiveMembers(t, data)
			var m Manifest
			var h NativeHistory
			if err := decodeJSON(members["manifest.json"], &m); err != nil {
				t.Fatal(err)
			}
			if err := decodeJSON(members[NativeHistoryMember], &h); err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "classification":
				for i := range m.Sources {
					if m.Sources[i].Family == "issues" {
						m.Sources[i].History = FamilyUnversioned
					}
				}
			case "count":
				for i := range m.Sources {
					if m.Sources[i].Family == "events" {
						m.Sources[i].Rows++
					}
				}
			case "current-row":
				h.Current["events"].Rows[0]["comment"] = "forged current audit"
			case "image-identity":
				h.Images[0].CommitID = h.Head
			case "drop-image":
				h.Images = h.Images[1:]
			case "invent-complete":
				h.Complete = false
			case "marker-omission":
				m.SourceFamilyContract = ""
			case "marker-null":
				var fields map[string]any
				if err := json.Unmarshal(members["manifest.json"], &fields); err != nil {
					t.Fatal(err)
				}
				fields["source_family_contract"] = nil
				members["manifest.json"] = mustJSON(t, fields)
				if _, err := Validate(bytes.NewReader(reframeSourcePlaneArchive(t, members))); err == nil {
					t.Fatal("null native contract admitted")
				}
				return
			}
			members[NativeHistoryMember] = mustJSON(t, h)
			members["manifest.json"] = mustJSON(t, m)
			if _, err := Validate(bytes.NewReader(reframeSourcePlaneArchive(t, members))); err == nil {
				t.Fatalf("forged native %s admitted", mode)
			}
		})
	}
}

func TestHistoryEqualRawEdgeCommentIDsRemainDistinct(t *testing.T) {
	s, inventory := historyFixture(t)
	inventory.SourcePlaneAddressContract = SourcePlaneAddressContract
	for _, image := range append([]map[string]Table{s.fixtureSource}, func() []map[string]Table {
		out := []map[string]Table{}
		for _, v := range s.images {
			out = append(out, v)
		}
		return out
	}()...) {
		image["dependencies"].Rows[0]["id"] = "1"
		image["comments"].Rows[0]["id"] = "1"
	}
	manifest, h := sourceCapture(t, buildFixture(t, s, inventory))
	if !h.Range.Complete {
		t.Fatalf("equal raw identities refused: %+v", h.Range)
	}
	for _, frame := range h.Range.Frames {
		count := 0
		for _, r := range frame.Records {
			if r.ID == "1" && (r.Kind == "edge" || r.Kind == "comment") {
				count++
			}
		}
		if count != 2 {
			t.Fatal("equal physical identity dropped or rewritten")
		}
		if err := validateHistoryImage(append(frame.Records, frame.Records[0]), manifest); err == nil {
			t.Fatal("exact typed duplicate admitted")
		}
	}
}

func TestNativeHistoryGraphKeepsMergeParentsAndMissingAncestryRefuses(t *testing.T) {
	a, b, c, d := strings.Repeat("a", 32), strings.Repeat("b", 32), strings.Repeat("c", 32), strings.Repeat("d", 32)
	log := SourceCommitLog{Head: d, Commits: []SourceCommit{{ID: d, Parents: []string{c, b}, Metadata: Row{}}, {ID: c, Parents: []string{a}, Metadata: Row{}}, {ID: a, Parents: []string{}, Metadata: Row{}}, {ID: b, Parents: []string{a}, Metadata: Row{}}}}
	ordered, err := nativeCommitOrder(log)
	if err != nil || len(ordered) != 4 || ordered[0].ID != a || ordered[3].ID != d {
		t.Fatalf("actual graph: %v %v", ordered, err)
	}
	log.Commits = log.Commits[:3]
	if _, err := nativeCommitOrder(log); err == nil || !strings.Contains(fmt.Sprint(err), "commit-ancestry-missing:"+b) {
		t.Fatalf("missing retained parent not named: %v", err)
	}
}

func TestNativeHistoryBindsPortableCommitCensus(t *testing.T) {
	for _, mode := range []string{"native-truncation", "portable-truncation", "portable-parent", "portable-metadata"} {
		t.Run(mode, func(t *testing.T) {
			s, inventory := nativeHistoryFixtureForTest(t)
			members := archiveMembers(t, buildFixture(t, s, inventory))
			var native NativeHistory
			var portable SourceHistory
			if err := decodeJSON(members[NativeHistoryMember], &native); err != nil {
				t.Fatal(err)
			}
			if err := decodeJSON(members[SourceHistoryMember], &portable); err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "native-truncation":
				last := native.Commits[len(native.Commits)-1]
				last.Parents = []string{}
				native.Commits = []SourceCommit{last}
				image := native.Images[len(native.Images)-1]
				image.Sequence = 1
				native.Images = []NativeHistoryImage{image}
				// The forged census still claims complete, with no named gaps.
			case "portable-truncation":
				last := portable.Commits[len(portable.Commits)-1]
				last.Parents = []string{}
				portable.Commits = []SourceCommit{last}
			case "portable-parent":
				portable.Commits[len(portable.Commits)-1].Parents = []string{}
			case "portable-metadata":
				portable.Commits[0].Metadata["message"] = "conflicting retained metadata"
			}
			members[NativeHistoryMember] = mustJSON(t, native)
			members[SourceHistoryMember] = mustJSON(t, portable)
			_, err := Validate(bytes.NewReader(reframeSourcePlaneArchive(t, members)))
			if err == nil || !strings.Contains(err.Error(), "native-portable-commit-census-mismatch") {
				t.Fatalf("contradictory %s was not named: %v", mode, err)
			}
		})
	}
}

func TestNativeHistoryEquivalentMergeCensusesAllowDifferentOrdering(t *testing.T) {
	s, inventory := nativeHistoryFixtureForTest(t)
	s.log.Commits[2].Parents = []string{s.log.Commits[0].ID}
	s.log.Commits[3].Parents = []string{s.log.Commits[2].ID, s.log.Commits[1].ID}
	members := archiveMembers(t, buildFixture(t, s, inventory))
	var portable SourceHistory
	if err := decodeJSON(members[SourceHistoryMember], &portable); err != nil {
		t.Fatal(err)
	}
	for i, j := 0, len(portable.Commits)-1; i < j; i, j = i+1, j-1 {
		portable.Commits[i], portable.Commits[j] = portable.Commits[j], portable.Commits[i]
	}
	for i := range portable.Commits {
		parents := portable.Commits[i].Parents
		for j, k := 0, len(parents)-1; j < k; j, k = j+1, k-1 {
			parents[j], parents[k] = parents[k], parents[j]
		}
	}
	members[SourceHistoryMember] = mustJSON(t, portable)
	if _, err := Validate(bytes.NewReader(reframeSourcePlaneArchive(t, members))); err != nil {
		t.Fatalf("equivalent actual merge graph refused: %v", err)
	}
}

func TestNativeFamilyManifestAndHistoryAgreeWithPublishedSchemas(t *testing.T) {
	s, inventory := nativeHistoryFixtureForTest(t)
	members := archiveMembers(t, buildFixture(t, s, inventory))
	for schemaFile, member := range map[string]string{"manifest.schema.json": "manifest.json", "native-history.schema.json": NativeHistoryMember} {
		path, err := filepath.Abs(schemaFile)
		if err != nil {
			t.Fatal(err)
		}
		compiler := jsonschema.NewCompiler()
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var schemaDoc any
		if err := json.Unmarshal(data, &schemaDoc); err != nil {
			t.Fatal(err)
		}
		url := "https://example.invalid/" + schemaFile
		if err := compiler.AddResource(url, schemaDoc); err != nil {
			t.Fatal(err)
		}
		schema, err := compiler.Compile(url)
		if err != nil {
			t.Fatal(err)
		}
		var document any
		if err := json.Unmarshal(members[member], &document); err != nil {
			t.Fatal(err)
		}
		if err := schema.Validate(document); err != nil {
			t.Fatalf("published %s contract: %v", schemaFile, err)
		}
	}
}
