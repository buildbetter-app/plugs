package dumpformat

import (
	"bytes"
	"sort"
	"strings"
	"testing"
)

// Declared synthetic head-only controls exercise the private attachment reader.
// They are not a complete physical capture set or an import proof.
func nativeDeltaDispositionHead(t *testing.T, nonempty bool) map[string][]byte {
	t.Helper()
	members := sourcePlaneReaderMembers(t, nativeSyntheticFixture(t, "pl"))
	delete(members, NativeHistoryMember)
	var m Manifest
	if err := decodeJSON(members["manifest.json"], &m); err != nil {
		t.Fatal(err)
	}
	m.NativeHistoryContract = NativeDeltaFormat
	records := []Record{}
	for _, line := range bytes.Split(bytes.TrimSpace(members["records/beads-archive.ndjson"]), []byte{'\n'}) {
		var r Record
		if err := decodeJSON(line, &r); err != nil {
			t.Fatal(err)
		}
		records = append(records, r)
	}
	for _, table := range []string{"federation_peers", "interactions", "routes"} {
		fields := Row{}
		for _, column := range strings.Fields(sourceTableColumns(m.MainVersion)[table]) {
			fields[column] = nil
		}
		switch table {
		case "federation_peers":
			fields["name"] = "inert-public-peer"
		case "interactions":
			fields["id"], fields["prompt"] = "inert-public-interaction", "retained, never executed"
		case "routes":
			fields["prefix"], fields["path"] = "inert-public-route", "/inert/public/path"
		}
		if nonempty {
			records = append(records, Record{Kind: "beads-archive", ID: table + "/" + digest(fixtureJSON(t, fields)), Source: Provenance{Table: table, Fields: fields}, Body: Row{"read_only": true, "redactable": true, "history_verified": false, "attribution_verified": false}})
		}
		for i := range m.Sources {
			if m.Sources[i].Family == table {
				if nonempty {
					m.Sources[i].Rows++
				}
				m.Sources[i].Action = "archive"
			}
		}
	}
	sort.Slice(records, func(i, j int) bool {
		return compareSourceRecords(records[i], records[j], m.SourcePlaneAddressContract) < 0
	})
	members["records/beads-archive.ndjson"] = nil
	for _, r := range records {
		members["records/beads-archive.ndjson"] = append(members["records/beads-archive.ndjson"], fixtureJSON(t, r)...)
	}
	for i := range m.Kinds {
		if m.Kinds[i].Name == "beads-archive" {
			m.Kinds[i].Count = len(records)
		}
	}
	members["manifest.json"] = fixtureJSON(t, m)
	return members
}

func TestNativeDeltaHeadOpaqueDispositionsPrecedeLegacyGuard(t *testing.T) {
	empty := nativeDeltaDispositionHead(t, false)
	if _, err := readArchive(bytes.NewReader(nativeFixtureMembers(t, empty)), true); err != nil {
		t.Fatal("private v2 archive actions refused at zero rows", err)
	}
	members := nativeDeltaDispositionHead(t, true)
	data := nativeFixtureMembers(t, members)
	a, err := readArchive(bytes.NewReader(data), true)
	if err != nil {
		t.Fatal("private v2 head refused known nonempty inert families", err)
	}
	raw, _ := a.Member("records/beads-archive.ndjson")
	if !bytes.Equal(raw, members["records/beads-archive.ndjson"]) {
		t.Fatal("opaque source bytes changed")
	}
	for _, table := range []string{"federation_peers", "interactions", "routes"} {
		count := 0
		for _, r := range a.Records("beads-archive") {
			if r.Source.Table == table {
				count++
			}
		}
		found := false
		for _, d := range a.Manifest().Sources {
			if d.Family == table {
				found = true
				if d.Action != "archive" || d.Rows != 1 || count != 1 {
					t.Fatal("opaque disposition or source count differs", table)
				}
			}
		}
		if !found || actionFor(table) != "refuse-if-nonempty" || sourcePlaneArchiveTable(table) {
			t.Fatal("portable rule changed or native family disappeared", table)
		}
	}
	if _, err := Read(bytes.NewReader(data)); err == nil {
		t.Fatal("portable reader accepted a delta-only head")
	}
	var m Manifest
	if err := decodeJSON(members["manifest.json"], &m); err != nil {
		t.Fatal(err)
	}
	m.NativeHistoryContract = ""
	members["manifest.json"] = fixtureJSON(t, m)
	legacy := nativeFixtureMembers(t, members)
	if _, err := readArchive(bytes.NewReader(legacy), true); err == nil {
		t.Fatal("missing v2 marker accepted by private attachment")
	}
	if _, err := Read(bytes.NewReader(legacy)); err == nil || !strings.Contains(err.Error(), "unsupported or operational source record") {
		t.Fatal("legacy nonempty guard changed", err)
	}
	if _, err := Read(bytes.NewReader(nativeSyntheticFixture(t, "pl"))); err != nil {
		t.Fatal("original legacy native empty-family contract regressed", err)
	}
}

func TestNativeDeltaHeadOpaqueGuardsRemainExact(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*Manifest, []Record) []Record
	}{
		{"wrong-count", func(m *Manifest, r []Record) []Record {
			for i := range m.Sources {
				if m.Sources[i].Family == "routes" {
					m.Sources[i].Rows = 0
				}
			}
			return r
		}},
		{"wrong-action", func(m *Manifest, r []Record) []Record {
			for i := range m.Sources {
				if m.Sources[i].Family == "routes" {
					m.Sources[i].Action = "refuse-if-nonempty"
				}
			}
			return r
		}},
		{"unknown-family", func(m *Manifest, r []Record) []Record {
			for i := range r {
				if r[i].Source.Table == "routes" {
					r[i].Source.Table = "unknown_family"
				}
			}
			return r
		}},
		{"missing-column", func(m *Manifest, r []Record) []Record {
			for i := range r {
				if r[i].Source.Table == "routes" {
					delete(r[i].Source.Fields, "prefix")
				}
			}
			return r
		}},
		{"active-body", func(m *Manifest, r []Record) []Record {
			for i := range r {
				if r[i].Source.Table == "routes" {
					r[i].Body["read_only"] = false
				}
			}
			return r
		}},
		{"clear-peer-credential", func(m *Manifest, r []Record) []Record {
			for i := range r {
				if r[i].Source.Table == "federation_peers" {
					r[i].Source.Fields["password_encrypted"] = "must-refuse-raw-credential"
				}
			}
			return r
		}},
		{"missing-family-contract", func(m *Manifest, r []Record) []Record { m.SourceFamilyContract = ""; return r }},
		{"physical-family-in-portable-records", func(m *Manifest, r []Record) []Record {
			for i := range r {
				if r[i].Source.Table == "routes" {
					r[i].Source.Table = "dolt_schemas"
				}
			}
			return r
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			members := nativeDeltaDispositionHead(t, true)
			var m Manifest
			if err := decodeJSON(members["manifest.json"], &m); err != nil {
				t.Fatal(err)
			}
			records := []Record{}
			for _, line := range bytes.Split(bytes.TrimSpace(members["records/beads-archive.ndjson"]), []byte{'\n'}) {
				var r Record
				if err := decodeJSON(line, &r); err != nil {
					t.Fatal(err)
				}
				records = append(records, r)
			}
			records = tc.edit(&m, records)
			for i := range records {
				records[i].ID = records[i].Source.Table + "/" + digest(fixtureJSON(t, records[i].Source.Fields))
			}
			sort.Slice(records, func(i, j int) bool {
				return compareSourceRecords(records[i], records[j], m.SourcePlaneAddressContract) < 0
			})
			members["records/beads-archive.ndjson"] = nil
			for _, r := range records {
				members["records/beads-archive.ndjson"] = append(members["records/beads-archive.ndjson"], fixtureJSON(t, r)...)
			}
			members["manifest.json"] = fixtureJSON(t, m)
			if _, err := readArchive(bytes.NewReader(nativeFixtureMembers(t, members)), true); err == nil {
				t.Fatal("inconsistent opaque head accepted")
			}
		})
	}
	// Exact source/current reconciliation still rejects changed opaque rows.
	a, err := Read(bytes.NewReader(nativeSyntheticFixture(t, "pl")))
	if err != nil {
		t.Fatal(err)
	}
	h := a.NativeSourceHistories()[0]
	h.deltaVerified = true
	changed := h.Current["routes"]
	changed.Rows = append(changed.Rows, Row{"prefix": "invented"})
	h.Current["routes"] = changed
	if bindNativeCurrent(h, a.byKind, false) == nil {
		t.Fatal("native current rows escaped portable conservation")
	}
	if len(a.NativeSourceHistories()[0].Current["routes"].Rows) != 0 {
		t.Fatal("detached history edit changed original")
	}
}
