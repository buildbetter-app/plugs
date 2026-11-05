package plugdump

import (
	"encoding/json"
	"fmt"
	"strings"
)

func validateSourcePlaneContract(contract string) error {
	if contract != "" && contract != SourcePlaneAddressContract {
		return fmt.Errorf("unsupported source plane address contract")
	}
	return nil
}

// JSON presence is distinct from a typed caller's legacy zero value: explicit
// null/empty and unknown values must not silently become legacy omission.
func validateSourcePlaneMarker(data []byte, contract string) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	for key := range fields {
		// encoding/json accepts folded matches for tagged struct fields. Such
		// aliases must not turn a present marker into legacy omission or enable
		// the capability without its canonical wire spelling.
		if key != "source_plane_address_contract" && strings.EqualFold(key, "source_plane_address_contract") {
			return fmt.Errorf("noncanonical source_plane_address_contract key %q", key)
		}
	}
	if _, present := fields["source_plane_address_contract"]; present && contract != SourcePlaneAddressContract {
		return fmt.Errorf("unsupported source plane address contract")
	}
	return validateSourcePlaneContract(contract)
}

func compareRecords(a, b Record, contract string) int {
	if contract != SourcePlaneAddressContract || a.Kind != "edge" || b.Kind != "edge" {
		return strings.Compare(recordKey(a), recordKey(b))
	}
	// Compare valid UTF-8 components bytewise, not delimiter-concatenated keys.
	// Raw physical IDs are unchanged, including equal IDs on different planes.
	if c := strings.Compare(a.Project, b.Project); c != 0 {
		return c
	}
	if c := strings.Compare(a.Source.Table, b.Source.Table); c != 0 {
		return c
	}
	return strings.Compare(a.ID, b.ID)
}

func validateSourcePlanes(projects []Project, beads, edges []Record, rows map[string][]Row, customStatuses map[string]bool) error {
	m := mapping{projects: projects, beads: map[string]string{}, wisps: map[string]string{}, records: map[string][]Record{}, sourcePlanes: true, customStatuses: customStatuses}
	for _, bead := range beads {
		m.beads[bead.ID] = bead.Project
	}
	if err := m.wispRows(rows["wisps"]); err != nil {
		return err
	}
	for _, table := range []string{"wisp_labels", "wisp_comments", "wisp_events", "wisp_child_counters"} {
		if err := m.wispAuxiliary(table, rows[table]); err != nil {
			return err
		}
	}
	for _, edge := range edges {
		if err := m.dependency(edge.Source.Table, edge.Source.Fields); err != nil {
			return err
		}
		mapped := m.records["edge"][len(m.records["edge"])-1]
		if edge.Project != mapped.Project {
			return fmt.Errorf("edge project does not match its physical source plane")
		}
	}
	return nil
}
