package dumpformat

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// SourcePlaneSidecarOrigins describes the actual captured configuration
// entries, including absent entries. Raw commits the original whole entry;
// Parsed commits its captured values JSON, not inferred effective settings.
// Path/state/family/precedence are copied literally from the declared input.
// Any nested observation bytes, modes and hashes remain inside Raw/Parsed.
func (a *Archive) SourcePlaneSidecarOrigins() ([]driver.SourcePlaneOriginV1, error) {
	if a == nil || a.manifest.SourcePlaneAddressContract != SourcePlaneAddressContract {
		return nil, fmt.Errorf("source plane sidecars require the explicit contract")
	}
	var origins []driver.SourcePlaneOriginV1
	if err := a.visitConfigEntries(a.decodedBudget(), func(value externalEntry, entry []byte) error {
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(entry, &fields); err != nil {
			return err
		}
		origin := driver.SourcePlaneOriginV1{Table: "config/sources.json", ID: value.Input.Path, Action: "origin-only", Raw: sourcePlaneByteCommitment(entry), Path: value.Input.Path, Mode: value.Input.State, Layer: value.Input.Family, Precedence: []string{strconv.Itoa(value.Input.Precedence)}, Parsed: driver.SourcePlaneBytesV1{Presence: "absent"}}
		if value.Identity != nil {
			origin.Project = value.Identity.Project
		}
		for key, bytes := range fields {
			origin.Fields = append(origin.Fields, driver.SourcePlaneFieldV1{Name: key, Value: sourcePlaneByteCommitment(bytes)})
		}
		sort.Slice(origin.Fields, func(i, j int) bool { return origin.Fields[i].Name < origin.Fields[j].Name })
		if parsed, ok := fields["values"]; ok {
			origin.Parsed = sourcePlaneByteCommitment(parsed)
		}
		origins = append(origins, origin)
		return nil
	}); err != nil {
		return nil, err
	}
	sort.Slice(origins, func(i, j int) bool {
		a, b := origins[i], origins[j]
		if a.Project != b.Project {
			return a.Project < b.Project
		}
		if a.ID != b.ID {
			return a.ID < b.ID
		}
		return a.Layer < b.Layer
	})
	return origins, nil
}

func sourcePlaneByteCommitment(data []byte) driver.SourcePlaneBytesV1 {
	var value any
	_ = decodeJSON(data, &value) // The caller already checked this JSON.
	presence := "null"
	switch value.(type) {
	case map[string]any:
		presence = "object"
	case []any:
		presence = "array"
	case string:
		presence = "text"
	case bool:
		presence = "boolean"
	case json.Number:
		presence = "number"
	}
	return driver.SourcePlaneBytesV1{Presence: presence, Size: uint64(len(data)), SHA256: digest(data)}
}
