package dumpformat

import (
	"bytes"
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
	input, ok := a.configReader()
	if !ok {
		return nil, fmt.Errorf("missing captured configuration")
	}
	bounded := a.collection != nil || a.delta != nil
	owner := a.decodedBudget()
	var origins []driver.SourcePlaneOriginV1
	if err := visitJSONElements(input, bounded, configAccounting{decoded: owner}, func(entry []byte, footprint uint64) error {
		if bounded && owner != nil {
			work := owner.Scope()
			defer work.Release()
			if err := work.ChargeBytes("config_element_decoded", footprint); err != nil {
				return err
			}
		}
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(entry, &fields); err != nil {
			return err
		}
		var value externalEntry
		if err := json.Unmarshal(fields["input"], &value.Input); err != nil {
			return err
		}
		if identity, ok := fields["identity"]; ok {
			if err := json.Unmarshal(identity, &value.Identity); err != nil {
				return err
			}
		}
		origin := driver.SourcePlaneOriginV1{Table: SourceConfigMember, ID: value.Input.Path, Action: "origin-only", Raw: sourcePlaneByteCommitment(entry), Path: value.Input.Path, Mode: value.Input.State, Layer: value.Input.Family, Precedence: []string{strconv.Itoa(value.Input.Precedence)}, Parsed: driver.SourcePlaneBytesV1{Presence: "absent"}}
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
		if bounded {
			retained := uint64(len(origin.ID)+len(origin.Layer)+len(origin.Project)+len(origin.Mode)) + 1024
			if err := a.ChargeNativeDeltaBytes("config_sidecar_origins", retained); err != nil {
				return err
			}
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
	// The entry was already validated. The first byte gives the value's shape
	// without decoding a second copy of a potentially large string or object.
	presence := "number"
	switch bytes.TrimSpace(data)[0] {
	case '{':
		presence = "object"
	case '[':
		presence = "array"
	case '"':
		presence = "text"
	case 't', 'f':
		presence = "boolean"
	case 'n':
		presence = "null"
	}
	return driver.SourcePlaneBytesV1{Presence: presence, Size: uint64(len(data)), SHA256: digest(data)}
}
