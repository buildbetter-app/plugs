package driver

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
)

func IsNativeHeadHistoryMarker(ref Ref, kind string) bool {
	return kind == "beads-archive" && ref.IsID() && ref.Branch.IsZero() && ref.ID == OfflineHistoryEvidenceID
}

// ReadNativeHistoryMarkerFields keeps the original JSON values and rejects
// duplicate outer fields. The serialized marker is already a bounded record.
func ReadNativeHistoryMarkerFields(raw []byte) (map[string]json.RawMessage, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	if token, err := decoder.Token(); err != nil || token != json.Delim('{') {
		return nil, fmt.Errorf("native history marker must be an object")
	}
	fields := map[string]json.RawMessage{}
	for decoder.More() {
		token, err := decoder.Token()
		name, ok := token.(string)
		if err != nil || !ok || fields[name] != nil || len(fields) == 64 {
			return nil, fmt.Errorf("invalid native history marker field")
		}
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return nil, err
		}
		fields[name] = value
	}
	if token, err := decoder.Token(); err != nil || token != json.Delim('}') {
		return nil, fmt.Errorf("incomplete native history marker")
	}
	if _, err := decoder.Token(); err != io.EOF {
		return nil, fmt.Errorf("trailing native history marker data")
	}
	return fields, nil
}

// CheckReverseRecordJSON bounds JSON expansion before decoding. A transcript
// marker streams its two ordered ledgers one field at a time, so the bound
// applies to each ledger entry instead of the aggregate number of entries.
func CheckReverseRecordJSON(ref Ref, kind string, raw []byte, reserve int) error {
	if !IsNativeHeadHistoryMarker(ref, kind) {
		_, err := ReverseJSONStructure(raw, reserve)
		return err
	}
	fields, err := ReadNativeHistoryMarkerFields(raw)
	if err != nil {
		return err
	}
	for name, value := range fields {
		if name != "mapping" && name != "sourceFrames" {
			if err := CheckReverseJSONShape(value); err != nil {
				return err
			}
			continue
		}
		// Some legacy marker formats spell an empty ordered ledger as null.
		if bytes.Equal(value, []byte("null")) {
			continue
		}
		decoder := json.NewDecoder(bytes.NewReader(value))
		if token, err := decoder.Token(); err != nil || token != json.Delim('[') {
			return fmt.Errorf("invalid native history marker ledger")
		}
		var entry json.RawMessage
		for decoder.More() {
			if err := decoder.Decode(&entry); err != nil {
				return err
			}
			if err := CheckReverseJSONShape(entry); err != nil {
				return err
			}
		}
		if token, err := decoder.Token(); err != nil || token != json.Delim(']') {
			return fmt.Errorf("incomplete native history marker ledger")
		}
	}
	return nil
}
