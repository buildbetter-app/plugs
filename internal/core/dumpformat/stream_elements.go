package dumpformat

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
)

// streamJSONElements visits the raw members of one top-level JSON array.
// It retains only the current element. Callers perform their strict typed
// validation on that element before retaining any decoded representation.
func streamJSONElements(data []byte, visit func([]byte) error) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	token, err := decoder.Token()
	if err != nil {
		return fmt.Errorf("configuration provenance must be a JSON array: %w", err)
	}
	delim, ok := token.(json.Delim)
	if !ok || delim != '[' {
		return fmt.Errorf("configuration provenance must be a JSON array")
	}
	for decoder.More() {
		var raw json.RawMessage
		if err := decoder.Decode(&raw); err != nil {
			return fmt.Errorf("invalid configuration element: %w", err)
		}
		if err := visit([]byte(raw)); err != nil {
			return err
		}
	}
	token, err = decoder.Token()
	if err != nil {
		return fmt.Errorf("invalid configuration array: %w", err)
	}
	if delim, ok := token.(json.Delim); !ok || delim != ']' {
		return fmt.Errorf("invalid configuration array terminator")
	}
	if _, err := decoder.Token(); err != io.EOF {
		if err == nil {
			return fmt.Errorf("configuration provenance has trailing JSON data")
		}
		return fmt.Errorf("configuration provenance has trailing data: %w", err)
	}
	return nil
}

func streamConfigEntries(data []byte, visit func(externalEntry, []byte) error) error {
	return streamJSONElements(data, func(raw []byte) error {
		var entry externalEntry
		if err := decodeJSON(raw, &entry); err != nil {
			return err
		}
		return visit(entry, raw)
	})
}
