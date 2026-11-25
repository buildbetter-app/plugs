package dumpformat

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

const sourceConfigMember = "config/sources.json"

func streamJSONElements(data []byte, visit func([]byte) error) error {
	return visitJSONElements(bytes.NewReader(data), func(raw []byte, _ uint64) error { return visit(raw) })
}

// visitJSONElements frames one JSON array element at a time. It checks the
// wire limit during framing, before an oversized element can grow the buffer.
func visitJSONElements(input io.Reader, visit func([]byte, uint64) error) error {
	reader := bufio.NewReader(input)
	first, err := readJSONNonspace(reader)
	if err != nil || first != '[' {
		return fmt.Errorf("configuration provenance must be a JSON array")
	}
	next, err := readJSONNonspace(reader)
	var raw []byte
	for err == nil && next != ']' {
		raw, err = readJSONElement(reader, raw[:0], next)
		if err != nil {
			return err
		}
		footprint, err := driver.ReverseJSONFootprint(raw, 1)
		if err != nil {
			return err
		}
		if !json.Valid(raw) {
			return fmt.Errorf("invalid configuration element")
		}
		if err := visit(raw, footprint); err != nil {
			return err
		}
		next, err = readJSONNonspace(reader)
		if err == nil && next == ',' {
			next, err = readJSONNonspace(reader)
			if err == nil && next != ']' {
				continue
			}
			return fmt.Errorf("missing configuration element after separator")
		}
		break
	}
	if err != nil || next != ']' {
		return fmt.Errorf("invalid configuration array terminator")
	}
	if _, err := readJSONNonspace(reader); err != io.EOF {
		return fmt.Errorf("configuration provenance has trailing data")
	}
	return nil
}

func readJSONNonspace(reader *bufio.Reader) (byte, error) {
	for {
		b, err := reader.ReadByte()
		if err != nil || !jsonSpace(b) {
			return b, err
		}
	}
}

func jsonSpace(b byte) bool {
	return b == ' ' || b == '\t' || b == '\r' || b == '\n'
}

// Delimiter tracking is purely lexical. json.Valid and decodeJSON check
// grammar and the typed shape once the bounded frame is available.
func readJSONElement(reader *bufio.Reader, raw []byte, first byte) ([]byte, error) {
	container := first == '{' || first == '['
	scalar := !container && first != '"'
	depth := 0
	quoted, escaped := false, false
	for b := first; ; {
		if scalar && len(raw) != 0 && (b == ',' || b == ']' || jsonSpace(b)) {
			if err := reader.UnreadByte(); err != nil {
				return nil, err
			}
			return raw, nil
		}
		if len(raw) == driver.NativeHeadRecordBytes {
			return nil, &driver.ReverseJSONLimitError{
				Resource: "value_bytes", Measured: driver.NativeHeadRecordBytes + 1, Limit: driver.NativeHeadRecordBytes,
			}
		}
		raw = append(raw, b)
		switch {
		case escaped:
			escaped = false
		case quoted:
			escaped = b == '\\'
			if b == '"' {
				quoted = false
				if !container && !scalar {
					return raw, nil
				}
			}
		case b == '"':
			quoted = true
		case b == '[' || b == '{':
			depth++
			if depth+1 > driver.NativeHeadJSONDepth {
				return nil, &driver.ReverseJSONLimitError{
					Resource: "nesting_depth", Measured: uint64(depth + 1), Limit: driver.NativeHeadJSONDepth,
				}
			}
		case b == ']' || b == '}':
			depth--
			if container && depth == 0 {
				return raw, nil
			}
		}
		var err error
		b, err = reader.ReadByte()
		if err != nil {
			return nil, fmt.Errorf("incomplete configuration element: %w", err)
		}
	}
}

func streamConfigEntries(data []byte, visit func(externalEntry, []byte) error) error {
	return streamConfigReader(bytes.NewReader(data), nil, visit)
}

func streamConfigReader(input io.Reader, owner *NativeDeltaBudget, visit func(externalEntry, []byte) error) error {
	return visitJSONElements(input, func(raw []byte, footprint uint64) error {
		if owner != nil {
			work := owner.Scope()
			defer work.Release()
			if err := work.ChargeBytes("config_element_decoded", footprint); err != nil {
				return err
			}
		}
		var entry externalEntry
		if err := decodeJSON(raw, &entry); err != nil {
			return err
		}
		return visit(entry, raw)
	})
}

func (a *Archive) configReader() (io.Reader, bool) {
	if a.collection != nil && a.collection.configOutput != nil {
		return io.NewSectionReader(a.collection.configOutput, 0, a.manifest.Files[sourceConfigMember].Size), true
	}
	data, ok := a.members[sourceConfigMember]
	return bytes.NewReader(data), ok
}

func (a *Archive) visitConfigEntries(owner *NativeDeltaBudget, visit func(externalEntry, []byte) error) error {
	input, ok := a.configReader()
	if !ok {
		return fmt.Errorf("missing captured configuration")
	}
	return streamConfigReader(input, owner, visit)
}
