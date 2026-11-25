package dumpformat

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

const SourceConfigMember = "config/sources.json"

func streamJSONElements(data []byte, visit func([]byte) error) error {
	return visitJSONElements(bytes.NewReader(data), true, configAccounting{}, func(raw []byte, _ uint64) error { return visit(raw) })
}

// visitJSONElements frames one JSON array element at a time. It checks the
// wire limit during framing, before an oversized element can grow the buffer.
func visitJSONElements(input io.Reader, bounded bool, accounting configAccounting, visit func([]byte, uint64) error) error {
	const readerBytes = 4096
	work := accounting.scope()
	defer work.release()
	if err := work.charge("config_reader_buffer", readerBytes); err != nil {
		return err
	}
	reader := bufio.NewReaderSize(input, readerBytes)
	limit := MaxArchiveBytes
	if bounded {
		limit = driver.NativeHeadRecordBytes
	}
	first, err := readJSONNonspace(reader)
	if err != nil || first != '[' {
		return fmt.Errorf("configuration provenance must be a JSON array")
	}
	next, err := readJSONNonspace(reader)
	var raw []byte
	for err == nil && next != ']' {
		raw, err = readJSONElement(reader, raw[:0], next, limit, bounded, work)
		if err != nil {
			return err
		}
		var footprint uint64
		if bounded {
			footprint, err = driver.ReverseJSONFootprint(raw, 1)
			if err != nil {
				return err
			}
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
func readJSONElement(reader *bufio.Reader, raw []byte, first byte, limit int, bounded bool, work configAccounting) ([]byte, error) {
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
		if len(raw) == limit {
			return nil, &driver.ReverseJSONLimitError{
				Resource: "value_bytes", Measured: uint64(limit) + 1, Limit: uint64(limit),
			}
		}
		if len(raw) == cap(raw) {
			size := min(limit, max(4096, cap(raw)*2))
			if err := work.charge("config_wire_element", uint64(size)); err != nil {
				return nil, err
			}
			next := make([]byte, len(raw), size)
			copy(next, raw)
			if cap(raw) != 0 {
				work.releaseBytes("config_wire_element", uint64(cap(raw)))
			}
			raw = next
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
			if bounded && depth+1 > driver.NativeHeadJSONDepth {
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

// configEntrySource is a reusable view over capture bytes. A scoped view
// presents one project's entries with their aggregate identity removed.
type configEntrySource struct {
	data       []byte
	accounting configAccounting
	bounded    bool
	identity   *SourceIdentity
}

func (s configEntrySource) visit(visit func(externalEntry) error) error {
	return streamConfigReader(bytes.NewReader(s.data), s.bounded, s.accounting, func(entry externalEntry, _ []byte) error {
		if s.identity != nil {
			if entry.Identity == nil || *entry.Identity != *s.identity {
				return nil
			}
			entry.Identity = nil
		}
		return visit(entry)
	})
}

func (s configEntrySource) reserve(phase string, bytes uint64) error {
	if s.bounded {
		return s.accounting.charge(phase, bytes)
	}
	return nil
}

func streamConfigEntries(data []byte, visit func(externalEntry, []byte) error) error {
	return streamConfigReader(bytes.NewReader(data), false, configAccounting{}, visit)
}

func streamConfigReader(input io.Reader, bounded bool, accounting configAccounting, visit func(externalEntry, []byte) error) error {
	return visitJSONElements(input, bounded, accounting, func(raw []byte, footprint uint64) error {
		if bounded {
			work := accounting.scope()
			defer work.release()
			if err := work.charge("config_element_decoded", footprint); err != nil {
				return err
			}
		}
		var entry externalEntry
		if err := decodeJSONAtDepth(raw, &entry, 1); err != nil {
			return err
		}
		return visit(entry, raw)
	})
}

func (a *Archive) configReader() (io.Reader, bool) {
	if a.collection != nil && a.collection.configOutput != nil {
		return io.NewSectionReader(a.collection.configOutput, 0, a.manifest.Files[SourceConfigMember].Size), true
	}
	data, ok := a.members[SourceConfigMember]
	return bytes.NewReader(data), ok
}

func (a *Archive) visitConfigEntries(owner *NativeDeltaBudget, visit func(externalEntry, []byte) error) error {
	input, ok := a.configReader()
	if !ok {
		return fmt.Errorf("missing captured configuration")
	}
	return streamConfigReader(input, a.collection != nil || a.delta != nil, configAccounting{decoded: owner}, visit)
}

// A reverse head's scratch uses the shared decoded budget and the tighter
// retained-head buffer allowance. Each frame releases both reservations.
type configAccounting struct {
	decoded    *NativeDeltaBudget
	headWindow *NativeDeltaBudget
}

func (a configAccounting) scope() configAccounting {
	if a.decoded != nil {
		a.decoded = a.decoded.Scope()
	}
	if a.headWindow != nil {
		a.headWindow = a.headWindow.Scope()
	}
	return a
}

func (a configAccounting) charge(phase string, bytes uint64) error {
	if a.headWindow != nil {
		if err := a.headWindow.ChargeBytes(phase, bytes); err != nil {
			return err
		}
	}
	if a.decoded != nil {
		if err := a.decoded.ChargeBytes(phase, bytes); err != nil {
			if a.headWindow != nil {
				a.headWindow.ReleaseBytes(phase, bytes)
			}
			return err
		}
	}
	return nil
}

func (a configAccounting) releaseBytes(phase string, bytes uint64) {
	if a.headWindow != nil {
		a.headWindow.ReleaseBytes(phase, bytes)
	}
	if a.decoded != nil {
		a.decoded.ReleaseBytes(phase, bytes)
	}
}

func (a configAccounting) release() {
	if a.headWindow != nil {
		a.headWindow.Release()
	}
	if a.decoded != nil {
		a.decoded.Release()
	}
}
