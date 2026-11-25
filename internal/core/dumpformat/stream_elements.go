package dumpformat

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"unicode/utf8"

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
		limit = driver.ReverseJSONElementBytes
	}
	first, err := readJSONNonspace(reader)
	if err != nil || first != '[' {
		return fmt.Errorf("configuration provenance must be a JSON array")
	}
	next, err := readJSONNonspace(reader)
	var raw []byte
	var index uint64
	for err == nil && next != ']' {
		raw, err = readJSONElement(reader, raw[:0], next, limit, bounded, work)
		if err != nil {
			return driver.LocateReverseJSONLimit(err, SourceConfigMember, "$[]", index)
		}
		var footprint uint64
		if bounded {
			footprint, err = driver.ReverseJSONElementFootprint(raw, 1)
			if err != nil {
				return driver.LocateReverseJSONLimit(err, SourceConfigMember, "$[]", index)
			}
		}
		if !json.Valid(raw) {
			return fmt.Errorf("invalid configuration element")
		}
		if err := visit(raw, footprint); err != nil {
			return err
		}
		index++
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
		var entry externalEntry
		if bounded {
			work := accounting.scope()
			defer work.release()
			// The graph and scalar unescaping share this reservation. The
			// framer accounts its input slice separately.
			if err := work.charge("config_element_decoded", footprint); err != nil {
				return err
			}
			if err := decodeStreamedConfig(raw, &entry); err != nil {
				return err
			}
		} else if err := decodeJSONAtDepth(raw, &entry, 1); err != nil {
			return err
		}
		return visit(entry, raw)
	})
}

// decodeStreamedConfig receives a JSON-valid, depth-admitted element. It
// checks the exact typed shape and uses the checked map directly so a wide
// values object is decoded once. Number spellings stay as json.Number.
func decodeStreamedConfig(raw []byte, entry *externalEntry) error {
	if !utf8.Valid(raw) {
		return fmt.Errorf("JSON must be UTF-8")
	}
	if err := checkUnicodeEscapes(raw); err != nil {
		return err
	}
	reader := retainedHeadJSON{rest: raw}
	value, err := reader.decodeAny()
	if err != nil {
		return err
	}
	if err := checkShape(value, reflect.TypeFor[externalEntry]()); err != nil {
		return err
	}
	fields := value.(map[string]any)
	input := fields["input"].(map[string]any)
	precedence, err := strconv.Atoi(string(input["precedence"].(json.Number)))
	if err != nil {
		return fmt.Errorf("invalid configuration precedence")
	}
	entry.Input.Family = input["family"].(string)
	entry.Input.Path = input["path"].(string)
	entry.Input.State = input["state"].(string)
	entry.Input.Precedence = precedence
	if identityValue, ok := fields["identity"]; ok {
		if err := checkShape(identityValue, reflect.TypeFor[SourceIdentity]()); err != nil {
			return fmt.Errorf("member identity: %w", err)
		}
		identityFields := identityValue.(map[string]any)
		identity := SourceIdentity{StoreID: identityFields["store_id"].(string), Project: identityFields["project"].(string)}
		if !validSourceIdentity(identity) {
			return fmt.Errorf("source identity requires nonempty unambiguous store/project")
		}
		entry.Identity = &identity
	}
	entry.Values, _ = fields["values"].(map[string]any)
	return nil
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

// One config frame and its decoded representation share the capture's
// decoded budget. The retained head wire window accounts the source bytes.
type configAccounting struct {
	decoded *NativeDeltaBudget
}

func (a configAccounting) scope() configAccounting {
	if a.decoded != nil {
		a.decoded = a.decoded.Scope()
	}
	return a
}

func (a configAccounting) charge(phase string, bytes uint64) error {
	if a.decoded != nil {
		return a.decoded.ChargeBytes(phase, bytes)
	}
	return nil
}

func (a configAccounting) releaseBytes(phase string, bytes uint64) {
	if a.decoded != nil {
		a.decoded.ReleaseBytes(phase, bytes)
	}
}

func (a configAccounting) release() {
	if a.decoded != nil {
		a.decoded.Release()
	}
}
