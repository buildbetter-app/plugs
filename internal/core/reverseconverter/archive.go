package reverseconverter

import (
	"archive/tar"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"sort"
	"strings"
	"time"
	"unicode/utf8"
)

// Read validates a bounded USTAR plug-dump/1 archive and returns detached
// records. It never opens a Plug store or invokes a Beads binary.
func Read(r io.Reader) (Dump, error) {
	if r == nil {
		return Dump{}, refusal("nil archive reader")
	}
	tr := tar.NewReader(io.LimitReader(r, MaxArchiveSize+1))
	members := make(map[string][]byte)
	var names []string
	var total int64
	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return Dump{}, refusal("read tar archive: %v", err)
		}
		if header.Name == "" || path.Clean(header.Name) != header.Name || strings.HasPrefix(header.Name, "/") || strings.HasPrefix(header.Name, "../") || header.Name == ".." {
			return Dump{}, refusal("unsafe archive member %q", header.Name)
		}
		if header.Typeflag != tar.TypeReg && header.Typeflag != tar.TypeRegA {
			return Dump{}, refusal("unsupported archive member %q type %d", header.Name, header.Typeflag)
		}
		if header.Mode != 0o600 || header.Uid != 0 || header.Gid != 0 || header.Linkname != "" || header.Uname != "" || header.Gname != "" || header.Devmajor != 0 || header.Devminor != 0 || len(header.PAXRecords) != 0 || header.Format != tar.FormatUSTAR || !canonicalArchiveTime(header.ModTime) || !canonicalArchiveTime(header.AccessTime) || !canonicalArchiveTime(header.ChangeTime) {
			return Dump{}, refusal("noncanonical archive metadata for %q", header.Name)
		}
		if _, exists := members[header.Name]; exists {
			return Dump{}, refusal("duplicate archive member %q", header.Name)
		}
		if header.Size < 0 || header.Size > MaxArchiveSize || total > MaxArchiveSize-header.Size {
			return Dump{}, refusal("archive exceeds %d bytes", MaxArchiveSize)
		}
		data, err := io.ReadAll(io.LimitReader(tr, MaxArchiveSize+1))
		if err != nil {
			return Dump{}, refusal("read archive member %q: %v", header.Name, err)
		}
		if int64(len(data)) != header.Size {
			return Dump{}, refusal("archive member %q size mismatch", header.Name)
		}
		total += int64(len(data))
		members[header.Name] = append([]byte(nil), data...)
		names = append(names, header.Name)
	}
	if len(names) == 0 || names[0] != "manifest.json" {
		return Dump{}, refusal("manifest.json must be the first archive member")
	}
	for i := 2; i < len(names); i++ {
		if names[i-1] >= names[i] {
			return Dump{}, refusal("archive members are not strictly sorted")
		}
	}
	var manifest Manifest
	if err := decodeStrict(members["manifest.json"], &manifest); err != nil {
		return Dump{}, refusal("decode manifest: %v", err)
	}
	if manifest.Format != PlugDumpFormat || manifest.Schema != 1 || manifest.Reference != BeadsReference || manifest.MainVersion != 72 || manifest.IgnoredVersion != 28 || manifest.StoreID == "" || manifest.Machine == "" || manifest.Scope != "offline-head-and-source-archives" {
		return Dump{}, refusal("unsupported plug-dump format, schema, reference or scope")
	}
	if manifest.History != "unavailable" && manifest.History != "available-unverified" {
		return Dump{}, refusal("history disposition must be unavailable or available-unverified")
	}
	if len(manifest.Warnings) == 0 || len(manifest.Verification) == 0 {
		return Dump{}, refusal("manifest is missing conversion disclosures")
	}
	if err := verifyMembers(manifest, members); err != nil {
		return Dump{}, err
	}
	if data := members["config/sources.json"]; len(data) == 0 {
		return Dump{}, refusal("missing configuration provenance")
	} else {
		var sources []map[string]any
		if err := decodeStrict(data, &sources); err != nil {
			return Dump{}, refusal("decode configuration provenance: %v", err)
		}
	}
	records := make(map[string][]Record)
	for _, kind := range manifest.Kinds {
		rows, err := decodeRecords(kind.Name, members[kind.File])
		if err != nil {
			return Dump{}, refusal("decode %s: %v", kind.File, err)
		}
		if len(rows) != kind.Count {
			return Dump{}, refusal("kind %q declares %d rows but contains %d", kind.Name, kind.Count, len(rows))
		}
		records[kind.Name] = rows
	}
	return Dump{Manifest: manifest, Records: records, Members: cloneMembers(members)}, nil
}

func canonicalArchiveTime(value time.Time) bool {
	return value.IsZero() || value.Equal(time.Unix(0, 0).UTC())
}

func ReadBytes(data []byte) (Dump, error) { return Read(bytes.NewReader(data)) }

func decodeStrict(data []byte, dst any) error {
	if !utf8.Valid(data) {
		return fmt.Errorf("JSON must be UTF-8")
	}
	if err := rejectDuplicateJSONKeys(data); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(dst); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return fmt.Errorf("trailing JSON value")
		}
		return err
	}
	return nil
}

// rejectDuplicateJSONKeys is deliberately run before decoding into maps. The
// standard library keeps the last duplicate key, which would make a malformed
// validated dump appear to have a different source record than the bytes that
// were signed/checksummed.
func rejectDuplicateJSONKeys(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := checkJSONValue(decoder, 0); err != nil {
		return err
	}
	if _, err := decoder.Token(); err != io.EOF {
		if err == nil {
			return fmt.Errorf("trailing JSON value")
		}
		return err
	}
	return nil
}

func checkJSONValue(decoder *json.Decoder, depth int) error {
	if depth > 128 {
		return fmt.Errorf("JSON nesting exceeds 128")
	}
	token, err := decoder.Token()
	if err != nil {
		return fmt.Errorf("invalid JSON: %w", err)
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delim {
	case '{':
		seen := map[string]bool{}
		for decoder.More() {
			key, err := decoder.Token()
			if err != nil {
				return fmt.Errorf("invalid JSON object: %w", err)
			}
			name, ok := key.(string)
			if !ok || seen[name] {
				return fmt.Errorf("duplicate or invalid JSON object key")
			}
			seen[name] = true
			if err := checkJSONValue(decoder, depth+1); err != nil {
				return err
			}
		}
	case '[':
		for decoder.More() {
			if err := checkJSONValue(decoder, depth+1); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("invalid JSON delimiter")
	}
	if _, err := decoder.Token(); err != nil {
		return fmt.Errorf("invalid JSON container: %w", err)
	}
	return nil
}

func decodeRecords(kind string, data []byte) ([]Record, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("record member is empty")
	}
	if data[len(data)-1] != '\n' {
		return nil, fmt.Errorf("record member is not newline terminated")
	}
	var rows []Record
	lastKey := ""
	lines := bytes.Split(data, []byte{'\n'})
	for i, line := range lines {
		if len(bytes.TrimSpace(line)) == 0 {
			if i == len(lines)-1 {
				continue
			}
			return nil, fmt.Errorf("blank record line %d", i+1)
		}
		var row Record
		if err := decodeStrict(line, &row); err != nil {
			return nil, fmt.Errorf("line %d: %w", i+1, err)
		}
		if row.Kind != kind || row.ID == "" || row.Body == nil || row.Source.Table == "" || row.Source.Fields == nil {
			return nil, fmt.Errorf("line %d has incomplete record identity/provenance", i+1)
		}
		key := row.Project + "\x00" + row.ID
		if key <= lastKey {
			return nil, fmt.Errorf("line %d is unordered or duplicates a prior record", i+1)
		}
		lastKey = key
		rows = append(rows, row)
	}
	return rows, nil
}

func verifyMembers(manifest Manifest, members map[string][]byte) error {
	if _, ok := members["manifest.json"]; !ok {
		return refusal("missing manifest.json")
	}
	declared := make(map[string]bool, len(manifest.Files))
	for name, file := range manifest.Files {
		if name == "manifest.json" || name == "" || path.Clean(name) != name {
			return refusal("invalid manifest file entry %q", name)
		}
		if name != "config/sources.json" && (!strings.HasPrefix(name, "records/") || !strings.HasSuffix(name, ".ndjson")) {
			return refusal("unsupported manifest file entry %q", name)
		}
		data, ok := members[name]
		if !ok {
			return refusal("manifest declares missing member %q", name)
		}
		if file.Size != int64(len(data)) {
			return refusal("manifest size mismatch for %q", name)
		}
		sum := sha256.Sum256(data)
		if file.SHA256 != hex.EncodeToString(sum[:]) {
			return refusal("manifest checksum mismatch for %q", name)
		}
		declared[name] = true
	}
	for name := range members {
		if name != "manifest.json" && !declared[name] {
			return refusal("unaccounted archive member %q", name)
		}
	}
	seenKinds := map[string]bool{}
	for i, kind := range manifest.Kinds {
		if kind.Name == "" || kind.Version != 1 || kind.File != "records/"+kind.Name+".ndjson" || kind.Count < 1 || seenKinds[kind.Name] || i > 0 && manifest.Kinds[i-1].Name >= kind.Name {
			return refusal("invalid or unsorted kind registry")
		}
		if !strings.HasPrefix(kind.File, "records/") || path.Clean(kind.File) != kind.File || !declared[kind.File] || seenKinds[kind.File] {
			return refusal("invalid kind registry member %q", kind.Name)
		}
		seenKinds[kind.Name] = true
		seenKinds[kind.File] = true
	}
	if len(manifest.Kinds) == 0 {
		return refusal("empty kind registry")
	}
	return nil
}

func cloneMembers(input map[string][]byte) map[string][]byte {
	output := make(map[string][]byte, len(input))
	for key, value := range input {
		output[key] = append([]byte(nil), value...)
	}
	return output
}

func refusal(format string, args ...any) *Refusal {
	return &Refusal{Reasons: []string{fmt.Sprintf(format, args...)}}
}

func sortedRecordKinds(records map[string][]Record) []string {
	kinds := make([]string, 0, len(records))
	for kind := range records {
		kinds = append(kinds, kind)
	}
	sort.Strings(kinds)
	return kinds
}
