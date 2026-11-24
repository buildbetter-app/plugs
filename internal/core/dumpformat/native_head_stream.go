package dumpformat

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// NativeHeadStream holds the validated metadata and ranges for records and evidence
// blobs. Its caller owns the input and must keep it open through publication.
type NativeHeadStream struct {
	Archive *Archive
	input   io.ReaderAt
	ranges  map[string]int64
}

const nativeHeadCopyBytes = 1 << 20
const MaxNativeHeadManifestBytes = 1 << 20

// ReadNativeHeadStream admits the offline native head profile. Only the
// small metadata members are retained. Record and blob bodies remain in the
// caller's seekable input until publication.
func ReadNativeHeadStream(ctx context.Context, raw io.ReaderAt, size int64) (*NativeHeadStream, error) {
	if raw == nil || size < 1024 || size%512 != 0 {
		return nil, fmt.Errorf("invalid native tar size")
	}
	section := io.NewSectionReader(raw, 0, size)
	archiveHash := sha256.New()
	reader := tar.NewReader(io.TeeReader(section, archiveHash))
	out := &NativeHeadStream{input: raw, ranges: map[string]int64{}}
	buf := make([]byte, nativeHeadCopyBytes)
	nativeHash := sha256.New()
	members := map[string][]byte{}
	var manifest Manifest
	var observed int
	var material int64
	last := ""
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		header, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		path := header.Name
		if header.Typeflag != tar.TypeReg || header.Size < 0 || header.Size > MaxArchiveBytes ||
			header.Mode != 0o600 || header.Uid != 0 || header.Gid != 0 || header.Linkname != "" || header.Uname != "" || header.Gname != "" || header.Devmajor != 0 || header.Devminor != 0 ||
			len(header.PAXRecords) != 0 || header.Format != tar.FormatUSTAR || !header.ModTime.Equal(time.Unix(0, 0)) || !allowedMember(path) {
			return nil, fmt.Errorf("noncanonical native member %q", path)
		}
		if observed == 0 {
			if path != "manifest.json" || header.Size > MaxNativeHeadManifestBytes {
				return nil, fmt.Errorf("native manifest must be first and within its buffer bound")
			}
			manifestRaw := make([]byte, header.Size)
			if _, err := io.ReadFull(reader, manifestRaw); err != nil {
				return nil, err
			}
			if err := decodeJSON(manifestRaw, &manifest); err != nil {
				return nil, err
			}
			if err := validateNativeHeadManifest(manifest); err != nil {
				return nil, err
			}
			var fields map[string]json.RawMessage
			if err := json.Unmarshal(manifestRaw, &fields); err != nil {
				return nil, err
			}
			for _, name := range []string{"source_fields_contract", "source_plane_address_contract", "status_category_contract", "native_history_contract", "source_family_contract", "source_identity_contract"} {
				if _, ok := fields[name]; ok {
					return nil, fmt.Errorf("offline source marker %q is unsupported in a native head", name)
				}
			}
			observed++
			continue
		}
		file, exists := manifest.Files[path]
		if path <= last || path == "manifest.json" || !exists || header.Size != file.Size {
			return nil, fmt.Errorf("unbound, repeated, or noncanonical native member %q", path)
		}
		last = path
		observed++
		_, _ = io.WriteString(nativeHash, path)
		memberHash := sha256.New()
		sink := io.MultiWriter(memberHash, nativeHash)
		name, isBlob := strings.CutPrefix(path, "blobs/")
		if isBlob {
			decoded, err := hex.DecodeString(name)
			if err != nil || len(decoded) != sha256.Size || name != file.SHA256 {
				return nil, fmt.Errorf("invalid native evidence path %q", path)
			}
		} else {
			material += header.Size
			if material > MaxArchiveBytes {
				return nil, fmt.Errorf("native material image exceeds buffer bound")
			}
		}
		switch {
		case isBlob || path == "records/native.ndjson":
			offset, err := section.Seek(0, io.SeekCurrent)
			if err != nil {
				return nil, err
			}
			out.ranges[path] = offset
			n, err := io.CopyBuffer(sink, reader, buf)
			if err != nil || n != header.Size {
				return nil, fmt.Errorf("incomplete native member %q", path)
			}
		case path == NativeAllocatorMember || path == "config/sources.json" || path == "versions.ndjson" || path == "changesets.ndjson":
			if header.Size > MaxNativeHeadManifestBytes {
				return nil, fmt.Errorf("native metadata %q exceeds buffer bound", path)
			}
			data := make([]byte, header.Size)
			n, err := io.ReadFull(io.TeeReader(reader, sink), data)
			if err != nil || int64(n) != header.Size {
				return nil, fmt.Errorf("incomplete native material member %q", path)
			}
			members[path] = data
		default:
			return nil, fmt.Errorf("unexpected reverse head member %q", path)
		}
		if hex.EncodeToString(memberHash.Sum(nil)) != file.SHA256 {
			return nil, fmt.Errorf("native checksum mismatch for %q", path)
		}
	}
	// The reverse writer emits exactly two zero EOF blocks. Check both even
	// when tar.Reader reports EOF on an already exhausted stream.
	end := make([]byte, 1024)
	if _, err := raw.ReadAt(end, size-1024); err != nil || !bytes.Equal(end, make([]byte, len(end))) {
		return nil, fmt.Errorf("native archive end blocks differ")
	}
	offset, err := section.Seek(0, io.SeekCurrent)
	if err != nil || offset != size {
		return nil, fmt.Errorf("trailing or incomplete native tar data")
	}
	if observed != len(manifest.Files)+1 || hex.EncodeToString(nativeHash.Sum(nil)) != manifest.NativeDigest {
		return nil, fmt.Errorf("native archive inventory or digest differs")
	}
	if _, ok := out.ranges["records/native.ndjson"]; !ok {
		return nil, fmt.Errorf("native archive lacks records/native.ndjson")
	}
	for _, path := range []string{NativeAllocatorMember, "config/sources.json", "versions.ndjson", "changesets.ndjson"} {
		if _, ok := members[path]; !ok {
			return nil, fmt.Errorf("native archive lacks %q", path)
		}
	}
	if err := validateNativeAllocatorMember(members[NativeAllocatorMember]); err != nil {
		return nil, err
	}
	if len(members["versions.ndjson"])+len(members["changesets.ndjson"]) != 0 {
		return nil, fmt.Errorf("reverse archive cannot declare version history")
	}
	out.Archive = &Archive{manifest: manifest, sha256: hex.EncodeToString(archiveHash.Sum(nil)), native: true, members: members}
	return out, nil
}

func validateNativeHeadManifest(m Manifest) error {
	if m.Format != Format || m.Schema != 1 || m.Scope != "plug-native-store" || m.History != "head" ||
		m.NativeStoreID == "" || m.NativeHead == "" || m.NativeIncarnation == "" || m.NativeDigest == "" ||
		m.RecordCount < 0 || m.VersionCount != 0 || m.ChangesetCount != 0 ||
		m.SourceFieldsContract != "" || m.SourcePlaneAddressContract != "" || m.StatusCategoryContract != "" ||
		m.NativeHistoryContract != "" || m.SourceFamilyContract != "" {
		return fmt.Errorf("unsupported streamed native head manifest")
	}
	if p := m.NativeReverseProjection; p != nil {
		if p.ProfileCopies < 0 || p.ProfileCopies > m.RecordCount ||
			p.DiscardedOrigins != nil && (p.DiscardedOrigins.Records <= 0 || p.DiscardedOrigins.Fields < p.DiscardedOrigins.Records) {
			return fmt.Errorf("invalid reverse projection counts")
		}
	}
	return nil
}

func (s *NativeHeadStream) Member(path string) (*io.SectionReader, bool) {
	offset, ok := s.ranges[path]
	if !ok {
		return nil, false
	}
	return io.NewSectionReader(s.input, offset, s.Archive.manifest.Files[path].Size), true
}

// DecodeReverseOriginFields applies the same strict JSON validation as a
// native archive record before its body can be reduced to an exact count.
func DecodeReverseOriginFields(raw []byte) (int, error) {
	if err := driver.CheckReverseJSONShape(raw); err != nil {
		return 0, err
	}
	var body map[string]any
	if err := decodeJSON(raw, &body); err != nil {
		return 0, err
	}
	if body == nil {
		return 0, fmt.Errorf("discarded native origin must be an object")
	}
	return CountFieldInstances(body), nil
}

// ValidateNativeHeadRecord validates one streamed native NDJSON record.
func ValidateNativeHeadRecord(raw []byte) error {
	if err := driver.CheckReverseJSONWire(raw); err != nil {
		return err
	}
	var record struct {
		Ref      model.Ref       `json:"ref"`
		Kind     string          `json:"kind"`
		Revision model.Revision  `json:"revision"`
		Body     json.RawMessage `json:"body"`
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&record); err != nil || !knownNativeKinds[record.Kind] ||
		record.Ref.Validate() != nil || len(record.Body) == 0 {
		return fmt.Errorf("invalid streamed native record envelope")
	}
	if err := driver.CheckReverseRecordJSON(record.Ref, record.Kind, record.Body, 1); err != nil {
		return err
	}
	_, err := strictReverseFieldCount(raw, true)
	return err
}

// CountReverseHistoryMarkerFields uses a token walk so counting a complete
// transcript never materializes its decoded arrays.
func CountReverseHistoryMarkerFields(raw []byte) (int, error) {
	return strictReverseFieldCount(raw, false)
}

func strictReverseFieldCount(raw []byte, envelope bool) (int, error) {
	if !utf8.Valid(raw) {
		return 0, fmt.Errorf("invalid UTF-8 in streamed native record")
	}
	if err := checkUnicodeEscapes(raw); err != nil {
		return 0, err
	}
	check := json.NewDecoder(bytes.NewReader(raw))
	check.UseNumber()
	var fields int
	var err error
	if envelope {
		err = checkNativeHeadEnvelope(check)
	} else {
		fields, err = countJSONValue(check, 0)
	}
	if err != nil {
		return 0, err
	}
	if _, err := check.Token(); err != io.EOF {
		return 0, fmt.Errorf("trailing native JSON data")
	}
	return fields, nil
}

// encoding/json accepts case-insensitive struct keys. The streamed transport
// binds exact wire names and requires each of the four record fields once.
func checkNativeHeadEnvelope(decoder *json.Decoder) error {
	if token, err := decoder.Token(); err != nil || token != json.Delim('{') {
		return fmt.Errorf("native record must be an object")
	}
	seen := make(map[string]bool, 4)
	for decoder.More() {
		token, err := decoder.Token()
		name, ok := token.(string)
		if err != nil || !ok || seen[name] {
			return fmt.Errorf("invalid or duplicate native record field")
		}
		switch name {
		case "ref", "kind", "revision", "body":
			seen[name] = true
		default:
			return fmt.Errorf("unsupported native record field")
		}
		if err := checkValue(decoder, 1); err != nil {
			return err
		}
	}
	if token, err := decoder.Token(); err != nil || token != json.Delim('}') || len(seen) != 4 {
		return fmt.Errorf("incomplete native record envelope")
	}
	return nil
}

func CountFieldInstances(value any) int {
	switch value := value.(type) {
	case map[string]any:
		if len(value) == 0 {
			return 1
		}
		total := 0
		for _, child := range value {
			total += CountFieldInstances(child)
		}
		return total
	case []any:
		if len(value) == 0 {
			return 1
		}
		total := 0
		for _, child := range value {
			total += CountFieldInstances(child)
		}
		return total
	default:
		return 1
	}
}
