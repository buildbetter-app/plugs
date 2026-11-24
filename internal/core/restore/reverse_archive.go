package restore

import (
	"archive/tar"
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// MaxReverseArchiveBytes is the operational disk/I/O ceiling for the offline
// reverse transport. Ordinary dumps and restores keep their existing ceiling.
const MaxReverseArchiveBytes = 2 << 30
const reverseCopyBytes = 1 << 20

// Allow room within the 128 MiB archive working set for the pinned scan,
// individual decoders, profile expansion, and the SQLite publication encoder.
// These limits count material buffers separately from the streamed blob bytes.
const (
	reverseRecordBytes   = driver.NativeHeadRecordBytes
	reverseMaterialBytes = driver.NativeHeadMaterialBytes
	reverseIndexBytes    = 4 << 20
)

const reverseRecordsMember = "records/native.ndjson"

type reverseMember struct {
	size int64
	open func() (io.ReadCloser, error)
}

func reverseBytesMember(raw []byte) reverseMember {
	return reverseMember{int64(len(raw)), func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(raw)), nil
	}}
}

// WriteReverseHead writes a current native backup for an offline reverse
// trial. No bytes are delivered before size admission and validation of all
// input members. As with any writer, an output I/O error may leave a partial
// write. A caller publishing a file should rename an owned temporary file.
func WriteReverseHead(ctx context.Context, store driver.Store, out io.Writer) (dumpformat.Manifest, error) {
	return writeReverseHead(ctx, store, out, MaxReverseArchiveBytes)
}

func writeReverseHead(ctx context.Context, store driver.Store, out io.Writer, limit int64) (dumpformat.Manifest, error) {
	fail := func(err error) (dumpformat.Manifest, error) { return dumpformat.Manifest{}, err }
	ctx = driver.WithNativeReverseRead(ctx)
	source, err := openNativeDumpSource(ctx, store)
	if err != nil {
		return fail(err)
	}
	defer source.reader.Close()
	records, err := os.CreateTemp("", "plug-reverse-records-*")
	if err != nil {
		return fail(err)
	}
	defer os.Remove(records.Name())
	defer records.Close()
	recordMember, count, evidence, projection, err := stageReverseRecords(ctx, source, records)
	if err != nil {
		return fail(err)
	}
	members := map[string]reverseMember{
		reverseRecordsMember:             recordMember,
		dumpformat.NativeAllocatorMember: reverseBytesMember(source.allocatorData),
		"config/sources.json":            reverseBytesMember([]byte("{\"source\":\"plug\",\"history\":\"head\"}\n")),
		"versions.ndjson":                reverseBytesMember(nil),
		"changesets.ndjson":              reverseBytesMember(nil),
	}
	if err := collectReverseMembers(ctx, source.reader, evidence, members); err != nil {
		return fail(err)
	}
	paths := make([]string, 0, len(members))
	files := make(map[string]dumpformat.File, len(members))
	placeholder := memberDigest(nil)
	for path, member := range members {
		paths = append(paths, path)
		files[path] = dumpformat.File{SHA256: placeholder, Size: member.size}
	}
	sort.Strings(paths)
	manifest := nativeDumpManifest(source, "head", placeholder, files, count, 0, 0)
	manifest.NativeReverseProjection = projection
	bound, err := json.Marshal(manifest)
	if err != nil {
		return fail(err)
	}
	archiveSize := int64(1024) + reverseTarMemberSize(int64(len(bound)+1))
	var materialBytes int64
	for path, member := range members {
		archiveSize += reverseTarMemberSize(member.size)
		if !strings.HasPrefix(path, "blobs/") {
			materialBytes += member.size
		}
	}
	if archiveSize > limit {
		return fail(&ArchiveTooLargeError{ArchiveBytes: archiveSize, LimitBytes: limit})
	}
	if len(bound)+1 > dumpformat.MaxNativeHeadManifestBytes ||
		len(source.allocatorData) > dumpformat.MaxNativeHeadManifestBytes || materialBytes > maxDumpBytes {
		return fail(fmt.Errorf("reverse archive metadata or material image exceeds buffer bound"))
	}

	body, err := os.CreateTemp("", "plug-reverse-members-*")
	if err != nil {
		return fail(err)
	}
	defer os.Remove(body.Name())
	defer body.Close()
	buf := make([]byte, reverseCopyBytes)
	nativeHash := sha256.New()
	tw := tar.NewWriter(body)
	for _, path := range paths {
		if err := ctx.Err(); err != nil {
			return fail(err)
		}
		member := members[path]
		if err := writeReverseHeader(tw, path, member.size); err != nil {
			return fail(err)
		}
		_, _ = io.WriteString(nativeHash, path)
		memberHash := sha256.New()
		r, err := member.open()
		if err != nil {
			return fail(err)
		}
		n, copyErr := io.CopyBuffer(io.MultiWriter(tw, nativeHash, memberHash), r, buf)
		if err := errors.Join(copyErr, r.Close()); err != nil {
			return fail(err)
		}
		digest := hex.EncodeToString(memberHash.Sum(nil))
		if n != member.size || strings.HasPrefix(path, "blobs/") && strings.TrimPrefix(path, "blobs/") != digest {
			return fail(fmt.Errorf("reverse archive member %q differs from its commitment", path))
		}
		files[path] = dumpformat.File{SHA256: digest, Size: n}
	}
	if err := tw.Close(); err != nil {
		return fail(err)
	}
	manifest.NativeDigest = hex.EncodeToString(nativeHash.Sum(nil))
	manifestRaw, err := json.Marshal(manifest)
	if err != nil {
		return fail(err)
	}
	if len(manifestRaw) != len(bound) {
		return fail(fmt.Errorf("reverse archive manifest size changed"))
	}
	if _, err := body.Seek(0, io.SeekStart); err != nil {
		return fail(err)
	}
	if err := ctx.Err(); err != nil {
		return fail(err)
	}
	// The body stage already has the final two tar end blocks. Flush only the
	// manifest header, bytes and padding before copying the staged suffix.
	prefix := tar.NewWriter(out)
	if err := writeReverseHeader(prefix, "manifest.json", int64(len(manifestRaw)+1)); err != nil {
		return fail(err)
	}
	if _, err := prefix.Write(append(manifestRaw, '\n')); err != nil {
		return fail(err)
	}
	if err := prefix.Flush(); err != nil {
		return fail(err)
	}
	if _, err := io.CopyBuffer(out, reverseArchiveInput{ctx, body}, buf); err != nil {
		return fail(err)
	}
	return manifest, nil
}

type reverseArchiveInput struct {
	ctx    context.Context
	reader io.Reader
}

func (r reverseArchiveInput) Read(dst []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(dst)
}

func reverseTarMemberSize(size int64) int64 {
	return 512 + (size+511)/512*512
}

func writeReverseHeader(tw *tar.Writer, name string, size int64) error {
	return tw.WriteHeader(&tar.Header{Name: name, Mode: 0o600, Size: size,
		Typeflag: tar.TypeReg, ModTime: time.Unix(0, 0), Format: tar.FormatUSTAR})
}

type reverseRecordSpan struct {
	key         string
	start, size int64
}

// Snapshot scans one record at a time. Keep only the canonical sort index in
// memory, so retained bodies never accumulate while preparing the tar.
func stageReverseRecords(ctx context.Context, s nativeDumpSource, records *os.File) (reverseMember, int, []nativeRecord, *dumpformat.NativeReverseProjection, error) {
	fail := func(err error) (reverseMember, int, []nativeRecord, *dumpformat.NativeReverseProjection, error) {
		return reverseMember{}, 0, nil, nil, err
	}
	binding, err := driver.GetNativeHeadRecords(ctx, s.reader, []model.Ref{
		model.RefByID(reverseImportBindingID, model.Natural64(0)),
		model.RefByID("restore/archive", model.Natural64(0)),
	})
	if err != nil {
		return fail(err)
	}
	var bound []nativeRecord
	for _, row := range binding {
		bound = append(bound, nativeRecord{Ref: row.Ref, Kind: row.Kind, Body: row.Body})
	}
	history := nativeReverseHistoryBinding(ctx, s.reader, bound)
	_, joined := nativeReverseEvidenceDigests(bound)
	var profile json.RawMessage
	if joined && history != nil {
		for _, row := range bound {
			if row.Ref.ID == reverseImportBindingID && row.Kind == "instance_config" {
				profile = reverseBoundProfile(row.Body)
			}
		}
	}
	var projection dumpformat.NativeReverseProjection
	seen := make(map[string]bool)
	var offset, indexBytes int64
	var index []reverseRecordSpan
	var evidence []nativeRecord
	var materialBytes int
	visit := func(row driver.Record) error {
		if err := driver.CheckReverseRecordJSON(row.Ref, row.Kind, row.Body, 1); err != nil {
			return err
		}
		if !json.Valid(row.Body) {
			return fmt.Errorf("reverse archive record has invalid JSON")
		}
		record := nativeRecord{Ref: row.Ref, Kind: row.Kind, Revision: row.Revision, Body: row.Body}
		if record.Ref.IsID() && record.Ref.Branch.IsZero() {
			record.Ref.Branch = model.Natural64(0)
		}
		key, err := driver.EncodeLockKey(driver.LockKey{Kind: driver.LockRecord, Ref: &record.Ref})
		if err != nil {
			return err
		}
		indexBytes += int64(len(key) + 96)
		if indexBytes > reverseIndexBytes || seen[key] {
			return fmt.Errorf("duplicate reverse record or exhausted record index")
		}
		seen[key] = true
		if profile != nil && driver.IsReverseDiscardedOrigin(record.Ref, record.Kind) {
			fields, err := dumpformat.DecodeReverseOriginFields(record.Body)
			if err != nil {
				return err
			}
			if projection.DiscardedOrigins == nil {
				projection.DiscardedOrigins = &dumpformat.NativeReverseOriginLoss{}
			}
			projection.DiscardedOrigins.Records++
			projection.DiscardedOrigins.Fields += fields
			return nil
		}
		bindNativeDumpRecord(&record, s.meta, history)
		materialBytes += len(record.Body)
		if materialBytes > reverseMaterialBytes {
			return fmt.Errorf("reverse archive material image exceeds buffer bound")
		}
		if profile != nil && isReverseProjectRecord(record.Ref, record.Kind) {
			compacted, ok := replaceReverseProfile(record.Body, profile, []byte("null"))
			if ok {
				record.Body = compacted
				projection.ProfileCopies++
			}
		}
		if record.Ref.ID == "restore/archive" {
			evidence = append(evidence, record)
		}
		size, err := writeReverseRecord(records, record)
		if err != nil {
			return err
		}
		indexBytes += int64(len(key) + 64)
		if indexBytes > reverseIndexBytes || offset+size > maxDumpBytes {
			return fmt.Errorf("reverse archive material image exceeds buffer bound")
		}
		index = append(index, reverseRecordSpan{key, offset, size})
		offset += size
		return nil
	}
	if scanner, ok := s.reader.(driver.NativeHeadRecordReader); ok {
		if err := scanner.VisitNativeHeadRecords(ctx, driver.NativeHeadStorageRowBytes, visit); err != nil {
			return fail(err)
		}
	} else if err := visitReversePages(ctx, s.reader, visit); err != nil {
		return fail(err)
	}
	sort.Slice(index, func(i, j int) bool { return index[i].key < index[j].key })

	member := reverseMember{offset, func() (io.ReadCloser, error) {
		return io.NopCloser(&reverseRecordReader{file: records, spans: index}), nil
	}}
	if projection.ProfileCopies == 0 && projection.DiscardedOrigins == nil {
		return member, len(index), evidence, nil, nil
	}
	return member, len(index), evidence, &projection, nil
}

func visitReversePages(ctx context.Context, reader driver.Reader, visit func(driver.Record) error) error {
	var cursor *driver.Cursor
	for {
		page, err := reader.Query(ctx, driver.Query{Limit: 1, Cursor: cursor})
		if err != nil {
			return err
		}
		for _, row := range page.Items {
			if err := visit(row); err != nil {
				return err
			}
		}
		if page.NextCursor == nil {
			return nil
		}
		if reflect.DeepEqual(cursor, page.NextCursor) {
			return fmt.Errorf("reverse archive cursor did not advance")
		}
		cursor = page.NextCursor
	}
}

// Preserve an existing single-line record body verbatim. JSON whitespace
// inside a body belongs to its persisted image and need not be reformatted.
func writeReverseRecord(out io.Writer, record nativeRecord) (int64, error) {
	body := record.Body
	if bytes.IndexByte(body, '\n') >= 0 {
		var compact bytes.Buffer
		if err := json.Compact(&compact, body); err != nil {
			return 0, err
		}
		body = compact.Bytes()
	}
	if err := driver.CheckReverseRecordJSON(record.Ref, record.Kind, body, 1); err != nil {
		return 0, err
	}
	record.Body = nil
	envelope, err := json.Marshal(record)
	if err != nil {
		return 0, err
	}
	prefix := envelope[:len(envelope)-len("null}")]
	size := len(prefix) + len(body) + 1
	if size > reverseRecordBytes {
		return 0, &driver.ReverseJSONLimitError{Resource: "value_bytes", Measured: uint64(size), Limit: reverseRecordBytes}
	}
	for _, chunk := range [][]byte{prefix, body, []byte("}\n")} {
		if _, err := out.Write(chunk); err != nil {
			return 0, err
		}
	}
	return int64(size + 1), nil
}

type reverseRecordReader struct {
	file  *os.File
	spans []reverseRecordSpan
}

func (r *reverseRecordReader) Read(p []byte) (int, error) {
	if len(r.spans) == 0 {
		return 0, io.EOF
	}
	span := &r.spans[0]
	n, err := r.file.ReadAt(p[:min(int64(len(p)), span.size)], span.start)
	// This reader is opened only once and owns the mutable span iterator.
	span.start += int64(n)
	span.size -= int64(n)
	if span.size == 0 {
		r.spans = r.spans[1:]
	}
	return n, err
}

func collectReverseMembers(ctx context.Context, reader driver.Reader, evidence []nativeRecord, members map[string]reverseMember) error {
	digests, joined := nativeReverseEvidenceDigests(evidence)
	if len(digests) == 0 {
		return nil
	}
	streams, ok := reader.(driver.BlobStreamReader)
	if !ok {
		return fmt.Errorf("%w: reverse archive requires snapshot blob streams", driver.ErrUnsupported)
	}
	for digest := range digests {
		hash := driver.Hash(digest)
		r, size, err := streams.OpenBlob(ctx, hash)
		if errors.Is(err, driver.ErrNotFound) && !joined {
			continue
		}
		if err != nil {
			return err
		}
		if err := r.Close(); err != nil {
			return err
		}
		if size < 0 || size > maxDumpBytes {
			return fmt.Errorf("reverse archive evidence member exceeds per-member bound")
		}
		members["blobs/"+digest] = reverseMember{size, func() (io.ReadCloser, error) {
			r, actual, err := streams.OpenBlob(ctx, hash)
			if err == nil && actual != size {
				return nil, errors.Join(fmt.Errorf("reverse archive evidence member size changed"), r.Close())
			}
			return r, err
		}}
	}
	return nil
}

// RestoreReverseHead publishes a streamed offline snapshot into an empty store.
// It shares native restore's protected-record, allocator, and commit checks.
// The host supplies the exact manifest and archive digests and owns the
// seekable input until this operation returns.
func (m *Manager) RestoreReverseHead(ctx context.Context, raw io.ReaderAt, size int64, expectedManifest, expectedArchive string,
	principal model.PrincipalRef, meta model.WriteMeta, presence Presence) (model.Changeset, error) {
	ctx = driver.WithNativeReverseRead(ctx)
	if err := m.requireLease(); err != nil {
		return model.Changeset{}, err
	}
	if err := m.verifyPresence(ctx, "admin.restore.begin", expectedManifest, presence); err != nil {
		return model.Changeset{}, err
	}
	if size > MaxReverseArchiveBytes {
		return model.Changeset{}, &ArchiveTooLargeError{ArchiveBytes: size, LimitBytes: MaxReverseArchiveBytes}
	}
	if _, err := m.emptyTarget(ctx); err != nil {
		return model.Changeset{}, err
	}
	stream, err := dumpformat.ReadNativeHeadStream(ctx, raw, size)
	if err != nil {
		return model.Changeset{}, &model.Error{Data: model.ErrorData{Code: "restore_checksum_failed", Hint: err.Error()}}
	}
	archive := stream.Archive
	manifest := archive.Manifest()
	manifestRaw, err := json.Marshal(manifest)
	if err != nil {
		return model.Changeset{}, err
	}
	if memberDigest(manifestRaw) != expectedManifest || archive.SHA256() != expectedArchive {
		return model.Changeset{}, &model.Error{Data: model.ErrorData{Code: "restore_checksum_failed", Hint: "reverse archive or manifest checksum mismatch"}}
	}
	if err := m.verifyPresence(ctx, "admin.restore.finish", expectedManifest, presence); err != nil {
		return model.Changeset{}, err
	}
	if err := validateSupportedArchive(archive); err != nil {
		return model.Changeset{}, restoreAdmissionError(err)
	}
	return m.apply(ctx, archive, principal, meta, stream)
}

func reverseHeadBlobSource(stream *dumpformat.NativeHeadStream) driver.NativeRestoreBlobSource {
	manifest := stream.Archive.Manifest()
	var paths []string
	for path := range manifest.Files {
		if strings.HasPrefix(path, "blobs/") {
			paths = append(paths, path)
		}
	}
	sort.Strings(paths)
	var blobs driver.NativeRestoreBlobSource
	if len(paths) != 0 {
		blobs = func(write func(driver.Hash, int64, io.Reader) error) error {
			for _, path := range paths {
				r, ok := stream.Member(path)
				if !ok {
					return fmt.Errorf("validated reverse blob missing")
				}
				if err := write(driver.Hash(strings.TrimPrefix(path, "blobs/")), r.Size(), r); err != nil {
					return err
				}
			}
			return nil
		}
	}
	return blobs
}

func restoreStreamRecords(ctx context.Context, stream *dumpformat.NativeHeadStream) ([]driver.Record, error) {
	raw, ok := stream.Member(reverseRecordsMember)
	if !ok {
		return nil, fmt.Errorf("reverse records member missing")
	}
	manifest := stream.Archive.Manifest()
	head, err := strconv.ParseUint(manifest.NativeHead, 10, 64)
	if err != nil {
		return nil, err
	}
	if size := raw.Size(); size > 0 {
		var last [1]byte
		if _, err := raw.ReadAt(last[:], size-1); err != nil || last[0] != '\n' {
			return nil, fmt.Errorf("unterminated reverse records member")
		}
	}
	hash := sha256.New()
	scan := bufio.NewScanner(io.TeeReader(reverseArchiveInput{ctx, raw}, hash))
	// Leave one byte for the delimiter or the first byte over the limit.
	scan.Buffer(make([]byte, 32<<10), reverseRecordBytes+1)
	scan.Split(scanReverseRecord)
	seen := map[string]bool{}
	var records []driver.Record
	var indexBytes, materialBytes int
	for scan.Scan() {
		if len(records) == manifest.RecordCount {
			return nil, fmt.Errorf("reverse record count differs")
		}
		line := scan.Bytes()
		if len(line)+materialBytes > reverseMaterialBytes {
			return nil, fmt.Errorf("native material image exceeds buffer bound")
		}
		if err := dumpformat.ValidateNativeHeadRecord(line); err != nil {
			return nil, err
		}
		record, err := decodeNativeRecordForArchive(line, true)
		if err != nil {
			return nil, err
		}
		key, err := driver.EncodeLockKey(driver.LockKey{Kind: driver.LockRecord, Ref: &record.Ref})
		revision, revisionErr := record.Revision.Uint64()
		indexBytes += len(key) + 128
		if err != nil || seen[key] || revisionErr != nil || revision == 0 || revision > head || indexBytes > reverseIndexBytes {
			return nil, fmt.Errorf("invalid, duplicate, or oversized reverse record inventory")
		}
		seen[key] = true
		materialBytes += len(record.Body)
		records = append(records, record)
	}
	if err := scan.Err(); err != nil {
		return nil, err
	}
	if len(records) != manifest.RecordCount || hex.EncodeToString(hash.Sum(nil)) != manifest.Files[reverseRecordsMember].SHA256 {
		return nil, fmt.Errorf("reverse record count or checksum differs")
	}
	sort.Slice(records, func(i, j int) bool { return recordSortKey(records[i]) < recordSortKey(records[j]) })
	if err := service.ValidateNativeRestoreRecords(records); err != nil {
		return nil, err
	}
	return records, nil
}

func scanReverseRecord(data []byte, atEOF bool) (int, []byte, error) {
	if len(data) > reverseRecordBytes && bytes.IndexByte(data[:reverseRecordBytes+1], '\n') < 0 {
		return 0, nil, &driver.ReverseJSONLimitError{Resource: "value_bytes", Measured: reverseRecordBytes + 1, Limit: reverseRecordBytes}
	}
	return bufio.ScanLines(data, atEOF)
}

const reverseImportBindingID = "instance_config/restore-import/binding"
const reverseProjectPrefix = "instance_config/restore-import/project/"

func isReverseProjectRecord(ref model.Ref, kind string) bool {
	return kind == "instance_config" && ref.IsID() && ref.Branch.IsZero() && strings.HasPrefix(ref.ID, reverseProjectPrefix)
}

func reverseBoundProfile(body []byte) json.RawMessage {
	var binding struct {
		Profile json.RawMessage `json:"profile"`
	}
	if json.Unmarshal(body, &binding) != nil {
		return nil
	}
	var profile struct {
		SourcePlanes json.RawMessage `json:"sourcePlanes"`
	}
	if json.Unmarshal(binding.Profile, &profile) != nil || len(profile.SourcePlanes) == 0 || bytes.Equal(profile.SourcePlanes, []byte("null")) {
		return nil
	}
	return binding.Profile
}

// Replaces exactly the top-level profile value. Only a byte-equivalent
// canonical copy can be compacted. Reversing the replacement restores the
// original object member order and all other bytes.
func hasReverseProfile(body, value []byte) bool {
	var project struct {
		Profile json.RawMessage `json:"importProfile"`
	}
	return json.Unmarshal(body, &project) == nil && bytes.Equal(project.Profile, value)
}

func replaceReverseProfile(body, from, to []byte) ([]byte, bool) {
	const field = `"importProfile":`
	if !hasReverseProfile(body, from) || bytes.Count(body, []byte(field)) != 1 {
		return nil, false
	}
	offset := bytes.Index(body, []byte(field)) + len(field)
	if !bytes.HasPrefix(body[offset:], from) {
		return nil, false
	}
	result := make([]byte, 0, len(body)-len(from)+len(to))
	result = append(result, body[:offset]...)
	result = append(result, to...)
	result = append(result, body[offset+len(from):]...)
	return result, true
}

func expandReverseProjection(records []driver.Record, projection *dumpformat.NativeReverseProjection) error {
	var profile json.RawMessage
	var archive *driver.Record
	for i := range records {
		record := &records[i]
		if record.Ref.ID == reverseImportBindingID && record.Kind == "instance_config" {
			profile = reverseBoundProfile(record.Body)
		}
		if record.Ref.ID == "restore/archive" && record.Kind == "restore-archive" {
			archive = record
		}
	}
	if profile == nil || archive == nil {
		return fmt.Errorf("reverse projection lacks joint import evidence")
	}
	var evidence nativeReverseEvidence
	if json.Unmarshal(archive.Body, &evidence) != nil || len(evidence.NativeCaptureFiles) == 0 || !strings.HasPrefix(evidence.SourceStore, "merge-") {
		return fmt.Errorf("reverse projection lacks joined capture evidence")
	}
	var total int64
	for _, record := range records {
		total += int64(len(record.Body))
	}
	var copies int
	for i := range records {
		record := &records[i]
		if projection.DiscardedOrigins != nil && driver.IsReverseDiscardedOrigin(record.Ref, record.Kind) {
			return fmt.Errorf("reverse projection repeats discarded archive origins")
		}
		if !isReverseProjectRecord(record.Ref, record.Kind) {
			continue
		}
		if hasReverseProfile(record.Body, []byte("null")) {
			total += int64(len(profile) - len("null"))
			if total > reverseMaterialBytes || len(record.Body)+len(profile)-len("null") >= reverseRecordBytes {
				return fmt.Errorf("expanded native material image exceeds buffer bound")
			}
			full, ok := replaceReverseProfile(record.Body, []byte("null"), profile)
			if !ok {
				return fmt.Errorf("invalid compact reverse project profile")
			}
			record.Body = full
			copies++
		}
	}
	if copies != projection.ProfileCopies {
		return fmt.Errorf("reverse projection profile copy count differs")
	}
	if projection.DiscardedOrigins == nil {
		return nil
	}
	loss := *projection.DiscardedOrigins
	if old := evidence.ReverseOriginLoss; old != nil {
		if old.Records <= 0 || old.Fields < old.Records || loss.Records > math.MaxInt-old.Records || loss.Fields > math.MaxInt-old.Fields {
			return fmt.Errorf("reverse projection loss counts overflow or differ")
		}
		loss.Records += old.Records
		loss.Fields += old.Fields
	}
	evidence.ReverseOriginLoss = &loss
	if evidence.NativeIncarnation == "" {
		return fmt.Errorf("reverse projection lacks an incarnation binding")
	}
	updated, err := json.Marshal(evidence)
	if err != nil {
		return err
	}
	archive.Body = updated
	return nil
}
