// Package restore owns Plug's engine-neutral dump and empty-store restore
// seam. It intentionally uses only the storage driver contract: no SQL or
// driver-specific handle crosses this package.
package restore

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

const (
	defaultPageSize = 1 << 20
	maxPageSize     = 64 << 20
	maxDumpBytes    = dumpformat.MaxArchiveBytes
	maxRestorePages = 4096
)

// Presence is deliberately transport-neutral. Authentication and the full
// D32 proof are owned by the host's trusted admin policy. The restore manager
// never treats a client-shaped proof as sufficient on its own: a host must
// inject a verifier that binds the proof to the operation and sensitive
// archive digest.
type Presence map[string]any

// PresenceVerifier is the host-owned D32 admission seam. Implementations must
// verify an issued challenge/session and bind it to method and digest. A nil
// verifier is a fail-closed configuration: dump and restore are refused.
type PresenceVerifier interface {
	Verify(context.Context, string, string, Presence) error
}

// PresenceVerifierFunc adapts a function to PresenceVerifier. It is useful for
// a host's injected verifier and for focused conformance fixtures; production
// callers must provide a verifier backed by their trusted presence/session
// authority.
type PresenceVerifierFunc func(context.Context, string, string, Presence) error

func (f PresenceVerifierFunc) Verify(ctx context.Context, method, digest string, presence Presence) error {
	if f == nil {
		return fmt.Errorf("trusted human presence verifier is unavailable")
	}
	return f(ctx, method, digest, presence)
}

func validatePresence(p Presence) error {
	mode, _ := p["mode"].(string)
	challenge, _ := p["challenge"].(string)
	proof, _ := p["proof"].(string)
	if mode != "presence_check" && mode != "interactive_session" {
		return fmt.Errorf("human presence mode is required")
	}
	if strings.TrimSpace(challenge) == "" || strings.TrimSpace(proof) == "" || len(challenge) > 4096 || len(proof) > 4096 {
		return fmt.Errorf("human presence challenge and proof are required")
	}
	if mode == "interactive_session" {
		session, _ := p["sessionId"].(string)
		if strings.TrimSpace(session) == "" || len(session) > 4096 {
			return fmt.Errorf("interactive presence session is required")
		}
	}
	return nil
}

func presenceError(err error) error {
	hint := "trusted human presence verifier is unavailable"
	if err != nil {
		hint = err.Error()
	}
	return &model.Error{Data: model.ErrorData{Code: "human_presence_required", Hint: hint}}
}

// ImportHook is the optional trusted host bridge for validated offline
// converter archives. The manager owns the authenticated paged session and
// supplies the complete archive only after its checksum, manifest and caller
// checks have succeeded. Native archives never use this hook. The explicit
// history policy is passed separately from write metadata so it cannot be
// confused with an ordinary graph write field.
type ImportHook func(context.Context, string, model.PrincipalRef, []byte, string, driver.RestoreHistoryMode, string, string, model.WriteMeta, Presence) (RestoreStatus, error)

type nativeRecord struct {
	Ref      model.Ref       `json:"ref"`
	Kind     string          `json:"kind"`
	Revision model.Revision  `json:"revision"`
	Body     json.RawMessage `json:"body"`
}

// nativeReverseEvidence is the durable converter evidence retained in the
// restore/archive record. Native dumps copy only blobs named by this ledger,
// which keeps ordinary native backups compact while preserving the exact
// content-addressed source envelope needed by reverse conversion.
type nativeReverseEvidence struct {
	ArchiveSHA256      string                             `json:"archive_sha256"`
	DerivedSHA256      string                             `json:"derived_sha256"`
	Profile            string                             `json:"profile"`
	Members            map[string]dumpformat.File         `json:"members"`
	NativeCaptureFiles map[string]dumpformat.File         `json:"native_capture_files,omitempty"`
	SourceStore        string                             `json:"source_store"`
	NativeIncarnation  string                             `json:"native_incarnation,omitempty"`
	HistoryBinding     *dumpformat.HistoryBindingEvidence `json:"history_binding,omitempty"`
}

func archiveDigest(paths []string, members map[string][]byte) string {
	h := sha256.New()
	for _, path := range paths {
		if path == "manifest.json" {
			continue
		}
		_, _ = io.WriteString(h, path)
		_, _ = h.Write(members[path])
	}
	return hex.EncodeToString(h.Sum(nil))
}

func memberDigest(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// Dump captures one immutable reader snapshot and returns a deterministic
// plug-dump/1 tar. A caller may page the returned bytes at the RPC boundary.
func Dump(ctx context.Context, store driver.Store, history string) ([]byte, dumpformat.Manifest, error) {
	if err := ctx.Err(); err != nil {
		return nil, dumpformat.Manifest{}, err
	}
	if history == "" {
		history = "full"
	}
	if history != "full" && history != "head" {
		return nil, dumpformat.Manifest{}, fmt.Errorf("history must be full or head")
	}
	meta, err := store.Meta(ctx)
	if err != nil {
		return nil, dumpformat.Manifest{}, err
	}
	// Bind the record image and retained history to the same head advertised
	// by the manifest. A writer may advance the store after Meta returns.
	reader, err := store.Read(ctx, driver.View{Incarnation: meta.Incarnation, At: &meta.Head})
	if err != nil {
		return nil, dumpformat.Manifest{}, err
	}
	defer reader.Close()

	snapshotReader, ok := reader.(driver.AllocatorSnapshotReader)
	if !ok {
		return nil, dumpformat.Manifest{}, fmt.Errorf("%w: native snapshot requires revision-bound allocator inventory", driver.ErrUnsupported)
	}
	bound := reader.Snapshot()
	if bound.At == nil || bound.At.String() != meta.Head.String() || bound.Incarnation != meta.Incarnation {
		return nil, dumpformat.Manifest{}, driver.NewNativeAllocatorSnapshotError(driver.NativeAllocatorSourceMismatch, fmt.Errorf("reader is not bound to the captured native view"))
	}
	allocatorSnapshot, err := snapshotReader.AllocatorSnapshot(ctx)
	if err != nil {
		return nil, dumpformat.Manifest{}, err
	}
	if err := allocatorSnapshot.Validate(); err != nil {
		return nil, dumpformat.Manifest{}, err
	}
	if allocatorSnapshot.StoreID != meta.StoreID || allocatorSnapshot.Incarnation != meta.Incarnation || allocatorSnapshot.At.String() != meta.Head.String() {
		return nil, dumpformat.Manifest{}, driver.NewNativeAllocatorSnapshotError(driver.NativeAllocatorSourceMismatch, fmt.Errorf("allocator snapshot is not bound to the pinned store view"))
	}
	allocatorData, err := driver.EncodeAllocatorSnapshot(allocatorSnapshot)
	if err != nil {
		return nil, dumpformat.Manifest{}, err
	}

	var records []nativeRecord
	var cursor *driver.Cursor
	for {
		page, e := reader.Query(ctx, driver.Query{Limit: 10000, Cursor: cursor})
		if e != nil {
			return nil, dumpformat.Manifest{}, e
		}
		for _, record := range page.Items {
			if !json.Valid(record.Body) {
				return nil, dumpformat.Manifest{}, fmt.Errorf("record %v has invalid JSON body", record.Ref)
			}
			// Omitted and explicit zero branches name the same ID. Emit one
			// spelling whether the driver serves current or replayed content.
			ref := record.Ref
			if ref.IsID() && ref.Branch.IsZero() {
				ref.Branch = model.Natural64(0)
			}
			records = append(records, nativeRecord{Ref: ref, Kind: record.Kind, Revision: record.Revision, Body: append(json.RawMessage(nil), record.Body...)})
		}
		if page.NextCursor == nil {
			break
		}
		cursor = page.NextCursor
	}
	sort.Slice(records, func(i, j int) bool { return nativeRecordKey(records[i]) < nativeRecordKey(records[j]) })
	if historyBinding := nativeReverseHistoryBinding(ctx, reader, records); historyBinding != nil {
		for i := range records {
			if records[i].Ref.ID != "restore/archive" {
				continue
			}
			var evidence nativeReverseEvidence
			if json.Unmarshal(records[i].Body, &evidence) != nil {
				continue
			}
			if !hasConverterReverseEvidence(evidence) {
				continue
			}
			evidence.HistoryBinding = historyBinding
			evidence.NativeIncarnation = meta.Incarnation
			body, marshalErr := json.Marshal(evidence)
			if marshalErr == nil {
				records[i].Body = body
			}
		}
	}

	// A converter restore retains its source envelope and referenced members as
	// content-addressed blobs. Carry those exact blobs into a native backup so
	// a later native restore can satisfy the same authenticated reverse path.
	// Missing blobs are deliberately omitted for compatibility with older
	// native-only backups. Their restore remains valid, and reverse retains its
	// typed invalid_source_evidence refusal instead of inventing provenance.
	reverseBlobs, err := nativeReverseEvidenceBlobs(ctx, reader, records)
	if err != nil {
		return nil, dumpformat.Manifest{}, err
	}

	refs := make(map[string]model.Ref, len(records))
	for _, record := range records {
		key, e := driver.EncodeLockKey(driver.LockKey{Kind: driver.LockRecord, Ref: &record.Ref})
		if e != nil {
			return nil, dumpformat.Manifest{}, e
		}
		refs[key] = record.Ref
	}
	var changesets []model.Changeset
	var changesCursor *driver.Cursor
	if !meta.HistoryFloor.IsZero() {
		// A restored lifetime starts at its floor. Do not ask the driver for
		// history that the restore explicitly discarded. Full-history dumps
		// cannot truthfully represent that discarded prefix.
		if history == "full" {
			return nil, dumpformat.Manifest{}, driver.ErrTruncated{Floor: meta.HistoryFloor}
		}
		changesCursor = &driver.Cursor{Incarnation: meta.Incarnation, Revision: meta.HistoryFloor, Method: "changesets"}
	}
	for {
		entries, e := reader.Changesets(ctx, changesCursor, 10000)
		if e != nil {
			return nil, dumpformat.Manifest{}, e
		}
		if len(entries) == 0 {
			break
		}
		changesets = append(changesets, entries...)
		for _, entry := range entries {
			for _, change := range entry.Changes {
				key, ce := driver.EncodeLockKey(driver.LockKey{Kind: driver.LockRecord, Ref: &change.Ref})
				if ce == nil {
					refs[key] = change.Ref
				}
			}
		}
		last := entries[len(entries)-1].Revision
		changesCursor = &driver.Cursor{Incarnation: meta.Incarnation, Revision: last, Method: "changesets"}
		if len(entries) < 10000 {
			break
		}
	}

	var versions []driver.Version
	refKeys := make([]string, 0, len(refs))
	for key := range refs {
		refKeys = append(refKeys, key)
	}
	sort.Strings(refKeys)
	for _, key := range refKeys {
		var versionCursor *driver.Cursor
		for {
			page, e := reader.Versions(ctx, refs[key], driver.PageReq{Limit: 10000, Cursor: versionCursor})
			if e != nil {
				return nil, dumpformat.Manifest{}, e
			}
			versions = append(versions, page.Items...)
			if page.NextCursor == nil {
				break
			}
			versionCursor = page.NextCursor
		}
	}
	if history == "head" {
		latest := map[string]driver.Version{}
		for _, version := range versions {
			key, e := driver.EncodeLockKey(driver.LockKey{Kind: driver.LockRecord, Ref: &version.Ref})
			if e != nil {
				return nil, dumpformat.Manifest{}, e
			}
			latest[key] = version
		}
		versions = versions[:0]
		for _, version := range latest {
			versions = append(versions, version)
		}
		changesets = nil
	}
	sort.Slice(versions, func(i, j int) bool { return nativeVersionKey(versions[i]) < nativeVersionKey(versions[j]) })

	var recordData, versionData, changesetData bytes.Buffer
	for _, record := range records {
		if err := encodeLine(&recordData, record); err != nil {
			return nil, dumpformat.Manifest{}, err
		}
	}
	for _, version := range versions {
		if err := encodeLine(&versionData, version); err != nil {
			return nil, dumpformat.Manifest{}, err
		}
	}
	for _, changeset := range changesets {
		if err := encodeLine(&changesetData, changeset); err != nil {
			return nil, dumpformat.Manifest{}, err
		}
	}
	configData := []byte("{\"source\":\"plug\",\"history\":\"" + history + "\"}\n")
	members := map[string][]byte{
		"config/sources.json":            configData,
		"records/native.ndjson":          recordData.Bytes(),
		"versions.ndjson":                versionData.Bytes(),
		"changesets.ndjson":              changesetData.Bytes(),
		dumpformat.NativeAllocatorMember: allocatorData,
	}
	for digest, data := range reverseBlobs {
		members["blobs/"+digest] = data
	}
	paths := make([]string, 0, len(members)+1)
	for path := range members {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	paths = append([]string{"manifest.json"}, paths...)
	files := make(map[string]dumpformat.File, len(members))
	for path, data := range members {
		files[path] = dumpformat.File{SHA256: memberDigest(data), Size: int64(len(data))}
	}
	manifest := dumpformat.Manifest{
		Format: dumpformat.Format, Schema: 1, Reference: "plug-native", StoreID: string(meta.StoreID),
		Machine: "plug", Scope: "plug-native-store", History: history,
		Projects: []dumpformat.Project{}, Kinds: []dumpformat.Kind{}, Sources: []dumpformat.Disposition{},
		Files: files, Warnings: []string{}, Verification: map[string]string{},
		NativeStoreID: string(meta.StoreID), NativeIncarnation: meta.Incarnation,
		NativeHead: meta.Head.String(), NativeDigest: archiveDigest(paths, members),
		NativeAllocatorProfile: allocatorSnapshot.Version, NativeAllocatorDigest: memberDigest(allocatorData),
		RecordCount: len(records), VersionCount: len(versions), ChangesetCount: len(changesets),
	}
	manifestData, err := json.Marshal(manifest)
	if err != nil {
		return nil, dumpformat.Manifest{}, err
	}
	manifestData = append(manifestData, '\n')
	members["manifest.json"] = manifestData
	data, err := writeTar(paths, members)
	if err != nil {
		return nil, dumpformat.Manifest{}, err
	}
	if len(data) > maxDumpBytes {
		return nil, dumpformat.Manifest{}, fmt.Errorf("dump exceeds %d bytes", maxDumpBytes)
	}
	return data, manifest, nil
}

func nativeReverseEvidenceBlobs(ctx context.Context, reader driver.Reader, records []nativeRecord) (map[string][]byte, error) {
	digests := map[string]bool{}
	for _, record := range records {
		if record.Ref.ID != "restore/archive" {
			continue
		}
		var evidence nativeReverseEvidence
		if json.Unmarshal(record.Body, &evidence) != nil {
			continue
		}
		if !hasConverterReverseEvidence(evidence) {
			continue
		}
		if isDigest(evidence.ArchiveSHA256) {
			digests[evidence.ArchiveSHA256] = true
		}
		for _, file := range evidence.Members {
			if isDigest(file.SHA256) {
				digests[file.SHA256] = true
			}
		}
		for _, file := range evidence.NativeCaptureFiles {
			if isDigest(file.SHA256) {
				digests[file.SHA256] = true
			}
		}
	}
	paths := make([]string, 0, len(digests))
	for digest := range digests {
		paths = append(paths, digest)
	}
	sort.Strings(paths)
	result := make(map[string][]byte, len(paths))
	for _, digest := range paths {
		data, err := reader.Blob(ctx, driver.Hash(digest))
		if err != nil || memberDigest(data) != digest {
			continue
		}
		result[digest] = data
	}
	return result, nil
}

func hasConverterReverseEvidence(evidence nativeReverseEvidence) bool {
	// Native capture-set evidence belongs to the native history recovery
	// contract. It is intentionally not promoted into converter reverse
	// evidence here, preserving the P23 refusal boundary for native-only
	// backups. Offline converter archives have no capture-file ledger and
	// use the source envelope/member ledger below.
	if len(evidence.NativeCaptureFiles) != 0 || strings.HasPrefix(evidence.SourceStore, "native/") {
		return false
	}
	for path := range evidence.Members {
		if strings.HasPrefix(path, "records/") && path != "records/native.ndjson" {
			return true
		}
	}
	return false
}

func nativeReverseHistoryBinding(ctx context.Context, reader driver.Reader, records []nativeRecord) *dumpformat.HistoryBindingEvidence {
	var binding struct {
		Profile     driver.RestoreImportProfile `json:"profile"`
		RequestHash string                      `json:"request_hash"`
	}
	for _, record := range records {
		if record.Ref.ID == "instance_config/restore-import/binding" {
			if json.Unmarshal(record.Body, &binding) != nil || binding.Profile.HistoryDigest == "" {
				return nil
			}
			request, err := binding.Profile.RequestHash()
			if err != nil || request != binding.RequestHash {
				return nil
			}
			principal, err := driver.PrincipalKey(binding.Profile.Admission.Actor)
			if err != nil {
				return nil
			}
			receipt, err := reader.IdempotencyByKey(ctx, principal, binding.Profile.AgentLabel(), binding.Profile.Method, binding.Profile.IdempotencyKey)
			if err != nil || receipt == nil || receipt.RequestHash != request || receipt.ResponsePending || receipt.Changeset.Kind != model.ChangesetRestore || receipt.Changeset.Revision != receipt.Commit.Revision {
				return nil
			}
			return &dumpformat.HistoryBindingEvidence{RequestHash: request, Revision: string(receipt.Commit.Revision), Incarnation: receipt.Changeset.Incarnation}
		}
	}
	return nil
}

func isDigest(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size && hex.EncodeToString(decoded) == value
}

func nativeRecordKey(record nativeRecord) string {
	key, _ := driver.EncodeLockKey(driver.LockKey{Kind: driver.LockRecord, Ref: &record.Ref})
	return key
}

func nativeVersionKey(version driver.Version) string {
	key := nativeRecordKey(nativeRecord{Ref: version.Ref})
	return key + "\x00" + version.Revision.String()
}

func encodeLine(w io.Writer, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	_, err = w.Write(append(data, '\n'))
	return err
}

func writeTar(paths []string, members map[string][]byte) ([]byte, error) {
	var out bytes.Buffer
	tw := tar.NewWriter(&out)
	for _, path := range paths {
		data, ok := members[path]
		if !ok {
			return nil, fmt.Errorf("missing member %q", path)
		}
		h := &tar.Header{Name: path, Mode: 0o600, Size: int64(len(data)), Typeflag: tar.TypeReg, ModTime: time.Unix(0, 0), Format: tar.FormatUSTAR}
		if err := tw.WriteHeader(h); err != nil {
			return nil, err
		}
		if _, err := tw.Write(data); err != nil {
			return nil, err
		}
	}
	if err := tw.Close(); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}

type restoreSession struct {
	id            string
	manifest      map[string]any
	history       string
	historyMode   driver.RestoreHistoryMode
	historyDigest string
	historyTime   string
	hash          string
	principal     model.PrincipalRef
	agentLabel    string
	idempotency   string
	pages         map[uint64][]byte
	state         string
	warnings      []model.Warning
	incarnation   string
}

// Manager is safe for concurrent RPC calls. A restore session is in-memory
// until finish; the store itself is changed only by one atomic driver commit.
type Manager struct {
	store      driver.Store
	lease      driver.ActiveLease
	verifier   PresenceVerifier
	mu         sync.Mutex
	dumps      map[string]dumpSnapshot
	restores   map[string]*restoreSession
	importHook ImportHook
}

type dumpSnapshot struct {
	data     []byte
	history  string
	pageSize int
	digest   string
}

func NewManager(store driver.Store, lease driver.ActiveLease, verifier ...PresenceVerifier) *Manager {
	var presenceVerifier PresenceVerifier
	if len(verifier) != 0 {
		presenceVerifier = verifier[0]
	}
	return &Manager{store: store, lease: lease, verifier: presenceVerifier, dumps: map[string]dumpSnapshot{}, restores: map[string]*restoreSession{}}
}

// SetImportHook installs the trusted composition bridge for converter
// archives. It is configured before the registry is exposed and is optional
// so the CLI and native-only callers retain their existing behavior.
func (m *Manager) SetImportHook(hook ImportHook) {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.importHook = hook
	m.mu.Unlock()
}

func (m *Manager) requirePresence(p Presence) error {
	if err := validatePresence(p); err != nil {
		return presenceError(err)
	}
	if m.verifier == nil {
		return presenceError(nil)
	}
	return nil
}

func (m *Manager) verifyPresence(ctx context.Context, method, digest string, p Presence) error {
	if err := m.requirePresence(p); err != nil {
		return err
	}
	if strings.TrimSpace(digest) == "" {
		return presenceError(fmt.Errorf("sensitive digest is required for human presence admission"))
	}
	if err := m.verifier.Verify(ctx, method, digest, p); err != nil {
		return presenceError(err)
	}
	return nil
}

type DumpPage struct {
	DumpID   string
	Page     uint64
	Data     []byte
	NextPage *uint64
	Done     bool
	SHA256   string
}

func (m *Manager) DumpPage(ctx context.Context, history, cursor string, pageSize int, presence Presence) (DumpPage, error) {
	if err := m.requirePresence(presence); err != nil {
		return DumpPage{}, err
	}
	if pageSize == 0 {
		pageSize = defaultPageSize
	}
	if pageSize < 1 || pageSize > maxPageSize {
		return DumpPage{}, &model.Error{Data: model.ErrorData{Code: "invalid_params", Hint: "pageSize must be between 1 and 67108864"}}
	}
	if history == "" {
		history = "full"
	}
	if history != "full" && history != "head" {
		return DumpPage{}, &model.Error{Data: model.ErrorData{Code: "invalid_params", Hint: "history must be full or head"}}
	}
	if err := ctx.Err(); err != nil {
		return DumpPage{}, err
	}
	if err := m.requireLease(); err != nil {
		return DumpPage{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	dumpID, page, err := parseDumpCursor(cursor)
	if err != nil {
		return DumpPage{}, &model.Error{Data: model.ErrorData{Code: "invalid_params", Hint: err.Error()}}
	}
	snapshot, found := m.dumps[dumpID]
	data := snapshot.data
	if dumpID == "" {
		data, _, err = Dump(ctx, m.store, history)
		if err != nil {
			return DumpPage{}, err
		}
		digest := memberDigest(data)
		if err := m.verifyPresence(ctx, "admin.dump", digest, presence); err != nil {
			return DumpPage{}, err
		}
		dumpID = newID("dump")
		snapshot = dumpSnapshot{data: data, history: history, pageSize: pageSize, digest: digest}
		m.dumps[dumpID] = snapshot
		page = 0
	} else if !found {
		return DumpPage{}, &model.Error{Data: model.ErrorData{Code: "not_found", Hint: "dump snapshot not found"}}
	} else if err := m.verifyPresence(ctx, "admin.dump", snapshot.digest, presence); err != nil {
		return DumpPage{}, err
	}
	if snapshot.data != nil && (snapshot.history != history || snapshot.pageSize != pageSize) {
		return DumpPage{}, &model.Error{Data: model.ErrorData{Code: "invalid_params", Hint: "dump cursor is bound to history and pageSize"}}
	}
	pageCount := (uint64(len(data)) + uint64(pageSize) - 1) / uint64(pageSize)
	if page >= pageCount || page > math.MaxUint64/uint64(pageSize) {
		return DumpPage{}, &model.Error{Data: model.ErrorData{Code: "not_found", Hint: "dump page is outside the snapshot"}}
	}
	start := page * uint64(pageSize)
	end := start + uint64(pageSize)
	if end > uint64(len(data)) {
		end = uint64(len(data))
	}
	chunk := append([]byte(nil), data[start:end]...)
	done := end == uint64(len(data))
	result := DumpPage{DumpID: dumpID, Page: page, Data: chunk, Done: done, SHA256: memberDigest(chunk)}
	if !done {
		next := page + 1
		result.NextPage = &next
	}
	return result, nil
}

func parseDumpCursor(cursor string) (string, uint64, error) {
	if cursor == "" {
		return "", 0, nil
	}
	parts := strings.Split(cursor, ":")
	if len(parts) != 2 || parts[0] == "" {
		return "", 0, fmt.Errorf("cursor must be dumpId:page")
	}
	page, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return "", 0, fmt.Errorf("invalid dump cursor")
	}
	return parts[0], page, nil
}

func (m *Manager) requireLease() error {
	if m.lease == nil {
		return &model.Error{Data: model.ErrorData{Code: "lease_not_held", Hint: "admin dump/restore requires an active storage lease"}}
	}
	select {
	case <-m.lease.Lost():
		return &model.Error{Data: model.ErrorData{Code: "lease_not_held", Hint: "active storage lease was lost"}}
	default:
		return nil
	}
}

// emptyTarget performs the read-side half of the empty-store admission check.
// Finish repeats this check while holding the publication transaction, because
// a caller may have written after Begin but before the last restore page.
func (m *Manager) emptyTarget(ctx context.Context) (driver.Meta, error) {
	meta, err := m.store.Meta(ctx)
	if err != nil {
		return driver.Meta{}, err
	}
	if !meta.Head.IsZero() || !meta.HistoryFloor.IsZero() {
		return meta, &model.Error{Data: model.ErrorData{Code: "restore_requires_empty", Hint: "restore target is initialized; restore requires zero committed revisions"}}
	}
	reader, err := m.store.Read(ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		return meta, err
	}
	defer reader.Close()
	page, err := reader.Query(ctx, driver.Query{Limit: 1})
	if err != nil {
		return meta, err
	}
	if len(page.Items) != 0 {
		return meta, &model.Error{Data: model.ErrorData{Code: "restore_requires_empty", Hint: "restore target contains records"}}
	}
	changesets, err := reader.Changesets(ctx, nil, 1)
	if err != nil {
		return meta, err
	}
	if len(changesets) != 0 {
		return meta, &model.Error{Data: model.ErrorData{Code: "restore_requires_empty", Hint: "restore target contains history"}}
	}
	return meta, nil
}

type RestoreStatus struct {
	RestoreID     string
	State         string
	ReceivedPages uint64
	Warnings      []model.Warning
	Incarnation   string
}

func (m *Manager) Begin(ctx context.Context, manifest map[string]any, history string, presence Presence) (RestoreStatus, error) {
	return m.BeginAs(ctx, model.PrincipalRef{}, manifest, history, presence)
}

// BeginAs binds the admitted caller to the paged restore session. Presence is
// still checked by the injected verifier; the principal binding prevents a
// different user from inheriting a session between begin and finish.
func (m *Manager) BeginAs(ctx context.Context, principal model.PrincipalRef, manifest map[string]any, history string, presence Presence) (RestoreStatus, error) {
	return m.BeginAsWithMetaAndHistory(ctx, principal, manifest, history, "", "", model.WriteMeta{}, presence)
}

// BeginAsWithMeta is the transport-facing form of BeginAs. The metadata is
// retained privately for the converter finish hook so a retry uses the same
// G4 scope and operation key that was admitted at begin.
func (m *Manager) BeginAsWithMeta(ctx context.Context, principal model.PrincipalRef, manifest map[string]any, history string, meta model.WriteMeta, presence Presence) (RestoreStatus, error) {
	return m.BeginAsWithMetaAndHistory(ctx, principal, manifest, history, "", "", meta, presence)
}

// BeginAsWithMetaAndHistory extends the transport-facing begin seam with
// optional history bindings. Empty bindings preserve the historical behavior.
func (m *Manager) BeginAsWithMetaAndHistory(ctx context.Context, principal model.PrincipalRef, manifest map[string]any, history, historyDigest, historyTime string, meta model.WriteMeta, presence Presence) (RestoreStatus, error) {
	if err := m.requirePresence(presence); err != nil {
		return RestoreStatus{}, err
	}
	if err := ctx.Err(); err != nil {
		return RestoreStatus{}, err
	}
	if err := m.requireLease(); err != nil {
		return RestoreStatus{}, err
	}
	historyMode, modeErr := driver.ParseRestoreHistoryMode(history)
	if modeErr != nil {
		return RestoreStatus{}, restoreHistoryModelError(modeErr)
	}
	archiveHistory := "full"
	if historyMode == driver.RestoreHistoryModeSnapshot {
		archiveHistory = "head"
	}
	manifestData, err := json.Marshal(manifest)
	if err != nil {
		return RestoreStatus{}, &model.Error{Data: model.ErrorData{Code: "restore_checksum_failed", Hint: err.Error()}}
	}
	var parsed dumpformat.Manifest
	if err := json.Unmarshal(manifestData, &parsed); err != nil || parsed.Format != dumpformat.Format || parsed.Schema != 1 || (parsed.Scope != dumpformat.Scope && parsed.Scope != "plug-native-store") {
		return RestoreStatus{}, &model.Error{Data: model.ErrorData{Code: "restore_checksum_failed", Hint: "unsupported dump manifest"}}
	}
	if err := driver.ValidateRestoreHistoryPolicy(historyMode, parsed.History); err != nil {
		// Converter archives carrying source-native histories declare their
		// portable history as available-unverified. Full-mode composition binds
		// the exact history digest and restore time here, then performs the
		// native-history verification during finish. Keep the driver policy
		// unchanged for native archives, partial restores, and every other
		// history declaration.
		admitBoundConverterHistory := parsed.Scope == dumpformat.Scope && historyMode == driver.RestoreHistoryModeFull && parsed.History == driver.RestoreHistoryAvailableUnverified && historyDigest != "" && historyTime != ""
		if !admitBoundConverterHistory {
			return RestoreStatus{}, restoreHistoryModelError(err)
		}
	}
	// Converter imports perform the empty-target check in the trusted adapter,
	// which first attempts read-only receipt recovery for a same-bound retry.
	// Native restore keeps the existing early empty-target refusal.
	if parsed.Scope != dumpformat.Scope {
		if _, err := m.emptyTarget(ctx); err != nil {
			return RestoreStatus{}, err
		}
	}
	canonical, err := json.Marshal(parsed)
	if err != nil {
		return RestoreStatus{}, &model.Error{Data: model.ErrorData{Code: "restore_checksum_failed", Hint: err.Error()}}
	}
	hash := memberDigest(canonical)
	if err := m.verifyPresence(ctx, "admin.restore.begin", hash, presence); err != nil {
		return RestoreStatus{}, err
	}
	id := newID("restore")
	m.mu.Lock()
	m.restores[id] = &restoreSession{id: id, manifest: cloneMap(manifest), history: archiveHistory, historyMode: historyMode, historyDigest: historyDigest, historyTime: historyTime, hash: hash, principal: principal, agentLabel: meta.AgentLabel, idempotency: meta.IdempotencyKey, pages: map[uint64][]byte{}, state: "receiving"}
	m.mu.Unlock()
	return RestoreStatus{RestoreID: id, State: "receiving", ReceivedPages: 0, Warnings: []model.Warning{}}, nil
}

func (m *Manager) Chunk(ctx context.Context, id string, page uint64, data []byte, checksum string, presence Presence) (RestoreStatus, error) {
	return m.ChunkAs(ctx, model.PrincipalRef{}, id, page, data, checksum, presence)
}

// ChunkAs rechecks the caller binding and operation-bound presence for every
// uploaded page. It intentionally retains the old Chunk wrapper for local
// fixtures that use the anonymous in-process API.
func (m *Manager) ChunkAs(ctx context.Context, principal model.PrincipalRef, id string, page uint64, data []byte, checksum string, presence Presence) (RestoreStatus, error) {
	if err := m.requirePresence(presence); err != nil {
		return RestoreStatus{}, err
	}
	if err := ctx.Err(); err != nil {
		return RestoreStatus{}, err
	}
	if err := m.requireLease(); err != nil {
		return RestoreStatus{}, err
	}
	if id == "" || len(data) > maxPageSize {
		return RestoreStatus{}, &model.Error{Data: model.ErrorData{Code: "invalid_params", Hint: "invalid restore page"}}
	}
	if memberDigest(data) != strings.ToLower(checksum) {
		return RestoreStatus{}, &model.Error{Data: model.ErrorData{Code: "restore_checksum_failed", Hint: "restore page checksum mismatch"}}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	session := m.restores[id]
	if session == nil {
		return RestoreStatus{}, &model.Error{Data: model.ErrorData{Code: "not_found", Hint: "restore session not found"}}
	}
	if !samePrincipal(session.principal, principal) {
		return RestoreStatus{}, &model.Error{Data: model.ErrorData{Code: "forbidden", Hint: "restore session caller changed"}}
	}
	if session.state != "receiving" {
		return RestoreStatus{}, &model.Error{Data: model.ErrorData{Code: "restore_violation", Hint: "restore session is no longer receiving"}}
	}
	if err := m.verifyPresence(ctx, "admin.restore.chunk", session.hash, presence); err != nil {
		return RestoreStatus{}, err
	}
	if page != uint64(len(session.pages)) {
		return RestoreStatus{}, &model.Error{Data: model.ErrorData{Code: "restore_violation", Hint: "restore pages must be contiguous and unique"}}
	}
	if len(session.pages) >= maxRestorePages {
		return RestoreStatus{}, &model.Error{Data: model.ErrorData{Code: "restore_violation", Hint: "restore has too many pages"}}
	}
	var total int
	for _, prior := range session.pages {
		total += len(prior)
	}
	if total+len(data) > maxDumpBytes {
		return RestoreStatus{}, &model.Error{Data: model.ErrorData{Code: "restore_violation", Hint: "restore archive is too large"}}
	}
	session.pages[page] = append([]byte(nil), data...)
	return restoreStatus(session), nil
}

func (m *Manager) Finish(ctx context.Context, id, manifestHash string, principal model.PrincipalRef, meta model.WriteMeta, presence Presence) (RestoreStatus, error) {
	if err := m.requirePresence(presence); err != nil {
		return RestoreStatus{}, err
	}
	if err := ctx.Err(); err != nil {
		return RestoreStatus{}, err
	}
	if err := m.requireLease(); err != nil {
		return RestoreStatus{}, err
	}
	m.mu.Lock()
	session := m.restores[id]
	if session == nil {
		m.mu.Unlock()
		return RestoreStatus{}, &model.Error{Data: model.ErrorData{Code: "not_found", Hint: "restore session not found"}}
	}
	if !samePrincipal(session.principal, principal) {
		m.mu.Unlock()
		return RestoreStatus{}, &model.Error{Data: model.ErrorData{Code: "forbidden", Hint: "restore session caller changed"}}
	}
	if session.state != "receiving" {
		status := restoreStatus(session)
		m.mu.Unlock()
		return status, nil
	}
	if manifestHash == "" {
		m.mu.Unlock()
		return RestoreStatus{}, &model.Error{Data: model.ErrorData{Code: "restore_checksum_failed", Hint: "manifest checksum mismatch"}}
	}
	admittedManifestHash := session.hash
	m.mu.Unlock()
	if err := m.verifyPresence(ctx, "admin.restore.finish", admittedManifestHash, presence); err != nil {
		return RestoreStatus{}, err
	}
	m.mu.Lock()
	session = m.restores[id]
	if session == nil {
		m.mu.Unlock()
		return RestoreStatus{}, &model.Error{Data: model.ErrorData{Code: "not_found", Hint: "restore session not found"}}
	}
	if !samePrincipal(session.principal, principal) {
		m.mu.Unlock()
		return RestoreStatus{}, &model.Error{Data: model.ErrorData{Code: "forbidden", Hint: "restore session caller changed"}}
	}
	if session.state != "receiving" {
		status := restoreStatus(session)
		m.mu.Unlock()
		return status, nil
	}
	archiveBytes := make([]byte, 0)
	for i := uint64(0); i < uint64(len(session.pages)); i++ {
		archiveBytes = append(archiveBytes, session.pages[i]...)
	}
	session.state = "validating"
	m.mu.Unlock()

	archive, err := dumpformat.Read(bytes.NewReader(archiveBytes))
	if err != nil {
		return m.fail(id, &model.Error{Data: model.ErrorData{Code: "restore_checksum_failed", Hint: err.Error()}})
	}
	if err := ctx.Err(); err != nil {
		return m.fail(id, err)
	}
	m.mu.Lock()
	var importHook ImportHook
	importMeta := meta
	var historyMode driver.RestoreHistoryMode
	var historyDigest, historyTime string
	if current := m.restores[id]; current != nil {
		historyMode = current.historyMode
		historyDigest, historyTime = current.historyDigest, current.historyTime
		importHook = m.importHook
		// The converter profile is bound to the begin operation, not to a
		// caller-chosen finish key or agent label. Native restore continues to
		// use the finish metadata below.
		if current.idempotency != "" {
			importMeta.IdempotencyKey = current.idempotency
		}
		importMeta.AgentLabel = current.agentLabel
		importMeta.Agent = current.agentLabel
	}
	m.mu.Unlock()
	manifest := archive.Manifest()
	if historyMode == driver.RestoreHistoryModeFull && manifest.Scope == "plug-native-store" && manifest.History != "full" {
		return m.fail(id, &model.Error{Data: model.ErrorData{Code: "restore_checksum_failed", Hint: "archive history does not match restore request"}})
	}
	canonicalManifest, err := json.Marshal(manifest)
	if err != nil {
		return m.fail(id, &model.Error{Data: model.ErrorData{Code: "restore_checksum_failed", Hint: err.Error()}})
	}
	canonicalHash := memberDigest(canonicalManifest)
	if !strings.EqualFold(manifestHash, admittedManifestHash) || canonicalHash != admittedManifestHash {
		return m.fail(id, &model.Error{Data: model.ErrorData{Code: "restore_checksum_failed", Hint: "manifest checksum mismatch"}})
	}
	if manifest.Scope == dumpformat.Scope {
		if importHook == nil {
			// Preserve the standalone manager's historical refusal when no
			// trusted converter boundary is installed. Composition installs the
			// hook for admitted converter imports; direct/native-only managers
			// must retain the B17 unsupported-profile contract.
			return m.fail(id, &model.Error{Data: model.ErrorData{Code: "restore_violation", Hint: "converter archive restore is unavailable; source disclosures and allocator state must be preserved by the converter boundary"}})
		}
		m.mu.Lock()
		if current := m.restores[id]; current != nil {
			current.state = "applying"
		}
		m.mu.Unlock()
		status, importErr := importHook(ctx, id, principal, archiveBytes, canonicalHash, historyMode, historyDigest, historyTime, importMeta, presence)
		if importErr != nil {
			if errors.Is(importErr, driver.ErrRestoreTargetNotEmpty) {
				importErr = &model.Error{Data: model.ErrorData{Code: "restore_requires_empty", Hint: "restore target is initialized; restore requires a fully empty store"}}
			} else {
				var commitConflict driver.ErrRestoreCommitConflict
				var bindingMismatch driver.ErrRestoreBindingMismatch
				var fenced driver.ErrFenced
				switch {
				case errors.As(importErr, &fenced):
					importErr = &model.Error{Data: model.ErrorData{Code: "busy", Retryable: true, Hint: "active storage lease was lost"}}
				case errors.As(importErr, &commitConflict):
					importErr = &model.Error{Data: model.ErrorData{Code: "busy", Retryable: true, Hint: commitConflict.Error()}}
				case errors.As(importErr, &bindingMismatch):
					importErr = &model.Error{Data: model.ErrorData{Code: "restore_violation", Hint: bindingMismatch.Error()}}
				}
			}
			return m.fail(id, importErr)
		}
		m.mu.Lock()
		if current := m.restores[id]; current != nil {
			current.state = "complete"
			current.incarnation = status.Incarnation
		}
		status = restoreStatus(m.restores[id])
		m.mu.Unlock()
		return status, nil
	}
	if err := validateSupportedArchive(archive); err != nil {
		var allocatorErr *driver.NativeAllocatorSnapshotError
		if errors.As(err, &allocatorErr) {
			return m.fail(id, restoreAdmissionError(err))
		}
		return m.fail(id, &model.Error{Data: model.ErrorData{Code: "restore_violation", Hint: err.Error()}})
	}
	_, err = m.emptyTarget(ctx)
	if err != nil {
		return m.fail(id, err)
	}
	m.mu.Lock()
	if current := m.restores[id]; current != nil {
		current.state = "applying"
	}
	m.mu.Unlock()

	committed, err := m.apply(ctx, archive, principal, meta, archiveBytes)
	if err != nil {
		return m.fail(id, err)
	}
	// The commit is already durable at this point. Use a detached read for the
	// observation so caller cancellation cannot turn a successful restore
	// into a misleading failed status. The acknowledged changeset identifies
	// this operation even if a later operation changes current metadata.
	_, observationErr := m.store.Meta(context.Background())
	if observationErr != nil {
		// Observation failure does not undo an acknowledged durable commit.
		slog.WarnContext(context.Background(), "restore committed; current store metadata observation unavailable", "restoreId", id)
	}
	m.mu.Lock()
	if current := m.restores[id]; current != nil {
		current.state = "complete"
		current.incarnation = committed.Incarnation
	}
	status := restoreStatus(m.restores[id])
	m.mu.Unlock()
	return status, nil
}

func samePrincipal(left, right model.PrincipalRef) bool {
	if left.Kind == "" && left.ID == "" {
		return true
	}
	leftJSON, leftErr := json.Marshal(left)
	rightJSON, rightErr := json.Marshal(right)
	return leftErr == nil && rightErr == nil && bytes.Equal(leftJSON, rightJSON)
}

func restoreHistoryModelError(err error) error {
	if err == nil {
		return nil
	}
	if code, details, ok := driver.ErrorMapping(err); ok {
		return &model.Error{Data: model.ErrorData{Code: code, Hint: err.Error(), Details: details}}
	}
	return &model.Error{Data: model.ErrorData{Code: "restore_violation", Hint: err.Error()}}
}

func (m *Manager) fail(id string, err error) (RestoreStatus, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if session := m.restores[id]; session != nil {
		session.state = "failed"
		return restoreStatus(session), err
	}
	return RestoreStatus{}, err
}

func restoreStatus(s *restoreSession) RestoreStatus {
	if s == nil {
		return RestoreStatus{}
	}
	return RestoreStatus{RestoreID: s.id, State: s.state, ReceivedPages: uint64(len(s.pages)), Warnings: append([]model.Warning(nil), s.warnings...), Incarnation: s.incarnation}
}

func cloneMap(input map[string]any) map[string]any {
	data, _ := json.Marshal(input)
	var output map[string]any
	_ = json.Unmarshal(data, &output)
	return output
}

func newID(prefix string) string {
	value := time.Now().UTC().UnixNano()
	return prefix + "-" + strconv.FormatInt(value, 10)
}

func (m *Manager) apply(ctx context.Context, archive *dumpformat.Archive, principal model.PrincipalRef, meta model.WriteMeta, archiveBytes []byte) (model.Changeset, error) {
	records, err := restoreRecords(archive)
	if err != nil {
		return model.Changeset{}, &model.Error{Data: model.ErrorData{Code: "restore_violation", Hint: err.Error()}}
	}
	allocatorSnapshot, err := decodeAllocatorSnapshot(archive)
	if err != nil {
		return model.Changeset{}, restoreAdmissionError(err)
	}
	ancestorPlan, err := nativeAncestorPlanForArchive(ctx, records, archive.Native())
	if err != nil {
		return model.Changeset{}, &model.Error{Data: model.ErrorData{Code: "restore_violation", Hint: err.Error()}}
	}
	if m.lease == nil {
		return model.Changeset{}, &model.Error{Data: model.ErrorData{Code: "lease_not_held", Hint: "restore requires an active storage lease"}}
	}
	tx, err := m.store.Write(ctx, m.lease)
	if err != nil {
		return model.Changeset{}, err
	}
	defer tx.Close()
	capability, ok := tx.(driver.NativeRestoreTx)
	if !ok {
		return model.Changeset{}, &model.Error{Data: model.ErrorData{Code: "restore_violation", Hint: "native restore capability is unsupported"}}
	}
	allocatorRestore, ok := tx.(driver.NativeAllocatorRestoreTx)
	if !ok {
		return model.Changeset{}, &model.Error{Data: model.ErrorData{Code: "restore_violation", Hint: "native allocator restore capability is unsupported"}}
	}
	locks, sentinels, err := restoreLocks(records, ancestorPlan, allocatorSnapshot)
	if err != nil {
		return model.Changeset{}, &model.Error{Data: model.ErrorData{Code: "restore_violation", Hint: err.Error()}}
	}
	states, err := tx.Lock(ctx, locks)
	if err != nil {
		return model.Changeset{}, err
	}
	if len(states) != len(locks) {
		return model.Changeset{}, &model.Error{Data: model.ErrorData{Code: "restore_violation", Hint: "driver acquired an incomplete restore lock set"}}
	}
	for _, state := range states {
		if state.Key.Kind == driver.LockRecord && state.Present {
			return model.Changeset{}, &model.Error{Data: model.ErrorData{Code: "restore_requires_empty", Hint: "restore target contains records"}}
		}
	}
	head, err := tx.Head(ctx)
	if err != nil {
		return model.Changeset{}, err
	}
	if !head.IsZero() {
		return model.Changeset{}, &model.Error{Data: model.ErrorData{Code: "restore_requires_empty", Hint: "restore target changed during restore"}}
	}
	page, err := tx.Query(ctx, driver.Query{Limit: 1})
	if err != nil {
		return model.Changeset{}, err
	}
	if len(page.Items) != 0 {
		return model.Changeset{}, &model.Error{Data: model.ErrorData{Code: "restore_requires_empty", Hint: "restore target contains records"}}
	}
	changesets, err := tx.Changesets(ctx, nil, 1)
	if err != nil {
		return model.Changeset{}, err
	}
	if len(changesets) != 0 {
		return model.Changeset{}, &model.Error{Data: model.ErrorData{Code: "restore_requires_empty", Hint: "restore target contains history"}}
	}
	if meta.Method == "" {
		meta.Method = "admin.restore.finish"
	}
	if meta.IdempotencyKey == "" {
		meta.IdempotencyKey = memberDigest(archiveBytes)
	}
	requestHash := memberDigest(archiveBytes)
	allocatorBinding, err := allocatorSnapshot.Binding()
	if err != nil {
		return model.Changeset{}, restoreAdmissionError(err)
	}
	prepared, err := capability.PrepareNativeRestore(ctx, driver.NativeRestoreProfile{
		StoreID: driver.StoreID(archive.Manifest().NativeStoreID), AllocatorBinding: allocatorBinding, Principal: principal,
		Method: meta.Method, AgentLabel: meta.AgentLabel, IdempotencyKey: meta.IdempotencyKey, RequestHash: requestHash,
	})
	if err != nil {
		return model.Changeset{}, restoreAdmissionError(err)
	}
	if err := allocatorRestore.RestoreNativeAllocators(ctx, allocatorSnapshot); err != nil {
		return model.Changeset{}, restoreAdmissionError(err)
	}
	for _, record := range records {
		if err := pipeline.PutSourceRecord(ctx, tx, record); err != nil {
			return model.Changeset{}, err
		}
	}
	// Native dumps may carry converter reverse evidence as content-addressed
	// blobs. Publish each authenticated blob in the same atomic restore as its
	// records. Legacy native archives simply have no matching members here.
	blobPaths := make([]string, 0)
	for path := range archive.Manifest().Files {
		if strings.HasPrefix(path, "blobs/") {
			blobPaths = append(blobPaths, path)
		}
	}
	sort.Strings(blobPaths)
	for _, path := range blobPaths {
		name := strings.TrimPrefix(path, "blobs/")
		if !isDigest(name) {
			return model.Changeset{}, fmt.Errorf("native evidence blob path is not content-addressed")
		}
		data, ok := archive.NativeMember(path)
		if !ok || memberDigest(data) != name {
			return model.Changeset{}, fmt.Errorf("native evidence blob digest mismatch")
		}
		actual, err := tx.PutBlob(ctx, data)
		if err != nil {
			return model.Changeset{}, err
		}
		if string(actual) != name {
			return model.Changeset{}, fmt.Errorf("native evidence blob digest mismatch")
		}
	}
	if ancestorPlan != nil && len(ancestorPlan.rows) != 0 {
		if err := tx.PutDerived(ctx, driver.DerivedAncestorClosure, nil, ancestorPlan.rows); err != nil {
			return model.Changeset{}, err
		}
	}
	if err := tx.BumpSentinels(ctx, sentinels); err != nil {
		return model.Changeset{}, err
	}
	// Without complete typed publication, legacy untyped writes need not bump
	// graph/blocking. Do not create a reusable certificate on that path.
	_, typedGraph := tx.(driver.GraphSourceWriter)
	if ancestorPlan != nil && len(ancestorPlan.rows) != 0 && typedGraph {
		if err := tx.BumpReadFacts(ctx, []driver.ReadFact{pipeline.GraphValidityReadFact}); err != nil {
			return model.Changeset{}, err
		}
	}
	changes := make([]model.ChangeRecord, 0, len(records))
	results := make([]model.OpResult, 0, len(records))
	nextRevision, err := head.Uint64()
	if err != nil || nextRevision == ^uint64(0) {
		return model.Changeset{}, fmt.Errorf("restore revision space is exhausted")
	}
	nextRevision++
	for _, record := range records {
		changes = append(changes, model.ChangeRecord{Ref: record.Ref, Op: model.OpCreate, FromRevision: model.Natural64(0)})
		results = append(results, model.OpResult{Ref: record.Ref, Revision: model.Natural64(nextRevision)})
	}
	header := driver.ChangesetHeader{
		Branch: model.Natural64(0), Incarnation: prepared.Incarnation, Kind: model.ChangesetRestore,
		Time: time.Now().UTC(), Principal: principal, IdempotencyKey: meta.IdempotencyKey,
		Method: meta.Method, AgentLabel: meta.AgentLabel, RequestHash: requestHash,
		Response: model.Commit{Results: results, Warnings: []model.Warning{}}, Changes: changes, LockKeys: lockKeysOnly(locks),
	}
	committed, err := tx.Commit(ctx, header)
	return committed, restoreAdmissionError(err)
}

func restoreAdmissionError(err error) error {
	if err == nil {
		return nil
	}
	var allocatorErr *driver.NativeAllocatorSnapshotError
	if errors.As(err, &allocatorErr) {
		reason := allocatorErr.Reason
		if reason.Validate() != nil {
			reason = driver.NativeAllocatorInventoryInvalid
		}
		details := map[string]any{"reason": string(reason)}
		return &model.Error{Data: model.ErrorData{Code: "restore_violation", Hint: err.Error(), Details: details}}
	}
	if errors.Is(err, driver.ErrRestoreTargetNotEmpty) {
		return &model.Error{Data: model.ErrorData{Code: "restore_requires_empty", Hint: "native restore target is initialized; restore requires a fully empty store"}}
	}
	return err
}

// validateSupportedArchive is deliberately stricter than dumpformat.Read.
// The driver contract can publish one current image atomically, but it cannot
// inject source versions, changesets, or converter disclosures. Allocator
// continuation is admitted only through the complete bound native payload. Accepting those archives here would silently flatten them.
// Refuse before the publication transaction instead.
func validateSupportedArchive(archive *dumpformat.Archive) error {
	if archive == nil {
		return fmt.Errorf("restore archive is nil")
	}
	manifest := archive.Manifest()
	if !archive.Native() {
		return fmt.Errorf("converter archive restore is unavailable; source disclosures and allocator state must be preserved by the converter boundary")
	}
	if manifest.NativeStoreID == "" || strings.TrimSpace(manifest.NativeStoreID) != manifest.NativeStoreID || strings.ContainsRune(manifest.NativeStoreID, '\x00') || manifest.StoreID != manifest.NativeStoreID {
		return fmt.Errorf("native archive requires canonical matching StoreID and NativeStoreID")
	}
	if manifest.History != "head" {
		return fmt.Errorf("native full-history restore is unavailable; source versions and changesets cannot be replayed by this driver")
	}
	if len(manifest.Projects) != 0 {
		return fmt.Errorf("native archive allocator state is unsupported; restore requires a head archive without project metadata")
	}
	if len(manifest.Warnings) != 0 {
		return fmt.Errorf("native archive disclosures are unsupported by this restore seam: %s", strings.Join(manifest.Warnings, "; "))
	}
	snapshot, err := decodeAllocatorSnapshot(archive)
	if err != nil {
		return err
	}
	if snapshot.StoreID != driver.StoreID(manifest.NativeStoreID) || snapshot.Incarnation != manifest.NativeIncarnation || snapshot.At.String() != manifest.NativeHead {
		return driver.NewNativeAllocatorSnapshotError(driver.NativeAllocatorSourceMismatch, fmt.Errorf("allocator snapshot does not match native manifest"))
	}
	return nil
}

func decodeAllocatorSnapshot(archive *dumpformat.Archive) (driver.AllocatorSnapshot, error) {
	if archive == nil || !archive.Native() {
		return driver.AllocatorSnapshot{}, driver.NewNativeAllocatorSnapshotError(driver.NativeAllocatorSnapshotMissing, nil)
	}
	raw, ok := archive.NativeMember(dumpformat.NativeAllocatorMember)
	if !ok {
		return driver.AllocatorSnapshot{}, driver.NewNativeAllocatorSnapshotError(driver.NativeAllocatorSnapshotMissing, nil)
	}
	manifest := archive.Manifest()
	if manifest.NativeAllocatorProfile != driver.NativeAllocatorVersion {
		return driver.AllocatorSnapshot{}, driver.NewNativeAllocatorSnapshotError(driver.NativeAllocatorProfileUnsupported, nil)
	}
	if manifest.NativeAllocatorDigest != memberDigest(raw) {
		return driver.AllocatorSnapshot{}, driver.NewNativeAllocatorSnapshotError(driver.NativeAllocatorDigestMismatch, nil)
	}
	snapshot, err := driver.DecodeAllocatorSnapshot(raw)
	if err != nil {
		return driver.AllocatorSnapshot{}, err
	}
	return snapshot, nil
}

func restoreRecords(archive *dumpformat.Archive) ([]driver.Record, error) {
	if archive.Native() {
		if err := validateNativeHistory(archive); err != nil {
			return nil, err
		}
		data, ok := archive.NativeMember("records/native.ndjson")
		if !ok {
			return nil, fmt.Errorf("native archive has no records")
		}
		records, err := decodeNativeRecords(data)
		if err != nil {
			return nil, err
		}
		head, _ := strconv.ParseUint(archive.Manifest().NativeHead, 10, 64)
		for _, record := range records {
			revision, err := record.Revision.Uint64()
			if err != nil || record.Revision.IsZero() || revision > head {
				return nil, fmt.Errorf("native record has invalid revision")
			}
		}
		// Native publication bypasses the ordinary Writer, so apply the same
		// document/ontology field rules before apply can open a transaction or
		// mutate allocator state. The validator names the offending record and
		// the caller maps it to the typed restore_violation envelope.
		if err := service.ValidateNativeRestoreRecords(records); err != nil {
			return nil, err
		}
		return records, nil
	}
	var result []driver.Record
	seen := map[string]bool{}
	for _, kind := range []string{"bead", "edge", "comment", "memory", "config", "beads-archive"} {
		for _, source := range archive.Records(kind) {
			ref := model.RefByID(source.ID, model.Natural64(0))
			key, err := driver.EncodeLockKey(driver.LockKey{Kind: driver.LockRecord, Ref: &ref})
			if err != nil {
				return nil, err
			}
			if seen[key] {
				return nil, fmt.Errorf("duplicate restore record %s", source.ID)
			}
			seen[key] = true
			body := source.Body
			if kind == "bead" {
				body, err = convertBead(source)
				if err != nil {
					return nil, err
				}
			}
			encoded, err := json.Marshal(body)
			if err != nil {
				return nil, err
			}
			result = append(result, driver.Record{Ref: ref, Kind: kind, Body: encoded})
		}
	}
	sort.Slice(result, func(i, j int) bool { return recordSortKey(result[i]) < recordSortKey(result[j]) })
	return result, nil
}

func validateNativeHistory(archive *dumpformat.Archive) error {
	manifest := archive.Manifest()
	head, err := strconv.ParseUint(manifest.NativeHead, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid native archive head")
	}
	versionsData, ok := archive.NativeMember("versions.ndjson")
	if !ok {
		return fmt.Errorf("native archive has no versions")
	}
	seenVersions := map[string]bool{}
	for _, line := range nativeLines(versionsData) {
		var version driver.Version
		if err := decodeNativeLine(line, &version); err != nil {
			return fmt.Errorf("invalid native version: %w", err)
		}
		if err := version.Ref.Validate(); err != nil {
			return fmt.Errorf("invalid native version identity")
		}
		revision, revisionErr := version.Revision.Uint64()
		if revisionErr != nil || version.Revision.IsZero() {
			return fmt.Errorf("invalid native version identity")
		}
		if revision > head {
			return fmt.Errorf("native version is beyond archive head")
		}
		key, err := driver.EncodeLockKey(driver.LockKey{Kind: driver.LockRecord, Ref: &version.Ref})
		if err != nil {
			return err
		}
		key += "\x00" + version.Revision.String()
		if seenVersions[key] {
			return fmt.Errorf("duplicate native version")
		}
		seenVersions[key] = true
		if version.Deleted && (len(version.Body) != 0 || len(version.Patch) != 0) {
			return fmt.Errorf("native tombstone carries a body")
		}
		if !version.Deleted && version.Base && !json.Valid(version.Body) {
			return fmt.Errorf("native base version has invalid body")
		}
		if !version.Deleted && !version.Base && (len(version.Patch) == 0 || !json.Valid(version.Patch)) {
			return fmt.Errorf("native patch version is empty")
		}
	}
	changesetsData, ok := archive.NativeMember("changesets.ndjson")
	if !ok {
		return fmt.Errorf("native archive has no changesets")
	}
	var lastRevision uint64
	seenChangesets := map[string]bool{}
	for _, line := range nativeLines(changesetsData) {
		var changeset model.Changeset
		if err := decodeNativeLine(line, &changeset); err != nil {
			return fmt.Errorf("invalid native changeset: %w", err)
		}
		changesetRevision, revisionErr := changeset.Revision.Uint64()
		if revisionErr != nil || changeset.Revision.IsZero() || changesetRevision > head || changeset.Incarnation != manifest.NativeIncarnation || !changeset.Branch.IsZero() {
			return fmt.Errorf("invalid native changeset identity")
		}
		if lastRevision != 0 && changesetRevision <= lastRevision {
			return fmt.Errorf("native changesets are not strictly ordered")
		}
		lastRevision = changesetRevision
		if seenChangesets[changeset.Revision.String()] {
			return fmt.Errorf("duplicate native changeset")
		}
		seenChangesets[changeset.Revision.String()] = true
		for _, change := range changeset.Changes {
			if err := change.Ref.Validate(); err != nil {
				return fmt.Errorf("invalid native changeset record reference")
			}
			fromRevision, fromErr := change.FromRevision.Uint64()
			if fromErr != nil || fromRevision >= changesetRevision {
				return fmt.Errorf("invalid native changeset record reference")
			}
		}
	}
	if manifest.History == "head" && len(seenChangesets) != 0 {
		return fmt.Errorf("head-only native archive contains changesets")
	}
	return nil
}

func nativeLines(data []byte) [][]byte {
	data = bytes.TrimSuffix(data, []byte{'\n'})
	if len(data) == 0 {
		return nil
	}
	return bytes.Split(data, []byte{'\n'})
}

func decodeNativeLine(line []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(line))
	decoder.UseNumber()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return fmt.Errorf("multiple JSON values")
		}
		return err
	}
	return nil
}

func decodeNativeRecords(data []byte) ([]driver.Record, error) {
	var result []driver.Record
	seen := map[string]bool{}
	for _, line := range nativeLines(data) {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var value nativeRecord
		if err := decodeNativeLine(line, &value); err != nil || value.Kind == "" || !json.Valid(value.Body) {
			return nil, fmt.Errorf("invalid native record")
		}
		if err := value.Ref.Validate(); err != nil {
			return nil, err
		}
		var body map[string]any
		if err := json.Unmarshal(value.Body, &body); err != nil || body == nil {
			return nil, fmt.Errorf("native record body must be an object")
		}
		// A native head image carries the stored protected source, not an
		// asserted runtime snapshot. Validate both identity and source before
		// apply can open a publication transaction, preserving its raw body.
		ontologyID := value.Ref.IsID() && (value.Ref.ID == "ontology" || strings.HasPrefix(value.Ref.ID, "ontology/"))
		if value.Kind == "ontology" || ontologyID {
			if value.Kind != "ontology" || !ontologyID || !value.Ref.Branch.IsZero() {
				return nil, fmt.Errorf("native ontology requires a protected branch-zero ontology ID and kind")
			}
			source, err := service.DecodeOntologySource(value.Body)
			if err != nil {
				return nil, fmt.Errorf("invalid native ontology source: %w", err)
			}
			if _, problems := ontology.Compile(source.Base, source.Overlays, source.Plugins); len(problems) != 0 {
				return nil, fmt.Errorf("native ontology source does not compile")
			}
		}
		key, err := driver.EncodeLockKey(driver.LockKey{Kind: driver.LockRecord, Ref: &value.Ref})
		if err != nil || seen[key] {
			return nil, fmt.Errorf("duplicate or invalid native record")
		}
		seen[key] = true
		result = append(result, driver.Record{Ref: value.Ref, Kind: value.Kind, Revision: value.Revision, Body: append([]byte(nil), value.Body...)})
	}
	sort.Slice(result, func(i, j int) bool { return recordSortKey(result[i]) < recordSortKey(result[j]) })
	return result, nil
}

func recordSortKey(record driver.Record) string {
	key, _ := driver.EncodeLockKey(driver.LockKey{Kind: driver.LockRecord, Ref: &record.Ref})
	return key
}

func convertBead(source dumpformat.Record) (map[string]any, error) {
	if source.ID == "" {
		return nil, fmt.Errorf("bead ID is required")
	}
	typ, _ := source.Body["type"].(string)
	if typ == "" {
		typ = "issue"
	}
	project := source.Project
	if project == "" {
		project, _ = source.Body["project"].(string)
	}
	fields := map[string]any{}
	for key, value := range source.Body {
		if key == "id" || key == "type" || key == "project" {
			continue
		}
		fields[key] = value
	}
	if _, ok := fields["status"]; !ok {
		category, _ := fields["status_category"].(string)
		status := map[string]string{"unstarted": "todo", "started": "in_progress", "completed": "done", "canceled": "canceled", "backlog": "backlog"}[category]
		if status == "" {
			status = "todo"
		}
		if result, _ := fields["result"].(string); category == "completed" && result == "failure" {
			status = "failed"
		}
		fields["status"] = status
	}
	doc := pipeline.Document{ID: source.ID, Type: typ, Project: project, Fields: fields}
	return documentMap(doc)
}

func documentMap(doc pipeline.Document) (map[string]any, error) {
	data, err := json.Marshal(doc)
	if err != nil {
		return nil, err
	}
	var value map[string]any
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	if err := dec.Decode(&value); err != nil {
		return nil, err
	}
	return value, nil
}

func restoreLocks(records []driver.Record, ancestorPlan *nativeAncestorPlan, allocatorSnapshot driver.AllocatorSnapshot) ([]driver.LockRequest, []driver.Sentinel, error) {
	byKey := map[string]driver.LockRequest{}
	sentinelSet := map[driver.Sentinel]bool{}
	add := func(key driver.LockKey, mode driver.LockMode) error {
		encoded, err := driver.EncodeLockKey(key)
		if err != nil {
			return err
		}
		byKey[encoded] = driver.LockRequest{Key: key, Mode: mode}
		return nil
	}
	for _, record := range records {
		ref := record.Ref
		if err := add(driver.LockKey{Kind: driver.LockRecord, Ref: &ref}, driver.LockUpdate); err != nil {
			return nil, nil, err
		}
		if record.Kind == "bead" {
			sentinel, err := driver.NewBeadSentinel(ref.ID)
			if err != nil {
				return nil, nil, err
			}
			sentinelSet[sentinel] = true
		}
	}
	nativeLock, err := driver.NativeAllocatorRestoreLock()
	if err != nil {
		return nil, nil, err
	}
	if err := add(nativeLock, driver.LockUpdate); err != nil {
		return nil, nil, err
	}
	for _, project := range allocatorSnapshot.Inventory.Projects {
		lock, err := driver.AllocatorLock(project.Prefix)
		if err != nil {
			return nil, nil, err
		}
		if err := add(lock, driver.LockUpdate); err != nil {
			return nil, nil, err
		}
	}
	if ancestorPlan != nil {
		if err := add(pipeline.GraphValidityLockKey(), driver.LockUpdate); err != nil {
			return nil, nil, err
		}
		for _, lock := range ancestorPlan.locks {
			if err := add(lock.Key, lock.Mode); err != nil {
				return nil, nil, err
			}
		}
		for _, sentinel := range ancestorPlan.sentinels {
			sentinelSet[sentinel] = true
		}
	}
	sentinelSet[driver.SentinelGraphBlocking] = true
	for sentinel := range sentinelSet {
		if err := add(driver.LockKey{Kind: driver.LockSentinel, Sentinel: sentinel}, driver.LockUpdate); err != nil {
			return nil, nil, err
		}
	}
	keys := make([]string, 0, len(byKey))
	for key := range byKey {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	locks := make([]driver.LockRequest, 0, len(keys))
	for _, key := range keys {
		locks = append(locks, byKey[key])
	}
	if err := driver.ValidateLockSet(locks); err != nil {
		return nil, nil, err
	}
	sentinels := make([]driver.Sentinel, 0, len(sentinelSet))
	for sentinel := range sentinelSet {
		sentinels = append(sentinels, sentinel)
	}
	sort.Slice(sentinels, func(i, j int) bool { return string(sentinels[i]) < string(sentinels[j]) })
	return locks, sentinels, nil
}

func lockKeysOnly(locks []driver.LockRequest) []driver.LockKey {
	keys := make([]driver.LockKey, len(locks))
	for i, lock := range locks {
		keys[i] = lock.Key
	}
	return keys
}
