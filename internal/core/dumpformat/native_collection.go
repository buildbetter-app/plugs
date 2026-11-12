package dumpformat

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"sort"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// NativeJoinedInput binds one ordered ORIGINAL capture set. ArchiveSHA256 is
// always its root JSON digest, never a hash of a repackaged archive.
type NativeJoinedInput struct {
	RehearsalProject string              `json:"rehearsal_project"`
	Database         string              `json:"database"`
	SourceStoreID    string              `json:"source_store_id"`
	SourceProject    string              `json:"source_project"`
	ArchiveSHA256    string              `json:"archive_sha256"`
	Files            []NativeCaptureFile `json:"files"`
}

type NativeJoinedBinding struct {
	Format        string              `json:"format"`
	PolicySHA256  string              `json:"policy_sha256"`
	ProfileSHA256 string              `json:"profile_sha256"`
	Sources       []NativeJoinedInput `json:"sources"`
}

type nativeCaptureCollection struct {
	binding    NativeJoinedBinding
	metadata   []NativeHistoryMetadata
	files      map[string]File
	added      int
	ready      bool
	commitment []byte
}

// NativeCaptureCollection keeps only the joint CURRENT portable image and
// source/file commitments. It owns no source Archive, preparation, historical
// body, opener or original file. The verifier owns the separate disk transcript.
// Only authenticated capture-set Archives can append to this private builder.
type NativeCaptureCollection struct {
	archive *Archive
	entries []externalEntry
	closed  bool
}

func NewNativeCaptureCollection(ctx context.Context, binding NativeJoinedBinding) (*NativeCaptureCollection, error) {
	if ctx == nil || ctx.Err() != nil {
		return nil, fmt.Errorf("native collection requires live context")
	}
	budget := NativeDeltaBudgetFromContext(ctx)
	if budget == nil {
		return nil, fmt.Errorf("native collection requires shared decoded owner")
	}
	owner := budget.Scope()
	ok := false
	defer func() {
		if !ok {
			owner.Release()
		}
	}()
	if binding.Format != NativeJoinedInputsFormatV2 || !validDeltaSHA256(binding.PolicySHA256) || !validDeltaSHA256(binding.ProfileSHA256) || len(binding.Sources) != 2 {
		return nil, fmt.Errorf("native collection requires ordered v2 binding")
	}
	if err := owner.ChargeJSON("collection_input_binding", binding); err != nil {
		return nil, err
	}
	if err := owner.ChargeJSON("collection_input_encoding", binding); err != nil {
		return nil, err
	}
	validation := owner.Scope()
	defer validation.Release()
	if err := validation.ChargeJSON("collection_binding_validation", binding); err != nil {
		return nil, err
	}
	for i, source := range binding.Sources {
		if !validSourceIdentity(SourceIdentity{source.SourceStoreID, source.SourceProject}) || !validDeltaSHA256(source.ArchiveSHA256) || len(source.Files) < 3 {
			return nil, fmt.Errorf("native collection source binding differs")
		}
		for j, file := range source.Files {
			name := "capture-set.json"
			if j == 1 {
				name = "head.tar"
			}
			if j > 1 {
				name = fmt.Sprintf("parts/%020d.part", j-1)
			}
			if file.Name != name || file.Bytes == 0 || file.Bytes > math.MaxInt64 || !validDeltaSHA256(file.SHA256) || j == 0 && file.SHA256 != source.ArchiveSHA256 {
				return nil, fmt.Errorf("native collection file binding differs")
			}
		}
		if i > 0 && source.SourceStoreID == binding.Sources[0].SourceStoreID {
			return nil, fmt.Errorf("native collection sources overlap")
		}
	}
	// Admission precedes the detached canonical commitment and decoded binding.
	raw, err := json.Marshal(binding)
	if err != nil {
		return nil, err
	}
	var detached NativeJoinedBinding
	if err = decodeJSON(raw, &detached); err != nil {
		return nil, err
	}
	state := &nativeCaptureCollection{binding: detached, files: map[string]File{}, commitment: raw}
	a := &Archive{sha256: digest(raw), nativeBudget: owner, collection: state, byKind: map[string][]Record{}, members: map[string][]byte{}}
	a.manifest.StoreID = "merge-" + a.sha256
	ok = true
	return &NativeCaptureCollection{archive: a}, nil
}

func (c *NativeCaptureCollection) Owner() *Archive {
	if c == nil || c.closed {
		return nil
	}
	return c.archive
}

// Append authenticates the exact bound source and transfers detached current
// candidates to the collection owner. The caller must drop the source and its
// preparations before admitting the next source; this builder never retains it.
func (c *NativeCaptureCollection) Append(ctx context.Context, source *Archive) error {
	if c == nil || c.closed || c.archive.collection.ready || source == nil || !source.HasNativeDelta() || source.decodedBudget().root() != c.archive.decodedBudget().root() {
		return fmt.Errorf("native collection source/owner differs")
	}
	state := c.archive.collection
	if state.added >= 2 {
		return fmt.Errorf("native collection has extra source")
	}
	want := state.binding.Sources[state.added]
	if source.SHA256() != want.ArchiveSHA256 || source.manifest.StoreID != want.SourceStoreID || len(source.manifest.Projects) != 1 || source.manifest.Projects[0].Prefix != want.SourceProject {
		return fmt.Errorf("native collection ordered source differs")
	}
	prepared, err := source.PrepareNativeDelta(ctx)
	if err != nil {
		return err
	}
	validation := c.archive.nativeBudget.Scope()
	defer validation.Release()
	ledger, err := prepared.NativeCaptureFileLedger(validation)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(ledger, want.Files) {
		return fmt.Errorf("native collection authenticated file ledger differs")
	}
	ledger = nil
	if state.added > 0 {
		if err := compatibleManifests(c.archive.manifest, source.manifest); err != nil {
			return err
		}
	}
	if err := c.archive.ChargeNativeDeltaJSON("collection_current_manifest", source.manifest); err != nil {
		return err
	}
	manifest := source.Manifest()
	if state.added == 0 {
		c.archive.manifest = manifest
		c.archive.manifest.Projects = nil
		c.archive.manifest.Sources = nil
		c.archive.manifest.Ontology = nil
		c.archive.manifest.StoreID = "merge-" + c.archive.sha256
		c.archive.manifest.Machine = "native-capture-collection"
		c.archive.manifest.SourceIdentityContract = SourceIdentityContract
	}
	identity := SourceIdentity{want.SourceStoreID, want.SourceProject}
	c.archive.manifest.Projects = append(c.archive.manifest.Projects, manifest.Projects...)
	c.archive.manifest.Ontology = append(c.archive.manifest.Ontology, manifest.Ontology...)
	for _, disposition := range manifest.Sources {
		v := disposition
		v.Identity = &identity
		c.archive.manifest.Sources = append(c.archive.manifest.Sources, v)
	}
	for kind, rows := range source.byKind {
		if err := c.archive.ChargeNativeDeltaJSON("collection_portable_current", rows); err != nil {
			return err
		}
		if err := c.archive.ChargeNativeDeltaBytes("collection_current_source_keys", uint64(len(rows))*(uint64(len(identity.StoreID)+len(identity.Project))+64)); err != nil {
			return err
		}
		for _, row := range source.Records(kind) {
			c.archive.byKind[kind] = append(c.archive.byKind[kind], namespaceRecord(row, identity))
		}
	}
	if err := c.archive.ChargeNativeDeltaBytes("collection_config_decode", uint64(len(source.members["config/sources.json"]))*4); err != nil {
		return err
	}
	var entries []externalEntry
	if err := decodeJSON(source.members["config/sources.json"], &entries); err != nil {
		return err
	}
	for _, entry := range entries {
		entry.Identity = &identity
		c.entries = append(c.entries, entry)
	}
	metadata, err := source.NativeSourceHistoryMetadata()
	if err != nil {
		return err
	}
	if len(metadata) != 1 {
		return fmt.Errorf("native collection source metadata differs")
	}
	if err := c.archive.ChargeNativeDeltaJSON("collection_source_metadata", metadata); err != nil {
		return err
	}
	// Metadata is already a detached small view; it holds no graph or tables.
	state.metadata = append(state.metadata, metadata...)
	for _, file := range want.Files {
		if err := c.archive.ChargeNativeDeltaBytes("collection_original_ledger", uint64(len(file.Name))+256); err != nil {
			return err
		}
		name := NativeCollectionFileName(state.added, file.Name)
		state.files[name] = File{SHA256: file.SHA256, Size: int64(file.Bytes)}
	}
	state.added++
	return ctx.Err()
}

// NativeCollectionFileName is an ordinal-qualified provenance path, never a
// rewrite of an original file's own name or hash.
func NativeCollectionFileName(ordinal int, name string) string {
	return fmt.Sprintf("sources/%020d/%s", ordinal, name)
}

func (c *NativeCaptureCollection) Finish() (*Archive, error) {
	if c == nil || c.closed || c.archive.collection.ready || c.archive.collection.added != 2 {
		return nil, fmt.Errorf("native collection is incomplete or consumed")
	}
	a := c.archive
	if err := validateProjects(a.manifest.Projects); err != nil {
		return nil, &NativeOntologyConflictError{Kind: "project", Count: 1}
	}
	// Each source's declarations were already validated in its own project.
	// Directive IDs and status/type names may repeat across two namespaces.
	a.manifest.Files = map[string]File{}
	for i := range a.manifest.Kinds {
		kind := &a.manifest.Kinds[i]
		rows := a.byKind[kind.Name]
		sort.Slice(rows, func(i, j int) bool {
			return compareSourceRecords(rows[i], rows[j], a.manifest.SourcePlaneAddressContract) < 0
		})
		var size uint64
		for _, row := range rows {
			n, err := NativeDeltaJSONSize(row)
			if err != nil {
				return nil, err
			}
			if err = deltaAddCount(&size, n); err != nil {
				return nil, err
			}
		}
		if err := a.ChargeNativeDeltaBytes("collection_current_members", size); err != nil {
			return nil, err
		}
		var data bytes.Buffer
		for _, row := range rows {
			if err := json.NewEncoder(&data).Encode(row); err != nil {
				return nil, err
			}
		}
		kind.Count = len(rows)
		a.members[kind.File] = data.Bytes()
		a.manifest.Files[kind.File] = File{SHA256: digest(data.Bytes()), Size: int64(data.Len())}
	}
	sort.Slice(c.entries, func(i, j int) bool { return compareExternalEntries(c.entries[i], c.entries[j]) < 0 })
	if err := a.ChargeNativeDeltaJSON("collection_sidecars", c.entries); err != nil {
		return nil, err
	}
	config, err := json.Marshal(c.entries)
	if err != nil {
		return nil, err
	}
	config = append(config, '\n')
	a.members["config/sources.json"] = config
	a.manifest.Files["config/sources.json"] = File{SHA256: digest(config), Size: int64(len(config))}
	if err := a.ChargeNativeDeltaJSON("collection_manifest_encoding", a.manifest); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(a.manifest)
	if err != nil {
		return nil, err
	}
	a.members["manifest.json"] = raw
	a.collection.ready = true
	c.entries = nil
	return a, nil
}

func (a *Archive) HasNativeCaptureCollection() bool {
	return a != nil && a.collection != nil && a.collection.ready
}

func (a *Archive) NativeCollectionFiles() (map[string]File, error) {
	if !a.HasNativeCaptureCollection() {
		return nil, fmt.Errorf("native collection unavailable")
	}
	if err := a.ChargeNativeDeltaJSON("collection_detached_file_ledger", a.collection.files); err != nil {
		return nil, err
	}
	result := make(map[string]File, len(a.collection.files))
	for name, file := range a.collection.files {
		result[name] = file
	}
	return result, nil
}

func (a *Archive) NativeCollectionCommitment() ([]byte, error) {
	if !a.HasNativeCaptureCollection() {
		return nil, fmt.Errorf("native collection unavailable")
	}
	if err := a.ChargeNativeDeltaBytes("collection_detached_commitment", uint64(len(a.collection.commitment))); err != nil {
		return nil, err
	}
	return bytes.Clone(a.collection.commitment), nil
}

func (c *NativeCaptureCollection) Close() {
	if c == nil || c.closed {
		return
	}
	c.closed = true
	a := c.archive
	c.archive = nil
	c.entries = nil
	if a != nil {
		a.byKind = nil
		a.members = nil
		a.collection = nil
		a.manifest = Manifest{}
		a.ReleaseNativeDecoded()
	}
}

// DiscardNativeCaptureSet is for an exclusive source owner after its last
// callback. It drops actual Archive/preparation/raw aliases before releasing
// the source reservation. No borrowed Archive/view may outlive this boundary.
func (a *Archive) DiscardNativeCaptureSet() {
	if a == nil {
		return
	}
	budget := a.decodedBudget()
	a.ClearNativeDeltaPreparation()
	*a = Archive{}
	if budget != nil {
		budget.Release()
	}
}

// NativeOntologyConflictError reports only a fixed conflict category/count.
type NativeOntologyConflictError struct {
	Kind  string `json:"kind"`
	Count int    `json:"count"`
}

func (e *NativeOntologyConflictError) Error() string {
	return fmt.Sprintf("native_ontology_conflict: kind=%s count=%d", e.Kind, e.Count)
}
func (e *NativeOntologyConflictError) Unwrap() error { return driver.ErrUnsupported }
