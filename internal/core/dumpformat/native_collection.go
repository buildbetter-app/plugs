package dumpformat

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"reflect"
	"sort"
	"strings"

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
	binding      NativeJoinedBinding
	metadata     []NativeHistoryMetadata
	files        map[string]File
	config       *os.File
	configRefs   *NativeDeltaBudget
	configOutput *os.File
	elements     []nativeConfigElement
	added        int
	ready        bool
	commitment   []byte
}

type nativeConfigElement struct {
	precedence int
	family     string
	path       string
	identity   string
	offset     int64
	size       int64
}

// NativeCaptureCollection keeps only the joint CURRENT portable image and
// source/file commitments. It owns no source Archive, preparation, historical
// body, opener or original file. The verifier owns the separate disk transcript.
// Only authenticated capture-set Archives can append to this private builder.
type NativeCaptureCollection struct {
	archive *Archive
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
	config, err := os.CreateTemp("", "plug-native-config-*")
	if err != nil {
		return nil, err
	}
	state := &nativeCaptureCollection{binding: detached, files: map[string]File{}, commitment: raw, config: config, configRefs: owner.Scope()}
	defer func() {
		if !ok {
			_ = config.Close()
			_ = os.Remove(config.Name())
		}
	}()
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
	identity := SourceIdentity{want.SourceStoreID, want.SourceProject}
	if err := c.appendCurrent(source, identity); err != nil {
		return err
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

// appendCurrent is the single post-authentication compact-current transfer.
// It has no source-history or original-file ownership.
func (c *NativeCaptureCollection) appendCurrent(source *Archive, identity SourceIdentity) error {
	if c.archive.collection.added > 0 {
		if err := compatibleManifests(c.archive.manifest, source.manifest); err != nil {
			return err
		}
	}
	if err := c.archive.ChargeNativeDeltaJSON("collection_current_manifest", source.manifest); err != nil {
		return err
	}
	manifest := source.Manifest()
	if c.archive.collection.added == 0 {
		c.archive.manifest = manifest
		c.archive.manifest.Kinds = nil
		c.archive.manifest.Projects = nil
		c.archive.manifest.Sources = nil
		c.archive.manifest.Ontology = nil
		c.archive.manifest.StoreID = "merge-" + c.archive.sha256
		c.archive.manifest.Machine = "native-capture-collection"
		c.archive.manifest.SourceIdentityContract = SourceIdentityContract
	}
	// An admitted source may declare kinds absent from earlier sources. Keep
	// one charged, detached registry entry for each kind before finalization.
	for _, kind := range manifest.Kinds {
		present := false
		for _, retained := range c.archive.manifest.Kinds {
			if retained.Name == kind.Name {
				present = true
				break
			}
		}
		if present {
			continue
		}
		if err := c.archive.ChargeNativeDeltaJSON("collection_kind_registry", kind); err != nil {
			return err
		}
		kind.Name, kind.File = strings.Clone(kind.Name), strings.Clone(kind.File)
		c.archive.manifest.Kinds = append(c.archive.manifest.Kinds, kind)
	}
	sort.Slice(c.archive.manifest.Kinds, func(i, j int) bool {
		return c.archive.manifest.Kinds[i].Name < c.archive.manifest.Kinds[j].Name
	})
	c.archive.manifest.Projects = append(c.archive.manifest.Projects, manifest.Projects...)
	for _, directive := range manifest.Ontology {
		if nativeCollectionDirectiveUsed(source, directive) {
			c.archive.manifest.Ontology = append(c.archive.manifest.Ontology, directive)
		}
	}
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
	return c.appendConfig(source, identity)
}

func (c *NativeCaptureCollection) appendConfig(source *Archive, identity SourceIdentity) error {
	state := c.archive.collection
	return source.visitConfigEntries(c.archive.nativeBudget, func(entry externalEntry, _ []byte) error {
		entry.Identity = &identity
		work := c.archive.nativeBudget.Scope()
		defer work.Release()
		if err := work.ChargeJSON("collection_config_encoding", entry); err != nil {
			return err
		}
		encoded, err := json.Marshal(entry)
		if err != nil {
			return err
		}
		// Reserve the retained key strings and the ref slice, including
		// slack and both backing arrays during a growth.
		refBytes := uint64(len(identity.StoreID)+len(identity.Project)+len(entry.Input.Family)+len(entry.Input.Path))*8 + 512
		if err := state.configRefs.ChargeBytes("collection_config_refs", refBytes); err != nil {
			return err
		}
		identityKey := externalEntryKey(entry)
		offset, err := state.config.Seek(0, io.SeekEnd)
		if err != nil {
			return err
		}
		if _, err := state.config.Write(encoded); err != nil {
			return err
		}
		state.elements = append(state.elements, nativeConfigElement{
			precedence: entry.Input.Precedence, family: entry.Input.Family,
			path: entry.Input.Path, identity: identityKey, offset: offset, size: int64(len(encoded)),
		})
		return nil
	})
}

func nativeCollectionDirectiveUsed(source *Archive, directive OntologyDirective) bool {
	// Native status catalogs project gate records to the built-in task shape
	// before reverse conversion. Their map_type declaration is therefore not
	// consumed by the strict reverse converter even when a gate row exists in
	// the source image.
	if directive.Target.Type == "gate" && source.manifest.SourcePlaneAddressContract == SourcePlaneAddressContract && source.manifest.StatusCategoryContract == StatusCategoryContract {
		return false
	}
	for _, record := range source.Records("bead") {
		if record.Project != directive.Target.Project || directive.Target.ID != "" && record.ID != directive.Target.ID {
			continue
		}
		// The body source_type can be rewritten by the P32 alias/import
		// projection. The authenticated physical issue_type is authoritative
		// for deciding whether this directive belongs in the collection.
		typ, _ := record.Source.Fields["issue_type"].(string)
		if typ == directive.Target.Type {
			return true
		}
	}
	return false
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
	configFile, commitment, err := a.collection.writeConfig(a.nativeBudget)
	if err != nil {
		return nil, err
	}
	a.manifest.Files[SourceConfigMember] = commitment
	if err := a.ChargeNativeDeltaJSON("collection_manifest_encoding", a.manifest); err != nil {
		closeConfigFile(configFile)
		return nil, err
	}
	raw, err := json.Marshal(a.manifest)
	if err != nil {
		closeConfigFile(configFile)
		return nil, err
	}
	a.members["manifest.json"] = raw
	state := a.collection
	state.configOutput = configFile
	state.ready = true
	closeConfigFile(state.config)
	state.config = nil
	state.elements = nil
	state.configRefs.Release()
	return a, nil
}

func (state *nativeCaptureCollection) writeConfig(owner *NativeDeltaBudget) (*os.File, File, error) {
	sort.Slice(state.elements, func(i, j int) bool {
		a, b := state.elements[i], state.elements[j]
		if a.precedence != b.precedence {
			return a.precedence < b.precedence
		}
		if a.family != b.family {
			return a.family < b.family
		}
		if a.path != b.path {
			return a.path < b.path
		}
		return a.identity < b.identity
	})
	work := owner.Scope()
	defer work.Release()
	const copyBytes = 32 << 10
	if err := work.ChargeBytes("collection_config_copy_buffer", copyBytes); err != nil {
		return nil, File{}, err
	}
	file, err := os.CreateTemp("", "plug-native-config-output-*")
	if err != nil {
		return nil, File{}, err
	}
	ok := false
	defer func() {
		if !ok {
			closeConfigFile(file)
		}
	}()
	hash := sha256.New()
	output := io.MultiWriter(file, hash)
	if _, err := io.WriteString(output, "["); err != nil {
		return nil, File{}, err
	}
	size := int64(1)
	buffer := make([]byte, copyBytes)
	for i, element := range state.elements {
		if i != 0 {
			if _, err := io.WriteString(output, ","); err != nil {
				return nil, File{}, err
			}
			size++
		}
		n, err := io.CopyBuffer(output, io.NewSectionReader(state.config, element.offset, element.size), buffer)
		if err != nil {
			return nil, File{}, err
		}
		if n != element.size {
			return nil, File{}, io.ErrUnexpectedEOF
		}
		size += n
	}
	if _, err := io.WriteString(output, "]\n"); err != nil {
		return nil, File{}, err
	}
	ok = true
	return file, File{SHA256: hex.EncodeToString(hash.Sum(nil)), Size: size + 2}, nil
}

func closeConfigFile(file *os.File) {
	if file != nil {
		_ = file.Close()
		_ = os.Remove(file.Name())
	}
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
	if a != nil {
		if a.collection != nil {
			closeConfigFile(a.collection.config)
			closeConfigFile(a.collection.configOutput)
		}
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
