package dumpformat

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
	"os"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

var prefixPattern = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9_-]{0,31}$`)

var knownKinds = map[string]bool{
	"bead": true, "edge": true, "comment": true, "memory": true,
	"config": true, "beads-archive": true,
}

// Native records include the protected ontology source stored by Plug. The
// offline converter has its own registry; unknown native kinds still refuse.
var knownNativeKinds = map[string]bool{
	"bead": true, "edge": true, "comment": true, "memory": true,
	"config": true, "beads-archive": true, "ontology": true,
	"instance_config": true, "restore-archive": true,
}

var externalFamilies = []string{
	"user-legacy-config", "user-xdg-config", "user-native-config",
	"project-config", "beads-dir-config", "project-local-config",
	"environment-and-flags", "metadata-json", "legacy-config-json",
	"beads-hooks", "legacy-hooks", "git-hooks", "external-hook-managers",
	"routes-jsonl", "formulas", "formula-search-paths", "issues-jsonl",
	"interactions-jsonl", "dolt-ignore", "dolt-nonlocal-tables",
	"dolt-working-set", "dolt-commit-graph", "dolt-remotes",
	"dolt-conflicts", "dolt-history-maintenance", "store-files-and-quiescence",
}

var verificationLimits = map[string]string{
	"capture":      "operator declarations; freeze/remotes/working-set not independently verified",
	"schema":       "paired migration hashes and exact column names; SQL types, indexes and constraints not independently verified",
	"history":      "available source rows archived only; no A6 replay or completeness claim",
	"a3":           "not-run",
	"authz.verify": "not-run",
	"derived":      "not-run",
	"plugin_lock":  "not-produced",
	"ontology":     "built-in status mapping only",
}

// Read opens and validates the complete archive in memory. It retains member
// bytes so an incomplete restore can hand the original archive to a later,
// authorized seam without silently dropping source evidence.
func ReadFile(path string) (*Archive, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	return Read(file)
}

func Read(reader io.Reader) (*Archive, error) {
	return readArchive(reader, false)
}

func readArchive(reader io.Reader, nativeDeltaAttachment bool) (*Archive, error) {
	return readArchiveWithBudget(reader, nativeDeltaAttachment, NewNativeDeltaBudget(0))
}

// ReadWithNativeDeltaAccounting preserves Read's validation and byte domains,
// sharing an optional receiving budget when recovery reopens retained evidence.
// The returned archive owns its reservations until ReleaseNativeDecoded.
func ReadWithNativeDeltaAccounting(ctx context.Context, reader io.Reader) (*Archive, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	budget := NativeDeltaBudgetFromContext(ctx)
	if budget == nil {
		budget = NewNativeDeltaBudget(0)
	}
	return readArchiveWithBudget(reader, false, budget)
}

func readArchiveWithBudget(reader io.Reader, nativeDeltaAttachment bool, budget *NativeDeltaBudget) (*Archive, error) {
	owner := budget.Scope()
	data, err := nativeDeltaReadBounded(reader, MaxArchiveBytes, owner, "source_tar")
	if err != nil {
		owner.Release()
		return nil, fmt.Errorf("read archive: %w", err)
	}
	archive, err := readArchiveData(data, nativeDeltaAttachment, owner)
	if err != nil {
		owner.Release()
		return nil, err
	}
	if len(archive.tarBytes) == 0 {
		// Native-store validation keeps members, not the source envelope.
		owner.ReleaseBytes("source_tar", uint64(cap(data)))
	}
	return archive, nil
}

// The v2 attachment already owns an admitted head buffer; parse it directly
// rather than creating another complete io.ReadAll/tar copy.
func readArchiveData(data []byte, nativeDeltaAttachment bool, budget *NativeDeltaBudget) (*Archive, error) {
	if err := validateTarEnd(bytes.NewReader(data), int64(len(data))); err != nil {
		return nil, err
	}
	return readArchiveMembers(bytes.NewReader(data), data, digest(data), nativeDeltaAttachment, budget, nil)
}

func validateTarEnd(raw io.ReaderAt, size int64) error {
	var end, zero [1024]byte
	if size < int64(len(end)) || size%512 != 0 {
		return fmt.Errorf("invalid tar length or end blocks")
	}
	if _, err := raw.ReadAt(end[:], size-int64(len(end))); err != nil || end != zero {
		return fmt.Errorf("invalid tar length or end blocks")
	}
	return nil
}

type tarPadding struct{}

func (tarPadding) Write(raw []byte) (int, error) {
	for _, value := range raw {
		if value != 0 {
			return 0, fmt.Errorf("unaccounted trailing archive data")
		}
	}
	return len(raw), nil
}

func readArchiveMembers(input io.Reader, data []byte, sha string, nativeDeltaAttachment bool, budget, bufferBound *NativeDeltaBudget) (*Archive, error) {
	var decodeWork *NativeDeltaBudget
	if budget != nil {
		decodeWork = budget.Scope()
		defer decodeWork.Release()
	}
	charge := func(owner *NativeDeltaBudget, phase string, size uint64) error {
		if bufferBound != nil {
			if err := bufferBound.ChargeBytes(phase, size); err != nil {
				return err
			}
		}
		return owner.ChargeBytes(phase, size)
	}

	members := map[string][]byte{}
	var manifest Manifest
	var manifestFields map[string]json.RawMessage
	var sourceHistory *SourceHistory
	paths := []string{}
	tr := tar.NewReader(input)
	for {
		header, nextErr := tr.Next()
		if nextErr == io.EOF {
			break
		}
		if nextErr != nil {
			return nil, fmt.Errorf("invalid tar member: %w", nextErr)
		}
		if header.Typeflag != tar.TypeReg || header.Size < 0 || header.Size > MaxArchiveBytes ||
			header.Mode != 0o600 || header.Uid != 0 || header.Gid != 0 || header.Linkname != "" || header.Uname != "" || header.Gname != "" || header.Devmajor != 0 || header.Devminor != 0 ||
			len(header.PAXRecords) != 0 || header.Format != tar.FormatUSTAR || !header.ModTime.Equal(time.Unix(0, 0)) {
			return nil, fmt.Errorf("noncanonical tar member %q", header.Name)
		}
		if !allowedMember(header.Name) {
			return nil, fmt.Errorf("unknown archive member %q", header.Name)
		}
		if _, exists := members[header.Name]; exists {
			return nil, fmt.Errorf("duplicate archive member %q", header.Name)
		}
		var member []byte
		var readErr error
		if budget == nil {
			member, readErr = io.ReadAll(tr)
		} else {
			if err := charge(budget, "head_member_payloads", uint64(header.Size)); err != nil {
				return nil, err
			}
			member = make([]byte, int(header.Size))
			_, readErr = io.ReadFull(tr, member)
		}
		if readErr != nil {
			return nil, fmt.Errorf("read archive member %q: %w", header.Name, readErr)
		}
		switch {
		case header.Name == SourceConfigMember && nativeDeltaAttachment:
			// The encoded member stays available for exact source readback.
			// Only one decoded element is live across each validation pass.
			if err := visitJSONElements(bytes.NewReader(member), true, configAccounting{decoded: decodeWork}, func([]byte, uint64) error { return nil }); err != nil {
				return nil, fmt.Errorf("validate retained head member %q: %w", header.Name, err)
			}
		case bufferBound != nil && header.Name == "manifest.json":
			manifestFields, readErr = decodeRetainedHeadJSON(member, header.Name, budget, &manifest)
		case bufferBound != nil && header.Name == SourceHistoryMember:
			sourceHistory = new(SourceHistory)
			_, readErr = decodeRetainedHeadJSON(member, header.Name, budget, sourceHistory)
		case bufferBound != nil && !strings.HasSuffix(header.Name, ".ndjson") && !strings.HasPrefix(header.Name, "blobs/"):
			// A capture-set head has no allocator or separate native history
			// attachments. Refuse them before attempting any JSON decode.
			return nil, fmt.Errorf("unexpected JSON attachment in retained capture head")
		case decodeWork != nil && !strings.HasPrefix(header.Name, "blobs/"):
			// Head members are JSON or NDJSON. Every retained decoded value is
			// admitted from its compact escaped source preimage before any
			// legacy validator parses or transforms it.
			var count uint64
			admit := func(line []byte, index uint64) error {
				if bufferBound != nil {
					footprint, err := driver.ReverseJSONElementFootprint(line, 0)
					if err != nil {
						return driver.LocateReverseJSONLimit(err, header.Name, "$", index)
					}
					if err := budget.ChargeBytes("head_decoded_shapes", footprint); err != nil {
						return err
					}
				}
				c := deltaSizeCounter{}
				if err := c.raw(line); err != nil {
					return err
				}
				if err := deltaAddCount(&count, c.n+1); err != nil {
					return err
				}
				return nil
			}
			for index, rest := uint64(0), member; len(rest) != 0; index++ {
				line := rest
				if strings.HasSuffix(header.Name, ".ndjson") {
					line, rest, _ = bytes.Cut(rest, []byte{'\n'})
				} else {
					rest = nil
				}
				if len(bytes.TrimSpace(line)) == 0 {
					continue
				}
				if err := admit(line, index); err != nil {
					return nil, err
				}
			}
			if err := budget.ChargeBytes("head_decoded_payloads", count); err != nil {
				return nil, err
			}
			for _, phase := range []string{"head_validation_input", "head_shape_input", "head_typed_input"} {
				if err := decodeWork.ChargeBytes(phase, uint64(len(member))); err != nil {
					return nil, err
				}
			}
			for _, phase := range []string{"head_validation_tokens", "head_shape_values"} {
				if err := decodeWork.ChargeBytes(phase, count); err != nil {
					return nil, err
				}
			}
		}
		if readErr != nil {
			return nil, fmt.Errorf("validate retained head member %q: %w", header.Name, readErr)
		}
		members[header.Name] = member
		paths = append(paths, header.Name)
	}
	if _, err := io.Copy(tarPadding{}, input); err != nil {
		return nil, err
	}
	if len(paths) == 0 || paths[0] != "manifest.json" || !sort.StringsAreSorted(paths[1:]) {
		return nil, fmt.Errorf("manifest must be first and remaining members sorted")
	}

	if manifestFields == nil {
		if err := decodeJSON(members["manifest.json"], &manifest); err != nil {
			return nil, fmt.Errorf("invalid manifest: %w", err)
		}
		if err := json.Unmarshal(members["manifest.json"], &manifestFields); err != nil {
			return nil, err
		}
	}
	if _, present := manifestFields["source_fields_contract"]; present && manifest.SourceFieldsContract != IssueSourceFieldsContract {
		return nil, fmt.Errorf("unsupported source fields contract")
	}
	if _, present := manifestFields["source_plane_address_contract"]; present && manifest.SourcePlaneAddressContract != SourcePlaneAddressContract {
		return nil, fmt.Errorf("unsupported source plane address contract")
	}
	if err := validateStatusCategoryMarker(manifestFields, manifest); err != nil {
		return nil, err
	}
	if err := validateSourceFamilyMarker(manifestFields, manifest); err != nil {
		return nil, err
	}
	if err := validateSourceIdentityMarker(manifestFields, manifest); err != nil {
		return nil, err
	}
	if marker, present := manifestFields["native_history_contract"]; present {
		if !nativeDeltaAttachment || manifest.NativeHistoryContract != NativeDeltaFormat || string(marker) == "null" || manifest.SourceFamilyContract != SourceFamilyContract || manifest.Scope != Scope {
			return nil, fmt.Errorf("native delta history requires complete capture-set attachment")
		}
	} else if nativeDeltaAttachment {
		return nil, fmt.Errorf("capture-set head lacks native delta marker")
	}
	if manifest.Scope == "plug-native-store" {
		if manifest.SourceFieldsContract != "" || manifest.SourcePlaneAddressContract != "" || manifest.StatusCategoryContract != "" {
			return nil, fmt.Errorf("Beads source fields contract requires offline source scope")
		}
		if err := validateNativeArchive(manifest, members, paths); err != nil {
			return nil, err
		}
		return &Archive{sha256: sha, manifest: manifest, members: members, native: true, nativeBudget: budget}, nil
	}
	if _, present := members[NativeAllocatorMember]; present || manifest.NativeAllocatorProfile != "" || manifest.NativeAllocatorDigest != "" || manifest.NativeReverseProjection != nil {
		return nil, fmt.Errorf("native allocator snapshot requires plug-native-store scope")
	}
	config := configEntrySource{data: members[SourceConfigMember], accounting: configAccounting{decoded: decodeWork}, bounded: nativeDeltaAttachment}
	if err := validateManifest(manifest, members, config); err != nil {
		return nil, err
	}
	byKind, migration, err := validateRecords(manifest, members, config, nativeDeltaAttachment)
	if err != nil {
		return nil, err
	}
	retainedTar := data
	if budget == nil {
		retainedTar = append([]byte(nil), data...)
	}
	archive := &Archive{sha256: sha, tarBytes: retainedTar, manifest: manifest, members: members, byKind: byKind, migration: migration, nativeBudget: budget}
	if !nativeDeltaAttachment {
		if err := validateNativeSourceHistories(archive); err != nil {
			return nil, err
		}
	} else {
		for name := range members {
			if name == NativeHistoryMember {
				return nil, fmt.Errorf("delta head contains aggregate native history")
			}
			if _, ok := scopedNativeMember(name); ok {
				return nil, fmt.Errorf("delta head contains aggregate native history")
			}
		}
	}
	if err := validateSourceHistory(archive, sourceHistory); err != nil {
		return nil, err
	}
	return archive, nil
}

func Validate(reader io.Reader) (Manifest, error) {
	archive, err := Read(reader)
	if err != nil {
		return Manifest{}, err
	}
	return archive.Manifest(), nil
}

func allowedMember(name string) bool {
	if name == "manifest.json" || name == "config/sources.json" || name == SourceHistoryMember || name == NativeHistoryMember {
		return true
	}
	if _, ok := scopedNativeMember(name); ok {
		return true
	}
	if _, ok := scopedNativeSourceAttachment(name); ok {
		return true
	}
	if name == "records/native.ndjson" || name == "versions.ndjson" || name == "changesets.ndjson" || name == NativeAllocatorMember {
		return true
	}
	if strings.HasPrefix(name, "blobs/") && len(strings.TrimPrefix(name, "blobs/")) == 64 {
		return true
	}
	if !strings.HasPrefix(name, "records/") || !strings.HasSuffix(name, ".ndjson") {
		return false
	}
	return knownKinds[strings.TrimSuffix(strings.TrimPrefix(name, "records/"), ".ndjson")]
}

// validateNativeArchive validates the archive written by admin.dump. It is a
// deliberately small, typed transport envelope: storage drivers remain the
// authority for applying records and no source migration claims are inferred.
func validateNativeArchive(manifest Manifest, members map[string][]byte, paths []string) error {
	if manifest.Format != Format || manifest.Schema != 1 || manifest.Scope != "plug-native-store" ||
		manifest.NativeStoreID == "" || manifest.NativeIncarnation == "" || manifest.NativeHead == "" ||
		manifest.NativeDigest == "" || manifest.NativeReverseProjection != nil || manifest.RecordCount < 0 || manifest.VersionCount < 0 || manifest.ChangesetCount < 0 {
		return fmt.Errorf("unsupported native archive manifest")
	}
	if manifest.History != "full" && manifest.History != "head" {
		return fmt.Errorf("unsupported native history claim %q", manifest.History)
	}
	if _, err := strconv.ParseUint(manifest.NativeHead, 10, 64); err != nil {
		return fmt.Errorf("invalid native head revision")
	}
	if len(paths) < 4 || paths[0] != "manifest.json" || !sort.StringsAreSorted(paths[1:]) {
		return fmt.Errorf("native archive members are not canonical")
	}
	for path, file := range manifest.Files {
		member, ok := members[path]
		if !ok || path == "manifest.json" || file.Size != int64(len(member)) || file.SHA256 != digest(member) {
			return fmt.Errorf("native checksum/size mismatch for %q", path)
		}
		if strings.HasPrefix(path, "blobs/") {
			name := strings.TrimPrefix(path, "blobs/")
			decoded, err := hex.DecodeString(name)
			if err != nil || len(decoded) != sha256.Size || name != file.SHA256 {
				return fmt.Errorf("native evidence blob path is not content-addressed for %q", path)
			}
		}
	}
	if len(manifest.Files) != len(members)-1 || manifest.Files["records/native.ndjson"].Size < 0 ||
		manifest.Files["versions.ndjson"].Size < 0 || manifest.Files["changesets.ndjson"].Size < 0 {
		return fmt.Errorf("native archive file inventory is incomplete")
	}
	for _, path := range []string{"config/sources.json", "records/native.ndjson", "versions.ndjson", "changesets.ndjson"} {
		if _, ok := members[path]; !ok {
			return fmt.Errorf("native archive is missing %q", path)
		}
		if _, ok := manifest.Files[path]; !ok {
			return fmt.Errorf("native archive does not checksum %q", path)
		}
	}
	// The allocator member was added after the original native envelope. Keep
	// the envelope parser backward-compatible so the restore manager can map a
	// missing legacy member to its typed semantic refusal before publication.
	if allocator, ok := members[NativeAllocatorMember]; ok {
		if err := validateNativeAllocatorMember(allocator); err != nil {
			return err
		}
		if manifest.NativeAllocatorDigest != digest(allocator) {
			return fmt.Errorf("native allocator digest mismatch")
		}
		if file, ok := manifest.Files[NativeAllocatorMember]; !ok || file.Size != int64(len(allocator)) || file.SHA256 != digest(allocator) {
			return fmt.Errorf("native allocator snapshot checksum/size mismatch")
		}
	}
	for path := range members {
		if path != "manifest.json" {
			if _, ok := manifest.Files[path]; !ok {
				return fmt.Errorf("native archive member %q is not inventoried", path)
			}
		}
	}
	if err := validateNativeLines(members["records/native.ndjson"], manifest.RecordCount, "record"); err != nil {
		return err
	}
	if err := validateNativeLines(members["versions.ndjson"], manifest.VersionCount, "version"); err != nil {
		return err
	}
	if err := validateNativeLines(members["changesets.ndjson"], manifest.ChangesetCount, "changeset"); err != nil {
		return err
	}
	// NativeDigest binds all non-manifest members in canonical path order. The
	// manifest itself is intentionally excluded so it can carry the digest.
	h := sha256.New()
	for _, path := range append([]string(nil), paths[1:]...) {
		if path == "manifest.json" {
			continue
		}
		_, _ = io.WriteString(h, path)
		_, _ = h.Write(members[path])
	}
	if hex.EncodeToString(h.Sum(nil)) != manifest.NativeDigest {
		return fmt.Errorf("native archive digest mismatch")
	}
	return nil
}

// validateNativeAllocatorMember checks only the transport shape. The driver
// owns the typed codec, shared here to enforce exact counters and canonical
// bytes. Closed profile/coverage refusals remain semantic manager decisions.
func validateNativeAllocatorMember(raw []byte) error {
	var value struct {
		Version       string            `json:"version"`
		StoreID       string            `json:"store_id"`
		Incarnation   string            `json:"incarnation"`
		At            json.RawMessage   `json:"at"`
		CoverageFloor json.RawMessage   `json:"coverage_floor"`
		Projects      []json.RawMessage `json:"projects"`
		Parents       []json.RawMessage `json:"parents"`
	}
	check := json.NewDecoder(bytes.NewReader(raw))
	check.UseNumber()
	if err := checkValue(check, 0); err != nil {
		return fmt.Errorf("invalid native allocator snapshot: %w", err)
	}
	if _, err := check.Token(); err != io.EOF {
		return fmt.Errorf("invalid native allocator snapshot: trailing JSON data")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&value); err != nil {
		return fmt.Errorf("invalid native allocator snapshot: %w", err)
	}
	if value.Version == "" || value.StoreID == "" || value.Incarnation == "" || len(bytes.TrimSpace(value.At)) == 0 || len(bytes.TrimSpace(value.CoverageFloor)) == 0 || bytes.Equal(bytes.TrimSpace(value.At), []byte("null")) || bytes.Equal(bytes.TrimSpace(value.CoverageFloor), []byte("null")) || value.Projects == nil || value.Parents == nil {
		return fmt.Errorf("invalid native allocator snapshot envelope")
	}
	if _, err := driver.DecodeAllocatorSnapshot(raw); err != nil {
		var semantic *driver.NativeAllocatorSnapshotError
		if errors.As(err, &semantic) && (semantic.Reason == driver.NativeAllocatorProfileUnsupported || semantic.Reason == driver.NativeAllocatorCoverageUnavailable) {
			return nil
		}
		return fmt.Errorf("invalid native allocator snapshot: %w", err)
	}
	return nil
}

func validateNativeLines(data []byte, want int, label string) error {
	if want == 0 {
		if len(data) != 0 {
			return fmt.Errorf("native %s member is not empty", label)
		}
		return nil
	}
	if len(data) == 0 || data[len(data)-1] != '\n' {
		return fmt.Errorf("native %s member is unterminated", label)
	}
	lines := bytes.Split(data[:len(data)-1], []byte{'\n'})
	if len(lines) != want {
		return fmt.Errorf("native %s count mismatch", label)
	}
	for _, line := range lines {
		if err := validateNativeLine(line, label); err != nil {
			return err
		}
	}
	return nil
}

func validateNativeLine(line []byte, label string) error {
	var value any
	if err := decodeJSON(line, &value); err != nil {
		return fmt.Errorf("invalid native %s: %w", label, err)
	}
	object, ok := value.(map[string]any)
	if !ok {
		return fmt.Errorf("native %s must be an object", label)
	}
	if label == "record" {
		kind, ok := object["kind"].(string)
		if !ok || !knownNativeKinds[kind] {
			return fmt.Errorf("unknown native record kind %q", kind)
		}
		if _, ok := object["ref"].(map[string]any); !ok {
			return fmt.Errorf("native record ref must be an object")
		}
		if _, ok := object["body"]; !ok {
			return fmt.Errorf("native record body is required")
		}
	}
	return nil
}

func validateManifest(manifest Manifest, members map[string][]byte, config configEntrySource) error {
	if err := validateStatusCategoryContract(manifest); err != nil {
		return err
	}
	if manifest.SourcePlaneAddressContract != "" && manifest.SourcePlaneAddressContract != SourcePlaneAddressContract {
		return fmt.Errorf("unsupported source plane address contract")
	}
	if manifest.SourceFieldsContract != "" && manifest.SourceFieldsContract != IssueSourceFieldsContract {
		return fmt.Errorf("unsupported source fields contract")
	}
	if manifest.SourceFamilyContract == SourceFamilyContract {
		if _, err := NativeSchemaForVersion(manifest.MainVersion, manifest.IgnoredVersion); err != nil {
			return err
		}
	} else if manifest.MainVersion != 72 || manifest.IgnoredVersion != 28 {
		return NativeSchemaError{manifest.MainVersion, manifest.IgnoredVersion, "own-version import requires native source evidence"}
	}
	if manifest.Format != Format || manifest.Schema != 1 || manifest.Reference != BeadsReference || manifest.StoreID == "" ||
		manifest.Machine == "" || manifest.Scope != Scope {
		return fmt.Errorf("unsupported manifest schema, source or scope")
	}
	if manifest.History != "unavailable" && manifest.History != "available-unverified" && manifest.History != "verified" {
		return fmt.Errorf("unsupported history claim %q", manifest.History)
	}
	if len(manifest.Warnings) != 2 || manifest.Warnings[0] != "not a complete store migration or Plug restore certificate" || manifest.Warnings[1] != "source history and attribution are unverified; no Plug versions, changesets or live leases invented" {
		return fmt.Errorf("missing incomplete-conversion disclosures")
	}
	expectedVerification := make(map[string]string, len(verificationLimits))
	for key, value := range verificationLimits {
		expectedVerification[key] = value
	}
	if manifest.StatusCategoryContract == StatusCategoryContract {
		expectedVerification["ontology"] = NativeStatusOntologyVerification
	} else if declaration := manifest.Verification["ontology"]; declaration == explicitOntologyVerification || declaration == StrictUnionOntologyVerification {
		expectedVerification["ontology"] = declaration
	} else if len(manifest.Ontology) != 0 {
		return fmt.Errorf("explicit ontology directives require their verification disclosure")
	}
	wantVerification, _ := json.Marshal(expectedVerification)
	gotVerification, _ := json.Marshal(manifest.Verification)
	if !bytes.Equal(append(wantVerification, '\n'), append(gotVerification, '\n')) {
		return fmt.Errorf("unsupported verification claim")
	}
	if err := validateProjects(manifest.Projects); err != nil {
		return err
	}
	if manifest.SourceIdentityContract == SourceIdentityContract {
		for _, project := range manifest.Projects {
			scoped := scopedHistoryManifest(manifest, project.Prefix)
			if err := validateOntology(scoped.Ontology, scoped.Projects); err != nil {
				return err
			}
		}
		for _, directive := range manifest.Ontology {
			if !projectInManifest(directive.Target.Project, manifest.Projects) {
				return fmt.Errorf("undeclared ontology source project")
			}
		}
	} else if manifest.Verification["ontology"] == StrictUnionOntologyVerification {
		if err := validateMigrationDirectives(manifest.Ontology, manifest.Projects); err != nil {
			return err
		}
	} else if err := validateOntology(manifest.Ontology, manifest.Projects); err != nil {
		return err
	}
	if len(manifest.Files) != len(members)-1 {
		return fmt.Errorf("unlisted or missing archive member")
	}
	for path, file := range manifest.Files {
		member, ok := members[path]
		if !ok || path == "manifest.json" || file.Size != int64(len(member)) || file.SHA256 != digest(member) {
			return fmt.Errorf("checksum/size mismatch or missing member %q", path)
		}
	}
	if _, ok := manifest.Files["config/sources.json"]; !ok {
		return fmt.Errorf("missing configuration provenance")
	}
	if err := validateConfigProvenance(config); err != nil {
		return err
	}
	return nil
}

func validateProjects(input []Project) error {
	if len(input) == 0 {
		return fmt.Errorf("project allocator inventory required")
	}
	projects := append([]Project(nil), input...)
	sort.Slice(projects, func(i, j int) bool { return projects[i].Prefix < projects[j].Prefix })
	for i := range projects {
		project := projects[i]
		if !prefixPattern.MatchString(project.Prefix) || (project.IDMode != "hash" && project.IDMode != "sequential") || project.HashLength < 0 || project.HashLength > 64 || (project.IDMode == "hash" && project.HashLength == 0) {
			return fmt.Errorf("invalid project prefix or unsupported allocator mode")
		}
		if i > 0 && (project.Prefix == projects[i-1].Prefix || strings.HasPrefix(project.Prefix, projects[i-1].Prefix+"-")) {
			return fmt.Errorf("duplicate or ambiguous project prefix")
		}
		for parent, counter := range project.ChildCounters {
			if parent == "" || strings.TrimSpace(parent) != parent {
				return fmt.Errorf("invalid child counter parent")
			}
			_ = counter
		}
		for j, namespace := range project.HashNamespaces {
			if project.IDMode != "hash" || !validHashNamespace(namespace) || j > 0 && namespace <= project.HashNamespaces[j-1] {
				return fmt.Errorf("hash_namespaces requires sorted unique native namespaces in hash mode")
			}
		}
	}
	return nil
}

func validateRecords(manifest Manifest, members map[string][]byte, config configEntrySource, nativeDeltaAttachment bool) (map[string][]Record, *ImportMigration, error) {
	registry := map[string]bool{}
	byKind := map[string][]Record{}
	counts := map[string]int{}
	lastKind := ""
	for _, kind := range manifest.Kinds {
		versionOK := kind.Version == 1 || kind.Name == "edge" && IsTypedEdgeVersion(kind.Version)
		if !knownKinds[kind.Name] || !versionOK || kind.Name <= lastKind || kind.File != "records/"+kind.Name+".ndjson" || kind.Count < 1 {
			return nil, nil, fmt.Errorf("unknown, duplicate or unordered kind registry entry")
		}
		lastKind = kind.Name
		registry[kind.File] = true
		member, ok := members[kind.File]
		if !ok || len(member) == 0 || member[len(member)-1] != '\n' {
			return nil, nil, fmt.Errorf("missing or unterminated kind records for %s", kind.Name)
		}
		if bytes.Count(member, []byte{'\n'}) != kind.Count {
			return nil, nil, fmt.Errorf("record count mismatch for %s", kind.Name)
		}
		// Avoid an untrusted, up-front allocation of a slice per line.
		var rows []Record
		for rest := member; len(rest) != 0; {
			line, tail, _ := bytes.Cut(rest, []byte{'\n'})
			rest = tail
			var record Record
			if err := decodeJSON(line, &record); err != nil {
				return nil, nil, fmt.Errorf("invalid %s record: %w", kind.Name, err)
			}
			if record.Kind != kind.Name || record.ID == "" || record.Source.Table == "" || record.Source.Fields == nil || record.Body == nil {
				return nil, nil, fmt.Errorf("invalid typed record in %s", kind.Name)
			}
			if len(rows) > 0 && compareSourceRecords(rows[len(rows)-1], record, manifest.SourcePlaneAddressContract) >= 0 {
				return nil, nil, fmt.Errorf("duplicate or unordered typed record in %s", kind.Name)
			}
			rows = append(rows, record)
		}
		byKind[kind.Name] = rows
	}
	for path := range members {
		if strings.HasPrefix(path, "records/") && !registry[path] {
			return nil, nil, fmt.Errorf("unregistered record kind %q", path)
		}
	}
	_, hasHistory := members[SourceHistoryMember]
	if manifest.SourceIdentityContract == SourceIdentityContract {
		if !hasHistory && manifest.SourceFamilyContract != SourceFamilyContract {
			return nil, nil, ErrSourceHistory{Reason: "missing source project history"}
		}
		if err := validateScopedSourceRecords(manifest, byKind, config); err != nil {
			return nil, nil, err
		}
		return byKind, nil, nil
	}
	for _, rows := range byKind {
		for _, r := range rows {
			if r.Source.Identity != nil {
				return nil, nil, fmt.Errorf("record source identity requires explicit contract")
			}
		}
	}
	if err := config.visit(func(e externalEntry) error {
		if e.Identity != nil {
			return fmt.Errorf("configuration source identity requires explicit contract")
		}
		return nil
	}); err != nil {
		return nil, nil, err
	}
	planes := manifest.SourcePlaneAddressContract == SourcePlaneAddressContract
	var migration *ImportMigration
	var err error
	var statuses map[string]bool
	if planes {
		if manifest.StatusCategoryContract == StatusCategoryContract {
			var categories map[string]string
			categories, err = sourcePlaneCatalogCategoriesFor(byKind, config, true)
			statuses = make(map[string]bool, len(categories))
			for name := range categories {
				statuses[name] = true
			}
		} else {
			statuses, err = validateSourcePlaneCatalog(byKind, config)
		}
	}
	if err == nil && manifest.StatusCategoryContract == StatusCategoryContract {
		err = validateNativeStatusCatalogConservation(manifest, byKind, config)
	}
	if err == nil && (!planes || manifest.Verification["ontology"] == StrictUnionOntologyVerification) {
		migration, err = compileImportMigration(manifest, byKind, config, hasHistory)
	}
	if err != nil {
		return nil, nil, err
	}
	for _, kind := range manifest.Kinds {
		for _, record := range byKind[kind.Name] {
			// The private v2 attachment selects known opaque native records before
			// the portable refuse-if-nonempty guard. Shape/body/count validation
			// still runs; complete stream/current reconciliation follows below.
			if nativeDeltaAttachment && manifest.SourceFamilyContract == SourceFamilyContract && nativeDeltaOpaqueArchiveFamily(record.Source.Table) || planes && (record.Kind == "edge" || sourcePlaneArchiveTable(record.Source.Table) || nativeOperationalTable(record.Source.Table, manifest.SourceFamilyContract)) {
				if err := validateSourcePlaneRecord(record, manifest, counts, statuses); err != nil {
					return nil, nil, err
				}
				continue
			}
			if err := validateVersionedRecord(record, manifest.Projects, counts, migration, manifest.EdgeVersion(), manifest.MainVersion); err != nil {
				return nil, nil, err
			}
		}
	}
	if err := validateCaptureDispositionsForVersion(manifest.Sources, counts, config, migration, planes, manifest.MainVersion, nativeDeltaAttachment, manifest.SourceFamilyContract); err != nil {
		return nil, nil, err
	}
	if migration == nil {
		if err := validateIssueOntology(manifest.Ontology, manifest.Projects, byKind["bead"], true); err != nil {
			return nil, nil, err
		}
	}
	historyRows := counts["issue_versions"] + counts["issue_snapshots"] + counts["compaction_snapshots"]
	if _, captured := members[SourceHistoryMember]; !captured && (historyRows != 0) != (manifest.History == "available-unverified") {
		return nil, nil, fmt.Errorf("history declaration does not match archived source rows")
	}
	if err := validateSourcePlaneRelations(manifest, byKind); err != nil {
		return nil, nil, err
	}
	return byKind, migration, nil
}

func recordKey(record Record) string {
	if record.Source.Identity != nil {
		return identityRecordKey(record)
	}
	return record.Project + "\x00" + record.ID
}

func stringValue(value any) string { result, _ := value.(string); return result }

func digest(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func decodeJSON(data []byte, target any) error {
	return decodeJSONAtDepth(data, target, 0)
}

func decodeJSONAtDepth(data []byte, target any, depth int) error {
	if !utf8.Valid(data) {
		return fmt.Errorf("JSON must be UTF-8")
	}
	if err := checkUnicodeEscapes(data); err != nil {
		return err
	}
	check := json.NewDecoder(bytes.NewReader(data))
	check.UseNumber()
	if err := checkValue(check, depth); err != nil {
		return err
	}
	if _, err := check.Token(); err != io.EOF {
		return fmt.Errorf("trailing JSON data")
	}
	var shape any
	shapeDecoder := json.NewDecoder(bytes.NewReader(data))
	shapeDecoder.UseNumber()
	if err := shapeDecoder.Decode(&shape); err != nil {
		return fmt.Errorf("invalid JSON")
	}
	if err := checkShape(shape, reflect.TypeOf(target).Elem()); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return fmt.Errorf("trailing JSON data")
		}
		return err
	}
	return nil
}

func checkValue(decoder *json.Decoder, depth int) error {
	_, err := countJSONValue(decoder, depth)
	return err
}

func countJSONValue(decoder *json.Decoder, depth int) (int, error) {
	if depth > 128 {
		return 0, fmt.Errorf("JSON nesting exceeds 128")
	}
	token, err := decoder.Token()
	if err != nil {
		return 0, fmt.Errorf("invalid JSON")
	}
	delimiter, container := token.(json.Delim)
	if !container {
		return 1, nil
	}
	if delimiter != '{' && delimiter != '[' {
		return 0, fmt.Errorf("invalid JSON delimiter")
	}
	var seen map[string]bool
	if delimiter == '{' {
		seen = make(map[string]bool)
	}
	var total int
	for decoder.More() {
		if seen != nil {
			keyToken, err := decoder.Token()
			key, ok := keyToken.(string)
			if err != nil || !ok || seen[key] {
				return 0, fmt.Errorf("duplicate or invalid JSON key")
			}
			seen[key] = true
		}
		n, err := countJSONValue(decoder, depth+1)
		if err != nil {
			return 0, err
		}
		total += n
	}
	if _, err := decoder.Token(); err != nil {
		return 0, fmt.Errorf("invalid JSON container")
	}
	return max(total, 1), nil
}
