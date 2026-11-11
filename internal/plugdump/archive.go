package plugdump

import (
	"archive/tar"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"
	"unicode/utf8"
)

// MaxArchiveBytes bounds this initial in-memory exporter/validator slice.
const MaxArchiveBytes = 128 << 20

func digest(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func encode(manifest Manifest, records map[string][]Record, files map[string][]byte) ([]byte, error) {
	manifest.Files = map[string]File{}
	for kind, rows := range records {
		if !knownKinds[kind] {
			return nil, fmt.Errorf("unknown record kind")
		}
		version := 1
		if kind == "edge" {
			var err error
			version, err = edgeRegistryVersion(rows)
			if err != nil {
				return nil, err
			}
		}
		sort.Slice(rows, func(i, j int) bool { return compareRecords(rows[i], rows[j], manifest.SourcePlaneAddressContract) < 0 })
		var data []byte
		for i, record := range rows {
			if i > 0 && compareRecords(rows[i-1], record, manifest.SourcePlaneAddressContract) == 0 {
				return nil, fmt.Errorf("duplicate %s record identity", kind)
			}
			encoded, err := marshal(record)
			if err != nil {
				return nil, fmt.Errorf("serialize %s record: %w", kind, err)
			}
			data = append(data, encoded...)
		}
		path := "records/" + kind + ".ndjson"
		files[path] = data
		manifest.Kinds = append(manifest.Kinds, Kind{kind, version, path, len(rows)})
	}
	sort.Slice(manifest.Kinds, func(i, j int) bool { return manifest.Kinds[i].Name < manifest.Kinds[j].Name })
	sort.Slice(manifest.Sources, func(i, j int) bool {
		if manifest.Sources[i].Family != manifest.Sources[j].Family {
			return manifest.Sources[i].Family < manifest.Sources[j].Family
		}
		return manifest.Sources[i].Reason < manifest.Sources[j].Reason
	})
	paths := make([]string, 0, len(files))
	total := 0
	for path, data := range files {
		paths = append(paths, path)
		total += len(data)
		manifest.Files[path] = File{digest(data), int64(len(data))}
	}
	if total > MaxArchiveBytes-(1<<20) {
		return nil, fmt.Errorf("dump exceeds initial 128 MiB archive limit")
	}
	sort.Strings(paths)
	data, err := marshal(manifest)
	if err != nil {
		return nil, err
	}
	files["manifest.json"] = data
	paths = append([]string{"manifest.json"}, paths...)
	var output bytes.Buffer
	writer := tar.NewWriter(&output)
	for _, path := range paths {
		data := files[path]
		if err := writer.WriteHeader(&tar.Header{Name: path, Mode: 0o600, Size: int64(len(data)), Typeflag: tar.TypeReg, Format: tar.FormatUSTAR}); err != nil {
			return nil, err
		}
		if _, err := writer.Write(data); err != nil {
			return nil, err
		}
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	if _, err := Validate(bytes.NewReader(output.Bytes())); err != nil {
		return nil, fmt.Errorf("validate generated dump: %w", err)
	}
	return output.Bytes(), nil
}

func recordKey(record Record) string { return record.Project + "\x00" + record.ID }

func edgeRegistryVersion(records []Record) (int, error) {
	version := EdgeVersionTypedDependencies
	for _, record := range records {
		kind, _ := record.Body["type"].(string)
		if isTypedDependencyKind(kind) {
			continue
		}
		if !isValidLocalOpaqueDependencyKind(kind) {
			return 0, fmt.Errorf("unsupported dependency type")
		}
		targetKind, _ := record.Body["target_kind"].(string)
		if targetKind != "depends_on_issue_id" {
			return 0, fmt.Errorf("local opaque dependency requires an issue target")
		}
		version = EdgeVersionLocalOpaque
	}
	return version, nil
}

// Validate checks the declared schema, every member/checksum, registry and
// canonical record ordering without extracting files or touching a store.
// A valid archive still carries explicit unperformed migration checks.
func Validate(reader io.Reader) (Manifest, error) {
	data, err := io.ReadAll(io.LimitReader(reader, MaxArchiveBytes+1))
	if err != nil || len(data) > MaxArchiveBytes {
		return Manifest{}, fmt.Errorf("unreadable or oversized archive")
	}
	if len(data) < 1024 || len(data)%512 != 0 || !bytes.Equal(data[len(data)-1024:], make([]byte, 1024)) {
		return Manifest{}, fmt.Errorf("invalid tar length")
	}
	input := bytes.NewReader(data)
	tr := tar.NewReader(input)
	members := map[string][]byte{}
	var paths []string
	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return Manifest{}, fmt.Errorf("invalid tar member: %w", err)
		}
		if header.Typeflag != tar.TypeReg || header.Size < 0 || header.Size > MaxArchiveBytes || header.Mode != 0o600 || header.Uid != 0 || header.Gid != 0 || header.Linkname != "" || len(header.PAXRecords) != 0 || header.Format != tar.FormatUSTAR || !header.ModTime.Equal(time.Unix(0, 0)) {
			return Manifest{}, fmt.Errorf("noncanonical tar member")
		}
		if header.Name != "manifest.json" && header.Name != "config/sources.json" && header.Name != SourceHistoryMember && header.Name != NativeHistoryMember && !(strings.HasPrefix(header.Name, "records/") && strings.HasSuffix(header.Name, ".ndjson") && knownKinds[strings.TrimSuffix(strings.TrimPrefix(header.Name, "records/"), ".ndjson")]) {
			return Manifest{}, fmt.Errorf("unknown archive member")
		}
		if _, ok := members[header.Name]; ok {
			return Manifest{}, fmt.Errorf("duplicate archive member")
		}
		member, err := io.ReadAll(tr)
		if err != nil {
			return Manifest{}, err
		}
		members[header.Name] = member
		paths = append(paths, header.Name)
	}
	for _, value := range data[len(data)-input.Len():] {
		if value != 0 {
			return Manifest{}, fmt.Errorf("unaccounted trailing archive data")
		}
	}
	if len(paths) == 0 || paths[0] != "manifest.json" || !sort.StringsAreSorted(paths[1:]) {
		return Manifest{}, fmt.Errorf("manifest must be first and members sorted")
	}
	var manifest Manifest
	if err := decodeJSON(members["manifest.json"], &manifest); err != nil {
		return Manifest{}, fmt.Errorf("invalid manifest: %w", err)
	}
	// Presence is significant: an explicitly empty or unknown contract is not
	// legacy omission. The successor exporter emits this marker alongside the
	// edge registry version; old archives remain valid when it is absent.
	var manifestFields map[string]json.RawMessage
	if err := json.Unmarshal(members["manifest.json"], &manifestFields); err != nil {
		return Manifest{}, fmt.Errorf("invalid manifest: %w", err)
	}
	if _, present := manifestFields["source_fields_contract"]; present && manifest.SourceFieldsContract != IssueSourceFieldsContract {
		return Manifest{}, fmt.Errorf("unsupported source fields contract")
	}
	if err := validateSourceFamilyMarker(members["manifest.json"], manifest.SourceFamilyContract); err != nil {
		return Manifest{}, err
	}
	if err := validateSourcePlaneMarker(members["manifest.json"], manifest.SourcePlaneAddressContract); err != nil {
		return Manifest{}, err
	}
	profile, profileErr := schemaProfile(uint64(manifest.MainVersion), uint64(manifest.IgnoredVersion))
	if profileErr != nil || manifest.Format != Format || manifest.Schema != 1 || manifest.Reference != BeadsReference || manifest.StoreID == "" || manifest.Machine == "" || manifest.Scope != "offline-head-and-source-archives" {
		return Manifest{}, fmt.Errorf("unsupported manifest schema, source or scope")
	}
	if profile.MainVersion == 66 && manifest.SourceFamilyContract != SourceFamilyContract {
		return Manifest{}, fmt.Errorf("own-version capture requires native source family contract")
	}
	if manifest.History != "unavailable" && manifest.History != "available-unverified" && manifest.History != "verified" {
		return Manifest{}, fmt.Errorf("unsupported history claim")
	}
	if len(manifest.Warnings) == 0 || len(manifest.Verification) == 0 {
		return Manifest{}, fmt.Errorf("missing incomplete-conversion disclosures")
	}
	if err := validateStatusCategoryMarker(members["manifest.json"], manifest.StatusCategoryContract, manifest.Projects); err != nil {
		return Manifest{}, err
	}
	expectedVerification, _ := marshal(verificationLimitsForStatus(manifest.StatusCategoryContract))
	actualVerification, _ := marshal(manifest.Verification)
	if !bytes.Equal(expectedVerification, actualVerification) {
		return Manifest{}, fmt.Errorf("unsupported verification claim")
	}
	if _, err := copyProjects(manifest.Projects); err != nil {
		return Manifest{}, err
	}
	projects, _ := copyProjects(manifest.Projects)
	ontology, err := copyOntology(manifest.Ontology, projects)
	if err != nil {
		return Manifest{}, err
	}
	if len(ontology) != len(manifest.Ontology) {
		return Manifest{}, fmt.Errorf("ontology directives are not canonical")
	}
	if len(manifest.Files) != len(members)-1 {
		return Manifest{}, fmt.Errorf("unlisted or missing archive member")
	}
	for path, file := range manifest.Files {
		member, ok := members[path]
		if !ok || path == "manifest.json" || file.Size != int64(len(member)) || file.SHA256 != digest(member) {
			return Manifest{}, fmt.Errorf("checksum/size mismatch or missing member")
		}
	}
	if _, ok := manifest.Files["config/sources.json"]; !ok {
		return Manifest{}, fmt.Errorf("missing configuration provenance")
	}
	if err := validateConfigProvenance(members["config/sources.json"]); err != nil {
		return Manifest{}, err
	}
	registry := map[string]bool{}
	counts := map[string]int{}
	var beadRecords []Record
	var edgeRecords []Record
	var statusRows, typeRows []Row
	activeRows := map[string][]Row{}
	storedCatalogConfig := map[string]any{}
	beadIDs := map[string]bool{}
	wispIDs := map[string]bool{}
	lastKind := ""
	for _, kind := range manifest.Kinds {
		versionOK := kind.Version == 1 || kind.Name == "edge" && (kind.Version == EdgeVersionTypedDependencies || kind.Version == EdgeVersionLocalOpaque)
		if !knownKinds[kind.Name] || !versionOK || kind.Name <= lastKind || kind.File != "records/"+kind.Name+".ndjson" || kind.Count < 1 {
			return Manifest{}, fmt.Errorf("unknown, duplicate or unordered kind registry entry")
		}
		lastKind = kind.Name
		registry[kind.File] = true
		member, ok := members[kind.File]
		if !ok || len(member) == 0 || member[len(member)-1] != '\n' {
			return Manifest{}, fmt.Errorf("missing or unterminated kind records")
		}
		lines := bytes.Split(member[:len(member)-1], []byte{'\n'})
		if len(lines) != kind.Count {
			return Manifest{}, fmt.Errorf("record count mismatch")
		}
		var last Record
		for i, line := range lines {
			var record Record
			if err := decodeJSON(line, &record); err != nil || record.Kind != kind.Name || record.ID == "" || record.Source.Table == "" || record.Source.Fields == nil || record.Body == nil || (i > 0 && compareRecords(last, record, manifest.SourcePlaneAddressContract) >= 0) {
				return Manifest{}, fmt.Errorf("invalid, duplicate or unordered typed record")
			}
			if err := validateRecordAtProfile(record, manifest.Projects, counts, kind.Version, manifest.SourceFamilyContract, profile); err != nil {
				return Manifest{}, err
			}
			switch record.Source.Table {
			case "custom_statuses":
				statusRows = append(statusRows, record.Source.Fields)
			case "custom_types":
				typeRows = append(typeRows, record.Source.Fields)
			case "config":
				key := text(record.Source.Fields, "key")
				if key == "status.custom" || key == "types.custom" {
					if _, duplicate := storedCatalogConfig[key]; duplicate {
						return Manifest{}, fmt.Errorf("duplicate stored catalog configuration %q", key)
					}
					storedCatalogConfig[key] = record.Source.Fields["value"]
				}
			}
			if record.Kind == "bead" {
				beadRecords = append(beadRecords, record)
				beadIDs[record.ID] = true
			}
			if record.Kind == "edge" {
				edgeRecords = append(edgeRecords, record)
			}
			if record.Kind == "beads-archive" && record.Source.Table == "wisps" {
				if id := text(record.Source.Fields, "id"); id != "" {
					wispIDs[id] = true
				}
			}
			if manifest.SourcePlaneAddressContract == SourcePlaneAddressContract && record.Kind == "beads-archive" {
				encoded, err := marshal(record.Source.Fields)
				if err != nil || record.ID != record.Source.Table+"/"+digest(encoded) {
					return Manifest{}, fmt.Errorf("source archive identity does not bind its raw row")
				}
				activeRows[record.Source.Table] = append(activeRows[record.Source.Table], record.Source.Fields)
			}
			last = record
		}
	}
	for path := range members {
		if strings.HasPrefix(path, "records/") && !registry[path] {
			return Manifest{}, fmt.Errorf("unregistered record kind")
		}
	}
	yamlCatalogs, err := nativeYAMLCatalogValues(members["config/sources.json"])
	if err != nil {
		return Manifest{}, err
	}
	customStatuses := map[string]bool{}
	if err := readCatalogUnion(statusRows, typeRows, storedCatalogConfig, yamlCatalogs, customStatuses); err != nil {
		return Manifest{}, err
	}
	if manifest.StatusCategoryContract == StatusCategoryContract {
		if err := validateProjectStatusCatalogs(manifest.Projects, statusRows, typeRows, storedCatalogConfig, yamlCatalogs); err != nil {
			return Manifest{}, err
		}
	}
	if manifest.SourcePlaneAddressContract == SourcePlaneAddressContract {
		if err := validateSourcePlanes(manifest.Projects, beadRecords, edgeRecords, activeRows, customStatuses); err != nil {
			return Manifest{}, err
		}
	}
	if err := validateDispositionsAtProfile(manifest.Sources, counts, manifest.SourceFamilyContract, profile); err != nil {
		return Manifest{}, err
	}
	if err := validateEdgeEndpoints(edgeRecords, beadIDs, wispIDs); err != nil {
		return Manifest{}, err
	}
	if err := validateOntologyRecords(manifest.Ontology, manifest.Projects, beadRecords); err != nil {
		return Manifest{}, err
	}
	hasHistory := counts["issue_versions"]+counts["issue_snapshots"]+counts["compaction_snapshots"] != 0
	if _, captured := members[SourceHistoryMember]; !captured && hasHistory != (manifest.History == "available-unverified") {
		return Manifest{}, fmt.Errorf("history declaration does not match archived source rows")
	}
	if err := validateSourceHistory(manifest, members); err != nil {
		return Manifest{}, err
	}
	if err := validateNativeHistory(manifest, members); err != nil {
		return Manifest{}, err
	}
	return manifest, nil
}

func validateOntologyRecords(directives []OntologyDirective, projects []Project, records []Record) error {
	validated, err := copyOntology(directives, projects)
	if err != nil {
		return err
	}
	used := map[string]bool{}
	for _, record := range records {
		issueType := text(record.Source.Fields, "issue_type")
		if text(record.Body, "source_type") != issueType || text(record.Body, "type") != "issue" {
			return fmt.Errorf("bead source type/body mismatch")
		}
		if builtInIssueTypes[issueType] {
			continue
		}
		var matches []OntologyDirective
		for _, directive := range validated {
			if directive.Target.Project == record.Project && directive.Target.Type == issueType && directive.From == issueType && (directive.Target.ID == "" || directive.Target.ID == record.ID) {
				matches = append(matches, directive)
			}
		}
		if len(matches) != 1 {
			return fmt.Errorf("unsupported issue type; workflow/ontology mapping required")
		}
		used[matches[0].ID] = true
	}
	if len(used) != len(validated) {
		return fmt.Errorf("unaccounted ontology directive")
	}
	return nil
}

func validateRecord(record Record, projects []Project, counts map[string]int) error {
	return validateRecordVersioned(record, projects, counts, 1)
}

func validateRecordVersioned(record Record, projects []Project, counts map[string]int, edgeVersion int, familyContracts ...string) error {
	familyContract := ""
	if len(familyContracts) > 0 {
		familyContract = familyContracts[0]
	}
	profile, _ := schemaProfile(72, 28)
	return validateRecordAtProfile(record, projects, counts, edgeVersion, familyContract, profile)
}

func validateRecordAtProfile(record Record, projects []Project, counts map[string]int, edgeVersion int, familyContract string, profile NativeSchemaProfile) error {
	if record.Kind == "edge" {
		kind, _ := record.Body["type"].(string)
		switch kind {
		case "blocks", "parent-child", "related", "discovered-from":
			// These kinds are valid in both the legacy and successor edge
			// contracts.
		default:
			if isTypedDependencyKind(kind) {
				if edgeVersion != EdgeVersionTypedDependencies && edgeVersion != EdgeVersionLocalOpaque {
					return fmt.Errorf("dependency %s requires edge registry version %d or %d", kind, EdgeVersionTypedDependencies, EdgeVersionLocalOpaque)
				}
				if !supportsTypedDependency(kind, record.Source.Table, text(record.Body, "target_kind")) {
					return fmt.Errorf("unsupported dependency type")
				}
			} else if edgeVersion != EdgeVersionLocalOpaque || !isValidLocalOpaqueDependencyKind(kind) || text(record.Body, "target_kind") != "depends_on_issue_id" {
				return fmt.Errorf("unsupported dependency type")
			}
		}
	}
	table := record.Source.Table
	columns, ok := profile.tables()[table]
	if !ok {
		return fmt.Errorf("unknown record source table")
	}
	expectedKind := "beads-archive"
	switch table {
	case "issues":
		expectedKind = "bead"
	case "dependencies", "wisp_dependencies":
		expectedKind = "edge"
	case "comments":
		expectedKind = "comment"
	case "config", "metadata":
		expectedKind = "config"
		if table == "config" && strings.HasPrefix(text(record.Source.Fields, "key"), "kv.memory.") {
			expectedKind = "memory"
		}
	case "labels":
		return fmt.Errorf("normalized family cannot have source archive records")
	case "repo_mtimes", "leases":
		if familyContract != SourceFamilyContract {
			return fmt.Errorf("operational family requires source family contract")
		}
	}
	action, _ := familyAction(table, familyContract)
	if record.Kind != expectedKind || action == "refuse-if-nonempty" {
		return fmt.Errorf("unsupported kind/source mapping")
	}
	if table == "issues" {
		var filtered []string
		for _, column := range columns {
			if column != "row_lock" && column != "is_blocked" {
				filtered = append(filtered, column)
			}
		}
		columns = filtered
	}
	if len(columns) != len(record.Source.Fields) {
		return fmt.Errorf("record source schema drift")
	}
	for _, column := range columns {
		if _, ok := record.Source.Fields[column]; !ok {
			return fmt.Errorf("missing source record column")
		}
	}
	if table == "local_metadata" {
		if err := validateSecretReferences(map[string]any{text(record.Source.Fields, "key"): record.Source.Fields["value"]}); err != nil {
			return err
		}
	}
	if record.Kind == "bead" || record.Kind == "edge" || record.Kind == "comment" {
		if record.ID != text(record.Source.Fields, "id") {
			return fmt.Errorf("record/source identity mismatch")
		}
		projectFound := false
		for _, project := range projects {
			if project.Prefix == record.Project {
				projectFound = true
			}
		}
		if !projectFound {
			return fmt.Errorf("unaccounted record project")
		}
	} else if record.Project != "" {
		return fmt.Errorf("source archive/config has an unsupported project binding")
	}
	if record.Kind == "bead" {
		if err := validateStatusRecord(record, projects); err != nil {
			return err
		}
		for _, project := range projects {
			if project.Prefix != record.Project {
				continue
			}
			top, parent, child, err := issueIDParts(record.ID, project)
			if err != nil {
				return err
			}
			if top > project.LastID || parent != "" && child > project.ChildCounters[parent] {
				return fmt.Errorf("issue ID exceeds inventoried allocator counters")
			}
		}
		labels, ok := record.Body["labels"].([]any)
		if !ok {
			return fmt.Errorf("missing bead label set")
		}
		last := ""
		for _, label := range labels {
			value, ok := label.(string)
			if !ok || value <= last {
				return fmt.Errorf("invalid or unordered bead labels")
			}
			last = value
		}
		counts["labels"] += len(labels)
	}
	if table == "config" || table == "metadata" {
		key := text(record.Source.Fields, "key")
		policy, _ := record.Body["classification"].(string)
		if (policy != "public" && policy != "secret") || (secretKey(key) && policy != "secret") {
			return fmt.Errorf("unclassified or raw credential record")
		}
		if policy == "secret" {
			ref, ok := record.Source.Fields["value"].(map[string]any)
			if !ok || len(ref) != 1 || ref["$secret"] != table+"/"+key {
				return fmt.Errorf("credential record must contain only its secret reference")
			}
		}
		sourceValue, _ := marshal(record.Source.Fields["value"])
		bodyValue, _ := marshal(record.Body["value"])
		if !bytes.Equal(sourceValue, bodyValue) {
			return fmt.Errorf("config body/source value mismatch")
		}
	}
	if record.Kind == "beads-archive" && (record.Body["read_only"] != true || record.Body["redactable"] != true || record.Body["history_verified"] != false || record.Body["attribution_verified"] != false) {
		return fmt.Errorf("unsupported source archive verification claim")
	}
	counts[table]++
	return nil
}

func validateEdgeEndpoints(records []Record, beadIDs, wispIDs map[string]bool) error {
	for _, record := range records {
		table := record.Source.Table
		if table != "dependencies" && table != "wisp_dependencies" {
			return fmt.Errorf("unsupported edge source table")
		}
		from := text(record.Source.Fields, "issue_id")
		if from == "" || text(record.Body, "from") != from {
			return fmt.Errorf("edge source mirror mismatch")
		}
		if text(record.Body, "type") != text(record.Source.Fields, "type") {
			return fmt.Errorf("edge type/source mirror mismatch")
		}
		if table == "dependencies" {
			if !beadIDs[from] {
				return fmt.Errorf("durable edge source is not an archived bead")
			}
		} else if !wispIDs[from] {
			return fmt.Errorf("wisp edge source is not an archived wisp")
		}
		targetKind := text(record.Body, "target_kind")
		if targetKind != "depends_on_issue_id" && targetKind != "depends_on_wisp_id" && targetKind != "depends_on_external" {
			return fmt.Errorf("unsupported edge target arm")
		}
		target, ok := record.Body["to"].(string)
		if !ok || target == "" || !utf8.ValidString(target) {
			return fmt.Errorf("invalid edge target")
		}
		targetArms := 0
		for _, key := range []string{"depends_on_issue_id", "depends_on_wisp_id", "depends_on_external"} {
			value, present := record.Source.Fields[key]
			if !present || value == nil {
				continue
			}
			targetArms++
			candidate, ok := value.(string)
			if !ok || candidate == "" || candidate != target {
				return fmt.Errorf("edge target/source mirror mismatch")
			}
			if key != targetKind {
				return fmt.Errorf("edge has multiple or mismatched target arms")
			}
		}
		if targetArms != 1 {
			return fmt.Errorf("edge must have exactly one target arm")
		}
		if targetKind == "depends_on_issue_id" && !beadIDs[target] {
			return fmt.Errorf("edge issue target is not an archived bead")
		}
		if targetKind == "depends_on_wisp_id" && !wispIDs[target] {
			return fmt.Errorf("edge wisp target is not an archived wisp")
		}
		if targetKind == "depends_on_external" && !validExternalDependencyTarget(target) {
			return fmt.Errorf("invalid external edge target")
		}
		if text(record.Body, "thread_id") != text(record.Source.Fields, "thread_id") {
			return fmt.Errorf("edge thread mirror mismatch")
		}
		bodyMetadata, _ := marshal(record.Body["metadata"])
		sourceMetadata, _ := marshal(record.Source.Fields["metadata"])
		if !bytes.Equal(bodyMetadata, sourceMetadata) {
			return fmt.Errorf("edge metadata/source mirror mismatch")
		}
	}
	return nil
}

func validateConfigProvenance(data []byte) error {
	var entries []struct {
		Input  ExternalInput  `json:"input"`
		Values map[string]any `json:"values,omitempty"`
	}
	if err := decodeJSON(data, &entries); err != nil {
		return fmt.Errorf("invalid configuration provenance")
	}
	allowed, seen, paths := map[string]bool{}, map[string]bool{}, map[string]bool{}
	for _, family := range externalFamilies {
		allowed[family] = true
	}
	for _, entry := range entries {
		input := entry.Input
		key := input.Family + "\x00" + input.Path
		if !allowed[input.Family] || input.Path == "" || paths[key] || (input.State != "absent" && input.State != "json-config") || (input.State == "absent" && entry.Values != nil) || (input.State == "json-config" && entry.Values == nil) {
			return fmt.Errorf("unknown, duplicate or inconsistent configuration input")
		}
		if err := validateSecretReferences(entry.Values); err != nil {
			return err
		}
		if err := validateNativeObservations(input.Family, entry.Values); err != nil {
			return err
		}
		seen[input.Family], paths[key] = true, true
	}
	if len(seen) != len(allowed) {
		return fmt.Errorf("unaccounted configuration provenance family")
	}
	return nil
}

func validateDispositions(sources []Disposition, counts map[string]int, familyContracts ...string) error {
	familyContract := ""
	if len(familyContracts) > 0 {
		familyContract = familyContracts[0]
	}
	profile, _ := schemaProfile(72, 28)
	return validateDispositionsAtProfile(sources, counts, familyContract, profile)
}

func validateDispositionsAtProfile(sources []Disposition, counts map[string]int, familyContract string, profile NativeSchemaProfile) error {
	tables := profile.tables()
	seen := map[string]bool{}
	allowed := map[string]bool{"issues.row_lock": true, "issues.is_blocked": true}
	for name := range tables {
		allowed[name] = true
	}
	for _, name := range externalFamilies {
		allowed[name] = true
	}
	for _, source := range sources {
		if !allowed[source.Family] || source.Rows < 0 || source.Reason == "" {
			return fmt.Errorf("invalid source disposition")
		}
		switch source.Action {
		case "carry", "archive", "manifest/rebuild", "refuse-if-nonempty":
		default:
			return fmt.Errorf("unknown source disposition")
		}
		if source.Action == "refuse-if-nonempty" && source.Rows != 0 {
			return fmt.Errorf("unsupported nonempty source disposition")
		}
		if _, isTable := tables[source.Family]; isTable {
			expectedAction, _ := familyAction(source.Family, familyContract)
			if seen[source.Family] || source.Action != expectedAction || ((source.Family != "repo_mtimes" || familyContract == SourceFamilyContract) && source.Rows != counts[source.Family]) {
				return fmt.Errorf("source disposition/count does not match archived records")
			}
		}
		if (source.Family == "issues.row_lock" || source.Family == "issues.is_blocked") && (source.Action != "manifest/rebuild" || source.Rows != counts["issues"] || seen[source.Family]) {
			return fmt.Errorf("invalid derived/operational field disposition")
		}
		seen[source.Family] = true
	}
	if len(seen) != len(allowed) {
		return fmt.Errorf("unaccounted source disposition")
	}
	return nil
}
