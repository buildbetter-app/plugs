package plugdump

import (
	"archive/tar"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"
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
		sort.Slice(rows, func(i, j int) bool { return recordKey(rows[i]) < recordKey(rows[j]) })
		var data []byte
		for i, record := range rows {
			if i > 0 && recordKey(rows[i-1]) == recordKey(record) {
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
		manifest.Kinds = append(manifest.Kinds, Kind{kind, 1, path, len(rows)})
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
		if header.Name != "manifest.json" && header.Name != "config/sources.json" && !(strings.HasPrefix(header.Name, "records/") && strings.HasSuffix(header.Name, ".ndjson") && knownKinds[strings.TrimSuffix(strings.TrimPrefix(header.Name, "records/"), ".ndjson")]) {
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
	if manifest.Format != Format || manifest.Schema != 1 || manifest.Reference != BeadsReference || manifest.MainVersion != 72 || manifest.IgnoredVersion != 28 || manifest.StoreID == "" || manifest.Machine == "" || manifest.Scope != "offline-head-and-source-archives" {
		return Manifest{}, fmt.Errorf("unsupported manifest schema, source or scope")
	}
	if manifest.History != "unavailable" && manifest.History != "available-unverified" {
		return Manifest{}, fmt.Errorf("unsupported history claim")
	}
	if len(manifest.Warnings) == 0 || len(manifest.Verification) == 0 {
		return Manifest{}, fmt.Errorf("missing incomplete-conversion disclosures")
	}
	expectedVerification, _ := marshal(verificationLimits())
	actualVerification, _ := marshal(manifest.Verification)
	if !bytes.Equal(expectedVerification, actualVerification) {
		return Manifest{}, fmt.Errorf("unsupported verification claim")
	}
	if _, err := copyProjects(manifest.Projects); err != nil {
		return Manifest{}, err
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
	lastKind := ""
	for _, kind := range manifest.Kinds {
		if !knownKinds[kind.Name] || kind.Version != 1 || kind.Name <= lastKind || kind.File != "records/"+kind.Name+".ndjson" || kind.Count < 1 {
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
		last := ""
		for _, line := range lines {
			var record Record
			if err := decodeJSON(line, &record); err != nil || record.Kind != kind.Name || record.ID == "" || record.Source.Table == "" || record.Source.Fields == nil || record.Body == nil || recordKey(record) <= last {
				return Manifest{}, fmt.Errorf("invalid, duplicate or unordered typed record")
			}
			if err := validateRecord(record, manifest.Projects, counts); err != nil {
				return Manifest{}, err
			}
			last = recordKey(record)
		}
	}
	for path := range members {
		if strings.HasPrefix(path, "records/") && !registry[path] {
			return Manifest{}, fmt.Errorf("unregistered record kind")
		}
	}
	if err := validateDispositions(manifest.Sources, counts); err != nil {
		return Manifest{}, err
	}
	hasHistory := counts["issue_versions"]+counts["issue_snapshots"]+counts["compaction_snapshots"] != 0
	if hasHistory != (manifest.History == "available-unverified") {
		return Manifest{}, fmt.Errorf("history declaration does not match archived source rows")
	}
	return manifest, nil
}

func validateRecord(record Record, projects []Project, counts map[string]int) error {
	table := record.Source.Table
	if _, ok := tableColumns[table]; !ok {
		return fmt.Errorf("unknown record source table")
	}
	expectedKind := "beads-archive"
	switch table {
	case "issues":
		expectedKind = "bead"
	case "dependencies":
		expectedKind = "edge"
	case "comments":
		expectedKind = "comment"
	case "config", "metadata":
		expectedKind = "config"
		if table == "config" && strings.HasPrefix(text(record.Source.Fields, "key"), "kv.memory.") {
			expectedKind = "memory"
		}
	case "labels", "repo_mtimes", "leases":
		return fmt.Errorf("operational or normalized family cannot have source archive records")
	}
	action, _ := actionFor(table)
	if record.Kind != expectedKind || action == "refuse-if-nonempty" {
		return fmt.Errorf("unsupported kind/source mapping")
	}
	columns := columnsFor(table)
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
		seen[input.Family], paths[key] = true, true
	}
	if len(seen) != len(allowed) {
		return fmt.Errorf("unaccounted configuration provenance family")
	}
	return nil
}

func validateDispositions(sources []Disposition, counts map[string]int) error {
	seen := map[string]bool{}
	allowed := map[string]bool{"issues.row_lock": true, "issues.is_blocked": true}
	for name := range tableColumns {
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
		if _, isTable := tableColumns[source.Family]; isTable {
			expectedAction, _ := actionFor(source.Family)
			if seen[source.Family] || source.Action != expectedAction || (source.Family != "repo_mtimes" && source.Rows != counts[source.Family]) {
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
