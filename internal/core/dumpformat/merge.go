package dumpformat

import (
	"archive/tar"
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"
)

// Merge reads validated plug-dump/1 archives and returns one validated archive
// without opening a store or performing any live write. Every input is read to
// completion before a merged byte stream is built, so a refusal has no output.
func Merge(readers ...io.Reader) ([]byte, error) {
	if len(readers) == 0 {
		return nil, fmt.Errorf("offline merge requires at least one archive")
	}
	archives := make([]*Archive, len(readers))
	for i, reader := range readers {
		if reader == nil {
			return nil, fmt.Errorf("offline merge input %d is nil", i)
		}
		archive, err := Read(reader)
		if err != nil {
			return nil, fmt.Errorf("offline merge input %d: %w", i, err)
		}
		archives[i] = archive
	}
	return MergeArchives(archives...)
}

// MergeArchives merges already validated archives. It is useful to callers
// that have retained Read results and want to avoid reparsing the input.
func MergeArchives(archives ...*Archive) ([]byte, error) {
	if len(archives) == 0 {
		return nil, fmt.Errorf("offline merge requires at least one archive")
	}

	inputs := make([]mergeInput, 0, len(archives))
	for i, archive := range archives {
		if archive == nil || archive.members == nil {
			return nil, fmt.Errorf("offline merge input %d has not been validated by Read", i)
		}
		manifest := archive.Manifest()
		if manifest.Scope != Scope {
			return nil, fmt.Errorf("offline merge input %d has unsupported scope %q", i, manifest.Scope)
		}
		if err := rejectUnmerged(manifest); err != nil {
			return nil, fmt.Errorf("offline merge input %d: %w", i, err)
		}
		key := archiveKey(archive)
		histories, err := mergeInputHistories(archive)
		if err != nil {
			return nil, fmt.Errorf("offline merge input %d: %w", i, err)
		}
		inputs = append(inputs, mergeInput{archive: archive, manifest: manifest, key: key, histories: histories})
	}
	sort.Slice(inputs, func(i, j int) bool { return inputs[i].key < inputs[j].key })

	for i := 1; i < len(inputs); i++ {
		if err := compatibleManifests(inputs[0].manifest, inputs[i].manifest); err != nil {
			return nil, fmt.Errorf("incompatible archive %d: %w", i, err)
		}
	}
	versions := map[string]int{}
	for _, input := range inputs {
		for _, kind := range input.manifest.Kinds {
			if version, present := versions[kind.Name]; present && version != kind.Version {
				return nil, fmt.Errorf("incompatible archive kind version for %s", kind.Name)
			}
			versions[kind.Name] = kind.Version
		}
	}

	projects := make([]Project, 0)
	records := map[string]map[sourceRecordIdentity]Record{}
	entries := map[string]externalEntry{}
	historyScopes := make([]ProjectSourceHistory, 0)
	seenHistoryProjects := map[string]bool{}
	seenHistoryCommits := map[string]string{}
	seenHistoryEvents := map[string]string{}
	seenHistoryRecords := map[string]Record{}
	for _, input := range inputs {
		projects = append(projects, input.manifest.Projects...)
		for _, scoped := range input.histories {
			if seenHistoryProjects[scoped.Project] {
				return nil, HistoryMergeRefusal{Kind: "project", Project: scoped.Project, ID: scoped.Project, Reason: "project history is declared more than once"}
			}
			seenHistoryProjects[scoped.Project] = true
			for _, commit := range scoped.Commits {
				if previous, ok := seenHistoryCommits[commit.ID]; ok && previous != scoped.Project {
					return nil, HistoryMergeRefusal{Kind: "commit identity", Project: scoped.Project, OtherProject: previous, ID: commit.ID, Reason: "source revisions cannot be ordered across projects"}
				}
				seenHistoryCommits[commit.ID] = scoped.Project
			}
			for _, event := range scoped.Events {
				id := SourceEventID(event)
				if id == "" {
					continue
				}
				if previous, ok := seenHistoryEvents[id]; ok && previous != scoped.Project {
					return nil, HistoryMergeRefusal{Kind: "event identity", Project: scoped.Project, OtherProject: previous, ID: id, Reason: "source events cannot be shared across project histories"}
				}
				seenHistoryEvents[id] = scoped.Project
			}
			for _, frame := range scoped.Range.Frames {
				for _, record := range frame.Records {
					key := record.Kind + "\x00" + recordKey(record)
					if previous, ok := seenHistoryRecords[key]; ok {
						if previous.Project == scoped.Project {
							continue
						}
						if previous.Project == "" && record.Project == "" && equalJSON(previous, record) {
							continue
						}
						return nil, HistoryMergeRefusal{Kind: "record identity", Project: scoped.Project, OtherProject: previous.Project, ID: record.ID, Reason: "source record is shared across project histories"}
					}
					seenHistoryRecords[key] = record
				}
			}
			historyScopes = append(historyScopes, scoped)
		}
		for _, kind := range knownKindNames() {
			for _, record := range input.archive.Records(kind) {
				key := sourceRecordIdentity{Kind: recordIdentity(kind, record)}
				if input.manifest.SourcePlaneAddressContract == SourcePlaneAddressContract {
					key = sourceRecordIdentity{Kind: kind, Project: record.Project, ID: record.ID}
					if kind == "edge" {
						key.Table = record.Source.Table
					}
				}
				if previous, ok := records[kind][key]; ok {
					if !equalJSON(previous, record) {
						return nil, fmt.Errorf("conflicting %s record %q", kind, record.ID)
					}
					continue
				}
				if records[kind] == nil {
					records[kind] = map[sourceRecordIdentity]Record{}
				}
				records[kind][key] = record
			}
		}

		config, ok := input.archive.Member("config/sources.json")
		if !ok {
			return nil, fmt.Errorf("archive %q has no configuration provenance", input.manifest.StoreID)
		}
		var sourceEntries []externalEntry
		if err := decodeJSON(config, &sourceEntries); err != nil {
			return nil, fmt.Errorf("archive %q configuration provenance: %w", input.manifest.StoreID, err)
		}
		for _, entry := range sourceEntries {
			key := entry.Input.Family + "\x00" + entry.Input.Path
			if previous, ok := entries[key]; ok {
				if !equalJSON(previous, entry) {
					return nil, fmt.Errorf("conflicting configuration provenance %s", key)
				}
				continue
			}
			entries[key] = entry
		}
	}

	if err := validateProjects(projects); err != nil {
		for i := range projects {
			for j := i + 1; j < len(projects); j++ {
				if projects[i].Prefix == projects[j].Prefix || strings.HasPrefix(projects[i].Prefix, projects[j].Prefix+"-") || strings.HasPrefix(projects[j].Prefix, projects[i].Prefix+"-") {
					return nil, HistoryMergeRefusal{Kind: "project", Project: projects[i].Prefix, OtherProject: projects[j].Prefix, ID: projects[i].Prefix, Reason: "project prefixes overlap"}
				}
			}
		}
		return nil, fmt.Errorf("duplicate or conflicting project prefix: %w", err)
	}
	sort.Slice(projects, func(i, j int) bool { return projects[i].Prefix < projects[j].Prefix })

	mergedRecords := make(map[string][]Record, len(records))
	for _, kind := range knownKindNames() {
		byIdentity := records[kind]
		if len(byIdentity) == 0 {
			continue
		}
		rows := make([]Record, 0, len(byIdentity))
		for _, record := range byIdentity {
			rows = append(rows, record)
		}
		mergedRecords[kind] = rows
	}

	mergedEntries := make([]externalEntry, 0, len(entries))
	for _, entry := range entries {
		mergedEntries = append(mergedEntries, entry)
	}
	sort.Slice(mergedEntries, func(i, j int) bool {
		left, right := mergedEntries[i].Input, mergedEntries[j].Input
		if left.Precedence != right.Precedence {
			return left.Precedence < right.Precedence
		}
		if left.Family != right.Family {
			return left.Family < right.Family
		}
		return left.Path < right.Path
	})
	config, err := json.Marshal(mergedEntries)
	if err != nil {
		return nil, fmt.Errorf("serialize merged configuration provenance: %w", err)
	}

	sources, err := mergeSources(inputs, mergedRecords, mergedEntries)
	if err != nil {
		return nil, err
	}
	history := "unavailable"
	if len(historyScopes) != 0 {
		history = "verified"
		for _, scoped := range historyScopes {
			if scoped.Range.Status != "verified" || !scoped.Range.Complete {
				history = "available-unverified"
				break
			}
		}
	} else {
		for _, input := range inputs {
			if input.manifest.History == "available-unverified" {
				history = "available-unverified"
				break
			}
		}
	}

	manifest := inputs[0].manifest
	manifest.StoreID = "merge-" + mergeDigest(inputs)
	manifest.Machine = "offline-merge"
	manifest.History = history
	manifest.Projects = projects
	manifest.Sources = sources
	manifest.Kinds = nil
	for name, version := range versions {
		manifest.Kinds = append(manifest.Kinds, Kind{Name: name, Version: version})
	}
	var historyMember []byte
	if len(historyScopes) != 0 {
		sort.Slice(historyScopes, func(i, j int) bool { return historyScopes[i].Project < historyScopes[j].Project })
		mergedHistory := SourceHistory{Format: SourceHistoryFormat, ProjectHistories: historyScopes}
		historyMember, err = json.Marshal(mergedHistory)
		if err != nil {
			return nil, fmt.Errorf("serialize merged source history: %w", err)
		}
		historyMember = append(historyMember, '\n')
	}
	return encodeArchive(manifest, mergedRecords, append(config, '\n'), historyMember)
}

type mergeInput struct {
	archive   *Archive
	manifest  Manifest
	key       string
	histories []ProjectSourceHistory
}

func mergeInputHistories(archive *Archive) ([]ProjectSourceHistory, error) {
	history, ok := archive.SourceHistory()
	if !ok {
		return nil, nil
	}
	return sourceHistoryProjects(history, archive.Manifest())
}

func knownKindNames() []string {
	names := make([]string, 0, len(knownKinds))
	for name := range knownKinds {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func recordIdentity(kind string, record Record) string {
	return kind + "\x00" + record.Project + "\x00" + record.ID
}

func rejectUnmerged(manifest Manifest) error {
	for _, source := range manifest.Sources {
		switch source.Family {
		case "dolt-conflicts", "dolt-working-set", "dolt-remotes":
			if source.Rows != 0 {
				return fmt.Errorf("unmerged or dirty source family %q", source.Family)
			}
		}
	}
	return nil
}

func compatibleManifests(base, other Manifest) error {
	if base.Format != other.Format || base.Schema != other.Schema || base.Reference != other.Reference ||
		base.MainVersion != other.MainVersion || base.IgnoredVersion != other.IgnoredVersion || base.Scope != other.Scope || base.SourcePlaneAddressContract != other.SourcePlaneAddressContract || base.StatusCategoryContract != other.StatusCategoryContract {
		return fmt.Errorf("format, reference, scope or schema version differs")
	}
	if !equalJSON(base.Warnings, other.Warnings) || !equalJSON(base.Verification, other.Verification) {
		return fmt.Errorf("verification or disclosure contract differs")
	}
	baseKinds, otherKinds := kindVersions(base.Kinds), kindVersions(other.Kinds)
	for name, version := range baseKinds {
		if otherVersion, ok := otherKinds[name]; ok && otherVersion != version {
			return fmt.Errorf("kind %q has incompatible version", name)
		}
	}
	return compatibleSourceContracts(base.Sources, other.Sources)
}

func kindVersions(kinds []Kind) map[string]int {
	result := make(map[string]int, len(kinds))
	for _, kind := range kinds {
		result[kind.Name] = kind.Version
	}
	return result
}

func compatibleSourceContracts(base, other []Disposition) error {
	baseByFamily := map[string]Disposition{}
	otherByFamily := map[string]Disposition{}
	for _, source := range base {
		if isDatabaseDisposition(source.Family) {
			baseByFamily[source.Family] = source
		}
	}
	for _, source := range other {
		if isDatabaseDisposition(source.Family) {
			otherByFamily[source.Family] = source
		}
	}
	for family, expected := range baseByFamily {
		actual, ok := otherByFamily[family]
		if !ok || expected.Action != actual.Action || expected.Reason != actual.Reason {
			return fmt.Errorf("source disposition contract differs for %q", family)
		}
	}
	return nil
}

func isDatabaseDisposition(family string) bool {
	if family == "issues.row_lock" || family == "issues.is_blocked" {
		return true
	}
	_, ok := tableColumns[family]
	return ok
}

func mergeSources(inputs []mergeInput, records map[string][]Record, entries []externalEntry) ([]Disposition, error) {
	templates := map[string]Disposition{}
	for _, input := range inputs {
		for _, source := range input.manifest.Sources {
			if !isDatabaseDisposition(source.Family) {
				continue
			}
			if previous, ok := templates[source.Family]; ok {
				if previous.Action != source.Action || previous.Reason != source.Reason {
					return nil, fmt.Errorf("source disposition contract differs for %q", source.Family)
				}
				if source.Family == "repo_mtimes" {
					if previous.Rows+source.Rows < previous.Rows {
						return nil, fmt.Errorf("repository mtime census overflows")
					}
					previous.Rows += source.Rows
					templates[source.Family] = previous
				}
				continue
			}
			templates[source.Family] = source
		}
	}

	counts := map[string]int{}
	for _, rows := range records {
		for _, record := range rows {
			counts[record.Source.Table]++
			if record.Source.Table == "issues" {
				labels, ok := record.Body["labels"].([]any)
				if ok {
					counts["labels"] += len(labels)
				}
			}
		}
	}
	counts["issues.row_lock"] = counts["issues"]
	counts["issues.is_blocked"] = counts["issues"]

	families := make([]string, 0, len(templates))
	for family := range templates {
		families = append(families, family)
	}
	sort.Strings(families)
	sources := make([]Disposition, 0, len(families)+len(entries))
	for _, family := range families {
		source := templates[family]
		// Repository mtimes are excluded operational rows: their disposition is
		// the only surviving census, so retain its total instead of inventing 0.
		if family != "repo_mtimes" {
			source.Rows = counts[family]
		}
		sources = append(sources, source)
	}
	for _, entry := range entries {
		source := Disposition{Family: entry.Input.Family}
		if entry.Input.State == "absent" {
			source.Action = "manifest/rebuild"
			source.Reason = "requested source absent at declared path"
		} else {
			source.Rows = len(entry.Values)
			source.Action = "archive"
			source.Reason = "explicit JSON config; secrets are references; precedence retained, effective config not inferred"
		}
		sources = append(sources, source)
	}
	sort.Slice(sources, func(i, j int) bool {
		if sources[i].Family != sources[j].Family {
			return sources[i].Family < sources[j].Family
		}
		if sources[i].Reason != sources[j].Reason {
			return sources[i].Reason < sources[j].Reason
		}
		if sources[i].Rows != sources[j].Rows {
			return sources[i].Rows < sources[j].Rows
		}
		return sources[i].Action < sources[j].Action
	})
	return sources, nil
}

func archiveKey(archive *Archive) string {
	paths := make([]string, 0, len(archive.members))
	for path := range archive.members {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	hash := sha256.New()
	for _, path := range paths {
		_ = binary.Write(hash, binary.BigEndian, uint64(len(path)))
		_, _ = io.WriteString(hash, path)
		member := archive.members[path]
		_ = binary.Write(hash, binary.BigEndian, uint64(len(member)))
		_, _ = hash.Write(member)
	}
	return fmt.Sprintf("%x", hash.Sum(nil))
}

func mergeDigest(inputs []mergeInput) string {
	var data bytes.Buffer
	for _, input := range inputs {
		data.WriteString(input.key)
		data.WriteByte('\n')
	}
	sum := sha256.Sum256(data.Bytes())
	return fmt.Sprintf("%x", sum[:])
}

func encodeArchive(manifest Manifest, records map[string][]Record, config, history []byte) ([]byte, error) {
	if len(config) == 0 {
		return nil, fmt.Errorf("merged archive configuration provenance is empty")
	}
	files := map[string][]byte{"config/sources.json": append([]byte(nil), config...)}
	if len(history) != 0 {
		files[SourceHistoryMember] = append([]byte(nil), history...)
	}
	versions := kindVersions(manifest.Kinds)
	manifest.Kinds = nil
	manifest.Files = map[string]File{}
	for _, kind := range knownKindNames() {
		rows := append([]Record(nil), records[kind]...)
		if len(rows) == 0 {
			continue
		}
		sort.Slice(rows, func(i, j int) bool {
			return compareSourceRecords(rows[i], rows[j], manifest.SourcePlaneAddressContract) < 0
		})
		var data bytes.Buffer
		for i, record := range rows {
			if i > 0 && compareSourceRecords(rows[i-1], record, manifest.SourcePlaneAddressContract) == 0 {
				return nil, fmt.Errorf("duplicate %s record identity", kind)
			}
			encoded, err := json.Marshal(record)
			if err != nil {
				return nil, fmt.Errorf("serialize %s record: %w", kind, err)
			}
			data.Write(encoded)
			data.WriteByte('\n')
		}
		path := "records/" + kind + ".ndjson"
		files[path] = data.Bytes()
		version := versions[kind]
		if version == 0 {
			version = 1
		}
		manifest.Kinds = append(manifest.Kinds, Kind{Name: kind, Version: version, File: path, Count: len(rows)})
	}
	sort.Slice(manifest.Kinds, func(i, j int) bool { return manifest.Kinds[i].Name < manifest.Kinds[j].Name })
	for path, data := range files {
		manifest.Files[path] = File{SHA256: digest(data), Size: int64(len(data))}
	}
	manifestData, err := json.Marshal(manifest)
	if err != nil {
		return nil, fmt.Errorf("serialize merged manifest: %w", err)
	}
	manifestData = append(manifestData, '\n')
	files["manifest.json"] = manifestData
	paths := make([]string, 0, len(files)-1)
	for path := range files {
		if path != "manifest.json" {
			paths = append(paths, path)
		}
	}
	sort.Strings(paths)
	paths = append([]string{"manifest.json"}, paths...)
	var output bytes.Buffer
	writer := tar.NewWriter(&output)
	for _, path := range paths {
		data := files[path]
		if err := writer.WriteHeader(&tar.Header{Name: path, Mode: 0o600, Size: int64(len(data)), Typeflag: tar.TypeReg, Format: tar.FormatUSTAR, ModTime: time.Unix(0, 0)}); err != nil {
			return nil, err
		}
		if _, err := writer.Write(data); err != nil {
			return nil, err
		}
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	if output.Len() > MaxArchiveBytes {
		return nil, fmt.Errorf("merged archive exceeds %d bytes", MaxArchiveBytes)
	}
	result := output.Bytes()
	if _, err := Read(bytes.NewReader(result)); err != nil {
		return nil, fmt.Errorf("validate merged archive: %w", err)
	}
	return append([]byte(nil), result...), nil
}
