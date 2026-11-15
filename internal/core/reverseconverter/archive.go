package reverseconverter

import (
	"bytes"
	"encoding/json"
	"io"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
)

// Read is the rollback boundary for plug-dump/1. The canonical dumpformat
// reader owns the archive grammar, member allowlist, manifest census, typed
// source/body projections, relationship checks, secret-reference checks, and
// allocator bounds. Keeping that validator in one package prevents the
// reverse path from accepting an archive that the forward path would refuse.
//
// The returned Dump is detached from the validator's Archive. Read never opens
// a Plug store, invokes a Beads binary, or writes a target database.
func Read(r io.Reader) (Dump, error) {
	if r == nil {
		return Dump{}, refusal("nil archive reader")
	}
	archive, err := dumpformat.Read(r)
	if err != nil {
		return Dump{}, refusal("canonical plug-dump/1 validation failed: %v", err)
	}
	return adaptArchive(archive)
}

func ReadBytes(data []byte) (Dump, error) { return Read(bytes.NewReader(data)) }

// AdaptValidatedArchive detaches a dumpformat archive that has already passed
// its canonical validator, including native capture-set validation. Native
// capture heads are authenticated by dumpformat.ReadNativeCaptureSet rather
// than the portable Read entry point, so this preserves that validator's
// native disposition decisions without repacking the head into a portable tar.
func AdaptValidatedArchive(archive *dumpformat.Archive) (Dump, error) {
	if archive == nil {
		return Dump{}, refusal("nil validated archive")
	}
	return adaptArchive(archive)
}

func adaptArchive(archive *dumpformat.Archive) (Dump, error) {
	source := archive.Manifest()
	manifest := Manifest{
		Format:                     source.Format,
		Schema:                     source.Schema,
		Reference:                  source.Reference,
		StoreID:                    source.StoreID,
		Machine:                    source.Machine,
		MainVersion:                source.MainVersion,
		IgnoredVersion:             source.IgnoredVersion,
		Scope:                      source.Scope,
		History:                    source.History,
		SourceFieldsContract:       source.SourceFieldsContract,
		SourcePlaneAddressContract: source.SourcePlaneAddressContract,
		StatusCategoryContract:     source.StatusCategoryContract,
		Warnings:                   append([]string(nil), source.Warnings...),
		Verification:               cloneStringMap(source.Verification),
		Files:                      make(map[string]File, len(source.Files)),
	}
	for _, project := range source.Projects {
		manifest.Projects = append(manifest.Projects, Project{
			Prefix:         project.Prefix,
			IDMode:         project.IDMode,
			HashLength:     project.HashLength,
			HashNamespaces: append([]string(nil), project.HashNamespaces...),
			LastID:         project.LastID,
			ChildCounters:  cloneUintMap(project.ChildCounters),
		})
	}
	for _, kind := range source.Kinds {
		manifest.Kinds = append(manifest.Kinds, Kind{Name: kind.Name, Version: kind.Version, File: kind.File, Count: kind.Count})
	}
	for name, file := range source.Files {
		manifest.Files[name] = File{SHA256: file.SHA256, Size: file.Size}
	}
	for _, disposition := range source.Sources {
		encoded, err := json.Marshal(disposition)
		if err != nil {
			return Dump{}, refusal("copy source disposition: %v", err)
		}
		manifest.Sources = append(manifest.Sources, json.RawMessage(encoded))
	}

	dump := Dump{Manifest: manifest, Records: make(map[string][]Record), Members: make(map[string][]byte)}
	if member, ok := archive.Member("manifest.json"); ok {
		dump.Members["manifest.json"] = member
	}
	for name := range source.Files {
		if member, ok := archive.Member(name); ok {
			dump.Members[name] = member
		}
	}
	for _, kind := range source.Kinds {
		for _, record := range archive.Records(kind.Name) {
			dump.Records[kind.Name] = append(dump.Records[kind.Name], Record{
				NativeEdgeTokens: cloneStringMap(record.NativeEdgeTokens),
				Kind:             record.Kind,
				ID:               record.ID,
				Project:          record.Project,
				Body:             cloneMap(map[string]any(record.Body)),
				Source:           Provenance{Table: record.Source.Table, Fields: cloneMap(map[string]any(record.Source.Fields))},
			})
		}
	}
	return dump, nil
}

func cloneStringMap(input map[string]string) map[string]string {
	if input == nil {
		return nil
	}
	output := make(map[string]string, len(input))
	for key, value := range input {
		output[key] = value
	}
	return output
}

func cloneUintMap(input map[string]uint64) map[string]uint64 {
	if input == nil {
		return nil
	}
	output := make(map[string]uint64, len(input))
	for key, value := range input {
		output[key] = value
	}
	return output
}
