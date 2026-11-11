package converterroundtrip

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// NativePlugDefault counts an actual destination initializer. Its value is
// destination state, never a replacement cell in the retained source image.
type NativePlugDefault struct {
	TargetKind string          `json:"target_kind"`
	Field      string          `json:"field"`
	Value      json.RawMessage `json:"value"`
	Count      int             `json:"count"`
}

// NativeTargetOmission is deliberately separate from defaults and SQL NULL.
// These Beads columns have no modeled Plug field. No numeric correspondence
// with the workflow item's generation or the kernel revision is invented.
type NativeTargetOmission struct {
	Family      string `json:"family"`
	Column      string `json:"column"`
	TargetKind  string `json:"target_kind"`
	Disposition string `json:"disposition"`
	Count       int    `json:"count"`
}

type NativeSchemaReport struct {
	Source               dumpformat.SourceIdentity      `json:"source"`
	Profile              dumpformat.NativeSchemaProfile `json:"schema_profile"`
	SchemaDefaultCount   int                            `json:"schema_default_count"`
	PlugDefaults         []NativePlugDefault            `json:"plug_defaults"`
	TargetOmissions      []NativeTargetOmission         `json:"target_omissions"`
	OpaqueMetadataFields int                            `json:"opaque_metadata_fields,omitempty"`
}

func nativeSchemaAccounting(archive *dumpformat.Archive, profile driver.RestoreImportProfile, docs map[string]pipeline.Document, allocators []driver.OfflineAllocator) ([]NativeSchemaReport, error) {
	if archive.Manifest().MainVersion == 72 {
		// Preserve the original v72 receipt encoding and recovery contract.
		return nil, nil
	}
	if profile.SourcePlanes == nil {
		return nil, driver.ErrUnsupported
	}
	plan, err := profile.SourcePlanes.Plan()
	if err != nil {
		return nil, err
	}
	targetAllocators := make(map[string]driver.OfflineAllocator, len(allocators))
	for _, allocator := range allocators {
		if _, duplicate := targetAllocators[allocator.Prefix]; duplicate {
			return nil, driver.ErrUnsupported
		}
		targetAllocators[allocator.Prefix] = allocator
	}
	reports := []NativeSchemaReport{}
	metadata, err := archive.NativeSourceHistoryMetadata()
	if err != nil {
		return nil, err
	}
	for _, scope := range metadata {
		if scope.StoreID == "" || len(scope.Projects) != 1 || scope.Projects[0] == "" || scope.SchemaProfile == nil {
			return nil, fmt.Errorf("%w: source schema accounting lacks bound profile", driver.ErrUnsupported)
		}
		identity := dumpformat.SourceIdentity{StoreID: scope.StoreID, Project: scope.Projects[0]}
		p := *scope.SchemaProfile
		p.Absences = append([]dumpformat.NativeSchemaAbsence(nil), p.Absences...)
		report := NativeSchemaReport{Source: identity, Profile: p, PlugDefaults: []NativePlugDefault{}, TargetOmissions: []NativeTargetOmission{}}
		// Report the exact admitted allocator already selected by buildPlan.
		// This preserves its hash policy, source counters and project binding;
		// accounting never selects or replaces an allocator.
		targetAllocator, present := targetAllocators[identity.Project]
		if !present {
			return nil, driver.ErrUnsupported
		}
		allocator, err := json.Marshal(targetAllocator)
		if err != nil {
			return nil, err
		}
		report.PlugDefaults = append(report.PlugDefaults, NativePlugDefault{TargetKind: "instance_config", Field: "target_allocator", Value: allocator, Count: 1})
		for _, entity := range plan.Entities {
			if entity.Source.Project != identity.Project || entity.TargetKind != "bead" {
				continue
			}
			doc, present := docs[entity.TargetID]
			if !present {
				return nil, driver.ErrUnsupported
			}
			for _, disposition := range plan.Dispositions {
				if disposition.Key == entity.DispositionKey {
					for _, rule := range disposition.Fields {
						if rule.Disposition == "opaque-metadata" {
							if _, present := doc.Metadata[rule.Target]; !present {
								return nil, fmt.Errorf("%w: opaque destination field missing", driver.ErrUnsupported)
							}
							report.OpaqueMetadataFields++
						}
					}
				}
			}
		}
		for _, absence := range p.Absences {
			if absence.Column == "" {
				continue
			}
			counts := map[string]int{}
			for _, entity := range plan.Entities {
				if entity.Source.Project != identity.Project || entity.Source.Plane != absence.Family {
					continue
				}
				if entity.TargetKind == "bead" {
					doc, present := docs[entity.TargetID]
					if !present {
						return nil, fmt.Errorf("%w: default census target missing", driver.ErrUnsupported)
					}
					if _, present := doc.Fields[absence.Column]; present {
						return nil, fmt.Errorf("%w: unmodeled source column became a Plug field", driver.ErrUnsupported)
					}
				}
				counts[entity.TargetKind]++
			}
			for kind, count := range counts {
				report.TargetOmissions = append(report.TargetOmissions, NativeTargetOmission{Family: absence.Family, Column: absence.Column, TargetKind: kind, Disposition: "absent-source-column-no-target-slot", Count: count})
			}
		}
		sort.Slice(report.TargetOmissions, func(i, j int) bool {
			a, b := report.TargetOmissions[i], report.TargetOmissions[j]
			if a.Family != b.Family {
				return a.Family < b.Family
			}
			if a.Column != b.Column {
				return a.Column < b.Column
			}
			return a.TargetKind < b.TargetKind
		})
		reports = append(reports, report)
	}
	sort.Slice(reports, func(i, j int) bool {
		a, b := reports[i].Source, reports[j].Source
		if a.StoreID != b.StoreID {
			return a.StoreID < b.StoreID
		}
		return a.Project < b.Project
	})
	return reports, nil
}
