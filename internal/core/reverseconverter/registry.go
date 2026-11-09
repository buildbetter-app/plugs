package reverseconverter

import (
	"fmt"
	"sort"
	"strings"

	"github.com/buildbetter-app/plugs/internal/core/ontologymigration"
)

// RecordMapping is an executable reverse mapping, supplied by trusted host
// code, never by a request or configuration file. Convert must preserve the
// source content in a canonical record of BeadsKind. Canonical identity,
// provenance, field and graph validation still apply to its output.
type RecordMapping struct {
	Kind      string
	BeadsKind string
	Convert   func(Record) (Record, error)
}

// Converter owns both reverse conversion and kind admission. Its registry is
// immutable after construction. A nil or zero Converter uses only the built-in
// canonical conversions, so adding a label cannot admit native content.
type Converter struct {
	mappings map[string]RecordMapping
}

func NewConverter(mappings ...RecordMapping) (*Converter, error) {
	c := &Converter{mappings: make(map[string]RecordMapping, len(mappings))}
	for _, mapping := range mappings {
		if mapping.Kind == "" || strings.TrimSpace(mapping.Kind) != mapping.Kind || strings.ContainsAny(mapping.Kind, "/\\\x00") || supportedKinds[mapping.Kind] || operationalKind(mapping.Kind) {
			return nil, fmt.Errorf("invalid reverse source kind %q", mapping.Kind)
		}
		if !supportedKinds[mapping.BeadsKind] || operationalKind(mapping.BeadsKind) || mapping.Convert == nil {
			return nil, fmt.Errorf("reverse kind %q requires a canonical content target and record conversion", mapping.Kind)
		}
		if _, exists := c.mappings[mapping.Kind]; exists {
			return nil, fmt.Errorf("duplicate reverse kind %q", mapping.Kind)
		}
		c.mappings[mapping.Kind] = mapping
	}
	return c, nil
}

func (c *Converter) supports(kind string) bool {
	if supportedKinds[kind] {
		return true
	}
	if c != nil {
		_, ok := c.mappings[kind]
		return ok
	}
	return false
}

// Convert converts a complete detached inventory through this converter's
// actual mappings. Archive callers must still use the canonical Read boundary;
// registering a mapping does not loosen the archive grammar or restore load.
func (c *Converter) Convert(dump Dump) (Result, error) {
	return c.ConvertWithDirectives(dump, nil)
}

func (c *Converter) ConvertWithDirectives(dump Dump, directives []ontologymigration.Directive) (Result, error) {
	report := Report{InputDigest: digestDump(dump), Warnings: append([]string(nil), dump.Manifest.Warnings...)}
	if err := validateDumpShape(dump, &report, c); err != nil {
		normalizeReport(&report)
		return Result{Report: report}, withReport(err, report)
	}
	projected, err := c.project(dump)
	if err != nil {
		normalizeReport(&report)
		return Result{Report: report}, withReport(err, report)
	}
	return convertCanonical(projected, directives, report)
}

func (c *Converter) project(dump Dump) (Dump, error) {
	projected := Dump{Manifest: dump.Manifest, Records: make(map[string][]Record)}
	// Stable source ordering also makes callback execution deterministic.
	for _, kind := range dump.Manifest.Kinds {
		for _, record := range sortedRecords(dump.Records[kind.Name]) {
			record.Body = cloneMap(record.Body)
			record.Source.Fields = cloneMap(record.Source.Fields)
			if c != nil {
				if mapping, ok := c.mappings[kind.Name]; ok {
					converted, err := mapping.Convert(record)
					if err != nil {
						return Dump{}, refusal("kind %q record %q mapping: %v", kind.Name, record.ID, err)
					}
					if converted.Kind != mapping.BeadsKind {
						return Dump{}, refusal("kind %q record %q mapping emitted %q, want %q", kind.Name, record.ID, converted.Kind, mapping.BeadsKind)
					}
					record = converted
				}
			}
			projected.Records[record.Kind] = append(projected.Records[record.Kind], record)
		}
	}
	// The source manifest was validated before projection. Recount the derived
	// canonical inventory, which receives the same full conversion validation.
	projected.Manifest.Kinds = nil
	for kind, records := range projected.Records {
		projected.Manifest.Kinds = append(projected.Manifest.Kinds, Kind{Name: kind, Version: 1, File: "records/" + kind + ".ndjson", Count: len(records)})
	}
	sort.Slice(projected.Manifest.Kinds, func(i, j int) bool { return projected.Manifest.Kinds[i].Name < projected.Manifest.Kinds[j].Name })
	return projected, nil
}
