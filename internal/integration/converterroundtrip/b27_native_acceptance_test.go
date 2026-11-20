package converterroundtrip

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// These are detached producer observations, already authenticated by
// b27NativeInputs. A SQL empty table and an absent filesystem source remain
// different facts. No expected row is taken from a converter or target store.
type b27NativeSourceCensus struct {
	SQL map[string]struct {
		Columns []string `json:"columns"`
		Present bool     `json:"present"`
		Rows    int      `json:"rows"`
	} `json:"sql"`
	Filesystem map[string]struct {
		Present bool   `json:"present"`
		Bytes   int64  `json:"bytes"`
		SHA256  string `json:"sha256"`
	} `json:"filesystem"`
	Dispositions []dumpformat.Disposition `json:"native_dispositions"`
}

func b27NativeCensus(t *testing.T, input b27NativeInput) b27NativeSourceCensus {
	t.Helper()
	var census b27NativeSourceCensus
	b27NativeJSON(t, b27NativeBytes(t, filepath.Join(input.dir, "census.json")), &census)
	if len(census.SQL) != 33 || len(census.SQL) != len(input.current) || len(census.Filesystem) != 5 {
		t.Fatal("independent native source census changed", input.Project)
	}
	for family, table := range input.current {
		observed, exists := census.SQL[family]
		if !exists || !observed.Present || observed.Rows != len(table.Rows) {
			t.Fatal("independent SQL source family census differs", input.Project, family)
		}
		b27NativeEqual(t, input.Project+"/source-columns/"+family, observed.Columns, table.Columns)
	}
	for name, file := range census.Filesystem {
		switch name {
		case "config.yaml", "metadata.json":
			raw := b27NativeBytes(t, filepath.Join(input.dir, "filesystem", name))
			if !file.Present || int64(len(raw)) != file.Bytes || hashBytes(raw) != file.SHA256 {
				t.Fatal("independent source file commitment differs", input.Project, name)
			}
		case "routes.jsonl", "interactions.jsonl", "issues.jsonl":
			if file.Present || file.Bytes != 0 || file.SHA256 != "" {
				t.Fatal("independent source absence changed", input.Project, name)
			}
		default:
			t.Fatal("unexpected independent filesystem source", input.Project, name)
		}
	}
	b27NativeRelationCensus(t, input)
	return census
}

func b27NativeRelationCensus(t *testing.T, input b27NativeInput) {
	t.Helper()
	ids := map[string]bool{}
	for _, row := range input.current["issues"].Rows {
		id, ok := row["id"].(string)
		if !ok || id == "" || ids[id] {
			t.Fatal("independent issue census repeats an identity", input.Project)
		}
		ids[id] = true
	}
	seen := map[string]bool{}
	parents, unresolved := 0, 0
	for _, edge := range input.current["dependencies"].Rows {
		id, ok := edge["id"].(string)
		from, sourceText := edge["issue_id"].(string)
		arm, target := b27NativeDependencyTarget(t, edge)
		if !ok || id == "" || seen[id] || !sourceText || !ids[from] {
			t.Fatal("independent dependency census differs", input.Project)
		}
		seen[id] = true
		if arm != "depends_on_issue_id" || !ids[target] {
			unresolved++
		}
		if edge["type"] == "parent-child" {
			parents++
		}
	}
	if len(ids) != 12 || len(seen) != 5 || parents != 2 || unresolved != 0 {
		t.Fatal("authenticated relation census changed", input.Project)
	}
	// This source has no unresolved endpoint. Keep that supplied-input limit
	// explicit instead of inventing a missing-target case or successful repair.
	t.Logf("b27_source_relations project=%s issues=%d dependencies=%d parents=%d unresolved=%d missing_target_vector=not_supplied", input.Project, len(ids), len(seen), parents, unresolved)
}

// The installed issue schema maps the four ordinary fields below. Rich text,
// references and scheduling fields have explicit source metadata destinations;
// close_reason remains an original source field. Check both value and presence,
// including JSON null, empty strings and the complete user metadata key set.
func b27NativeSpecificFields(t *testing.T, input b27NativeInput, source dumpformat.Row, doc pipeline.Document, allowCurrentClose bool, expectedComments int) {
	t.Helper()
	id := source["id"].(string)
	// The restore contract keeps the four ordinary ontology fields and mirrors
	// a nonempty label set into the fields map for the writable document
	// projection. Require that exact optional mirror while rejecting every
	// other undeclared field.
	wantFieldCount := 4
	if len(doc.Labels) != 0 {
		wantFieldCount++
	}
	if len(doc.Fields) != wantFieldCount {
		t.Fatal("imported ordinary field census differs", id)
	}
	if len(doc.Labels) != 0 {
		labels, present := doc.Fields["labels"]
		if !present {
			t.Fatal("imported label field mirror absent", id)
		}
		b27NativeEqual(t, id+"/label-field-mirror", labels, doc.Labels)
	} else if _, present := doc.Fields["labels"]; present {
		t.Fatal("empty labels gained a field mirror", id)
	}
	for _, field := range []string{"design", "acceptance_criteria", "notes", "assignee", "owner", "created_at", "created_by", "updated_at", "closed_at", "due_at", "defer_until", "started_at", "spec_id", "external_ref", "pinned"} {
		want, present := source[field]
		if !present {
			t.Fatal("independent source field absent", id, field)
		}
		raw, present := doc.Metadata["beads.source."+field]
		if !present {
			t.Fatal("imported source field absent", id, field)
		}
		var got any
		b27NativeJSON(t, raw, &got)
		b27NativeEqual(t, id+"/source/"+field, got, want)
		if _, live := doc.Fields[field]; live {
			t.Fatal("origin-only source field became an undeclared live field", id, field)
		}
	}
	var fields map[string]any
	b27NativeJSON(t, doc.Metadata["beads.source_fields"], &fields)
	for _, field := range []string{"close_reason", "closed_by_session", "spec_id", "estimated_minutes"} {
		got, present := fields[field]
		want, sourcePresent := source[field]
		if !present || !sourcePresent {
			t.Fatal("explicit close/spec source field absent", id, field)
		}
		b27NativeEqual(t, id+"/close-spec/"+field, got, want)
	}
	var wantMetadata map[string]any
	raw := source["metadata"]
	if text, ok := raw.(string); ok {
		b27NativeJSON(t, []byte(text), &wantMetadata)
	} else {
		encoded, err := json.Marshal(raw)
		if err != nil {
			t.Fatal(err)
		}
		b27NativeJSON(t, encoded, &wantMetadata)
	}
	gotMetadata := map[string]any{}
	for key, raw := range doc.Metadata {
		if strings.HasPrefix(key, "beads.") || strings.HasPrefix(key, "plug.restore.") {
			continue
		}
		var value any
		b27NativeJSON(t, raw, &value)
		gotMetadata[key] = value
	}
	if wantMetadata == nil {
		wantMetadata = map[string]any{}
	}
	b27NativeEqual(t, id+"/complete-user-metadata", gotMetadata, wantMetadata)
	if len(doc.Comments) != expectedComments {
		t.Fatal("independent source comment census differs", id, len(doc.Comments), expectedComments)
	}
	// The retained rich fixture finishes after reopening the parent, so its
	// current rows stay open. The authenticated v2 positive fixture explicitly
	// supplies a genuine current-close control and checks its exact value in
	// the caller; keep the old open-only assertion for the retained path.
	if !allowCurrentClose && (source["close_reason"] != "" || source["closed_at"] != nil) {
		t.Fatal("original current close census changed", input.Project, id)
	}
}

func b27NativeAuxiliaryReadback(t *testing.T, store driver.Store, archive *dumpformat.Archive, input b27NativeInput, report Report) {
	t.Helper()
	census := b27NativeCensus(t, input)
	want := map[string]dumpformat.Disposition{}
	for _, disposition := range census.Dispositions {
		switch disposition.Family {
		case "config", "metadata", "custom_statuses", "custom_types", "routes", "interactions", "issue_snapshots", "compaction_snapshots", "provenance_events", "events":
			if _, duplicate := want[disposition.Family]; duplicate {
				t.Fatal("independent auxiliary disposition repeated", input.Project, disposition.Family)
			}
			want[disposition.Family] = disposition
		}
	}
	if len(want) != 10 {
		t.Fatal("independent auxiliary disposition census differs", input.Project)
	}
	ctx := context.Background()
	meta, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := store.Read(ctx, driver.View{Incarnation: meta.Incarnation, At: &meta.Head})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	groups, err := queryNativeRowGroups(ctx, reader, meta.Incarnation, meta.Head, archive)
	if err != nil {
		t.Fatal(err)
	}
	identity := dumpformat.SourceIdentity{StoreID: input.StoreID, Project: input.Project}
	for family, disposition := range want {
		if disposition.Rows != len(input.current[family].Rows) {
			t.Fatal("independent auxiliary row census differs", input.Project, family)
		}
		declared := 0
		for _, actual := range archive.Manifest().Sources {
			if actual.Family == family {
				b27NativeEqual(t, input.Project+"/disposition/"+family, actual, disposition)
				declared++
			}
		}
		matched := 0
		for _, actual := range report.NativeFamilies {
			if actual.Source == identity && actual.Family == family {
				if actual.History != disposition.History || actual.SourceRows != disposition.Rows || actual.ImportedRows != disposition.Rows || actual.ExpiredRows != 0 {
					t.Fatal("imported auxiliary accounting differs", input.Project, family)
				}
				matched++
			}
		}
		if declared != 1 || matched != 1 {
			t.Fatal("auxiliary source omitted or repeated", input.Project, family)
		}
		records, exists := groups[nativeRowGroup{Source: identity, Table: family, History: disposition.History}]
		got, tableMeta, err := dumpformat.ReconstructNativeRowTable(identity, family, disposition.History, records, nil)
		if !exists || err != nil || tableMeta.RowCount != uint64(disposition.Rows) || len(records) != disposition.Rows+1 {
			t.Fatal("auxiliary table/header census differs", input.Project, family)
		}
		b27NativeTableEqual(t, input.Project+"/auxiliary/"+family, got, input.current[family])
		t.Logf("b27_auxiliary_source project=%s family=%s rows=%d source_action=%s retained=true", input.Project, family, disposition.Rows, disposition.Action)
	}
	b27NativeConfigSourceReadback(t, reader, archive, input, census)
}

func b27NativeConfigSourceReadback(t *testing.T, reader driver.Reader, archive *dumpformat.Archive, input b27NativeInput, census b27NativeSourceCensus) {
	t.Helper()
	raw, exists := archive.Member("config/sources.json")
	declared, declaredFile := archive.Manifest().Files["config/sources.json"]
	if !exists || !declaredFile || hashBytes(raw) != declared.SHA256 || int64(len(raw)) != declared.Size {
		t.Fatal("authenticated config source member absent", input.Project)
	}
	retained, err := reader.Blob(context.Background(), driver.Hash(declared.SHA256))
	if err != nil || !bytes.Equal(raw, retained) {
		t.Fatal("imported/reopened config source member differs", input.Project)
	}
	var sources []struct {
		Input struct {
			Family     string `json:"family"`
			State      string `json:"state"`
			Precedence int    `json:"precedence"`
		} `json:"input"`
		Values struct {
			Observations []struct {
				Path   string            `json:"path"`
				State  string            `json:"state"`
				SHA256 string            `json:"sha256"`
				Size   int64             `json:"size"`
				Bytes  map[string]string `json:"bytes"`
			} `json:"observations"`
		} `json:"values"`
	}
	b27NativeJSON(t, retained, &sources)
	selected := map[string]struct {
		file       string
		precedence int
	}{"project-config": {"config.yaml", 3}, "beads-dir-config": {"config.yaml", 4}, "metadata-json": {"metadata.json", 7}, "routes-jsonl": {"routes.jsonl", 13}, "issues-jsonl": {"issues.jsonl", 16}, "interactions-jsonl": {"interactions.jsonl", 17}}
	seen := map[string]bool{}
	for _, source := range sources {
		want, selectedSource := selected[source.Input.Family]
		if !selectedSource {
			continue
		}
		if seen[source.Input.Family] || source.Input.State != "json-config" || source.Input.Precedence != want.precedence || len(source.Values.Observations) != 1 {
			t.Fatal("independent configuration source binding differs", input.Project, source.Input.Family)
		}
		seen[source.Input.Family] = true
		file := census.Filesystem[want.file]
		observed := source.Values.Observations[0]
		if filepath.Base(observed.Path) != want.file {
			t.Fatal("configuration source path differs", input.Project, source.Input.Family)
		}
		if !file.Present {
			if observed.State != "absent" || observed.Bytes != nil || observed.SHA256 != "" || observed.Size != 0 {
				t.Fatal("source file absence became a present or SQL-empty file", input.Project, want.file)
			}
			continue
		}
		data, err := base64.StdEncoding.DecodeString(observed.Bytes["$bytes"])
		if err != nil || observed.State != "file" || observed.Size != file.Bytes || observed.SHA256 != file.SHA256 || len(observed.Bytes) != 1 || int64(len(data)) != file.Bytes || hashBytes(data) != file.SHA256 || !bytes.Equal(data, b27NativeBytes(t, filepath.Join(input.dir, "filesystem", want.file))) {
			t.Fatal("independent config source bytes differ", input.Project, want.file)
		}
	}
	if len(seen) != len(selected) {
		t.Fatal("configuration source census omitted a selected source", input.Project)
	}
}

func b27NativeCatalogReadback(t *testing.T, store driver.Store, archive *dumpformat.Archive, input b27NativeInput) {
	t.Helper()
	reference := &B27NativeReferenceForTest{inputs: []b27NativeInput{input}}
	statuses, origins := reference.Catalog(t, input.Project)
	typeUnion := map[string]bool{}
	for origin, names := range origins {
		if strings.HasSuffix(origin, "/type") {
			for _, name := range names {
				typeUnion[name] = true
			}
		}
	}
	types := make([]string, 0, len(typeUnion))
	for name := range typeUnion {
		types = append(types, name)
	}
	sort.Strings(types)
	b27NativeEqual(t, input.Project+"/independent-custom-type-union", types, []string{"rich_kind", "sql_only_kind", "unused_stored_kind", "yaml_only_kind"})
	// The original public sources explicitly map the used rich source type.
	// Pin that disposition instead of deriving the expected live type from
	// whichever directive the receiver happens to return.
	directives := archive.Manifest().Ontology
	if len(directives) != 1 || directives[0].Kind != "map_type" || directives[0].Target.Project != input.Project || directives[0].Target.Type != "rich_kind" || directives[0].Target.ID != "" || directives[0].From != "rich_kind" || directives[0].To != "issue" {
		t.Fatal("original custom type disposition differs", input.Project)
	}
	rows := b27NativeRows(t, store, nil)
	row, exists := rows[ImportedOntologySourceID]
	if !exists || row.Kind != "ontology" {
		t.Fatal("stored imported catalog absent", input.Project)
	}
	var installed dumpformat.NativeStatusOntology
	b27NativeJSON(t, row.Body, &installed)
	overlay, exists := installed.Overlays[input.Project]
	if !exists {
		t.Fatal("stored project catalog absent", input.Project)
	}
	got := map[string]string{}
	for name, status := range overlay.Statuses {
		got[name] = string(status.Category)
	}
	b27NativeEqual(t, input.Project+"/installed-stored-yaml-status-union", got, statuses)
	// Source type declarations stay independently retained; a map_type for a
	// used source type is its target disposition, not installation of unused
	// source type names. The existing native profile has no such promise.
	t.Logf("b27_source_catalog project=%s custom_types=%q status_count=%d type_origins_retained=true", input.Project, types, len(statuses))
}

// Whole-record commitments include every Ref/kind/revision/body without
// decoding opaque machine-local values. They are conservation evidence only.
type b27NativeConservation struct {
	Meta driver.Meta
	Rows map[string]string
}

func b27NativeConservationAt(t *testing.T, store driver.Store) b27NativeConservation {
	t.Helper()
	meta, err := store.Meta(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	result := b27NativeConservation{Meta: meta, Rows: map[string]string{}}
	for id, row := range b27NativeRows(t, store, &meta.Head) {
		raw, err := json.Marshal(row)
		if err != nil {
			t.Fatal(err)
		}
		result.Rows[id] = hashBytes(raw)
	}
	if len(result.Rows) == 0 {
		t.Fatal("nonempty conservation baseline is empty")
	}
	return result
}

// Refusal may consume its source preparation. Authenticate a separate original
// capture handle so the retained recovery archive keeps its readback lifetime.
func b27NativeRefusalArchive(t *testing.T, ctx context.Context, capture, expectedSHA256 string) *dumpformat.Archive {
	t.Helper()
	archive, err := dumpformat.ReadNativeCaptureSetDirectory(ctx, capture, dumpformat.NativeCaptureSetOptions{})
	if err != nil {
		t.Fatal("authenticate independent refusal source", err)
	}
	if archive.SHA256() != expectedSHA256 {
		archive.ReleaseNativeDecoded()
		t.Fatal("independent refusal source digest differs")
	}
	return archive
}

func b27NativeNonemptyRefusal(t *testing.T, ctx context.Context, store driver.Store, lease driver.ActiveLease, profile driver.RestoreImportProfile, archive *dumpformat.Archive, capture string) b27NativeConservation {
	t.Helper()
	before := b27NativeConservationAt(t, store)
	replay := b27NativeRefusalArchive(t, ctx, capture, archive.SHA256())
	replayReport, err := RestoreNativeCaptureSetImportWithLease(ctx, replay, store, lease, profile)
	if replayReport.Archive != nil && replayReport.Archive != replay {
		replayReport.Archive.ReleaseNativeDecoded()
	}
	if err != nil {
		replay.ReleaseNativeDecoded()
		t.Fatal("exact replay of original restore was not successful", err)
	}
	replay.ReleaseNativeDecoded()
	refusal := b27NativeRefusalArchive(t, ctx, capture, archive.SHA256())
	fresh := profile
	fresh.IdempotencyKey = hashBytes([]byte("b27-fresh-target-not-empty\x00" + archive.SHA256()))[:32]
	refusalReport, err := RestoreNativeCaptureSetImportWithLease(ctx, refusal, store, lease, fresh)
	if refusalReport.Archive != nil && refusalReport.Archive != refusal {
		refusalReport.Archive.ReleaseNativeDecoded()
	}
	refusal.ReleaseNativeDecoded()
	// This offline port returns the typed driver sentinel. The public RPC
	// adapter owns its restore_requires_empty code; do not invent that mapping
	// on the offline ErrorMapping interface.
	if !errors.Is(err, driver.ErrRestoreTargetNotEmpty) {
		t.Fatal("nonempty restore lost its typed target-not-empty refusal")
	}
	if !reflect.DeepEqual(before, b27NativeConservationAt(t, store)) {
		t.Fatal("nonempty restore refusal changed existing durable state")
	}
	return before
}
