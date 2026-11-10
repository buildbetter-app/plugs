package reverseconverter

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/ontologymigration"
)

var supportedIssueTypes = map[string]bool{
	"task": true, "bug": true, "feature": true, "epic": true, "chore": true,
}

var supportedKinds = map[string]bool{
	"bead": true, "edge": true, "comment": true, "memory": true,
	"config": true, "beads-archive": true,
}

var supportedStatuses = map[string]bool{
	"open": true, "in_progress": true, "hook" + "ed": true,
	"blocked": true, "deferred": true, "pinned": true, "frozen": true,
	"closed": true,
}

var supportedEdgeTypes = map[string]bool{
	"blocks": true, "parent-child": true, "related": true, "discovered-from": true,
}

var supportedBeadBodyFields = map[string]bool{
	"type": true, "source_type": true, "title": true, "description": true,
	"design": true, "acceptance_criteria": true, "notes": true, "priority": true,
	"assignee": true, "owner": true, "created_at": true, "created_by": true,
	"updated_at": true, "closed_at": true, "due_at": true, "defer_until": true,
	"started_at": true, "metadata": true, "spec_id": true, "external_ref": true,
	"pinned": true, "labels": true, "status_category": true, "result": true,
}

const (
	beadsAgentStateMetadataKey = "beads.agent_state"
	beadsAwaitIDMetadataKey    = "beads.await_id"
)

type dependencyLine struct {
	NativeExtensions  map[string]string `json:"-"`
	ID                string            `json:"id,omitempty"`
	IssueID           string            `json:"issue_id"`
	DependsOnID       string            `json:"depends_on_id,omitempty"`
	DependsOnExternal string            `json:"depends_on_external,omitempty"`
	Type              string            `json:"type"`
	CreatedAt         string            `json:"created_at,omitempty"`
	CreatedBy         string            `json:"created_by,omitempty"`
	Metadata          string            `json:"metadata,omitempty"`
	ThreadID          string            `json:"thread_id,omitempty"`
}

type commentLine struct {
	ID        string `json:"id"`
	IssueID   string `json:"issue_id"`
	Author    string `json:"author"`
	Text      string `json:"text"`
	CreatedAt string `json:"created_at"`
}

type issueLine struct {
	RecordType         string           `json:"_type"`
	ID                 string           `json:"id"`
	Title              string           `json:"title"`
	Description        string           `json:"description,omitempty"`
	Design             string           `json:"design,omitempty"`
	AcceptanceCriteria string           `json:"acceptance_criteria,omitempty"`
	Notes              string           `json:"notes,omitempty"`
	SpecID             string           `json:"spec_id,omitempty"`
	Status             string           `json:"status,omitempty"`
	Priority           int              `json:"priority"`
	IssueType          string           `json:"issue_type,omitempty"`
	Assignee           string           `json:"assignee,omitempty"`
	Owner              string           `json:"owner,omitempty"`
	CreatedAt          string           `json:"created_at"`
	CreatedBy          string           `json:"created_by,omitempty"`
	UpdatedAt          string           `json:"updated_at"`
	StartedAt          string           `json:"started_at,omitempty"`
	ClosedAt           string           `json:"closed_at,omitempty"`
	CloseReason        string           `json:"close_reason,omitempty"`
	ClosedBySession    string           `json:"closed_by_session,omitempty"`
	DueAt              string           `json:"due_at,omitempty"`
	DeferUntil         string           `json:"defer_until,omitempty"`
	ExternalRef        string           `json:"external_ref,omitempty"`
	SourceSystem       string           `json:"source_system,omitempty"`
	Metadata           map[string]any   `json:"metadata,omitempty"`
	Labels             []string         `json:"labels,omitempty"`
	Dependencies       []dependencyLine `json:"dependencies,omitempty"`
	Comments           []commentLine    `json:"comments,omitempty"`
	Pinned             bool             `json:"pinned,omitempty"`
	Ephemeral          bool             `json:"ephemeral,omitempty"`
	NoHistory          bool             `json:"no_history,omitempty"`
	SourcePlane        string           `json:"-"`
}

type memoryLine struct {
	RecordType string `json:"_type"`
	Key        string `json:"key"`
	Value      any    `json:"value"`
}

type headerLine struct {
	Schema string `json:"_schema"`
	Sort   string `json:"_sort"`
}

type workingIssue struct {
	Line       issueLine
	Category   string
	Outcome    string
	SourceStat string
	Plane      string
	Omitted    bool
}

type typeDirective struct {
	directive ontologymigration.Directive
	used      bool
}

type typeDirectives struct {
	entries []*typeDirective
}

func (d *typeDirectives) matching(project, id, sourceType string) []*typeDirective {
	var matches []*typeDirective
	for _, entry := range d.entries {
		target := entry.directive.Target
		if target.Project == project && target.Type == sourceType && (target.ID == "" || target.ID == id) {
			matches = append(matches, entry)
		}
	}
	return matches
}

func (d *typeDirectives) markUsed(entry *typeDirective, report *Report) {
	if entry.used {
		return
	}
	entry.used = true
	directive := entry.directive
	report.TypeMappings = append(report.TypeMappings, TypeMapping{
		ID: directive.ID, Project: directive.Target.Project, From: directive.From, To: directive.To,
	})
}

func prepareTypeDirectives(directives []ontologymigration.Directive) (typeDirectives, []string) {
	set := typeDirectives{entries: make([]*typeDirective, 0, len(directives))}
	var reasons []string
	seen := map[string]bool{}
	for _, directive := range directives {
		if strings.TrimSpace(directive.ID) == "" {
			reasons = append(reasons, "type directive requires an ID")
			continue
		}
		if seen[directive.ID] {
			reasons = append(reasons, fmt.Sprintf("duplicate type directive ID %q", directive.ID))
			continue
		}
		seen[directive.ID] = true
		if directive.Kind != ontologymigration.MapType {
			reasons = append(reasons, fmt.Sprintf("type directive %q must use map_type", directive.ID))
			continue
		}
		if directive.Field != "" || directive.Value != nil || directive.Conversion != "" || directive.Violation != "" || directive.Reason != "" {
			reasons = append(reasons, fmt.Sprintf("type directive %q contains members not valid for map_type", directive.ID))
			continue
		}
		if directive.Target.Project == "" {
			reasons = append(reasons, fmt.Sprintf("type directive %q requires a target project", directive.ID))
			continue
		}
		if directive.Target.Type == "" || directive.From == "" || directive.To == "" {
			reasons = append(reasons, fmt.Sprintf("type directive %q requires source and target types", directive.ID))
			continue
		}
		if directive.From != directive.Target.Type {
			reasons = append(reasons, fmt.Sprintf("type directive %q map_type source must equal the target's original type", directive.ID))
			continue
		}
		if directive.From == directive.To {
			reasons = append(reasons, fmt.Sprintf("type directive %q mapping must change its key", directive.ID))
			continue
		}
		set.entries = append(set.entries, &typeDirective{directive: directive})
	}
	sort.Slice(set.entries, func(i, j int) bool {
		a, b := set.entries[i].directive, set.entries[j].directive
		if a.ID != b.ID {
			return a.ID < b.ID
		}
		if a.Target.Project != b.Target.Project {
			return a.Target.Project < b.Target.Project
		}
		return a.Target.ID < b.Target.ID
	})
	return set, reasons
}

// Convert converts a detached, validated dump to deterministic Beads JSONL.
// It accumulates all structural refusals before returning, and never returns a
// successful result containing a partially converted issue graph. Unknown
// source issue types require an explicit map_type directive; the source type is
// retained in the JSONL rather than silently rewritten to a built-in type.
func Convert(dump Dump) (Result, error) {
	return ConvertWithOptions(dump, Options{})
}

// ConvertWithDirectives is Convert with explicit ontology map_type directives
// authorizing source types outside Plug's built-in reverse-converter vocabulary.
// The directives authorize the target Plug ontology type for the caller's
// migration plan; this converter deliberately preserves the original Beads
// issue_type in its JSONL output.
func ConvertWithDirectives(dump Dump, directives []ontologymigration.Directive) (Result, error) {
	return ConvertWithOptions(dump, Options{Directives: directives})
}

// ConvertWithOptions mirrors the pinned export's default Ephemeral filter.
// All explicitly retains every physical entity plane, matching bd export --all.
func ConvertWithOptions(dump Dump, options Options) (Result, error) {
	return (*Converter)(nil).ConvertWithOptions(dump, options)
}

func convertCanonical(dump Dump, options Options, report Report) (Result, error) {
	directives := options.Directives
	if err := validateDumpShape(dump, &report, nil); err != nil {
		normalizeReport(&report)
		return Result{Report: report}, withReport(err, report)
	}
	typeSet, directiveReasons := prepareTypeDirectives(directives)
	if len(directiveReasons) != 0 {
		normalizeReport(&report)
		return Result{Report: report}, withReport(&Refusal{Reasons: uniqueStrings(directiveReasons), Report: report}, report)
	}
	if dump.Manifest.History == "verified" {
		addUnique(&report.LostPostSwitchHistory, "verified source history is not replayed by a head-state Beads import")
	} else if dump.Manifest.History == "available-unverified" {
		addUnique(&report.LostPostSwitchHistory, "available-unverified source history is not representable in a head-state Beads import")
	} else {
		addUnique(&report.LostPostSwitchHistory, "Plug head-state dump contains no complete post-switch history")
	}
	working := make(map[string]*workingIssue)
	var reasons []string
	edgeIDs := make(map[[2]string]bool)
	commentIDs := make(map[[2]string]bool)
	entities, edges, comments, physicalReasons := physicalExportRecords(dump)
	reasons = append(reasons, physicalReasons...)
	planes := make(map[string]string)
	for _, record := range entities {
		if prior, exists := planes[record.ID]; exists && prior != record.Source.Table {
			normalizeReport(&report)
			return Result{Report: report}, withReport(refusal("%s", collisionReason(record.ID, prior, record.Source.Table)), report)
		}
		planes[record.ID] = record.Source.Table
	}
	for _, record := range sortedRecords(entities) {
		ephemeral, flagOK := sourceFlag(record.Source.Fields, "ephemeral")
		if !flagOK {
			reasons = append(reasons, fmt.Sprintf("bead %q has invalid ephemeral flag", record.ID))
			continue
		}
		if !options.All && ephemeral {
			report.OmittedEphemeralRows = append(report.OmittedEphemeralRows, omissionLine(record))
			if prior := working[record.ID]; prior != nil {
				reasons = append(reasons, collisionReason(record.ID, prior.Plane, record.Source.Table))
			} else {
				working[record.ID] = &workingIssue{Line: issueLine{ID: record.ID}, Plane: record.Source.Table, Omitted: true}
			}
			continue
		}
		issue, issueReasons := convertBead(record, &report, &typeSet)
		reasons = append(reasons, issueReasons...)
		if issue != nil {
			if _, exists := working[issue.Line.ID]; exists {
				reasons = append(reasons, collisionReason(issue.Line.ID, working[issue.Line.ID].Plane, record.Source.Table))
			} else {
				working[issue.Line.ID] = issue
			}
		}
	}
	for _, record := range sortedRecords(edges) {
		if from := working[text(record.Body["from"])]; from != nil && from.Omitted {
			continue
		}
		if record.ID != "" {
			if edgeIDs[[2]string{record.Source.Table, record.ID}] {
				reasons = append(reasons, fmt.Sprintf("duplicate edge id %q", record.ID))
			} else {
				edgeIDs[[2]string{record.Source.Table, record.ID}] = true
			}
		}
		reasons = append(reasons, convertEdge(record, working, manifestEdgeVersion(dump.Manifest), &report)...)
	}
	for _, record := range sortedRecords(comments) {
		if from := working[text(record.Body["issue_id"])]; from != nil && from.Omitted {
			continue
		}
		if record.ID != "" {
			if commentIDs[[2]string{record.Source.Table, record.ID}] {
				reasons = append(reasons, fmt.Sprintf("duplicate comment id %q", record.ID))
			} else {
				commentIDs[[2]string{record.Source.Table, record.ID}] = true
			}
		}
		reasons = append(reasons, convertComment(record, working, &report)...)
	}
	var memories []memoryLine
	memoryKeys := make(map[string]bool)
	for _, record := range sortedRecords(dump.Records["memory"]) {
		memory, err := convertMemory(record, false, &report)
		if err != nil {
			reasons = append(reasons, err.Error())
		} else if memoryKeys[memory.Key] {
			reasons = append(reasons, fmt.Sprintf("duplicate memory key %q", memory.Key))
		} else {
			memoryKeys[memory.Key] = true
			memories = append(memories, memory)
		}
	}
	for _, record := range sortedRecords(dump.Records["config"]) {
		memory, err := convertMemory(record, true, &report)
		if err != nil {
			reasons = append(reasons, err.Error())
		} else if memoryKeys[memory.Key] {
			reasons = append(reasons, fmt.Sprintf("duplicate memory key %q", memory.Key))
		} else {
			memoryKeys[memory.Key] = true
			memories = append(memories, memory)
			report.ConfigAsMemories++
		}
	}
	for _, record := range sortedRecords(dump.Records["beads-archive"]) {
		report.ArchiveRows++
		table := record.Source.Table
		if table == dumpformat.EphemeralEntityTable || table == dumpformat.EphemeralCommentTable || table == dumpformat.EphemeralLabelTable {
			continue
		}
		if table == "" {
			table = "unknown"
		}
		addUnique(&report.LostPostSwitchHistory, fmt.Sprintf("%s archive row %s retained only in the Plug report", table, record.ID))
		lowerTable := strings.ToLower(table)
		if strings.Contains(lowerTable, "blob") {
			addUnique(&report.Blobs, record.ID)
		}
		if containsByteReference(record.Body) || containsByteReference(record.Source.Fields) {
			addUnique(&report.Blobs, record.ID)
		}
		if strings.Contains(lowerTable, "principal") || strings.Contains(lowerTable, "identity") {
			addUnique(&report.Principals, record.ID)
		}
	}
	for kind, records := range dump.Records {
		if kind == "bead" || kind == "edge" || kind == "comment" || kind == "memory" || kind == "config" || kind == "beads-archive" {
			continue
		}
		if len(records) > 0 {
			addUnique(&report.PlugOnlyKinds, kind)
			reasons = append(reasons, fmt.Sprintf("Plug-only kind %q has %d record(s)", kind, len(records)))
		}
	}
	for _, entry := range typeSet.entries {
		if !entry.used {
			reasons = append(reasons, fmt.Sprintf("type directive %q is unused", entry.directive.ID))
		}
	}
	if len(reasons) > 0 {
		normalizeReport(&report)
		return Result{Report: report}, &Refusal{Reasons: uniqueStrings(reasons), Report: report}
	}

	ids := make([]string, 0, len(working))
	for id, issue := range working {
		if issue.Omitted {
			continue
		}
		ids = append(ids, id)
	}
	sort.Strings(ids)
	sort.Slice(memories, func(i, j int) bool {
		if memories[i].Key != memories[j].Key {
			return memories[i].Key < memories[j].Key
		}
		// Preserve the established byte ordering for every string-only case.
		left, leftString := memories[i].Value.(string)
		right, rightString := memories[j].Value.(string)
		if leftString && rightString {
			return left < right
		}
		a, _ := canonicalJSON(memories[i].Value)
		b, _ := canonicalJSON(memories[j].Value)
		return a < b
	})
	projection := Projection{}
	var out bytes.Buffer
	header, _ := json.Marshal(headerLine{Schema: BeadsJSONL, Sort: "stable-v1"})
	out.Write(header)
	out.WriteByte('\n')
	for _, id := range ids {
		issue := working[id]
		sort.Slice(issue.Line.Dependencies, func(i, j int) bool {
			a, b := issue.Line.Dependencies[i], issue.Line.Dependencies[j]
			if a.DependsOnExternal != b.DependsOnExternal {
				return a.DependsOnExternal < b.DependsOnExternal
			}
			if a.DependsOnID != b.DependsOnID {
				return a.DependsOnID < b.DependsOnID
			}
			if a.Type != b.Type {
				return a.Type < b.Type
			}
			return a.ID < b.ID
		})
		sort.Slice(issue.Line.Comments, func(i, j int) bool { return issue.Line.Comments[i].ID < issue.Line.Comments[j].ID })
		line, err := marshalNativeIssue(issue.Line)
		if err != nil {
			return Result{Report: report}, refusal("native edge serialization: %v", err)
		}
		out.Write(line)
		out.WriteByte('\n')
		projection.Issues = append(projection.Issues, IssueProjection{ID: id, Kind: issue.Line.IssueType, Status: issue.Line.Status, Category: issue.Category, Outcome: issue.Outcome})
		for _, dependency := range issue.Line.Dependencies {
			edge := EdgeProjection{ID: dependency.ID, From: dependency.IssueID, To: dependency.DependsOnID, Type: dependency.Type}
			if dependency.DependsOnExternal != "" {
				edge.To, edge.TargetKind = dependency.DependsOnExternal, "depends_on_external"
			}
			projection.Edges = append(projection.Edges, edge)
		}
		for _, comment := range issue.Line.Comments {
			projection.Comments = append(projection.Comments, CommentProjection{ID: comment.ID, IssueID: comment.IssueID})
		}
	}
	for _, memory := range memories {
		line, _ := json.Marshal(memory)
		out.Write(line)
		out.WriteByte('\n')
		projection.Memories = append(projection.Memories, MemoryProjection{Key: memory.Key, Value: memory.Value})
	}
	result := Result{JSONL: out.Bytes(), Projection: projection, Report: report}
	result.Digest = digestBytes(result.JSONL)
	result.Report.OutputDigest = result.Digest
	normalizeReport(&result.Report)
	return result, nil
}

func containsByteReference(value any) bool {
	switch value := value.(type) {
	case map[string]any:
		if _, ok := value["$bytes"]; ok {
			return true
		}
		for _, item := range value {
			if containsByteReference(item) {
				return true
			}
		}
	case []any:
		for _, item := range value {
			if containsByteReference(item) {
				return true
			}
		}
	}
	return false
}

// ConvertArchive is the one-shot convenience form used by rollback tooling.
func ConvertArchive(data []byte) (Result, error) {
	return ConvertArchiveWithDirectives(data, nil)
}

// ConvertArchiveWithDirectives is ConvertWithDirectives for a canonical archive.
// ConvertArchiveWithOptions validates the retained archive before applying
// the same default or explicit All selection as ConvertWithOptions.
func ConvertArchiveWithOptions(data []byte, options Options) (Result, error) {
	dump, err := ReadBytes(data)
	if err != nil {
		return Result{}, err
	}
	return ConvertWithOptions(dump, options)
}

func ConvertArchiveWithDirectives(data []byte, directives []ontologymigration.Directive) (Result, error) {
	dump, err := ReadBytes(data)
	if err != nil {
		return Result{}, err
	}
	return ConvertWithDirectives(dump, directives)
}

func withReport(err error, report Report) error {
	if refusal, ok := err.(*Refusal); ok {
		refusal.Report = report
	}
	return err
}

func manifestEdgeVersion(manifest Manifest) int {
	for _, kind := range manifest.Kinds {
		if kind.Name == "edge" {
			return kind.Version
		}
	}
	return 1
}

func validateDumpShape(dump Dump, report *Report, converter *Converter) error {
	m := dump.Manifest
	if m.SourcePlaneAddressContract != "" && m.SourcePlaneAddressContract != dumpformat.SourcePlaneAddressContract {
		return refusal("unsupported physical source address contract")
	}
	if m.SourceFieldsContract != "" && m.SourceFieldsContract != dumpformat.IssueSourceFieldsContract {
		return refusal("unsupported physical source fields contract")
	}
	if m.Format != PlugDumpFormat || m.Schema != 1 || m.Reference != BeadsReference || m.MainVersion != 72 || m.IgnoredVersion != 28 || m.StoreID == "" || m.Machine == "" || m.Scope != "offline-head-and-source-archives" {
		return refusal("unsupported plug-dump manifest")
	}
	if m.History != "unavailable" && m.History != "available-unverified" && m.History != "verified" {
		return refusal("unsupported history disposition %q", m.History)
	}
	if len(m.Kinds) == 0 {
		return refusal("empty kind registry")
	}
	registered := map[string]bool{}
	for i, kind := range m.Kinds {
		if !converter.supports(kind.Name) || (kind.Version != 1 && (kind.Name != "edge" || !dumpformat.IsTypedEdgeVersion(kind.Version))) || kind.File != "records/"+kind.Name+".ndjson" || kind.Count < 1 || registered[kind.Name] || i > 0 && m.Kinds[i-1].Name >= kind.Name {
			if !converter.supports(kind.Name) && kind.Name != "" {
				addUnique(&report.PlugOnlyKinds, kind.Name)
				addUnique(&report.UnsupportedFields, "kind/"+kind.Name)
			}
			return refusal("invalid or unsorted kind registry")
		}
		registered[kind.Name] = true
		if len(dump.Records[kind.Name]) != kind.Count {
			return refusal("kind %q count does not match records", kind.Name)
		}
	}
	for kind := range dump.Records {
		if !converter.supports(kind) {
			addUnique(&report.PlugOnlyKinds, kind)
			addUnique(&report.UnsupportedFields, "kind/"+kind)
			return refusal("unregistered or unsupported record kind %q", kind)
		}
		if !registered[kind] {
			return refusal("unregistered record kind %q", kind)
		}
	}
	for kind, records := range dump.Records {
		for i, record := range records {
			if record.Kind != kind || strings.TrimSpace(record.ID) != record.ID || (strings.ContainsRune(record.ID, '\x00') && !(kind == "edge" && m.SourcePlaneAddressContract == dumpformat.SourcePlaneAddressContract)) || record.ID == "" || record.Body == nil || record.Source.Table == "" || record.Source.Fields == nil {
				return refusal("kind %q record %d has incomplete identity or provenance", kind, i)
			}
		}
	}
	return nil
}

func convertBead(record Record, report *Report, directives *typeDirectives) (*workingIssue, []string) {
	body := record.Body
	var reasons []string
	reasons = append(reasons, unsupportedSourceFields(record, report)...)
	archivedSource := archivedSourceFields(record, report)
	if record.Source.Table != dumpformat.DurableEntityTable && record.Source.Table != dumpformat.EphemeralEntityTable {
		reasons = append(reasons, fmt.Sprintf("bead %q has source table %q, want issues", record.ID, record.Source.Table))
	}
	for key := range body {
		if !supportedBeadBodyFields[key] {
			report.UnsupportedFields = append(report.UnsupportedFields, "bead/"+record.ID+"/"+key)
			reasons = append(reasons, fmt.Sprintf("bead %q has unsupported field %q", record.ID, key))
		}
	}
	if record.ID == "" || text(body["type"]) != "issue" {
		reasons = append(reasons, fmt.Sprintf("bead %q is not a representable issue", record.ID))
		return nil, reasons
	}
	for _, field := range []string{"type", "source_type", "title", "description", "design", "acceptance_criteria", "notes", "created_at", "created_by", "updated_at", "closed_at", "due_at", "defer_until", "started_at", "spec_id", "external_ref", "assignee", "owner", "result", "status_category"} {
		if value, present := body[field]; present && value != nil {
			if _, ok := value.(string); !ok {
				reasons = append(reasons, fmt.Sprintf("bead %q field %q is not a string", record.ID, field))
			}
		}
	}
	pinned := false
	if value, present := body["pinned"]; present {
		var ok bool
		pinned, ok = parsePinned(value)
		if !ok {
			reasons = append(reasons, fmt.Sprintf("bead %q field %q is not a boolean or canonical numeric/textual 0/1", record.ID, "pinned"))
		} else if raw, textual := value.(string); textual {
			// Public paired SQL sources can store this column as TEXT. The
			// native JSONL boolean represents its meaning; retain the actual
			// source encoding separately rather than silently erasing it.
			archivedSource["pinned"] = raw
			addUnique(&report.ArchivedSourceFields, "bead/"+record.ID+"/source/pinned")
		}
	}
	typ := text(body["source_type"])
	if typ == "" {
		typ = text(record.Source.Fields["issue_type"])
	}
	if sourceType, present := record.Source.Fields["issue_type"]; present && sourceType != nil {
		if _, ok := sourceType.(string); !ok {
			reasons = append(reasons, fmt.Sprintf("bead %q source issue_type is not a string", record.ID))
		} else if text(sourceType) != typ {
			reasons = append(reasons, fmt.Sprintf("bead %q body/source issue type mismatch", record.ID))
		}
	}
	if !supportedIssueTypes[typ] {
		matches := directives.matching(record.Project, record.ID, typ)
		switch len(matches) {
		case 0:
			report.UnsupportedFields = append(report.UnsupportedFields, "bead/"+record.ID+"/source_type")
			reasons = append(reasons, fmt.Sprintf("bead %q has unsupported kind %q; explicit map_type directive required", record.ID, typ))
		case 1:
			directives.markUsed(matches[0], report)
		default:
			reasons = append(reasons, fmt.Sprintf("bead %q has conflicting map_type directives for kind %q", record.ID, typ))
		}
	}
	title := text(body["title"])
	if strings.TrimSpace(title) == "" {
		reasons = append(reasons, fmt.Sprintf("bead %q has no title", record.ID))
	}
	priority, ok := integer(body["priority"])
	if !ok || priority < 0 || priority > 4 {
		reasons = append(reasons, fmt.Sprintf("bead %q has invalid priority", record.ID))
	}
	category := text(body["status_category"])
	outcome := text(body["result"])
	if body["status_category"] != nil && category == "" {
		reasons = append(reasons, fmt.Sprintf("bead %q status category is not a string", record.ID))
	}
	if body["result"] != nil && outcome == "" {
		reasons = append(reasons, fmt.Sprintf("bead %q result is not a string", record.ID))
	}
	if category == "" {
		reasons = append(reasons, fmt.Sprintf("bead %q has no status category", record.ID))
	}
	statusValue, statusFieldPresent := record.Source.Fields["status"]
	status := text(statusValue)
	if statusFieldPresent && statusValue != nil && status == "" {
		reasons = append(reasons, fmt.Sprintf("bead %q status is not a string", record.ID))
	}
	statusFieldPresent = statusFieldPresent && status != ""
	if status != "" && !supportedStatuses[status] {
		reasons = append(reasons, fmt.Sprintf("bead %q has unsupported status %q", record.ID, status))
	}
	if status == "" {
		status = statusForCategory(category)
		if status == "" {
			reasons = append(reasons, fmt.Sprintf("bead %q category %q cannot be represented by Beads", record.ID, category))
		}
	} else if expected := categoryForStatus(status); expected != category {
		reasons = append(reasons, fmt.Sprintf("bead %q status/category mismatch (%q/%q)", record.ID, status, category))
	}
	if category == "completed" {
		if outcome != "success" && outcome != "failure" {
			reasons = append(reasons, fmt.Sprintf("bead %q completed status lacks success/failure outcome", record.ID))
		}
	} else if outcome != "" {
		reasons = append(reasons, fmt.Sprintf("bead %q has outcome on non-completed status", record.ID))
	}
	if class := text(record.Source.Fields["storage_class"]); class != "" && class != "versioned" && class != "ephemeral" {
		report.UnsupportedFields = append(report.UnsupportedFields, "bead/"+record.ID+"/storage_class")
		reasons = append(reasons, fmt.Sprintf("bead %q has unsupported storage class %q", record.ID, class))
	}
	createdAt := timestamp(body["created_at"])
	updatedAt := timestamp(body["updated_at"])
	for _, field := range []string{"created_at", "updated_at", "started_at", "closed_at", "due_at", "defer_until"} {
		if value, present := body[field]; present && value != nil && timestamp(value) == "" {
			reasons = append(reasons, fmt.Sprintf("bead %q field %q is not a valid RFC3339 timestamp", record.ID, field))
		}
	}
	if createdAt == "" || updatedAt == "" {
		reasons = append(reasons, fmt.Sprintf("bead %q lacks RFC3339 created_at/updated_at", record.ID))
	}
	metadata, metadataErr := dumpformat.SourceMetadataObject(body["metadata"])
	if metadataErr != nil {
		reasons = append(reasons, fmt.Sprintf("bead %q metadata is not an object: %v", record.ID, metadataErr))
	}
	if body["metadata"] != nil {
		if _, err := canonicalJSON(metadata); err != nil {
			reasons = append(reasons, fmt.Sprintf("bead %q metadata: %v", record.ID, err))
		}
	}
	metadata = cloneMap(metadata)
	if metadata == nil {
		metadata = map[string]any{}
	}
	for _, field := range dumpformat.OpaqueIssueFields() {
		value, present := record.Source.Fields[field]
		if !present {
			continue
		}
		if (field == "agent_state" || field == "await_id") && value != nil {
			if _, ok := value.(string); !ok {
				reasons = append(reasons, fmt.Sprintf("bead %q source field %q is not a string", record.ID, field))
				continue
			}
		}
		key := "beads." + field
		if existing, exists := metadata[key]; exists {
			if !jsonEquivalent(existing, value) {
				reasons = append(reasons, fmt.Sprintf("bead %q source field %q conflicts with metadata %q", record.ID, field, key))
			}
			continue
		}
		// Keep this value opaque. The Beads importer owns its meaning; Plug
		// carries the exact JSON value without workflow mapping.
		metadata[key] = cloneValue(value)
	}
	if len(archivedSource) > 0 {
		if _, exists := metadata["_plug_source_fields"]; exists {
			reasons = append(reasons, fmt.Sprintf("bead %q metadata reserves _plug_source_fields", record.ID))
		} else {
			// Beads normalizes metadata through float64. Preserve lossless
			// source numbers inside a canonical JSON string across readback.
			encoded, err := canonicalJSON(archivedSource)
			if err != nil {
				reasons = append(reasons, fmt.Sprintf("bead %q source archive: %v", record.ID, err))
			} else {
				metadata["_plug_source_fields"] = encoded
			}
		}
	}
	if !statusFieldPresent {
		if _, exists := metadata["_plug_status_category"]; exists {
			reasons = append(reasons, fmt.Sprintf("bead %q reserves _plug_status_category", record.ID))
		}
		metadata["_plug_status_category"] = category
	}
	if outcome != "" {
		if _, exists := metadata["_plug_status_outcome"]; exists {
			reasons = append(reasons, fmt.Sprintf("bead %q reserves _plug_status_outcome", record.ID))
		}
		metadata["_plug_status_outcome"] = outcome
	}
	labels, labelsErr := stringSlice(body["labels"])
	if labelsErr != nil {
		reasons = append(reasons, fmt.Sprintf("bead %q labels: %v", record.ID, labelsErr))
	}
	sort.Strings(labels)
	for i := 1; i < len(labels); i++ {
		if labels[i-1] == labels[i] {
			reasons = append(reasons, fmt.Sprintf("bead %q labels contain duplicate %q", record.ID, labels[i]))
			break
		}
	}
	if len(reasons) > 0 {
		return nil, reasons
	}
	line := issueLine{
		RecordType: BeadsJSONLIssueType, ID: record.ID, Title: title, Priority: int(priority), IssueType: typ,
		CreatedAt: createdAt, UpdatedAt: updatedAt, Status: status, Metadata: metadata, Labels: labels,
	}
	copyString := func(key string) string { return text(body[key]) }
	line.Description, line.Design, line.AcceptanceCriteria, line.Notes = copyString("description"), copyString("design"), copyString("acceptance_criteria"), copyString("notes")
	line.SpecID, line.Assignee, line.Owner, line.CreatedBy = copyString("spec_id"), copyString("assignee"), copyString("owner"), copyString("created_by")
	line.StartedAt, line.ClosedAt, line.DueAt, line.DeferUntil = timestamp(body["started_at"]), timestamp(body["closed_at"]), timestamp(body["due_at"]), timestamp(body["defer_until"])
	line.ExternalRef, line.Pinned = copyString("external_ref"), pinned
	line.CloseReason, line.ClosedBySession = text(record.Source.Fields["close_reason"]), text(record.Source.Fields["closed_by_session"])
	line.SourceSystem = text(record.Source.Fields["source_system"])
	var flagOK bool
	line.Ephemeral, flagOK = sourceFlag(record.Source.Fields, "ephemeral")
	if !flagOK {
		return nil, []string{fmt.Sprintf("bead %q has invalid ephemeral flag", record.ID)}
	}
	line.NoHistory, flagOK = sourceFlag(record.Source.Fields, "no_history")
	if !flagOK {
		return nil, []string{fmt.Sprintf("bead %q has invalid no_history flag", record.ID)}
	}
	line.SourcePlane = record.Source.Table
	return &workingIssue{Line: line, Category: category, Outcome: outcome, SourceStat: text(record.Source.Fields["status"]), Plane: record.Source.Table}, nil
}

const (
	BeadsJSONLIssueType = "issue"
)

func convertEdge(record Record, issues map[string]*workingIssue, edgeVersion int, report *Report) []string {
	body := record.Body
	var reasons []string
	extensions, extensionErr := dumpformat.NativeEdgeExtensions(dumpformat.Record{Kind: record.Kind, ID: record.ID, Body: record.Body, Source: dumpformat.Provenance{Table: record.Source.Table, Fields: record.Source.Fields}, NativeEdgeTokens: record.NativeEdgeTokens})
	if extensionErr != nil {
		reasons = append(reasons, fmt.Sprintf("edge %q: %v", record.ID, extensionErr))
	}
	if record.Source.Table != dumpformat.DurableDependencyTable && record.Source.Table != dumpformat.EphemeralDependencyTable {
		reasons = append(reasons, fmt.Sprintf("edge %q has source table %q, want dependencies", record.ID, record.Source.Table))
	}
	for key := range body {
		if !map[string]bool{"from": true, "to": true, "target_kind": true, "type": true, "metadata": true, "thread_id": true}[key] {
			report.UnsupportedFields = append(report.UnsupportedFields, "edge/"+record.ID+"/"+key)
			reasons = append(reasons, fmt.Sprintf("edge %q has unsupported field %q", record.ID, key))
		}
	}
	for _, field := range []string{"from", "to", "target_kind", "type", "thread_id"} {
		if value, present := body[field]; present && value != nil {
			if _, ok := value.(string); !ok {
				reasons = append(reasons, fmt.Sprintf("edge %q field %q is not a string", record.ID, field))
			}
		}
	}
	if metadata, present := body["metadata"]; present && metadata != nil {
		if _, err := canonicalJSON(metadata); err != nil {
			reasons = append(reasons, fmt.Sprintf("edge %q metadata: %v", record.ID, err))
		}
	}
	from, to, targetKind, edgeType := text(body["from"]), text(body["to"]), text(body["target_kind"]), text(body["type"])
	if issues[from] == nil {
		reasons = append(reasons, fmt.Sprintf("edge %q has missing source %q", record.ID, from))
	}
	if targetKind == "depends_on_issue_id" || targetKind == dumpformat.EphemeralDependencyTargetField {
		if issues[to] == nil {
			reasons = append(reasons, fmt.Sprintf("edge %q has missing target %q", record.ID, to))
		}
	} else if targetKind == "depends_on_external" {
		// Retain the census arm explicitly. Native importer support requires
		// independent producer/readback evidence for this typed JSONL field.
	} else {
		reasons = append(reasons, fmt.Sprintf("edge %q has unsupported target kind %q", record.ID, targetKind))
	}
	if issue := issues[from]; issue != nil {
		plane := dumpformat.DurableEntityTable
		if record.Source.Table == dumpformat.EphemeralDependencyTable {
			plane = dumpformat.EphemeralEntityTable
		}
		if issue.Plane != plane {
			reasons = append(reasons, fmt.Sprintf("edge %q source plane mismatch", record.ID))
		}
	}
	if targetKind != "depends_on_external" {
		plane := dumpformat.DurableEntityTable
		if targetKind == dumpformat.EphemeralDependencyTargetField {
			plane = dumpformat.EphemeralEntityTable
		}
		if issue := issues[to]; issue != nil && issue.Plane != plane {
			reasons = append(reasons, fmt.Sprintf("edge %q target plane mismatch", record.ID))
		}
	}
	var metadataProjection dumpformat.EdgeMetadataProjection
	versionedType := dumpformat.IsVersionedLocalKnownEdgeType(edgeVersion, targetKind, edgeType) || (dumpformat.IsTypedEdgeVersion(edgeVersion) && targetKind == dumpformat.EphemeralDependencyTargetField && dumpformat.IsCanonicalEdgeType(edgeType))
	localOpaque := dumpformat.IsLocalOpaqueEdgeType(edgeVersion, targetKind, edgeType)
	if targetKind != "depends_on_external" {
		if !supportedEdgeTypes[edgeType] && !versionedType && !localOpaque {
			reasons = append(reasons, fmt.Sprintf("edge %q has unsupported type %q", record.ID, edgeType))
		}
	} else if edgeType == "" || len(edgeType) > 32 || !utf8.ValidString(edgeType) {
		reasons = append(reasons, fmt.Sprintf("edge %q has unsupported type %q", record.ID, edgeType))
	}
	if dumpformat.IsTypedEdgeVersion(edgeVersion) {
		if _, present := body["metadata"]; !present {
			reasons = append(reasons, fmt.Sprintf("edge %q lacks metadata column", record.ID))
		}
		if _, present := record.Source.Fields["metadata"]; !present {
			reasons = append(reasons, fmt.Sprintf("edge %q lacks source metadata column", record.ID))
		}
		var err error
		metadataProjection, err = dumpformat.InspectSourceEdgeMetadata(edgeVersion, record.ID, edgeType, body["metadata"])
		if err != nil {
			reasons = append(reasons, fmt.Sprintf("edge %q metadata: %v", record.ID, err))
		}
	}
	if record.ID == "" || from == "" || to == "" {
		reasons = append(reasons, fmt.Sprintf("edge %q lacks identity or endpoints", record.ID))
	}
	if createdAt := record.Source.Fields["created_at"]; createdAt != nil && timestamp(createdAt) == "" {
		reasons = append(reasons, fmt.Sprintf("edge %q has invalid created_at", record.ID))
	}
	if createdBy := record.Source.Fields["created_by"]; createdBy != nil && text(createdBy) == "" {
		reasons = append(reasons, fmt.Sprintf("edge %q has invalid created_by", record.ID))
	}
	for _, field := range []string{"id", "issue_id", "type", "metadata", "thread_id", "created_at", "created_by"} {
		if value, present := record.Source.Fields[field]; present && value != nil {
			if field == "created_at" {
				if timestamp(value) == "" {
					reasons = append(reasons, fmt.Sprintf("edge %q source created_at is not a valid RFC3339 timestamp", record.ID))
				}
			} else if field == "created_by" {
				if _, ok := value.(string); !ok {
					reasons = append(reasons, fmt.Sprintf("edge %q source created_by is not a string", record.ID))
				}
			} else if field == "id" || field == "issue_id" || field == "type" || field == "thread_id" {
				if _, ok := value.(string); !ok {
					reasons = append(reasons, fmt.Sprintf("edge %q source %s is not a string", record.ID, field))
				}
			}
		}
	}
	if sourceValue, present := record.Source.Fields["id"]; present && text(sourceValue) != record.ID {
		reasons = append(reasons, fmt.Sprintf("edge %q source id mismatch", record.ID))
	}
	if sourceValue, present := record.Source.Fields["issue_id"]; present && text(sourceValue) != from {
		reasons = append(reasons, fmt.Sprintf("edge %q source issue_id mismatch", record.ID))
	}
	if sourceValue, present := record.Source.Fields["type"]; present && text(sourceValue) != edgeType {
		reasons = append(reasons, fmt.Sprintf("edge %q source type mismatch", record.ID))
	}
	if sourceValue, present := record.Source.Fields["metadata"]; present && !jsonEquivalent(sourceValue, body["metadata"]) {
		reasons = append(reasons, fmt.Sprintf("edge %q source metadata mismatch", record.ID))
	}
	if sourceValue, present := record.Source.Fields["thread_id"]; present && !jsonEquivalent(sourceValue, body["thread_id"]) {
		reasons = append(reasons, fmt.Sprintf("edge %q source thread mismatch", record.ID))
	}
	for _, sourceTarget := range []string{"depends_on_issue_id", dumpformat.EphemeralDependencyTargetField, "depends_on_external"} {
		if value, present := record.Source.Fields[sourceTarget]; present && value != nil {
			if dumpformat.IsTypedEdgeVersion(edgeVersion) {
				if _, ok := value.(string); !ok {
					reasons = append(reasons, fmt.Sprintf("edge %q source %s is not a string", record.ID, sourceTarget))
					continue
				}
			}
			if text(value) == "" {
				continue
			}
			if sourceTarget != targetKind || text(value) != to {
				reasons = append(reasons, fmt.Sprintf("edge %q source target mismatch", record.ID))
			}
		}
	}
	if len(reasons) > 0 {
		return reasons
	}
	// The pinned importer does not persist Dependency.ID; it derives a
	// surrogate from the endpoints. Keep the Plug edge identity in a reserved
	// metadata envelope so a Beads export/readback can recover it without
	// changing the graph endpoints. The source metadata, when present, is
	// carried verbatim as the envelope's _plug_metadata member.
	envelope := map[string]any{"_plug_edge_id": record.ID}
	if value := body["metadata"]; value != nil {
		envelope["_plug_metadata"] = value
	}
	var metadata string
	var err error
	if metadataProjection.Envelope {
		// A v2 source may already carry the exact flat identity envelope.
		// Keep SQL text byte-for-byte; wrapping again would invent a nested
		// reserved envelope and change the native source representation.
		if raw, ok := body["metadata"].(string); ok {
			metadata = raw
		} else {
			metadata, err = canonicalJSON(body["metadata"])
		}
	} else {
		if token, present := extensions["beads.metadata"]; present && body["metadata"] != nil {
			id, _ := json.Marshal(record.ID)
			metadata = "{\"_plug_edge_id\":" + string(id) + ",\"_plug_metadata\":" + token + "}"
		} else {
			metadata, err = canonicalJSON(envelope)
		}
	}
	if err != nil {
		return []string{fmt.Sprintf("edge %q metadata: %v", record.ID, err)}
	}
	dependency := dependencyLine{NativeExtensions: extensions, ID: record.ID, IssueID: from, DependsOnID: to, Type: edgeType, Metadata: metadata, ThreadID: text(body["thread_id"]), CreatedAt: timestamp(record.Source.Fields["created_at"]), CreatedBy: text(record.Source.Fields["created_by"])}
	if targetKind == "depends_on_external" {
		dependency.DependsOnID, dependency.DependsOnExternal = "", to
	}
	issues[from].Line.Dependencies = append(issues[from].Line.Dependencies, dependency)
	return nil
}

func convertComment(record Record, issues map[string]*workingIssue, report *Report) []string {
	body := record.Body
	var reasons []string
	reasons = append(reasons, unsupportedSourceFields(record, report)...)
	if record.Source.Table != "comments" && record.Source.Table != dumpformat.EphemeralCommentTable {
		reasons = append(reasons, fmt.Sprintf("comment %q has source table %q, want comments", record.ID, record.Source.Table))
	}
	for key := range body {
		if !map[string]bool{"id": true, "issue_id": true, "author": true, "text": true, "created_at": true}[key] {
			report.UnsupportedFields = append(report.UnsupportedFields, "comment/"+record.ID+"/"+key)
			reasons = append(reasons, fmt.Sprintf("comment %q has unsupported field %q", record.ID, key))
		}
	}
	for _, field := range []string{"id", "issue_id", "author", "text", "created_at"} {
		if _, present := body[field]; !present {
			reasons = append(reasons, fmt.Sprintf("comment %q is missing field %q", record.ID, field))
		}
	}
	issueID := text(body["issue_id"])
	if record.ID == "" || issueID == "" || issues[issueID] == nil {
		reasons = append(reasons, fmt.Sprintf("comment %q has missing issue %q", record.ID, issueID))
	}
	if issue := issues[issueID]; issue != nil {
		plane := dumpformat.DurableEntityTable
		if record.Source.Table == dumpformat.EphemeralCommentTable {
			plane = dumpformat.EphemeralEntityTable
		}
		if issue.Plane != plane {
			reasons = append(reasons, fmt.Sprintf("comment %q owner plane mismatch", record.ID))
		}
	}
	if text(body["id"]) != record.ID {
		reasons = append(reasons, fmt.Sprintf("comment %q body/source identity mismatch", record.ID))
	}
	for _, field := range []string{"id", "issue_id", "author", "text", "created_at"} {
		if value, present := body[field]; present && value != nil {
			if _, ok := value.(string); !ok {
				reasons = append(reasons, fmt.Sprintf("comment %q field %q is not a string", record.ID, field))
			}
		}
	}
	for _, field := range []string{"id", "issue_id", "author", "text", "created_at"} {
		if value, present := record.Source.Fields[field]; present && value != nil {
			if _, ok := value.(string); !ok {
				reasons = append(reasons, fmt.Sprintf("comment %q source field %q is not a string", record.ID, field))
			}
		}
	}
	for _, field := range []string{"id", "issue_id", "author", "text", "created_at"} {
		if sourceValue, present := record.Source.Fields[field]; present && !jsonEquivalent(sourceValue, body[field]) {
			reasons = append(reasons, fmt.Sprintf("comment %q source %s mismatch", record.ID, field))
		}
	}
	createdAt := timestamp(body["created_at"])
	if value, present := body["created_at"]; present && value != nil && createdAt == "" {
		reasons = append(reasons, fmt.Sprintf("comment %q created_at is not a valid RFC3339 timestamp", record.ID))
	}
	if createdAt == "" {
		reasons = append(reasons, fmt.Sprintf("comment %q lacks RFC3339 created_at", record.ID))
	}
	if len(reasons) > 0 {
		return reasons
	}
	issues[issueID].Line.Comments = append(issues[issueID].Line.Comments, commentLine{ID: record.ID, IssueID: issueID, Author: text(body["author"]), Text: text(body["text"]), CreatedAt: createdAt})
	return nil
}

func convertMemory(record Record, config bool, report *Report) (memoryLine, error) {
	if unsupported := unsupportedSourceFields(record, report); len(unsupported) > 0 {
		return memoryLine{}, errors.New(strings.Join(unsupported, "; "))
	}
	if record.Source.Table != "config" && record.Source.Table != "metadata" {
		return memoryLine{}, fmt.Errorf("%s %q has source table %q, want config or metadata", record.Kind, record.ID, record.Source.Table)
	}
	if config && record.Kind != "config" {
		return memoryLine{}, fmt.Errorf("memory %q is registered as config", record.ID)
	}
	if !config && record.Kind != "memory" {
		return memoryLine{}, fmt.Errorf("config %q is registered as memory", record.ID)
	}
	allowed := map[string]bool{"key": true, "value": true, "classification": true}
	for key := range record.Body {
		if !allowed[key] {
			addUnique(&report.UnsupportedFields, fmt.Sprintf("%s/%s/%s", record.Kind, record.ID, key))
			return memoryLine{}, fmt.Errorf("%s %q has unsupported field %q", record.Kind, record.ID, key)
		}
	}
	if classification, present := record.Body["classification"]; !present {
		return memoryLine{}, fmt.Errorf("%s %q has no classification", record.Kind, record.ID)
	} else if _, ok := classification.(string); !ok {
		return memoryLine{}, fmt.Errorf("%s %q classification is not a string", record.Kind, record.ID)
	}
	if value, present := record.Body["value"]; !present || value == nil && record.Kind != "memory" {
		return memoryLine{}, fmt.Errorf("%s %q has null value", record.Kind, record.ID)
	} else if stringValue, ok := value.(string); ok && stringValue == "" {
		addUnique(&report.UnsupportedFields, fmt.Sprintf("%s/%s/value", record.Kind, record.ID))
		return memoryLine{}, fmt.Errorf("%s %q has an empty memory value; pinned Beads import would omit it", record.Kind, record.ID)
	} else if _, err := canonicalJSON(value); err != nil {
		return memoryLine{}, fmt.Errorf("%s %q value: %w", record.Kind, record.ID, err)
	}
	bodyKey := text(record.Body["key"])
	sourceKey := text(record.Source.Fields["key"])
	if bodyKey == "" || sourceKey == "" {
		return memoryLine{}, fmt.Errorf("%s %q has empty body or source key", record.Kind, record.ID)
	}
	if rawKey, present := record.Source.Fields["key"]; !present || rawKey == nil {
		return memoryLine{}, fmt.Errorf("%s %q has no source key", record.Kind, record.ID)
	} else if _, ok := rawKey.(string); !ok {
		return memoryLine{}, fmt.Errorf("%s %q source key is not a string", record.Kind, record.ID)
	}
	if record.Kind == "memory" {
		if record.Source.Table != "config" || len(record.Source.Fields) != 2 || len(record.Body) != 3 || !strings.HasPrefix(sourceKey, "kv.memory.") || record.ID != strings.TrimPrefix(sourceKey, "kv.memory.") || bodyKey != record.ID {
			return memoryLine{}, fmt.Errorf("memory %q identity does not match kv.memory source key", record.ID)
		}
		classification := text(record.Body["classification"])
		if classification != "public" && classification != "secret" {
			return memoryLine{}, fmt.Errorf("memory %q has unsupported classification %q", record.ID, text(record.Body["classification"]))
		}
		if sourceValue, ok := record.Source.Fields["value"]; !ok || !jsonEquivalent(sourceValue, valueFromBody(record.Body)) {
			return memoryLine{}, fmt.Errorf("memory %q source/body value mismatch", record.ID)
		}
		value := valueFromBody(record.Body)
		if classification == "secret" {
			ref, ok := value.(map[string]any)
			if !ok || len(ref) != 1 || ref["$secret"] != "config/"+sourceKey {
				return memoryLine{}, fmt.Errorf("memory %q secret value is not its canonical reference", record.ID)
			}
		}
		if err := validateSecretReferences(value); err != nil {
			return memoryLine{}, fmt.Errorf("memory %q value: %w", record.ID, err)
		}
		return memoryLine{RecordType: "memory", Key: record.ID, Value: value}, nil
	} else if record.Kind == "config" {
		if !config || record.ID != record.Source.Table+"/"+sourceKey || bodyKey != record.ID {
			return memoryLine{}, fmt.Errorf("config %q identity does not match source key", record.ID)
		}
		classification := text(record.Body["classification"])
		if classification != "public" && classification != "secret" {
			return memoryLine{}, fmt.Errorf("config %q has unsupported classification %q", record.ID, classification)
		}
		if isSecretKey(sourceKey) && classification != "secret" {
			return memoryLine{}, fmt.Errorf("config %q sensitive key requires secret classification", record.ID)
		}
		if sourceValue, ok := record.Source.Fields["value"]; !ok || !jsonEquivalent(sourceValue, valueFromBody(record.Body)) {
			return memoryLine{}, fmt.Errorf("config %q source/body value mismatch", record.ID)
		}
		if classification == "secret" {
			ref, ok := valueFromBody(record.Body).(map[string]any)
			if !ok || len(ref) != 1 || ref["$secret"] != record.Source.Table+"/"+sourceKey {
				return memoryLine{}, fmt.Errorf("config %q secret value is not its canonical reference", record.ID)
			}
		}
		if err := validateSecretReferences(valueFromBody(record.Body)); err != nil {
			return memoryLine{}, fmt.Errorf("config %q value: %w", record.ID, err)
		}
		addUnique(&report.ConfigReferences, record.ID)
		for _, reference := range secretReferenceNames(valueFromBody(record.Body)) {
			addUnique(&report.ConfigReferences, reference)
		}
		value, err := memoryValue(valueFromBody(record.Body), record)
		if err != nil {
			return memoryLine{}, err
		}
		return memoryLine{RecordType: "memory", Key: ConfigMemoryPrefix + sourceKey, Value: value}, nil
	}
	return memoryLine{}, fmt.Errorf("unsupported memory kind %q", record.Kind)
}

func unsupportedSourceFields(record Record, report *Report) []string {
	allowed := representedSourceFields(record.Kind)
	var reasons []string
	for name, value := range record.Source.Fields {
		if value == nil || allowed[name] {
			continue
		}
		if record.Kind == "bead" && archivableBeadSourceFields[name] {
			continue
		}
		field := fmt.Sprintf("%s/%s/source/%s", record.Kind, record.ID, name)
		addUnique(&report.UnsupportedFields, field)
		reasons = append(reasons, fmt.Sprintf("%s %q has unsupported source field %q", record.Kind, record.ID, name))
	}
	return reasons
}

// These source fields have no native Beads JSONL destination, but are safe to
// retain as an explicit reserved metadata archive on the issue. Keeping the
// value in the emitted line makes the loss reviewable and prevents a source
// field from disappearing silently. Workflow-bearing fields stay fail-closed
// because representing them as an ordinary issue would change semantics.
var archivableBeadSourceFields = map[string]bool{
	"actor": true, "compacted_at": true, "compacted_at_commit": true,
	"compaction_level": true, "content_hash": true, "current_revision": true,
	"estimated_minutes": true, "last_activity": true, "no_history": true,
	"original_size": true, "participation_generation": true, "ri" + "g": true,
	"sender": true, "source_repo": true, "storage_class": true,
}

func representedSourceFields(kind string) map[string]bool {
	fields := map[string]bool{}
	switch kind {
	case "bead":
		names := []string{
			"acceptance_criteria", "assignee", "close_reason", "closed_at", "closed_by_session",
			"created_at", "created_by", "defer_until", "description", "design", "due_at",
			"external_ref", "id", "issue_type", "metadata", "notes", "owner", "pinned",
			"priority", "source_system", "spec_id", "started_at", "status", "title", "updated_at",
		}
		names = append(names, dumpformat.OpaqueIssueFields()...)
		for _, name := range names {
			fields[name] = true
		}
	case "edge":
		for _, name := range []string{"created_at", "created_by", "depends_on_external", "depends_on_issue_id", "id", "issue_id", "metadata", "thread_id", "type"} {
			fields[name] = true
		}
		fields[dumpformat.EphemeralDependencyTargetField] = true
	case "comment":
		for _, name := range []string{"author", "created_at", "id", "issue_id", "text"} {
			fields[name] = true
		}
	case "memory", "config":
		fields["key"], fields["value"] = true, true
	}
	return fields
}

func archivedSourceFields(record Record, report *Report) map[string]any {
	if record.Kind != "bead" {
		return nil
	}
	allowed := representedSourceFields(record.Kind)
	archived := map[string]any{}
	for name, value := range record.Source.Fields {
		if value == nil || allowed[name] || !archivableBeadSourceFields[name] {
			continue
		}
		archived[name] = cloneValue(value)
		addUnique(&report.ArchivedSourceFields, fmt.Sprintf("bead/%s/source/%s", record.ID, name))
	}
	return archived
}

func isSecretKey(key string) bool {
	lower := strings.ToLower(key)
	for _, part := range []string{"api_key", "api-key", "apikey", "secret", "token", "password", "passwd", "credential", "private_key", "privatekey", "privkey", "dsn", "access_key"} {
		if strings.Contains(lower, part) {
			return true
		}
	}
	for _, part := range strings.FieldsFunc(lower, func(r rune) bool { return r == '.' || r == '_' || r == '-' }) {
		switch part {
		case "key", "keys", "apikey", "pwd", "pat", "auth", "bearer", "cert", "credential", "credentials", "secret", "token", "password":
			return true
		}
	}
	return false
}

func validateSecretReferences(value any) error {
	switch value := value.(type) {
	case map[string]any:
		if ref, ok := value["$secret"]; ok {
			name, ok := ref.(string)
			if !ok || name == "" || len(value) != 1 {
				return fmt.Errorf("invalid secret reference")
			}
			return nil
		}
		for key, item := range value {
			if isSecretKey(key) {
				ref, ok := item.(map[string]any)
				if !ok || len(ref) != 1 {
					return fmt.Errorf("sensitive value %q requires a secret reference", key)
				}
				if name, ok := ref["$secret"].(string); !ok || name == "" {
					return fmt.Errorf("sensitive value %q has an invalid secret reference", key)
				}
			}
			if err := validateSecretReferences(item); err != nil {
				return err
			}
		}
	case []any:
		for _, item := range value {
			if err := validateSecretReferences(item); err != nil {
				return err
			}
		}
	}
	return nil
}

func memoryValue(value any, record Record) (string, error) {
	if textValue, ok := value.(string); ok {
		return textValue, nil
	}
	encoded, err := canonicalJSON(value)
	if err != nil {
		return "", fmt.Errorf("%s %q value: %w", record.Kind, record.ID, err)
	}
	return encoded, nil
}

func valueFromBody(body map[string]any) any { return body["value"] }

func jsonEquivalent(left, right any) bool {
	a, errA := canonicalJSON(left)
	b, errB := canonicalJSON(right)
	return errA == nil && errB == nil && a == b
}

func secretReferenceNames(value any) []string {
	var names []string
	var walk func(any)
	walk = func(current any) {
		switch current := current.(type) {
		case map[string]any:
			if ref, ok := current["$secret"].(string); ok && len(current) == 1 && ref != "" {
				names = append(names, ref)
			}
			for _, item := range current {
				walk(item)
			}
		case []any:
			for _, item := range current {
				walk(item)
			}
		}
	}
	walk(value)
	return uniqueStrings(names)
}

func sortedRecords(records []Record) []Record {
	result := append([]Record(nil), records...)
	sort.Slice(result, func(i, j int) bool {
		if result[i].ID != result[j].ID {
			return result[i].ID < result[j].ID
		}
		if result[i].Project != result[j].Project {
			return result[i].Project < result[j].Project
		}
		return result[i].Source.Table < result[j].Source.Table
	})
	return result
}

func statusForCategory(category string) string {
	switch category {
	case "unstarted":
		return "open"
	case "started":
		return "in_progress"
	case "backlog":
		return "blocked"
	case "completed":
		return "closed"
	default:
		return ""
	}
}

func categoryForStatus(status string) string {
	switch status {
	case "open":
		return "unstarted"
	case "in_progress", "hook" + "ed":
		return "started"
	case "blocked", "deferred", "pinned", "frozen":
		return "backlog"
	case "closed":
		return "completed"
	default:
		return ""
	}
}

func text(value any) string {
	textValue, _ := value.(string)
	return textValue
}

func timestamp(value any) string {
	textValue := textOrEmpty(value)
	if textValue == "" {
		return ""
	}
	if parsed, err := time.Parse(time.RFC3339Nano, textValue); err != nil || parsed.IsZero() {
		return ""
	}
	return textValue
}

func textOrEmpty(value any) string {
	if value == nil {
		return ""
	}
	textValue, _ := value.(string)
	return textValue
}

func integer(value any) (int64, bool) {
	switch value := value.(type) {
	case int:
		return int64(value), true
	case int64:
		return value, true
	case uint64:
		if value > uint64(^uint64(0)>>1) {
			return 0, false
		}
		return int64(value), true
	case json.Number:
		parsed, err := value.Int64()
		return parsed, err == nil
	case float64:
		return int64(value), value == float64(int64(value))
	default:
		return 0, false
	}
}

// parsePinned accepts JSON booleans, exact integer JSON values 0 and 1,
// and the exact textual SQL representations 0 and 1 from the paired public
// source. Decimal/exponent spellings, whitespace and truthiness still refuse.
func parsePinned(value any) (bool, bool) {
	switch value := value.(type) {
	case nil:
		// Keep the existing direct-Convert behavior for an explicit null: it
		// remains the omitted false value in Beads JSONL. The canonical source
		// exporter emits the SQL default as numeric 0/1 instead.
		return false, true
	case bool:
		return value, true
	case json.Number:
		switch value.String() {
		case "0":
			return false, true
		case "1":
			return true, true
		}
	case string:
		switch value {
		case "0":
			return false, true
		case "1":
			return true, true
		}
	}
	return false, false
}

func truthy(value any) bool {
	switch value := value.(type) {
	case bool:
		return value
	case string:
		return value != "" && value != "0" && value != "false"
	case json.Number:
		return value != "0"
	case float64:
		return value != 0
	default:
		return value != nil
	}
}

func nonEmpty(value any) bool {
	switch value := value.(type) {
	case nil:
		return false
	case string:
		return value != ""
	case bool:
		return value
	case json.Number:
		return value != "0"
	case float64:
		return value != 0
	case []any:
		return len(value) > 0
	case map[string]any:
		return len(value) > 0
	default:
		return true
	}
}

func stringSlice(value any) ([]string, error) {
	if value == nil {
		return nil, nil
	}
	values, ok := value.([]any)
	if !ok {
		if typed, ok := value.([]string); ok {
			return append([]string(nil), typed...), nil
		}
		return nil, fmt.Errorf("expected string array")
	}
	result := make([]string, 0, len(values))
	for _, item := range values {
		textValue, ok := item.(string)
		if !ok || textValue == "" {
			return nil, fmt.Errorf("labels must contain non-empty strings")
		}
		result = append(result, textValue)
	}
	return result, nil
}

func uniqueStrings(values []string) []string {
	seen := make(map[string]bool, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		if !seen[value] {
			seen[value] = true
			result = append(result, value)
		}
	}
	sort.Strings(result)
	return result
}

func digestDump(dump Dump) string {
	copyDump := struct {
		Manifest Manifest
		Records  map[string][]Record
	}{Manifest: dump.Manifest, Records: make(map[string][]Record, len(dump.Records))}
	for kind, records := range dump.Records {
		copyDump.Records[kind] = sortedRecords(records)
	}
	data, err := json.Marshal(copyDump)
	if err != nil {
		return ""
	}
	return digestBytes(data)
}
