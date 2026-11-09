package dumpformat

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

const SourceHistoryFormat = "beads-dolt-images/1"
const SourceHistoryMember = "history/source.json"

var historyKinds = []string{"bead", "edge", "comment", "memory", "config"}

// SourceCommit retains Dolt attribution as opaque provenance, never a Plug
// principal. Commits are oldest first. A verified range admits only a complete
// linear chain; merges and missing parents remain archived, unverified evidence.
type SourceCommit struct {
	ID       string   `json:"id"`
	Parents  []string `json:"parents"`
	Metadata Row      `json:"metadata"`
}

type HistoryGap struct {
	First  uint64 `json:"first"`
	Last   uint64 `json:"last"`
	Reason string `json:"reason"`
}

// SourceHistoryFrame is a complete portable business image read at CommitID,
// including labels/relations/comments/config. Events are the complete source
// audit table at that commit, not an inferred grouping of individual SQL writes.
type SourceHistoryFrame struct {
	Sequence uint64   `json:"sequence"`
	CommitID string   `json:"commit_id"`
	Records  []Record `json:"records"`
	Events   []Row    `json:"events"`
}

type SourceHistoryRange struct {
	Status         string               `json:"status"`
	Reason         string               `json:"reason"`
	Complete       bool                 `json:"complete"`
	First          uint64               `json:"first"`
	Head           uint64               `json:"head"`
	Baseline       []Record             `json:"baseline"`
	BaselineEvents []Row                `json:"baseline_events"`
	Gaps           []HistoryGap         `json:"gaps"`
	Frames         []SourceHistoryFrame `json:"frames"`
}

// ProjectSourceHistory keeps one source history under the exact project
// namespace that owned it. A legacy archive has one top-level history and is
// lifted into this shape only when it is merged with another project.
// Project-scoped histories never imply an ordering between independent
// projects; the merge and restore seams establish a deterministic import
// order without rewriting any source commit or event ID.
type ProjectSourceHistory struct {
	SourceStoreID string             `json:"source_store_id,omitempty"`
	Project       string             `json:"project"`
	Commits       []SourceCommit     `json:"commits"`
	Events        []Row              `json:"events"`
	Range         SourceHistoryRange `json:"range"`
}

func (h ProjectSourceHistory) MarshalJSON() ([]byte, error) {
	if h.Commits == nil {
		h.Commits = []SourceCommit{}
	}
	if h.Events == nil {
		h.Events = []Row{}
	}
	for i := range h.Commits {
		if h.Commits[i].Parents == nil {
			h.Commits[i].Parents = []string{}
		}
	}
	if h.Range.Baseline == nil {
		h.Range.Baseline = []Record{}
	}
	if h.Range.BaselineEvents == nil {
		h.Range.BaselineEvents = []Row{}
	}
	if h.Range.Gaps == nil {
		h.Range.Gaps = []HistoryGap{}
	}
	if h.Range.Frames == nil {
		h.Range.Frames = []SourceHistoryFrame{}
	}
	for i := range h.Range.Frames {
		if h.Range.Frames[i].Records == nil {
			h.Range.Frames[i].Records = []Record{}
		}
		if h.Range.Frames[i].Events == nil {
			h.Range.Frames[i].Events = []Row{}
		}
	}
	type plain ProjectSourceHistory
	return json.Marshal(plain(h))
}

type SourceHistory struct {
	Format           string                 `json:"format"`
	Commits          []SourceCommit         `json:"commits,omitempty"`
	Events           []Row                  `json:"events,omitempty"`
	Range            SourceHistoryRange     `json:"range,omitempty"`
	ProjectHistories []ProjectSourceHistory `json:"project_histories,omitempty"`
}

// MarshalJSON omits the legacy single-history members for a project-scoped
// envelope. Encoding them as null would be rejected by the strict archive
// decoder and would make an otherwise valid merged history ambiguous.
func (h SourceHistory) MarshalJSON() ([]byte, error) {
	if len(h.ProjectHistories) != 0 {
		return json.Marshal(struct {
			Format           string                 `json:"format"`
			ProjectHistories []ProjectSourceHistory `json:"project_histories"`
		}{Format: h.Format, ProjectHistories: h.ProjectHistories})
	}
	type plain SourceHistory
	return json.Marshal(plain(h))
}

func (h *SourceHistory) UnmarshalJSON(data []byte) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	if _, scoped := fields["project_histories"]; scoped {
		var wire struct {
			Format           string                 `json:"format"`
			ProjectHistories []ProjectSourceHistory `json:"project_histories"`
		}
		if err := decodeJSON(data, &wire); err != nil {
			return err
		}
		*h = SourceHistory{Format: wire.Format, ProjectHistories: wire.ProjectHistories}
		return nil
	}
	type plain SourceHistory
	var legacy plain
	if err := decodeJSON(data, &legacy); err != nil {
		return err
	}
	*h = SourceHistory(legacy)
	return nil
}

// HistoryMergeRefusal is returned when independently captured source
// histories cannot be combined without inventing an order or changing an
// identity. The named project and ID make the refusal actionable to the
// trusted host and keep a rejected merge atomic.
type HistoryMergeRefusal struct {
	Kind         string
	Project      string
	OtherProject string
	ID           string
	Reason       string
}

func (e HistoryMergeRefusal) Error() string {
	parts := []string{"offline history merge refused"}
	if e.Kind != "" {
		parts = append(parts, e.Kind)
	}
	if e.Project != "" {
		parts = append(parts, "project="+e.Project)
	}
	if e.OtherProject != "" {
		parts = append(parts, "other_project="+e.OtherProject)
	}
	if e.ID != "" {
		parts = append(parts, "id="+e.ID)
	}
	if e.Reason != "" {
		parts = append(parts, e.Reason)
	}
	return strings.Join(parts, ": ")
}

// ErrSourceHistory distinguishes an invalid replay proof from an unavailable
// capture. Trusted restore hosts map Reason to their typed history refusal.
type ErrSourceHistory struct{ Reason string }

func (e ErrSourceHistory) Error() string { return "invalid source history: " + e.Reason }

func (a *Archive) SourceHistory() (SourceHistory, bool) {
	if a == nil || a.history == nil {
		return SourceHistory{}, false
	}
	var h SourceHistory
	// The member was shape-checked by Read. Decode again to detach every nested
	// row while preserving JSON numbers and nulls.
	_ = decodeJSON(a.members[SourceHistoryMember], &h)
	return h, true
}

// HistoryImage exposes only an image already checked by Read. Sequence First-1
// selects the proven partial baseline; no arbitrary records can be projected.
func (a *Archive) HistoryImage(sequence uint64) (*Archive, error) {
	h, ok := a.SourceHistory()
	if !ok {
		return nil, ErrSourceHistory{Reason: "available-unverified"}
	}
	if len(h.ProjectHistories) != 0 {
		if len(h.ProjectHistories) != 1 {
			return nil, ErrSourceHistory{Reason: "project-scoped history requires a project"}
		}
		return a.historyImageForProject(h.ProjectHistories[0], sequence)
	}
	return a.historyImageForProject(ProjectSourceHistory{Commits: h.Commits, Events: h.Events, Range: h.Range}, sequence)
}

// HistoryImageForProject returns a verified image from one project namespace.
// It is the only image accessor that can select a member of a merged history;
// callers must name the project instead of accidentally combining independent
// source revisions.
func (a *Archive) HistoryImageForProject(project string, sequence uint64) (*Archive, error) {
	h, ok := a.SourceHistory()
	if !ok {
		return nil, ErrSourceHistory{Reason: "available-unverified"}
	}
	if len(h.ProjectHistories) == 0 {
		if len(a.Manifest().Projects) != 1 || a.Manifest().Projects[0].Prefix != project {
			return nil, ErrSourceHistory{Reason: "project-scoped history unavailable"}
		}
		return a.historyImageForProject(ProjectSourceHistory{Project: project, Commits: h.Commits, Events: h.Events, Range: h.Range}, sequence)
	}
	for _, scoped := range h.ProjectHistories {
		if scoped.Project == project {
			return a.historyImageForProject(scoped, sequence)
		}
	}
	return nil, ErrSourceHistory{Reason: "project history unavailable: " + project}
}

func (a *Archive) historyImageForProject(scoped ProjectSourceHistory, sequence uint64) (*Archive, error) {
	if scoped.Range.Status != "verified" {
		return nil, ErrSourceHistory{Reason: "available-unverified"}
	}
	var rows []Record
	if scoped.Range.First > 1 && sequence == scoped.Range.First-1 {
		rows = scoped.Range.Baseline
	} else if sequence >= scoped.Range.First && sequence <= scoped.Range.Head {
		rows = scoped.Range.Frames[sequence-scoped.Range.First].Records
	} else {
		return nil, ErrSourceHistory{Reason: "non-contiguous"}
	}
	image := &Archive{sha256: a.sha256, manifest: a.Manifest(), members: a.members, byKind: map[string][]Record{}}
	if scoped.Project != "" {
		image.manifest = scopedHistoryManifest(image.manifest, scoped.Project)
	}
	if scoped.SourceStoreID != "" {
		image.manifest.StoreID = scoped.SourceStoreID
		image.members = make(map[string][]byte, len(a.members))
		for path, data := range a.members {
			image.members[path] = data
		}
		config, err := sourceConfigForIdentity(a.members["config/sources.json"], SourceIdentity{StoreID: scoped.SourceStoreID, Project: scoped.Project})
		if err != nil {
			return nil, err
		}
		image.members["config/sources.json"] = config
		image.manifest.Files["config/sources.json"] = File{SHA256: digest(config), Size: int64(len(config))}
	}
	for _, r := range rows {
		image.byKind[r.Kind] = append(image.byKind[r.Kind], r)
	}
	// Read-only census/provenance is retained at restore time; it is never used
	// to fill missing historical business fields.
	for _, record := range a.Records("beads-archive") {
		if record.Source.Identity == nil || record.Source.Identity.Project == scoped.Project {
			image.byKind["beads-archive"] = append(image.byKind["beads-archive"], record)
		}
	}
	return image, nil
}

func validateSourceHistory(a *Archive) error {
	raw, present := a.members[SourceHistoryMember]
	if !present {
		if a.manifest.History == "verified" {
			return ErrSourceHistory{Reason: "available-unverified"}
		}
		return nil
	}
	var h SourceHistory
	if err := decodeJSON(raw, &h); err != nil {
		return fmt.Errorf("source history shape: %w", err)
	}
	fail := func(reason string) error { return ErrSourceHistory{Reason: reason} }
	if h.Format != SourceHistoryFormat || len(h.Commits) > 100000 || len(h.Range.Frames) > 100000 || len(h.ProjectHistories) > 100000 {
		return fail("unmappable")
	}
	scopes, err := sourceHistoryProjects(h, a.manifest)
	if err != nil {
		return err
	}
	if len(scopes) == 0 {
		return fail("unmappable")
	}
	seenProjects := map[string]bool{}
	seenCommits := map[string]string{}
	seenEvents := map[string]string{}
	namespaced := a.manifest.SourceIdentityContract == SourceIdentityContract
	if namespaced && len(scopes) != len(a.manifest.Projects) {
		return fail("missing source project history")
	}
	allVerified := true
	for _, scoped := range scopes {
		if scoped.Project == "" || seenProjects[scoped.Project] {
			return fail("unmappable")
		}
		seenProjects[scoped.Project] = true
		if !projectInManifest(scoped.Project, a.manifest.Projects) {
			return fail("unmappable")
		}
		if namespaced {
			identity := SourceIdentity{StoreID: scoped.SourceStoreID, Project: scoped.Project}
			if !validSourceIdentity(identity) {
				return fail("missing source store/project identity")
			}
			if err := requireCompleteSourceHistory(identity, scoped.Range); err != nil {
				return err
			}
			for _, rows := range a.byKind {
				for _, record := range rows {
					if record.Source.Identity.Project == scoped.Project && *record.Source.Identity != identity {
						return fail("history source store differs from record source")
					}
				}
			}
			seenCommits, seenEvents = map[string]string{}, map[string]string{}
		} else if scoped.SourceStoreID != "" {
			return fail("source store identity requires explicit contract")
		}
		if err := validateHistoryScope(a, scoped, seenCommits, seenEvents); err != nil {
			return err
		}
		if scoped.Range.Status != "verified" || !scoped.Range.Complete {
			allVerified = false
		}
	}
	if allVerified && len(scopes) > 1 {
		// Global rows retain Project="". The explicit project history envelope
		// owns their membership, independently of user-chosen config keys.
		witnessed := map[string]bool{}
		for _, scope := range scopes {
			for _, record := range scope.Range.Frames[len(scope.Range.Frames)-1].Records {
				witnessed[record.Kind+"\x00"+recordKey(record)] = true
			}
		}
		for _, kind := range historyKinds {
			for _, record := range a.byKind[kind] {
				if record.Project == "" && !witnessed[record.Kind+"\x00"+recordKey(record)] {
					return fail("unmappable")
				}
			}
		}
	}
	if allVerified && a.manifest.History != "verified" || !allVerified && a.manifest.History != "available-unverified" {
		return fail("non-contiguous")
	}
	a.history = &h
	return nil
}

func sourceHistoryProjects(h SourceHistory, manifest Manifest) ([]ProjectSourceHistory, error) {
	if len(h.ProjectHistories) != 0 {
		if len(h.Commits) != 0 || len(h.Events) != 0 || h.Range.Status != "" || len(h.Range.Frames) != 0 || len(h.Range.Baseline) != 0 {
			return nil, ErrSourceHistory{Reason: "ambiguous project-scoped history"}
		}
		projects := append([]ProjectSourceHistory(nil), h.ProjectHistories...)
		sort.Slice(projects, func(i, j int) bool { return projects[i].Project < projects[j].Project })
		return projects, nil
	}
	if len(manifest.Projects) != 1 {
		return nil, ErrSourceHistory{Reason: "legacy history is not project-scoped"}
	}
	return []ProjectSourceHistory{{Project: manifest.Projects[0].Prefix, Commits: h.Commits, Events: h.Events, Range: h.Range}}, nil
}

func projectInManifest(project string, projects []Project) bool {
	for _, candidate := range projects {
		if candidate.Prefix == project {
			return true
		}
	}
	return false
}

func validateHistoryScope(a *Archive, scoped ProjectSourceHistory, seenCommits map[string]string, seenEvents map[string]string) error {
	fail := func(reason string) error { return ErrSourceHistory{Reason: reason} }
	namespaced := a.manifest.SourceIdentityContract == SourceIdentityContract
	manifest := a.manifest
	if namespaced {
		manifest = scopedHistoryManifest(manifest, scoped.Project)
	}
	checkImage := func(rows []Record) error {
		for _, record := range rows {
			if namespaced {
				identity := SourceIdentity{StoreID: scoped.SourceStoreID, Project: scoped.Project}
				if record.Source.Identity == nil || *record.Source.Identity != identity || record.Project != "" && record.Project != scoped.Project {
					return fail("historical record source identity mismatch")
				}
			} else if record.Source.Identity != nil {
				return fail("historical source identity requires explicit contract")
			}
		}
		return validateHistoryImage(rows, manifest)
	}
	for _, event := range scoped.Events {
		if err := validateSecretReferences(map[string]any(event)); err != nil {
			return err
		}
		if id := SourceEventID(event); id != "" {
			if previous, ok := seenEvents[id]; ok && previous != scoped.Project {
				return HistoryMergeRefusal{Kind: "event identity", Project: scoped.Project, OtherProject: previous, ID: id, Reason: "source event is shared by project histories"}
			}
			seenEvents[id] = scoped.Project
		}
	}
	seenLocal := map[string]bool{}
	for _, c := range scoped.Commits {
		if !validCommitID(c.ID) || seenLocal[c.ID] || c.Metadata == nil {
			return fail("unmappable")
		}
		seenLocal[c.ID] = true
		if previous, ok := seenCommits[c.ID]; ok && previous != scoped.Project {
			return HistoryMergeRefusal{Kind: "commit identity", Project: scoped.Project, OtherProject: previous, ID: c.ID, Reason: "source revisions cannot be ordered across projects"}
		}
		seenCommits[c.ID] = scoped.Project
		if err := validateSecretReferences(map[string]any(c.Metadata)); err != nil {
			return err
		}
		for _, p := range c.Parents {
			if !validCommitID(p) {
				return fail("unmappable")
			}
		}
	}
	r := scoped.Range
	if r.Status == "unverified" {
		if strings.TrimSpace(r.Reason) == "" || r.Complete || r.First != 0 || r.Head != 0 || len(r.Baseline)+len(r.BaselineEvents)+len(r.Frames)+len(r.Gaps) != 0 {
			return fail("available-unverified")
		}
		if !sameEventImage(currentEventsForProject(a, scoped.Project), scoped.Events) {
			return fail("unmappable")
		}
		return nil
	}
	if r.Status != "verified" || r.Reason != "" || r.First == 0 || r.Head != uint64(len(scoped.Commits)) || r.Head < r.First || r.Head-r.First+1 != uint64(len(r.Frames)) {
		return fail("non-contiguous")
	}
	for i, c := range scoped.Commits {
		if i == 0 && len(c.Parents) != 0 || i > 0 && (len(c.Parents) != 1 || c.Parents[0] != scoped.Commits[i-1].ID) {
			return fail("non-contiguous")
		}
	}
	if r.First == 1 {
		if !r.Complete || len(r.Baseline)+len(r.BaselineEvents)+len(r.Gaps) != 0 {
			return fail("non-contiguous")
		}
	} else {
		if r.Complete {
			return fail("unmappable")
		}
		if err := checkImage(r.Baseline); err != nil {
			return fail("unmappable")
		}
		next := uint64(1)
		for _, gap := range r.Gaps {
			if gap.First != next || gap.Last < gap.First || gap.Last >= r.First || strings.TrimSpace(gap.Reason) == "" {
				return fail("non-contiguous")
			}
			next = gap.Last + 1
		}
		if next != r.First {
			return fail("non-contiguous")
		}
	}
	previous := r.BaselineEvents
	for i, frame := range r.Frames {
		if frame.Sequence != r.First+uint64(i) || frame.CommitID != scoped.Commits[frame.Sequence-1].ID {
			return fail("non-contiguous")
		}
		if err := checkImage(frame.Records); err != nil {
			return fail("unmappable")
		}
		if !extendsEvents(previous, frame.Events) {
			return fail("unmappable")
		}
		previous = frame.Events
	}
	if len(r.Frames) == 0 {
		return fail("unmappable")
	}
	globalMembership := map[string]bool{}
	for _, record := range r.Frames[len(r.Frames)-1].Records {
		if record.Project != "" && record.Project != scoped.Project {
			return fail("unmappable")
		}
		globalMembership[record.Kind+"\x00"+recordKey(record)] = true
	}
	current := []Record{}
	for _, kind := range historyKinds {
		for _, record := range a.byKind[kind] {
			if namespaced && record.Source.Identity.Project != scoped.Project {
				continue
			}
			if record.Project == scoped.Project || record.Project == "" && (len(a.manifest.Projects) == 1 || globalMembership[record.Kind+"\x00"+recordKey(record)]) {
				current = append(current, record)
			}
		}
	}
	if !sameRecordImage(current, r.Frames[len(r.Frames)-1].Records) {
		return fail("unmappable")
	}
	if !sameEventImage(scoped.Events, previous) {
		return fail("unmappable")
	}
	return nil
}

func currentEventsForProject(a *Archive, project string) []Row {
	beadProjects := map[string]string{}
	for _, record := range a.byKind["bead"] {
		beadProjects[record.ID] = record.Project
	}
	events := []Row{}
	for _, record := range a.byKind["beads-archive"] {
		if record.Source.Table != "events" {
			continue
		}
		if record.Source.Identity != nil && record.Source.Identity.Project != project {
			continue
		}
		issueID := stringValue(record.Source.Fields["issue_id"])
		if beadProjects[issueID] == project || beadProjects[issueID] == "" && len(a.manifest.Projects) == 1 {
			events = append(events, record.Source.Fields)
		}
	}
	return events
}

func validCommitID(id string) bool {
	if len(id) != 32 {
		return false
	}
	for _, c := range id {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'v') {
			return false
		}
	}
	return true
}

// A legacy image is already scoped by its enclosing source history. Its raw
// IDs are independent across kinds and physical tables, just like head rows.
func historyRecordKey(r Record) SourceRecordKey {
	if key, ok := r.SourceKey(); ok {
		return key
	}
	return SourceRecordKey{Project: r.Project, Kind: r.Kind, Table: r.Source.Table, ID: r.ID}
}

func lessHistoryRecord(a, b Record) bool {
	x, y := historyRecordKey(a), historyRecordKey(b)
	left := [...]string{x.StoreID, x.Project, x.Kind, x.Table, x.ID}
	right := [...]string{y.StoreID, y.Project, y.Kind, y.Table, y.ID}
	for i := range left {
		if left[i] != right[i] {
			return left[i] < right[i]
		}
	}
	return false
}

func validateHistoryImage(rows []Record, manifest Manifest) error {
	seen, beads := map[SourceRecordKey]bool{}, map[string]string{}
	counts := map[string]int{}
	beadRows := []Record{}
	for _, r := range rows {
		if r.Kind == "beads-archive" || !knownKinds[r.Kind] || seen[historyRecordKey(r)] {
			return fmt.Errorf("duplicate or nonportable history record")
		}
		seen[historyRecordKey(r)] = true
		if err := validateVersionedRecord(r, manifest.Projects, counts, nil, manifest.EdgeVersion()); err != nil {
			return err
		}
		if r.Kind == "bead" {
			if beads[r.ID] != "" {
				return fmt.Errorf("duplicate historical bead")
			}
			beads[r.ID] = r.Project
			beadRows = append(beadRows, r)
		}
	}
	for _, r := range rows {
		if r.Kind == "edge" || r.Kind == "comment" {
			if beads[stringValue(r.Source.Fields["issue_id"])] != r.Project || r.Kind == "edge" && r.Body["target_kind"] == "depends_on_issue_id" && beads[stringValue(r.Body["to"])] == "" {
				return fmt.Errorf("dangling historical relation")
			}
		}
	}
	if manifest.StatusCategoryContract == StatusCategoryContract {
		// Head-only filesystem/YAML and archived catalog census cannot witness
		// an older image. Require its own complete stored declarations; a
		// missing or changed historical catalog remains unmappable.
		byKind := map[string][]Record{}
		for _, record := range rows {
			byKind[record.Kind] = append(byKind[record.Kind], record)
		}
		if err := validateNativeStatusCatalogConservation(manifest, byKind, []byte("[]")); err != nil {
			return err
		}
	}
	return validateIssueOntology(manifest.Ontology, beadRows, false)
}

func sameRecordImage(a, b []Record) bool {
	canonical := func(rows []Record) []Record {
		copy := append([]Record{}, rows...)
		sort.Slice(copy, func(i, j int) bool { return lessHistoryRecord(copy[i], copy[j]) })
		return copy
	}
	return equalJSON(canonical(a), canonical(b))
}

// SourceEventID retains source IDs exactly. Numeric SQL identifiers are
// lossless decimal strings; an event is not a verified principal or revision.
func SourceEventID(r Row) string {
	switch id := r["id"].(type) {
	case string:
		return id
	case json.Number:
		return string(id)
	default:
		return ""
	}
}

func eventImage(rows []Row) (map[string]Row, bool) {
	result := map[string]Row{}
	for _, row := range rows {
		id := SourceEventID(row)
		if id == "" || result[id] != nil || len(row) != len(strings.Fields(tableColumns["events"])) {
			return nil, false
		}
		for _, name := range strings.Fields(tableColumns["events"]) {
			if _, ok := row[name]; !ok {
				return nil, false
			}
		}
		if validateSecretReferences(map[string]any(row)) != nil {
			return nil, false
		}
		result[id] = row
	}
	return result, true
}

func sameEventImage(a, b []Row) bool {
	left, ok := eventImage(a)
	right, otherOK := eventImage(b)
	return ok && otherOK && equalJSON(left, right)
}

func extendsEvents(before, after []Row) bool {
	left, ok := eventImage(before)
	right, otherOK := eventImage(after)
	if !ok || !otherOK {
		return false
	}
	for id, row := range left {
		if !equalJSON(row, right[id]) {
			return false
		}
	}
	return true
}
