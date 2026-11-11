package plugdump

import (
	"archive/tar"
	"bytes"
	"context"
	"fmt"
	"io"
	"sort"
	"strings"
)

// SourceCommitLog is a driver read of the actual reachable graph and working
// set. Clean is measured by the source reader, never copied from Inventory.
type SourceCommitLog struct {
	Head       string
	Clean      bool
	Commits    []SourceCommit
	Boundaries []NativeHistoryBoundary
	// Actual immutable parent order, separately authenticated by the physical
	// reader. Legacy sorted parent census behavior stays unchanged.
	OrderedParents map[string][]string `json:"-"`
	// Diagnostics are detached driver errors for locally observed boundaries.
	// They are never part of archive JSON or authenticated history metadata.
	BoundaryDiagnostics map[string]error `json:"-"`
}

// SourceHistoryReader is optional. Every image is a complete data-only census
// at the named source commit, read inside the same frozen scope as Source.
// Neither a database nor a transaction handle escapes through this port.
type SourceHistoryReader interface {
	ReadSourceCommits(context.Context) (SourceCommitLog, error)
	ReadSourceImage(context.Context, string) (map[string]Table, error)
}

type ErrSourceHistoryUnavailable struct {
	Reason              string
	BoundaryReason      string
	SchemaVersion       *uint64
	OrdinalFromHead     uint64
	VerifiedCommitCount *uint64
	SQLCodes            []uint16
	Cause               error
}

func (e ErrSourceHistoryUnavailable) Error() string { return "source history unavailable: " + e.Reason }
func (e ErrSourceHistoryUnavailable) Unwrap() error { return e.Cause }

type frozenHistoryTables map[string]Table

func (s frozenHistoryTables) Tables(context.Context) ([]string, error) { return sortedNames(s), nil }
func (s frozenHistoryTables) ReadTable(_ context.Context, name string) (Table, error) {
	return s[name], nil
}

func captureSourceHistory(ctx context.Context, src Source, inventory Inventory, current map[string][]Record, tables map[string]Table) (*SourceHistory, error) {
	port, ok := src.(SourceHistoryReader)
	if !ok {
		return nil, nil
	}
	log, err := port.ReadSourceCommits(ctx)
	if err != nil {
		return nil, err
	}
	h := &SourceHistory{Format: SourceHistoryFormat, Commits: log.Commits, Events: canonicalSourceEvents(tables["events"].Rows)}
	unverified := func(reason string) (*SourceHistory, error) {
		h.Range = SourceHistoryRange{Status: "unverified", Reason: reason, Baseline: []Record{}, BaselineEvents: []Row{}, Gaps: []HistoryGap{}, Frames: []SourceHistoryFrame{}}
		return h, nil
	}
	if len(log.Commits) == 0 || len(log.Commits) > 10000 {
		return nil, ErrSourceHistoryUnavailable{Reason: "commit-census-limit-or-empty"}
	}
	for _, c := range log.Commits {
		if !validCommitID(c.ID) || c.Metadata == nil {
			return nil, ErrSourceHistoryUnavailable{Reason: "invalid-commit-census"}
		}
		if err := validateSecretReferences(map[string]any(c.Metadata)); err != nil {
			return nil, err
		}
	}
	if len(log.Boundaries) > 0 {
		ordered, err := historyCommitOrder(log)
		if err != nil {
			return nil, err
		}
		final, err := port.ReadSourceCommits(ctx)
		if err != nil {
			return nil, err
		}
		if !log.Clean || !equalJSON(log, final) {
			return nil, ErrSourceHistoryUnavailable{Reason: "source-census-changed"}
		}
		h.Commits = ordered
		return unverified("native-history-boundary")
	}
	chain, ok := linearSourceCommits(log)
	if !ok {
		return unverified("non-contiguous")
	}
	h.Commits = chain
	if !log.Clean {
		return unverified("working-set-not-clean")
	}
	frames := make([]SourceHistoryFrame, len(chain))
	reasons := make([]string, len(chain))
	cut := 0
	total := 0
	headFailureReason := "head-image-not-proven"
	for i, c := range chain {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		image, readErr := port.ReadSourceImage(ctx, c.ID)
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if readErr != nil {
			reasons[i], cut = "unavailable", i+1
			continue
		}
		var records []Record
		var events []Row
		var imageErr error
		if i == 0 && image != nil && len(image) == 0 {
			// The complete table census at the proven root succeeded and was
			// empty. No schema or business/event rows existed to map. A nil
			// image, missing read, non-root image or partial schema still goes
			// through the ordinary closed-world refusal path below.
			records, events = []Record{}, []Row{}
		} else {
			records, events, imageErr = portableSourceImage(ctx, image, inventory)
		}
		if imageErr != nil {
			reasons[i], cut = "unmappable", i+1
			if i == len(chain)-1 && image != nil {
				// A successful census may prove that the native commit lacks
				// families required by the portable history contract. Name that
				// evidence gap; never fill it from the working set or with empty
				// tables. Read failures and other mismatches remain unlocalized.
				missing := []string{}
				for name := range tableColumns {
					if _, present := image[name]; !present {
						missing = append(missing, name)
					}
				}
				if inventory.SourceFamilyContract == SourceFamilyContract {
					headFailureReason = "portable-image-not-supported; native history is separately retained"
				} else if len(missing) > 0 {
					sort.Strings(missing)
					headFailureReason = "head-image-missing-families:" + strings.Join(missing, ",")
				}
			}
			continue
		}
		frame := SourceHistoryFrame{Sequence: uint64(i + 1), CommitID: c.ID, Records: records, Events: events}
		data, marshalErr := marshal(frame)
		if marshalErr != nil {
			return nil, marshalErr
		}
		total += len(data)
		if total > MaxArchiveBytes-(4<<20) {
			version, _ := commitCursor(image, "schema_migrations")
			return nil, historyFailureCoordinates(ErrSourceHistoryUnavailable{Reason: "image-census-limit"}, historyCommitOrdinal(log, c.ID), version)
		}
		frames[i] = frame
		if i > 0 && frames[i-1].CommitID != "" && !extendsEvents(frames[i-1].Events, events) {
			// The complete new image can be a later baseline, but this audit
			// transition cannot become a replayable source unit.
			reasons[i], cut = "unmappable", i
		}
	}
	// Immutable commit images do not prove that the surrounding source census
	// stayed fixed. Re-read it in the same caller-owned scope before admitting
	// a range, including for sources whose transaction cannot pin system tables.
	finalLog, err := port.ReadSourceCommits(ctx)
	if err != nil {
		return nil, err
	}
	finalChain, stable := linearSourceCommits(finalLog)
	if !stable || !finalLog.Clean || finalLog.Head != log.Head || !equalJSON(chain, finalChain) {
		return unverified("source-census-changed")
	}
	last := len(chain) - 1
	currentImage := []Record{}
	for _, kind := range historyKinds {
		currentImage = append(currentImage, current[kind]...)
	}
	if frames[last].CommitID == "" || !sameRecordImage(currentImage, frames[last].Records) || !sameEventImage(h.Events, frames[last].Events) {
		return unverified(headFailureReason)
	}
	if cut >= len(chain)-1 && cut != 0 {
		return unverified("verified-baseline-unavailable")
	}
	first := uint64(1)
	baseline, baselineEvents := []Record{}, []Row{}
	gaps := []HistoryGap{}
	if cut > 0 {
		first = uint64(cut + 2)
		baseline, baselineEvents = frames[cut].Records, frames[cut].Events
		for i := 0; i <= cut; i++ {
			reason := reasons[i]
			if reason == "" {
				reason = "not-retained"
			}
			sequence := uint64(i + 1)
			if len(gaps) > 0 && gaps[len(gaps)-1].Reason == reason {
				gaps[len(gaps)-1].Last = sequence
			} else {
				gaps = append(gaps, HistoryGap{First: sequence, Last: sequence, Reason: reason})
			}
		}
	}
	h.Range = SourceHistoryRange{Status: "verified", Reason: "", Complete: cut == 0, First: first, Head: uint64(len(chain)), Baseline: baseline, BaselineEvents: baselineEvents, Gaps: gaps, Frames: frames[first-1:]}
	return h, nil
}

func linearSourceCommits(log SourceCommitLog) ([]SourceCommit, bool) {
	byID := map[string]SourceCommit{}
	for _, c := range log.Commits {
		if _, ok := byID[c.ID]; ok {
			return nil, false
		}
		byID[c.ID] = c
	}
	chain := []SourceCommit{}
	seen := map[string]bool{}
	id := log.Head
	for id != "" {
		c, ok := byID[id]
		if !ok || seen[id] || len(c.Parents) > 1 {
			return nil, false
		}
		seen[id] = true
		chain = append(chain, c)
		id = ""
		if len(c.Parents) == 1 {
			id = c.Parents[0]
		}
	}
	if len(chain) != len(log.Commits) {
		return nil, false
	}
	for i, j := 0, len(chain)-1; i < j; i, j = i+1, j-1 {
		chain[i], chain[j] = chain[j], chain[i]
	}
	return chain, true
}

// Reuse the current closed-world mapper for each independently read image.
// It cannot borrow a missing business field or row from the current head.
func portableSourceImage(ctx context.Context, tables map[string]Table, inventory Inventory) ([]Record, []Row, error) {
	if inventory.StatusCategoryContract == StatusCategoryContract {
		// Filesystem YAML is captured only at export head. It cannot witness
		// the category of an older source image. The actual SQL catalog/config
		// at this commit must independently establish the entire head catalog;
		// otherwise the portable format has no per-image catalog to carry it.
		stored := map[string]any{}
		for _, row := range tables["config"].Rows {
			key := text(row, "key")
			if key == "status.custom" || key == "types.custom" {
				stored[key] = row["value"]
			}
		}
		if err := validateProjectStatusCatalogs(inventory.Projects, tables["custom_statuses"].Rows, tables["custom_types"].Rows, stored, nil); err != nil {
			return nil, nil, fmt.Errorf("historical SQL status catalog not proven: %w", err)
		}
	}
	// A head directive may be unused before that type existed. Select only
	// declarations used by this image; the same mapper still requires exactly
	// one correctly scoped declaration for every custom historical issue type.
	issues := []Record{}
	projects := mapping{projects: inventory.Projects}
	for _, row := range tables["issues"].Rows {
		id := text(row, "id")
		project, err := projects.project(id)
		if err != nil {
			return nil, nil, err
		}
		issues = append(issues, Record{ID: id, Project: project.Prefix, Source: Provenance{Table: "issues", Fields: row}})
	}
	inventory.Ontology = applicableHistoryOntology(inventory.Ontology, issues)
	// This is a portable projection of an already pinned image, not a second
	// native capture. The frozen reader cannot enumerate an independent native
	// commit census. Keep every portable/schema/status check enabled below.
	inventory.SourceFamilyContract = ""
	inventory.History = "unavailable"
	if len(tables["issue_versions"].Rows)+len(tables["issue_snapshots"].Rows)+len(tables["compaction_snapshots"].Rows) > 0 {
		inventory.History = "available-unverified"
	}
	data, err := Build(ctx, frozenHistoryTables(tables), inventory)
	if err != nil {
		return nil, nil, err
	}
	reader := tar.NewReader(bytes.NewReader(data))
	records := []Record{}
	var manifest Manifest
	for {
		header, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, nil, err
		}
		if header.Name == "manifest.json" {
			body, err := io.ReadAll(reader)
			if err != nil {
				return nil, nil, err
			}
			if err := decodeJSON(body, &manifest); err != nil {
				return nil, nil, err
			}
			continue
		}
		portable := false
		for _, kind := range historyKinds {
			if header.Name == "records/"+kind+".ndjson" {
				portable = true
				break
			}
		}
		if !portable {
			continue
		}
		body, err := io.ReadAll(reader)
		if err != nil {
			return nil, nil, err
		}
		for _, line := range bytes.Split(bytes.TrimSpace(body), []byte{'\n'}) {
			var record Record
			if err := decodeJSON(line, &record); err != nil {
				return nil, nil, err
			}
			records = append(records, record)
		}
	}
	sort.Slice(records, func(i, j int) bool { return historyRecordKey(records[i]) < historyRecordKey(records[j]) })
	if inventory.SourcePlaneAddressContract == SourcePlaneAddressContract {
		// Source-plane capture does not extend the separate portable history
		// contract. Keep every raw head row, but mark an unrepresentable commit
		// image unmappable rather than publishing a verified partial history.
		// Build derives allocator counters from this image's actual rows. The
		// native inventory has not derived them yet; using it here would reject
		// valid sequential IDs and dotted children before comparing the head.
		if err := validateHistoryImage(records, manifest); err != nil {
			return nil, nil, fmt.Errorf("source image is outside the portable history contract: %w", err)
		}
	}
	if _, ok := eventImage(tables["events"].Rows); !ok {
		return nil, nil, fmt.Errorf("invalid source audit image")
	}
	return records, canonicalSourceEvents(tables["events"].Rows), nil
}

// Event identity gives a deterministic wire order without implying chronology.
// Sort a copied slice so the caller's source images and stored values stay intact.
func canonicalSourceEvents(rows []Row) []Row {
	copy := append([]Row{}, rows...)
	sort.Slice(copy, func(i, j int) bool { return SourceEventID(copy[i]) < SourceEventID(copy[j]) })
	return copy
}

func applicableHistoryOntology(directives []OntologyDirective, issues []Record) []OntologyDirective {
	used := []OntologyDirective{}
	for _, directive := range directives {
		for _, issue := range issues {
			if directive.Target.Project == issue.Project && directive.Target.Type == text(issue.Source.Fields, "issue_type") && (directive.Target.ID == "" || directive.Target.ID == issue.ID) {
				used = append(used, directive)
				break
			}
		}
	}
	return used
}
