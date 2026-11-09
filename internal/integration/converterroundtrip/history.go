package converterroundtrip

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// BindHistoryImport is a trusted-host preparation step after admission. It
// binds the exact converted history and actual restore time before opening a
// target transaction. Existing RestoreImport entry points consume its returned
// profile unchanged. The source archive alone grants no restore authority.
func BindHistoryImport(ctx context.Context, input io.Reader, profile driver.RestoreImportProfile, when time.Time) (driver.RestoreImportProfile, error) {
	if err := ctx.Err(); err != nil {
		return driver.RestoreImportProfile{}, err
	}
	p, err := profile.Detached()
	if err != nil {
		return p, err
	}
	archive, err := dumpformat.Read(input)
	if err != nil {
		return p, historyReadError(p.HistoryMode, err)
	}
	if p.HistoryMode == driver.RestoreHistoryModeSnapshot || when.IsZero() || p.SourceStoreID != driver.StoreID(archive.Manifest().StoreID) || p.ArchiveDigest != archive.SHA256() {
		return p, fmt.Errorf("history binding requires admitted source, non-snapshot mode and actual restore time")
	}
	if err = validateArchiveHistoryPolicy(p.HistoryMode, archive); err != nil {
		return p, err
	}
	p.HistoryTime = when.UTC().Format(time.RFC3339Nano)
	plan, err := buildPlanWithProfile(archive, &p, when)
	if err != nil || plan.derivedDigest != p.DerivedDigest {
		return p, fmt.Errorf("%w: history current image/derived verification failed: %v", ErrRestoreUnavailable, err)
	}
	h, err := lowerSourceHistory(archive, p, when)
	if err != nil {
		return p, err
	}
	digest, err := driver.OfflineHistoryDigest(h)
	if err != nil {
		return p, err
	}
	if p.HistoryDigest != "" && p.HistoryDigest != digest {
		return p, driver.ErrRestoreHistoryRefused{Mode: p.HistoryMode, History: archive.Manifest().History, Reason: driver.RestoreHistoryReasonUnmappable}
	}
	p.HistoryDigest = digest
	return p.Detached()
}

func historyReadError(mode driver.RestoreHistoryMode, err error) error {
	if invalid, ok := err.(dumpformat.ErrSourceHistory); ok {
		return driver.ErrRestoreHistoryRefused{Mode: mode, Reason: driver.RestoreHistoryReason(invalid.Reason)}
	}
	return err
}

func validateArchiveHistoryPolicy(mode driver.RestoreHistoryMode, a *dumpformat.Archive) error {
	if mode == driver.RestoreHistoryModeSnapshot {
		return nil
	}
	scopes, ok := restoreHistoryScopes(a)
	if !ok {
		return driver.ValidateRestoreHistoryPolicy(mode, a.Manifest().History)
	}
	for _, scoped := range scopes {
		if scoped.Range.Status != "verified" {
			return driver.ValidateRestoreHistoryPolicy(mode, a.Manifest().History)
		}
		if mode == driver.RestoreHistoryModeFull && (!scoped.Range.Complete || scoped.Range.First != 1 || len(scoped.Range.Gaps) != 0 || len(scoped.Range.Baseline) != 0) {
			return driver.ErrRestoreHistoryRefused{Mode: mode, History: a.Manifest().History, Reason: driver.RestoreHistoryReasonNonContiguous}
		}
	}
	return nil
}

func portableHistoryRecords(plan restorePlan) []driver.Record {
	rows := []driver.Record{}
	for _, row := range plan.records {
		switch row.kind {
		case "bead", "edge", "comment", "memory", "config":
			rows = append(rows, driver.Record{Ref: row.ref, Kind: row.kind, Body: bytes.Clone(row.body)})
		}
	}
	return rows
}

func lowerSourceHistory(a *dumpformat.Archive, p driver.RestoreImportProfile, when time.Time) (driver.OfflineHistory, error) {
	refuse := func() (driver.OfflineHistory, error) {
		return driver.OfflineHistory{}, driver.ErrRestoreHistoryRefused{Mode: p.HistoryMode, History: a.Manifest().History, Reason: driver.RestoreHistoryReasonUnmappable}
	}
	scopes, ok := restoreHistoryScopes(a)
	if !ok || len(scopes) == 0 {
		return refuse()
	}
	if len(scopes) > 1 {
		// Independent project histories have no source-defined cross-project
		// revision order. Full histories can be imported in stable project order
		// while retaining every source commit/event ID; partial histories retain
		// independent floors and therefore refuse until the driver has a
		// project-scoped floor contract.
		for _, scoped := range scopes {
			if scoped.Range.Status != "verified" || !scoped.Range.Complete || scoped.Range.First != 1 || len(scoped.Range.Gaps) != 0 || len(scoped.Range.Baseline) != 0 {
				return refuse()
			}
		}
		sort.Slice(scopes, func(i, j int) bool { return scopes[i].Project < scopes[j].Project })
	}
	first := scopes[0].Range.First
	h := driver.OfflineHistory{Source: dumpformat.SourceHistoryFormat, Verified: true, Complete: true, First: first, Head: scopes[0].Range.Head,
		BaselineVerified: first > 1, Baseline: []driver.Record{}, Gaps: []driver.SourceHistoryGap{}, Batches: []driver.SourceHistoryBatch{}}
	if len(scopes) == 1 {
		r := scopes[0].Range
		h.Complete, h.First, h.Head, h.BaselineVerified = r.Complete, r.First, r.Head, r.First > 1
		for _, g := range r.Gaps {
			h.Gaps = append(h.Gaps, driver.SourceHistoryGap{First: g.First, Last: g.Last, Reason: g.Reason})
		}
	}
	image := func(sequence uint64) ([]driver.Record, error) {
		snapshot, err := a.HistoryImage(sequence)
		if err != nil {
			return nil, err
		}
		plan, err := buildPlanWithProfile(snapshot, &p, when)
		if err != nil {
			return nil, err
		}
		return portableHistoryRecords(plan), nil
	}
	if len(scopes) == 1 {
		r := scopes[0].Range
		before := []driver.Record{}
		if r.First > 1 {
			var err error
			before, err = image(r.First - 1)
			if err != nil {
				return refuse()
			}
			h.Baseline = before
		}
		previousEvents := map[string]bool{}
		for _, e := range r.BaselineEvents {
			previousEvents[dumpformat.SourceEventID(e)] = true
		}
		for _, frame := range r.Frames {
			after, err := image(frame.Sequence)
			if err != nil {
				return refuse()
			}
			batch := driver.SourceHistoryBatch{Sequence: frame.Sequence, CommitIDs: []string{frame.CommitID}, EventIDs: []string{}, Changes: historyDelta(before, after)}
			for _, e := range frame.Events {
				id := dumpformat.SourceEventID(e)
				if !previousEvents[id] {
					batch.EventIDs = append(batch.EventIDs, id)
				}
				previousEvents[id] = true
			}
			sort.Strings(batch.EventIDs)
			h.Batches = append(h.Batches, batch)
			before = after
		}
		return h, nil
	}
	var globalSequence uint64
	sharedGlobals := map[string]driver.Record{}
	for _, scoped := range scopes {
		before := []driver.Record{}
		previousEvents := map[string]bool{}
		for _, frame := range scoped.Range.Frames {
			after, err := a.HistoryImageForProject(scoped.Project, frame.Sequence)
			if err != nil {
				return refuse()
			}
			beforePlan, err := buildPlanWithProfile(after, &p, when)
			if err != nil {
				return refuse()
			}
			afterRecords, err := omitAlreadyImportedSharedGlobals(portableHistoryRecords(beforePlan), sharedGlobals)
			if err != nil {
				return refuse()
			}
			globalSequence++
			batch := driver.SourceHistoryBatch{Project: scoped.Project, Sequence: globalSequence, CommitIDs: []string{frame.CommitID}, EventIDs: []string{}, Changes: historyDelta(before, afterRecords)}
			for _, e := range frame.Events {
				id := dumpformat.SourceEventID(e)
				if !previousEvents[id] {
					batch.EventIDs = append(batch.EventIDs, id)
				}
				previousEvents[id] = true
			}
			sort.Strings(batch.EventIDs)
			h.Batches = append(h.Batches, batch)
			before = afterRecords
		}
	}
	h.Head = globalSequence
	h.First = 1
	return h, nil
}

func omitAlreadyImportedSharedGlobals(records []driver.Record, seen map[string]driver.Record) ([]driver.Record, error) {
	result := make([]driver.Record, 0, len(records))
	for _, record := range records {
		if record.Kind != "config" && record.Kind != "memory" {
			result = append(result, record)
			continue
		}
		key := record.Kind + "\x00" + record.Ref.ID
		if previous, ok := seen[key]; ok {
			if previous.Kind != record.Kind || previous.Ref != record.Ref || !bytes.Equal(previous.Body, record.Body) {
				return nil, fmt.Errorf("shared history record %s has conflicting project images", record.Ref.ID)
			}
			continue
		}
		seen[key] = record
		result = append(result, record)
	}
	return result, nil
}

func restoreHistoryScopes(a *dumpformat.Archive) ([]dumpformat.ProjectSourceHistory, bool) {
	history, ok := a.SourceHistory()
	if !ok {
		return nil, false
	}
	if len(history.ProjectHistories) != 0 {
		return append([]dumpformat.ProjectSourceHistory(nil), history.ProjectHistories...), true
	}
	projects := a.Manifest().Projects
	if len(projects) != 1 {
		return nil, false
	}
	return []dumpformat.ProjectSourceHistory{{Project: projects[0].Prefix, Commits: history.Commits, Events: history.Events, Range: history.Range}}, true
}

func historyDelta(before, after []driver.Record) []driver.SourceHistoryChange {
	index := func(rows []driver.Record) map[string]driver.Record {
		m := map[string]driver.Record{}
		for _, r := range rows {
			key, _ := driver.EncodeLockKey(driver.LockKey{Kind: driver.LockRecord, Ref: &r.Ref})
			m[key] = r
		}
		return m
	}
	left, right := index(before), index(after)
	keys := map[string]bool{}
	for key := range left {
		keys[key] = true
	}
	for key := range right {
		keys[key] = true
	}
	ordered := make([]string, 0, len(keys))
	for key := range keys {
		ordered = append(ordered, key)
	}
	sort.Strings(ordered)
	changes := []driver.SourceHistoryChange{}
	for _, key := range ordered {
		a, presentA := left[key]
		b, presentB := right[key]
		if presentA && presentB && a.Kind == b.Kind && bytes.Equal(a.Body, b.Body) {
			continue
		}
		change := driver.SourceHistoryChange{}
		if presentA {
			change.Before = &a
		}
		if presentB {
			change.After = &b
		}
		changes = append(changes, change)
	}
	return changes
}

func historyLockPlan(plan restorePlan, h driver.OfflineHistory) restorePlan {
	copy := plan
	copy.records = append([]restoreRecord{}, plan.records...)
	copy.sentinels = append([]driver.Sentinel{}, plan.sentinels...)
	add := func(r driver.Record) {
		copy.records = append(copy.records, restoreRecord{ref: r.Ref, kind: r.Kind, body: r.Body})
		if r.Kind == "bead" {
			if s, err := driver.NewBeadSentinel(r.Ref.ID); err == nil {
				copy.sentinels = append(copy.sentinels, s)
			}
		}
	}
	for _, r := range h.Baseline {
		add(r)
	}
	for _, batch := range h.Batches {
		for _, c := range batch.Changes {
			if c.Before != nil {
				add(*c.Before)
			}
			if c.After != nil {
				add(*c.After)
			}
		}
	}
	copy.records = append(copy.records, restoreRecord{ref: model.RefByID(driver.OfflineHistoryEvidenceID, model.Natural64(0)), kind: "beads-archive"})
	// Sentinels are also used for BumpSentinels, whose contract forbids
	// duplicates. Record lock requests are independently de-duplicated.
	seen := map[driver.Sentinel]bool{}
	unique := []driver.Sentinel{}
	for _, s := range copy.sentinels {
		if !seen[s] {
			seen[s] = true
			unique = append(unique, s)
		}
	}
	sort.Slice(unique, func(i, j int) bool { return unique[i] < unique[j] })
	copy.sentinels = unique
	return copy
}

func validateHistoryRecovery(ctx context.Context, r driver.Reader, p driver.RestoreImportProfile, evidence archiveEvidence, receipt *driver.IdempotencyRecord) error {
	file, ok := evidence.Members[dumpformat.SourceHistoryMember]
	if !ok || p.HistoryTime == "" {
		return driver.ErrConflict
	}
	raw, err := r.Blob(ctx, driver.Hash(file.SHA256))
	if err != nil || hashBytes(raw) != file.SHA256 || int64(len(raw)) != file.Size {
		return driver.ErrConflict
	}
	var source dumpformat.SourceHistory
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	if err = d.Decode(&source); err != nil || source.Range.Status != "verified" {
		return driver.ErrConflict
	}
	rows, err := r.Get(ctx, []model.Ref{model.RefByID(driver.OfflineHistoryEvidenceID, model.Natural64(0))})
	if err != nil || len(rows) != 1 {
		return driver.ErrConflict
	}
	var got driver.HistoryImportEvidence
	if err = json.Unmarshal(rows[0].Body, &got); err != nil {
		return driver.ErrConflict
	}
	want := driver.HistoryImportEvidence{Source: dumpformat.SourceHistoryFormat, Mode: p.HistoryMode, Complete: source.Range.Complete, First: source.Range.First, Head: source.Range.Head, Floor: model.Natural64(0), Gaps: []driver.SourceHistoryGap{}, Mapping: []driver.SourceImportRevision{}}
	for _, gap := range source.Range.Gaps {
		want.Gaps = append(want.Gaps, driver.SourceHistoryGap{First: gap.First, Last: gap.Last, Reason: gap.Reason})
	}
	var revision uint64
	if source.Range.First > 1 {
		revision = 1
		want.Floor = model.Natural64(1)
	}
	previous := map[string]bool{}
	for _, event := range source.Range.BaselineEvents {
		previous[dumpformat.SourceEventID(event)] = true
	}
	for _, frame := range source.Range.Frames {
		revision++
		ids := []string{}
		for _, event := range frame.Events {
			id := dumpformat.SourceEventID(event)
			if !previous[id] {
				ids = append(ids, id)
			}
			previous[id] = true
		}
		sort.Strings(ids)
		want.Mapping = append(want.Mapping, driver.SourceImportRevision{Sequence: frame.Sequence, Revision: model.Natural64(revision), EventIDs: ids, CommitIDs: []string{frame.CommitID}})
	}
	revision++
	left, _ := json.Marshal(got)
	right, _ := json.Marshal(want)
	if !bytes.Equal(left, right) || receipt.Commit.Revision != model.Natural64(revision) || receipt.Changeset.Revision != model.Natural64(revision) {
		return driver.ErrConflict
	}
	return nil
}
