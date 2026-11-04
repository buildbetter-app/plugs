package memory

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func admissionPlan(t *testing.T, r driver.Reader) driver.SelectionPlan {
	t.Helper()
	v := r.Snapshot()
	return admissionSeal(t, driver.SelectionPlan{Incarnation: v.Incarnation, Branch: v.Branch, ContentRevision: *v.At, AuthorityRevision: *v.At, Membership: driver.SelectionExpr{Op: driver.SelectionTrue}, Authorization: driver.SelectionExpr{Op: driver.SelectionTrue}, Mode: driver.SelectionCountMode}, nil)
}

// These are trusted storage-contract policies, not a fixed-policy compiler.
func admissionSeal(t *testing.T, p driver.SelectionPlan, host *driver.SelectionExpr) driver.SelectionPlan {
	t.Helper()
	control := model.RefByKey("selection-coverage-control")
	spec := driver.SelectionBoundarySpec{Version: 1, PreparedBinding: driver.SelectionDigest([]byte("selection-coverage-conformance-v1")), Incarnation: p.Incarnation, Branch: p.Branch, AuthorityRevision: p.AuthorityRevision, Authorization: p.Authorization, Chain: driver.SelectionChainCompiled, Host: driver.SelectionHostAbsent, Sources: []driver.SelectionSource{{Key: driver.LockKey{Kind: driver.LockRecord, Ref: &control}, Revision: model.Natural64(0)}}}
	if host != nil {
		spec.Host, spec.HostExpression = driver.SelectionHostCompiled, host
	}
	var err error
	p.Boundary, err = driver.PrepareSelectionBoundary(spec)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Validate(); err != nil {
		t.Fatal(err)
	}
	return p
}

func admissionUnavailable(t *testing.T, r driver.SelectionReader, p driver.SelectionPlan) {
	t.Helper()
	got, err := r.Select(context.Background(), p)
	if !errors.Is(err, driver.ErrSelectionUnavailable) || !reflect.DeepEqual(got, driver.SelectionResult{}) {
		t.Fatalf("unavailable selection leaked a result: %+v / %v", got, err)
	}
}

func admissionCount(t *testing.T, r driver.SelectionReader, p driver.SelectionPlan, total uint64) {
	t.Helper()
	got, err := r.Select(context.Background(), p)
	if err != nil || got.Total != total {
		t.Fatalf("certified selection=%+v/%v, want total %d", got, err, total)
	}
}

func admissionFresh(t *testing.T, s *Store, lease driver.ActiveLease, p driver.SelectionPlan, want bool, unavailable bool) {
	t.Helper()
	tx, _ := beginSourceTest(t, s, lease)
	defer tx.Close()
	p.Mode, p.Page, p.GroupBy, p.Cap, p.Excluded = driver.SelectionCountMode, nil, "", 0, nil
	p.ContentRevision = p.AuthorityRevision
	got, err := tx.FreshSelectionExists(context.Background(), p)
	if unavailable {
		if got || !errors.Is(err, driver.ErrSelectionUnavailable) {
			t.Fatalf("fresh unavailable=%v/%v", got, err)
		}
	} else if err != nil || got != want {
		t.Fatalf("fresh certified=%v/%v, want %v", got, err, want)
	}
}

// Exercise real public reads at every shortcut, with no synthetic state/ranges.
func admissionShortcuts(t *testing.T, r *reader, p driver.SelectionPlan) {
	t.Helper()
	for _, mode := range []driver.SelectionMode{driver.SelectionPageMode, driver.SelectionCountMode, driver.SelectionTypeSummary} {
		t.Run(string(mode), func(t *testing.T) {
			p := p
			p.Mode = mode
			if mode == driver.SelectionPageMode {
				p.Page = &driver.SelectionPage{Limit: 0}
			}
			if mode == driver.SelectionTypeSummary {
				p.GroupBy = driver.SelectionGroupType
			}
			for _, arm := range []struct {
				name string
				expr driver.SelectionExpr
			}{
				{"ordinary", driver.SelectionExpr{Op: driver.SelectionTrue}},
				{"false", driver.SelectionExpr{Op: driver.SelectionFalse}},
				{"empty_refs", driver.SelectionExpr{Op: driver.SelectionRefs, Refs: []model.Ref{}}},
				{"not_false", driver.SelectionExpr{Op: driver.SelectionNot, Args: []driver.SelectionExpr{{Op: driver.SelectionFalse}}}},
			} {
				t.Run(arm.name, func(t *testing.T) {
					q := p
					q.Membership = arm.expr
					q.Excluded = []model.Ref{sourceRef("coverage-source")}
					admissionUnavailable(t, r, admissionSeal(t, q, nil))
				})
			}
		})
	}
}

func admissionFact(record driver.Record, incarnation string, family driver.SelectionProjectionFamily) driver.DerivedSelectionRow {
	key := driver.DerivedRowKey{Table: family.Table, Key: "coverage/" + string(family.Kind) + "/" + record.Ref.ID}
	row := driver.DerivedRow{Key: key.Key, Value: []byte("unchanged logical " + string(family.Kind))}
	ref := record.Ref
	set := &driver.SelectionFacts{Kind: family.Kind}
	if family.Kind == driver.SelectionAncestorFact {
		set.Rows = []driver.SelectionFact{{Kind: family.Kind, Ref: &ref, Target: &ref}}
	}
	row.Index = &driver.SelectionIndex{Version: driver.SelectionIndexVersion, Row: key, Incarnation: incarnation, Branch: model.Natural64(0), ValueDigest: driver.SelectionDigest(row.Value), Source: driver.SelectionSource{Key: driver.LockKey{Kind: driver.LockRecord, Ref: &ref}, Revision: record.Revision, Present: true, Digest: driver.SelectionDigest(record.Body)}, Facts: set}
	return driver.DerivedSelectionRow{Table: key.Table, Row: row}
}

func admissionFactExpr(row driver.DerivedSelectionRow) driver.SelectionExpr {
	e := driver.SelectionExpr{Op: driver.SelectionExistsOp, Exists: &driver.SelectionExists{Row: row.Row.Index.Row, Kind: row.Row.Index.Facts.Kind}}
	if len(row.Row.Index.Facts.Rows) == 0 {
		return driver.SelectionExpr{Op: driver.SelectionNot, Args: []driver.SelectionExpr{e}}
	}
	return e
}

// Publish through actual Lock/Head/typed source/optional annotation/Commit APIs.
func admissionPublish(t *testing.T, s *Store, lease driver.ActiveLease, typed bool, families ...driver.SelectionProjectionFamily) driver.Record {
	t.Helper()
	ctx := context.Background()
	r := sourceReader(t, s, nil)
	meta := r.Snapshot()
	ref := sourceRef("coverage-source")
	observed, err := r.Observe(ctx, []driver.LockKey{{Kind: driver.LockRecord, Ref: &ref}})
	if err != nil {
		t.Fatal(err)
	}
	record := driver.Record{Ref: ref, Kind: "bead", Revision: observed[0].Revision, Body: []byte(fmt.Sprintf(`{"edit":%s}`, meta.At.String()))}
	var rows []driver.DerivedSelectionRow
	if typed {
		rows = append(rows, startupBeadRow(record, meta.Incarnation))
	}
	for _, family := range families {
		rows = append(rows, admissionFact(record, meta.Incarnation, family))
	}
	extra := []driver.LockKey{}
	closure, _ := driver.NewSentinel("closure/coverage-admission")
	extra = append(extra, driver.LockKey{Kind: driver.LockSentinel, Sentinel: closure})
	for _, row := range rows {
		key := row.Row.Index.Row
		extra = append(extra, driver.LockKey{Kind: driver.LockDerived, Derived: &key})
	}
	tx, header := beginSourceTestWithLocks(t, s, lease, extra, true, ref)
	if err := tx.PutGraphRecord(ctx, record, nil, nil); err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if err := tx.PutSelectionDerived(ctx, row.Table, nil, []driver.DerivedRow{row.Row}); err != nil {
			t.Fatal(err)
		}
	}
	commitSourceTest(t, tx, header)
	current, err := sourceReader(t, s, nil).Get(ctx, []model.Ref{ref})
	if err != nil || len(current) != 1 {
		t.Fatalf("published source: %v/%v", current, err)
	}
	return current[0]
}

func admissionBuild(t *testing.T, s *Store, lease driver.ActiveLease, families ...driver.SelectionProjectionFamily) {
	t.Helper()
	incarnation := sourceReader(t, s, nil).Snapshot().Incarnation
	decode := func(record driver.Record) (driver.GraphSourceMetadata, error) {
		metadata := driver.GraphSourceMetadata{}
		for _, family := range families {
			if family.Table == driver.DerivedSortKey {
				metadata.Selection = append(metadata.Selection, startupBeadRow(record, incarnation))
			} else {
				metadata.Selection = append(metadata.Selection, admissionFact(record, incarnation, family))
			}
		}
		return metadata, nil
	}
	projection := startupSortProjection()
	projection.Families = families
	if err := s.BuildGraphSourceIndexes(context.Background(), lease, decode, projection); err != nil {
		t.Fatal(err)
	}
}

func TestSelectionCoverageAdmission(t *testing.T) {
	sortFamily := driver.SelectionProjectionFamily{Table: driver.DerivedSortKey, Version: driver.SelectionIndexVersion}
	t.Run("empty_unbuilt", func(t *testing.T) {
		s := New()
		defer s.Close()
		lease := sourceLease(t, s)
		r := sourceReader(t, s, nil)
		admissionShortcuts(t, r, admissionPlan(t, r))
		admissionFresh(t, s, lease, admissionPlan(t, r), false, true)
		tx, header := beginSourceTest(t, s, lease)
		commitSourceTest(t, tx, header)
		current := sourceReader(t, s, nil)
		admissionUnavailable(t, current, admissionPlan(t, current))
		admissionFresh(t, s, lease, admissionPlan(t, current), false, true)
	})
	t.Run("repaired_image_gap", func(t *testing.T) {
		s := New()
		defer s.Close()
		lease := sourceLease(t, s)
		admissionPublish(t, s, lease, true)
		retained := sourceReader(t, s, nil)
		admissionCount(t, retained, admissionPlan(t, retained), 1)
		beforeFirst := admissionPlan(t, retained)
		beforeFirst.ContentRevision = model.Natural64(0)
		admissionUnavailable(t, retained, beforeFirst)
		admissionPublish(t, s, lease, false)
		admissionPublish(t, s, lease, true)
		current := sourceReader(t, s, nil)
		admissionShortcuts(t, current, admissionPlan(t, current))
		oldContent := admissionPlan(t, current)
		oldContent.ContentRevision = model.Natural64(1)
		admissionUnavailable(t, current, oldContent) // Certified content cannot hide an authority gap.
		admissionFresh(t, s, lease, admissionPlan(t, current), false, true)
		admissionBuild(t, s, lease, sortFamily)
		built := sourceReader(t, s, nil)
		admissionCount(t, built, admissionPlan(t, built), 1)
		admissionFresh(t, s, lease, admissionPlan(t, built), true, false)
		p := admissionPlan(t, built)
		p.ContentRevision = model.Natural64(2)
		admissionUnavailable(t, built, p)
		admissionCount(t, retained, admissionPlan(t, retained), 1)
	})
	t.Run("imported_history", func(t *testing.T) { admissionImportedHistory(t, sortFamily) })
	t.Run("before_build_head", func(t *testing.T) {
		s := New()
		defer s.Close()
		lease := sourceLease(t, s)
		admissionPublish(t, s, lease, false)
		admissionPublish(t, s, lease, true)
		unbuilt := sourceReader(t, s, nil)
		admissionUnavailable(t, unbuilt, admissionPlan(t, unbuilt))
		admissionPublish(t, s, lease, true)
		admissionBuild(t, s, lease, sortFamily)
		r := sourceReader(t, s, nil)
		admissionCount(t, r, admissionPlan(t, r), 1)
		p := admissionPlan(t, r)
		p.ContentRevision = model.Natural64(2) // Exact historical typed images still predate the build.
		admissionShortcuts(t, r, p)
	})
	t.Run("certified_range", func(t *testing.T) {
		s := New()
		defer s.Close()
		lease := sourceLease(t, s)
		admissionBuild(t, s, lease, sortFamily)
		empty := sourceReader(t, s, nil)
		admissionCount(t, empty, admissionPlan(t, empty), 0)
		admissionFresh(t, s, lease, admissionPlan(t, empty), false, false)
		admissionPublish(t, s, lease, true)
		admissionPublish(t, s, lease, true)
		r := sourceReader(t, s, nil)
		p := admissionPlan(t, r)
		admissionCount(t, r, p, 1)
		p.ContentRevision = model.Natural64(1)
		admissionCount(t, r, p, 1)
		admissionFresh(t, s, lease, admissionPlan(t, r), true, false)
	})
	t.Run("independent_families_and_planes", func(t *testing.T) {
		for _, family := range selectionFamilies()[1:] {
			t.Run(string(family.Kind), func(t *testing.T) { admissionFamilyPlanes(t, sortFamily, family) })
		}
		t.Run("independent_complete_envelopes", admissionIndependentEnvelopes)
	})
}

func admissionFamilyPlanes(t *testing.T, sortFamily, family driver.SelectionProjectionFamily) {
	s := New()
	defer s.Close()
	lease := sourceLease(t, s)
	admissionPublish(t, s, lease, false)
	record := admissionPublish(t, s, lease, true, family)
	admissionBuild(t, s, lease, sortFamily)
	r := sourceReader(t, s, nil)
	base := admissionPlan(t, r)
	admissionCount(t, r, base, 1) // Sort-only completeness needs no other family.
	expr := admissionFactExpr(admissionFact(record, r.Snapshot().Incarnation, family))
	for _, plane := range []string{"membership", "authorization", "host"} {
		t.Run(plane+"_eager", func(t *testing.T) {
			p := base
			hidden := driver.SelectionExpr{Op: driver.SelectionAny, Args: []driver.SelectionExpr{{Op: driver.SelectionTrue}, {Op: driver.SelectionAll, Args: []driver.SelectionExpr{{Op: driver.SelectionFalse}, expr}}}}
			var host *driver.SelectionExpr
			switch plane {
			case "membership":
				p.Membership = hidden
			case "authorization":
				p.Authorization = hidden
			case "host":
				host = &hidden
			}
			p.Mode, p.Page = driver.SelectionPageMode, &driver.SelectionPage{Limit: 0}
			admissionUnavailable(t, r, admissionSeal(t, p, host))
		})
	}
	fresh := base
	fresh.Membership = expr
	admissionFresh(t, s, lease, admissionSeal(t, fresh, nil), false, true)
	tx, header := beginSourceTest(t, s, lease)
	commitSourceTest(t, tx, header) // Advances SortKey, without certifying this family.
	admissionBuild(t, s, lease, sortFamily, family)
	current := sourceReader(t, s, nil)
	p := admissionPlan(t, current)
	p.ContentRevision = base.ContentRevision
	p.Authorization = expr
	admissionCount(t, current, admissionSeal(t, p, nil), 1) // Family required at current authority only.
	p.Authorization = driver.SelectionExpr{Op: driver.SelectionTrue}
	admissionCount(t, current, admissionSeal(t, p, &expr), 1) // Same rule for the host.
	p.Membership = expr
	admissionUnavailable(t, current, admissionSeal(t, p, nil)) // Valid old row, but no content-family interval.
	p.ContentRevision = p.AuthorityRevision
	p.ContentCommitted = true
	admissionCount(t, current, admissionSeal(t, p, nil), 1)
	admissionFresh(t, s, lease, admissionSeal(t, p, nil), true, false)
}

func admissionIndependentEnvelopes(t *testing.T) {
	for _, tc := range []struct {
		name         string
		table        driver.DerivedTable
		kind         driver.SelectionFactKind
		recordSource bool
	}{
		{"derived_label", driver.DerivedAttribute, driver.SelectionLabelFact, false},
		{"derived_link", driver.DerivedReferenceIndex, driver.SelectionLinkFact, false},
		{"derived_ancestor", driver.DerivedAncestorClosure, driver.SelectionAncestorFact, false},
		{"record_cut_ancestor", driver.DerivedCutClosure, driver.SelectionAncestorFact, true},
		{"record_grant", driver.DerivedGrant, driver.SelectionGrantFact, true},
		{"record_project", driver.DerivedAttribute, driver.SelectionProjectFact, true},
		{"record_team", driver.DerivedAttribute, driver.SelectionTeamFact, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := New()
			defer s.Close()
			lease := sourceLease(t, s)
			record := admissionPublish(t, s, lease, true)
			row := admissionFact(record, sourceReader(t, s, nil).Snapshot().Incarnation, driver.SelectionProjectionFamily{Table: tc.table, Kind: tc.kind})
			row.Row.Index.Facts.Rows = nil // Explicit complete-empty independent domain.
			key := row.Row.Index.Row
			if !tc.recordSource {
				row.Row.Index.Source = driver.SelectionSource{Key: driver.LockKey{Kind: driver.LockDerived, Derived: &key}, Revision: model.Natural64(0), Present: true, Digest: driver.SelectionDigest(row.Row.Value)}
			}
			closure, _ := driver.NewSentinel("closure/coverage-admission")
			grant, _ := driver.NewSentinel("grant/coverage-admission")
			tx, header := beginSourceTestWithLocks(t, s, lease, []driver.LockKey{{Kind: driver.LockDerived, Derived: &key}, {Kind: driver.LockSentinel, Sentinel: closure}, {Kind: driver.LockSentinel, Sentinel: grant}}, true, record.Ref)
			if err := tx.PutSelectionDerived(context.Background(), row.Table, nil, []driver.DerivedRow{row.Row}); err != nil {
				t.Fatal(err)
			}
			commitSourceTest(t, tx, header)
			r := sourceReader(t, s, nil)
			p := admissionPlan(t, r)
			p.Membership = admissionFactExpr(row)
			admissionCount(t, r, admissionSeal(t, p, nil), 1)
		})
	}
}

func admissionImportedHistory(t *testing.T, sortFamily driver.SelectionProjectionFamily) {
	ctx := context.Background()
	source := New()
	defer source.Close()
	capturedLease := sourceLease(t, source)
	first := admissionPublish(t, source, capturedLease, true)
	last := admissionPublish(t, source, capturedLease, true)
	capturedReader := sourceReader(t, source, nil)
	entries, err := capturedReader.Changesets(ctx, nil, 10)
	if err != nil || len(entries) != 2 {
		t.Fatalf("actual source history=%v/%v", entries, err)
	}
	// Replay only actual successful source commits and their captured images;
	// no private history map or invented revision is installed in the target.
	history := driver.OfflineHistory{Source: "memory-selection-conformance", Verified: true, Complete: true, First: 1, Head: 2, Batches: []driver.SourceHistoryBatch{
		{Sequence: 1, CommitIDs: []string{entries[0].Revision.String()}, Changes: []driver.SourceHistoryChange{{After: &first}}},
		{Sequence: 2, CommitIDs: []string{entries[1].Revision.String()}, Changes: []driver.SourceHistoryChange{{Before: &first, After: &last}}},
	}}
	s := New()
	defer s.Close()
	lease := sourceLease(t, s)
	sourceMeta, err := source.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	profile := driver.RestoreImportProfile{Admission: driver.RestoreImportAdmission{Mode: "authenticated", Actor: model.PrincipalRef{Kind: "user", ID: "restore-admin"}, Authority: "restore-admin", AdmissionDigest: strings.Repeat("a", 64)}, SourceStoreID: sourceMeta.StoreID, Method: "offline.restore.import", IdempotencyKey: "selection-coverage-history", ArchiveDigest: strings.Repeat("b", 64), DerivedDigest: strings.Repeat("c", 64), HistoryMode: driver.RestoreHistoryModeFull}
	when := time.Date(2026, 11, 3, 0, 0, 0, 0, time.UTC)
	profile.HistoryTime = when.Format(time.RFC3339Nano)
	profile.HistoryDigest, err = driver.OfflineHistoryDigest(history)
	if err != nil {
		t.Fatal(err)
	}
	marker := model.RefByID(driver.OfflineHistoryEvidenceID, model.Natural64(0))
	// Prepare requires Head with no staged writes or bumps. Retain the full
	// update-lock census, then stage every acquired sentinel after preparation.
	tx, header := beginSourceTestWithLocks(t, s, lease, []driver.LockKey{{Kind: driver.LockRecord, Ref: &marker}}, false, last.Ref)
	meta, err := tx.PrepareOfflineHistory(ctx, profile, history, when)
	if err != nil {
		t.Fatal(err)
	}
	sentinels := []driver.Sentinel{}
	for _, key := range header.LockKeys {
		if key.Kind == driver.LockSentinel {
			sentinels = append(sentinels, key.Sentinel)
		}
	}
	if err := tx.BumpSentinels(ctx, sentinels); err != nil {
		t.Fatal(err)
	}
	if err := tx.Put(ctx, last); err != nil {
		t.Fatal(err)
	}
	header.Incarnation, header.Kind, header.Time, header.Method, header.IdempotencyKey, header.Principal = meta.Incarnation, model.ChangesetRestore, when, profile.Method, profile.IdempotencyKey, profile.Admission.Actor
	header.RequestHash, err = profile.RequestHash()
	if err != nil {
		t.Fatal(err)
	}
	header.Changes = []model.ChangeRecord{{Ref: last.Ref, Op: model.OpCreate, FromRevision: model.Natural64(0)}}
	if _, err := tx.CommitOfflineHistory(ctx, header); err != nil {
		t.Fatal(err)
	}
	// Even an exact typed current projection added AFTER import is not temporal proof.
	key := startupBeadRow(last, meta.Incarnation).Row.Index.Row
	attach, h := beginSourceTestWithLocks(t, s, lease, []driver.LockKey{{Kind: driver.LockDerived, Derived: &key}}, true, last.Ref)
	row := startupBeadRow(last, meta.Incarnation)
	if err := attach.PutSelectionDerived(ctx, row.Table, nil, []driver.DerivedRow{row.Row}); err != nil {
		t.Fatal(err)
	}
	commitSourceTest(t, attach, h)
	r := sourceReader(t, s, nil)
	p := admissionPlan(t, r)
	admissionShortcuts(t, r, p)
	admissionFresh(t, s, lease, p, false, true)
	beforeVersions, err := r.Versions(ctx, last.Ref, driver.PageReq{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	admissionBuild(t, s, lease, sortFamily)
	built := sourceReader(t, s, nil)
	admissionCount(t, built, admissionPlan(t, built), 1)
	p = admissionPlan(t, built)
	p.ContentRevision = last.Revision
	admissionUnavailable(t, built, p)
	versions, err := built.Versions(ctx, last.Ref, driver.PageReq{Limit: 10})
	if err != nil || len(versions.Items) != 2 || !reflect.DeepEqual(versions, beforeVersions) {
		t.Fatalf("imported source versions changed: %+v/%v", versions, err)
	}
}
