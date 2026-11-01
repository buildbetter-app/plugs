package offlinehistory_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/integration/converterroundtrip"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// These bytes were emitted by SQLSource against the pinned Dolt engine on
// RunsOn, rather than assembled by the consumer fixture generator. The source
// commit, receipt and exact archive digest are recorded beside the archive.
func doltProducerFixture(t *testing.T) ([]byte, *dumpformat.Archive) {
	t.Helper()
	data, err := os.ReadFile("testdata/dolt-source-partial.tar")
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(data)
	if hex.EncodeToString(sum[:]) != "b56f1de4a36c22cca379647d5fe1b2962e4358d9976923e76007ea84b7712582" {
		t.Fatal("actual source archive bytes changed without new producer evidence")
	}
	a, err := dumpformat.Read(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	h, ok := a.SourceHistory()
	if !ok || h.Range.Status != "verified" || h.Range.Complete || h.Range.First != 3 || h.Range.Head != 3 || len(h.Commits) != 3 || len(h.Range.Frames) != 1 || len(h.Range.BaselineEvents) != 1 || len(h.Events) != 2 {
		t.Fatal("pinned Dolt fixture lost its proven partial range")
	}
	want := []dumpformat.HistoryGap{{First: 1, Last: 1, Reason: "unmappable"}, {First: 2, Last: 2, Reason: "not-retained"}}
	if !reflect.DeepEqual(h.Range.Gaps, want) {
		t.Fatalf("actual source prefix gaps changed: %+v", h.Range.Gaps)
	}
	return data, a
}

func TestDoltProducerArchiveRoundTripAllDrivers(t *testing.T) {
	data, archive := doltProducerFixture(t)
	history, _ := archive.SourceHistory()
	ctx := context.Background()
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		for _, mode := range []driver.RestoreHistoryMode{driver.RestoreHistoryModePartial, driver.RestoreHistoryModeSnapshot} {
			t.Run(engine+"/"+string(mode), func(t *testing.T) {
				inspection, err := converterroundtrip.InspectImport(ctx, bytes.NewReader(data))
				if err != nil {
					t.Fatal(err)
				}
				p := profile(mode)
				p.SourceStoreID, p.ArchiveDigest, p.DerivedDigest = inspection.SourceStoreID, inspection.ArchiveSHA256, inspection.DerivedSHA256
				when := time.Now().UTC()
				if mode == driver.RestoreHistoryModePartial {
					p, err = converterroundtrip.BindHistoryImport(ctx, bytes.NewReader(data), p, when)
					if err != nil {
						t.Fatal(err)
					}
				}
				s := openStore(t, engine)
				old, err := s.Meta(ctx)
				if err != nil {
					t.Fatal(err)
				}
				lease, err := s.Lease(ctx, "actual-dolt-history", driver.LeaseTTL)
				if err != nil {
					t.Fatal(err)
				}
				defer lease.Release(context.Background())
				report, err := converterroundtrip.RestoreImportWithLease(ctx, bytes.NewReader(data), s, lease, p)
				if err != nil {
					t.Fatal(err)
				}
				wantHead := model.Natural64(3)
				if mode == driver.RestoreHistoryModeSnapshot {
					wantHead = model.Natural64(1)
				}
				if report.Restored.Head != wantHead || report.Restored.HistoryFloor != model.Natural64(1) || report.Restored.Incarnation == old.Incarnation {
					t.Fatalf("source import metadata: %+v", report.Restored)
				}
				r, err := s.Read(ctx, driver.View{Incarnation: report.Restored.Incarnation})
				if err != nil {
					t.Fatal(err)
				}
				defer r.Close()
				assertProducerBeads(t, r, archive)
				for name, file := range archive.Manifest().Files {
					want, _ := archive.Member(name)
					got, err := r.Blob(ctx, driver.Hash(file.SHA256))
					if err != nil || !bytes.Equal(got, want) {
						t.Fatalf("source member %s was not retained byte-for-byte: %v", name, err)
					}
				}
				var truncated driver.ErrTruncated
				zero := model.Natural64(0)
				if _, err = s.Read(ctx, driver.View{Incarnation: report.Restored.Incarnation, At: &zero}); !errors.As(err, &truncated) {
					t.Fatalf("below-floor as-of did not refuse: %T %v", err, err)
				}
				if _, err = r.Changesets(ctx, nil, 10); !errors.As(err, &truncated) {
					t.Fatalf("below-floor changesets did not refuse: %T %v", err, err)
				}
				var stale driver.ErrIncarnationChanged
				if _, err = s.Read(ctx, driver.View{Incarnation: old.Incarnation}); !errors.As(err, &stale) {
					t.Fatalf("old incarnation was not fenced: %v", err)
				}
				ref := model.RefByID("demo-7", model.Natural64(0))
				versions, err := r.Versions(ctx, ref, driver.PageReq{Limit: 10})
				wantVersions := 2
				if mode == driver.RestoreHistoryModeSnapshot {
					wantVersions = 1
				}
				if err != nil || len(versions.Items) != wantVersions || !versions.Items[0].Base {
					t.Fatalf("source versions: %+v %v", versions, err)
				}
				marker, err := r.Get(ctx, []model.Ref{model.RefByID(driver.OfflineHistoryEvidenceID, model.Natural64(0))})
				if mode == driver.RestoreHistoryModeSnapshot {
					if err != nil || len(marker) != 0 {
						t.Fatalf("snapshot claimed a source replay: %+v %v", marker, err)
					}
					return
				}
				if err != nil || len(marker) != 1 {
					t.Fatalf("source history evidence missing: %v", err)
				}
				var evidence driver.HistoryImportEvidence
				if err = json.Unmarshal(marker[0].Body, &evidence); err != nil {
					t.Fatal(err)
				}
				wantEvidence := driver.HistoryImportEvidence{Source: dumpformat.SourceHistoryFormat, Mode: mode, Complete: false, First: 3, Head: 3, Floor: model.Natural64(1), Gaps: []driver.SourceHistoryGap{{First: 1, Last: 1, Reason: "unmappable"}, {First: 2, Last: 2, Reason: "not-retained"}}, Mapping: []driver.SourceImportRevision{{Sequence: 3, Revision: model.Natural64(2), EventIDs: []string{"2"}, CommitIDs: []string{history.Commits[2].ID}}}}
				if !reflect.DeepEqual(evidence, wantEvidence) {
					t.Fatalf("source/import mapping or exact gaps changed: %+v", evidence)
				}
				for revision, sequence := range map[uint64]uint64{1: 2, 2: 3} {
					at := model.Natural64(revision)
					image, err := s.Read(ctx, driver.View{Incarnation: report.Restored.Incarnation, At: &at})
					if err != nil {
						t.Fatal(err)
					}
					source, err := archive.HistoryImage(sequence)
					if err != nil {
						image.Close()
						t.Fatal(err)
					}
					assertProducerBeads(t, image, source)
					image.Close()
				}
				if versions.Items[0].Revision != model.Natural64(1) || versions.Items[1].Revision != model.Natural64(2) || versions.Items[1].Base {
					t.Fatalf("actual source update was flattened: %+v", versions.Items)
				}
				entries, err := r.Changesets(ctx, &driver.Cursor{Incarnation: report.Restored.Incarnation, Revision: report.Restored.HistoryFloor}, 10)
				if err != nil || len(entries) != 2 {
					t.Fatalf("source replay/receipt census: %+v %v", entries, err)
				}
				for _, entry := range entries {
					if !reflect.DeepEqual(entry.Principal, actor) || entry.Kind != model.ChangesetRestore || !entry.Time.Equal(when) {
						t.Fatalf("source identity/time promoted into authority: %+v", entry)
					}
				}
				if _, err = converterroundtrip.RecoverImport(ctx, s, p, converterroundtrip.Verification{ArchiveSHA256: p.ArchiveDigest, DerivedSHA256: p.DerivedDigest}); err != nil {
					t.Fatalf("actual source replay recovery: %v", err)
				}
			})
		}
	}
}

func assertProducerBeads(t *testing.T, r driver.Reader, source *dumpformat.Archive) {
	t.Helper()
	for _, bead := range source.Records("bead") {
		rows, err := r.Get(context.Background(), []model.Ref{model.RefByID(bead.ID, model.Natural64(0))})
		if err != nil || len(rows) != 1 {
			t.Fatalf("source bead %s missing: %v", bead.ID, err)
		}
		doc, err := pipeline.DecodeDocument(rows[0].Body)
		if err != nil || doc.Fields["title"] != bead.Body["title"] {
			t.Fatalf("source bead %s image changed: %+v %v", bead.ID, doc, err)
		}
		for _, field := range dumpformat.OpaqueIssueFields() {
			value, present := bead.Source.Fields[field]
			if !present {
				t.Fatalf("producer omitted opaque field %s", field)
			}
			want, err := json.Marshal(value)
			if err != nil || !bytes.Equal(doc.Metadata["beads."+field], want) {
				t.Fatalf("bead %s opaque %s changed: %s != %s (%v)", bead.ID, field, doc.Metadata["beads."+field], want, err)
			}
		}
		for _, comment := range doc.Comments {
			if !reflect.DeepEqual(comment.Principal, actor) || comment.Origin != "import" || comment.SourceAuthor == "" || comment.SourceTime == nil {
				t.Fatalf("source comment attribution was fabricated: %+v", comment)
			}
		}
	}
}

func TestDoltProducerPartialArchiveStrictRefusalIsAtomic(t *testing.T) {
	data, _ := doltProducerFixture(t)
	ctx := context.Background()
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		for _, mode := range []driver.RestoreHistoryMode{"", driver.RestoreHistoryModeFull} {
			t.Run(engine+"/"+string(mode), func(t *testing.T) {
				inspection, err := converterroundtrip.InspectImport(ctx, bytes.NewReader(data))
				if err != nil {
					t.Fatal(err)
				}
				p := profile(mode)
				p.SourceStoreID, p.ArchiveDigest, p.DerivedDigest = inspection.SourceStoreID, inspection.ArchiveSHA256, inspection.DerivedSHA256
				s := openStore(t, engine)
				before, err := s.Meta(ctx)
				if err != nil {
					t.Fatal(err)
				}
				_, err = converterroundtrip.RestoreImport(ctx, bytes.NewReader(data), s, p)
				var refused driver.ErrRestoreHistoryRefused
				if !errors.As(err, &refused) || refused.Mode != driver.RestoreHistoryModeFull || refused.Reason != driver.RestoreHistoryReasonNonContiguous {
					t.Fatalf("actual incomplete Dolt history was not strictly refused: %T %v", err, err)
				}
				after, err := s.Meta(ctx)
				if err != nil || !reflect.DeepEqual(before, after) {
					t.Fatalf("strict refusal changed target metadata: %+v %v", after, err)
				}
				r, err := s.Read(ctx, driver.View{Incarnation: before.Incarnation})
				if err != nil {
					t.Fatal(err)
				}
				defer r.Close()
				rows, err := r.Query(ctx, driver.Query{Limit: 1})
				if err != nil || len(rows.Items) != 0 {
					t.Fatalf("strict refusal published rows: %+v %v", rows, err)
				}
			})
		}
	}
}
