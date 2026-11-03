package offlinehistory_test

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/integration/converterroundtrip"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// Observe the real target boundary without hiding optional transaction
// capabilities. The unchanged archive must still restore through this wrapper.
type historyEdgeTarget struct {
	driver.Store
	leases, writes int
}

func (s *historyEdgeTarget) Lease(ctx context.Context, holder string, ttl time.Duration) (driver.ActiveLease, error) {
	s.leases++
	return s.Store.Lease(ctx, holder, ttl)
}

func (s *historyEdgeTarget) Write(ctx context.Context, lease driver.ActiveLease) (driver.WriteTx, error) {
	s.writes++
	return s.Store.Write(ctx, lease)
}

func historyEdgeDigest(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// Only negative inputs are repacked. Every other member is copied verbatim
// from the pinned SQLSource archive; no verified source history is generated.
func historyEdgeArchive(t *testing.T, source *dumpformat.Archive, omit bool) []byte {
	t.Helper()
	manifest := source.Manifest()
	members := map[string][]byte{}
	for name := range manifest.Files {
		members[name], _ = source.Member(name)
	}
	if omit {
		delete(members, dumpformat.SourceHistoryMember)
		delete(manifest.Files, dumpformat.SourceHistoryMember)
	} else {
		history, _ := source.SourceHistory()
		// Keep the original head, frame count and commit ID, but break the
		// retained sequence. Checksums cannot establish range continuity.
		history.Range.Frames[0].Sequence--
		data, err := json.Marshal(history)
		if err != nil {
			t.Fatal(err)
		}
		data = append(data, '\n')
		members[dumpformat.SourceHistoryMember] = data
		manifest.Files[dumpformat.SourceHistoryMember] = dumpformat.File{Size: int64(len(data)), SHA256: historyEdgeDigest(data)}
	}
	data, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	members["manifest.json"] = append(data, '\n')
	names := make([]string, 0, len(manifest.Files))
	for name := range manifest.Files {
		names = append(names, name)
	}
	sort.Strings(names)
	var out bytes.Buffer
	w := tar.NewWriter(&out)
	for _, name := range append([]string{"manifest.json"}, names...) {
		data := members[name]
		if err := w.WriteHeader(&tar.Header{Name: name, Size: int64(len(data)), Mode: 0600, Typeflag: tar.TypeReg, Format: tar.FormatUSTAR}); err != nil {
			t.Fatal(err)
		}
		if _, err := w.Write(data); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return out.Bytes()
}

func historyEdgeUnchanged(t *testing.T, target *historyEdgeTarget, before driver.Meta, source *dumpformat.Archive, p driver.RestoreImportProfile) {
	t.Helper()
	ctx := context.Background()
	if target.leases != 0 || target.writes != 0 {
		t.Fatalf("history refusal reached target lease/write: %d/%d", target.leases, target.writes)
	}
	after, err := target.Meta(ctx)
	if err != nil || after != before {
		t.Fatalf("history refusal changed durable metadata: %+v %v", after, err)
	}
	r, err := target.Read(ctx, driver.View{Incarnation: before.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	rows, err := r.Query(ctx, driver.Query{Limit: 1})
	if err != nil || len(rows.Items) != 0 || rows.NextCursor != nil {
		t.Fatalf("history refusal published records: %+v %v", rows, err)
	}
	entries, err := r.Changesets(ctx, nil, 1)
	if err != nil || len(entries) != 0 {
		t.Fatalf("history refusal published changesets: %+v %v", entries, err)
	}
	principal, err := driver.PrincipalKey(p.Admission.Actor)
	if err != nil {
		t.Fatal(err)
	}
	if receipt, err := r.IdempotencyByKey(ctx, principal, p.AgentLabel(), p.Method, p.IdempotencyKey); err != nil || receipt != nil {
		t.Fatalf("history refusal published a receipt: %+v %v", receipt, err)
	}
	for name, file := range source.Manifest().Files {
		if _, err := r.Blob(ctx, driver.Hash(file.SHA256)); !errors.Is(err, driver.ErrNotFound) {
			t.Fatalf("history refusal published archive member %s: %v", name, err)
		}
	}
	allocators, ok := r.(driver.AllocatorReader)
	if !ok {
		t.Fatal("real target lacks allocator inspection")
	}
	for _, project := range source.Manifest().Projects {
		if _, err := allocators.LoadAllocator(ctx, project.Prefix); !errors.Is(err, driver.ErrNotFound) {
			t.Fatalf("history refusal published allocator %s: %v", project.Prefix, err)
		}
	}
}

func TestDoltHistoryMemberRefusalBeforeTargetAccessAllDrivers(t *testing.T) {
	data, source := doltProducerFixture(t)
	ctx := context.Background()
	inspection, err := converterroundtrip.InspectImport(ctx, bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			target := &historyEdgeTarget{Store: openStore(t, engine)}
			before, err := target.Meta(ctx)
			if err != nil {
				t.Fatal(err)
			}
			p := profile(driver.RestoreHistoryModePartial)
			p.SourceStoreID, p.ArchiveDigest, p.DerivedDigest = inspection.SourceStoreID, inspection.ArchiveSHA256, inspection.DerivedSHA256
			p, err = converterroundtrip.BindHistoryImport(ctx, bytes.NewReader(data), p, time.Now().UTC())
			if err != nil {
				t.Fatal(err)
			}
			for _, omit := range []bool{true, false} {
				name, want := "discontinuous-history-member", driver.RestoreHistoryReasonNonContiguous
				if omit {
					name, want = "omitted-history-member", driver.RestoreHistoryReasonNotYetSupported
				}
				if !t.Run(name, func(t *testing.T) {
					negative := historyEdgeArchive(t, source, omit)
					a, err := dumpformat.Read(bytes.NewReader(negative))
					if omit {
						// The envelope and current source rows remain valid. The
						// member's absence removes the partial replay capability.
						if err != nil {
							t.Fatalf("omitted-member control failed envelope validation: %v", err)
						}
						if _, present := a.SourceHistory(); present {
							t.Fatal("omitted source history was reconstructed")
						}
					} else {
						var invalid dumpformat.ErrSourceHistory
						if !errors.As(err, &invalid) || invalid.Reason != string(want) {
							t.Fatalf("digest-valid control did not reach semantic range validation: %T %v", err, err)
						}
					}
					bound := p
					bound.ArchiveDigest = historyEdgeDigest(negative)
					_, err = converterroundtrip.RestoreImport(ctx, bytes.NewReader(negative), target, bound)
					var refused driver.ErrRestoreHistoryRefused
					if !errors.As(err, &refused) || refused.Mode != p.HistoryMode || refused.Reason != want {
						t.Fatalf("history member refusal=%T %v, want partial/%s", err, err, want)
					}
					historyEdgeUnchanged(t, target, before, source, bound)
					t.Logf("HISTORY_MEMBER_REFUSAL driver=%s case=%s reason=%s archive=%s leases=%d writes=%d", engine, name, refused.Reason, bound.ArchiveDigest, target.leases, target.writes)
				}) {
					return
				}
			}
			t.Run("valid-partial-after-refusals", func(t *testing.T) {
				report, err := converterroundtrip.RestoreImport(ctx, bytes.NewReader(data), target, p)
				if err != nil || target.leases != 1 || target.writes != 1 || report.Restored.Head != model.Natural64(3) || report.Restored.HistoryFloor != model.Natural64(1) || report.Restored.Incarnation == before.Incarnation {
					t.Fatalf("unchanged archive failed real partial restore: %+v leases=%d writes=%d err=%v", report.Restored, target.leases, target.writes, err)
				}
				r, err := target.Read(ctx, driver.View{Incarnation: report.Restored.Incarnation})
				if err != nil {
					t.Fatal(err)
				}
				defer r.Close()
				assertProducerBeads(t, r, source)
				when, err := time.Parse(time.RFC3339Nano, p.HistoryTime)
				if err != nil {
					t.Fatal(err)
				}
				ref := model.RefByID("demo-7", model.Natural64(0))
				images := make([][]byte, 2)
				for i, sequence := range []uint64{2, 3} {
					at := model.Natural64(uint64(i + 1))
					image, err := target.Read(ctx, driver.View{Incarnation: report.Restored.Incarnation, At: &at})
					if err != nil {
						t.Fatal(err)
					}
					original, err := source.HistoryImage(sequence)
					if err != nil {
						image.Close()
						t.Fatal(err)
					}
					assertProducerBeads(t, image, original)
					rows, err := image.Get(ctx, []model.Ref{ref})
					if err != nil || len(rows) != 1 || rows[0].Revision != at {
						image.Close()
						t.Fatalf("same-target source image at revision %s: %+v %v", at, rows, err)
					}
					images[i] = append([]byte(nil), rows[0].Body...)
					if err := image.Close(); err != nil {
						t.Fatal(err)
					}
				}
				versions, err := r.Versions(ctx, ref, driver.PageReq{Limit: 10})
				if err != nil || versions.NextCursor != nil || len(versions.Items) != 2 {
					t.Fatalf("same-target native version census: %+v %v", versions, err)
				}
				for i, version := range versions.Items {
					if version.Revision != model.Natural64(uint64(i+1)) || version.Ref != ref || version.Deleted || version.Base != (i == 0) || !version.CreatedAt.Equal(when) || !reflect.DeepEqual(version.Principal, p.Admission.Actor) {
						t.Fatalf("same-target source version was flattened or reattributed: %+v", version)
					}
				}
				if !bytes.Equal(versions.Items[0].Body, images[0]) || len(versions.Items[0].Patch) != 0 {
					t.Fatal("native base differs from same-target source baseline bytes")
				}
				var patch []struct {
					Op, Path string
					Value    json.RawMessage
				}
				if err := json.Unmarshal(versions.Items[1].Patch, &patch); err != nil || len(versions.Items[1].Body) != 0 || len(patch) != 1 || patch[0].Op != "replace" || patch[0].Path != "" || !bytes.Equal(patch[0].Value, images[1]) {
					t.Fatalf("native update differs from same-target source frame bytes: %+v %v", patch, err)
				}
				entries, err := r.Changesets(ctx, &driver.Cursor{Incarnation: report.Restored.Incarnation, Revision: report.Restored.HistoryFloor}, 10)
				if err != nil || len(entries) != 2 {
					t.Fatalf("same-target retained changeset census: %+v %v", entries, err)
				}
				for i, entry := range entries {
					if entry.Revision != model.Natural64(uint64(i+2)) || entry.Incarnation != report.Restored.Incarnation || !entry.Branch.IsZero() || entry.Kind != model.ChangesetRestore || !entry.Time.Equal(when) || !reflect.DeepEqual(entry.Principal, p.Admission.Actor) {
						t.Fatalf("same-target retained changeset order/attribution changed: %+v", entry)
					}
				}
				zero := model.Natural64(0)
				old, err := target.Read(ctx, driver.View{Incarnation: report.Restored.Incarnation, At: &zero})
				if err == nil {
					old.Close()
					t.Fatal("same-target read below retained floor succeeded")
				}
				var truncated driver.ErrTruncated
				if !errors.As(err, &truncated) || truncated.Floor != report.Restored.HistoryFloor {
					t.Fatalf("same-target read below retained floor: %v", err)
				}
				if _, err := r.Changesets(ctx, nil, 10); !errors.As(err, &truncated) || truncated.Floor != report.Restored.HistoryFloor {
					t.Fatalf("same-target changesets below retained floor: %v", err)
				}
				historyBytes, _ := source.Member(dumpformat.SourceHistoryMember)
				stored, err := r.Blob(ctx, driver.Hash(historyEdgeDigest(historyBytes)))
				if err != nil || !bytes.Equal(stored, historyBytes) {
					t.Fatalf("partial restore changed original history member bytes: %v", err)
				}
				rows, err := r.Get(ctx, []model.Ref{model.RefByID(driver.OfflineHistoryEvidenceID, model.Natural64(0))})
				if err != nil || len(rows) != 1 {
					t.Fatalf("partial restore lost source evidence: %+v %v", rows, err)
				}
				var evidence driver.HistoryImportEvidence
				if err := json.Unmarshal(rows[0].Body, &evidence); err != nil {
					t.Fatal(err)
				}
				history, _ := source.SourceHistory()
				gaps := make([]driver.SourceHistoryGap, 0, len(history.Range.Gaps))
				for _, gap := range history.Range.Gaps {
					gaps = append(gaps, driver.SourceHistoryGap{First: gap.First, Last: gap.Last, Reason: gap.Reason})
				}
				mapping := []driver.SourceImportRevision{{Sequence: history.Range.First, Revision: model.Natural64(2), EventIDs: []string{"2"}, CommitIDs: []string{history.Range.Frames[0].CommitID}}}
				want := driver.HistoryImportEvidence{Source: dumpformat.SourceHistoryFormat, Mode: p.HistoryMode, First: history.Range.First, Head: history.Range.Head, Floor: report.Restored.HistoryFloor, Gaps: gaps, Mapping: mapping}
				if !reflect.DeepEqual(evidence, want) {
					t.Fatalf("partial restore changed retained source range: %+v", evidence)
				}
				t.Logf("HISTORY_MEMBER_POSITIVE driver=%s archive=%s head=%s floor=%s leases=%d writes=%d", engine, p.ArchiveDigest, report.Restored.Head, report.Restored.HistoryFloor, target.leases, target.writes)
			})
		})
	}
}
