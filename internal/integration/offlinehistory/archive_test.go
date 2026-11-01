package offlinehistory_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/integration/converterroundtrip"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestSourceArchiveHistoryRoundTripAllDrivers(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		for _, mode := range []driver.RestoreHistoryMode{driver.RestoreHistoryModeFull, driver.RestoreHistoryModePartial, driver.RestoreHistoryModeSnapshot} {
			t.Run(engine+"/"+string(mode), func(t *testing.T) {
				ctx := context.Background()
				path := "testdata/full.tar"
				if mode == driver.RestoreHistoryModePartial {
					path = "testdata/partial.tar"
				}
				data, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				inspection, err := converterroundtrip.InspectImport(ctx, bytes.NewReader(data))
				if err != nil {
					t.Fatal(err)
				}
				p := profile(mode)
				p.SourceStoreID, p.ArchiveDigest, p.DerivedDigest = inspection.SourceStoreID, inspection.ArchiveSHA256, inspection.DerivedSHA256
				if mode != driver.RestoreHistoryModeSnapshot {
					p, err = converterroundtrip.BindHistoryImport(ctx, bytes.NewReader(data), p, time.Now().UTC())
					if err != nil {
						t.Fatal(err)
					}
				}
				s := openStore(t, engine)
				old, _ := s.Meta(ctx)
				lease, err := s.Lease(ctx, "archive-history", driver.LeaseTTL)
				if err != nil {
					t.Fatal(err)
				}
				defer lease.Release(context.Background())
				report, err := converterroundtrip.RestoreImportWithLease(ctx, bytes.NewReader(data), s, lease, p)
				if err != nil {
					t.Fatal(err)
				}
				if report.Restored.Incarnation == old.Incarnation {
					t.Fatal("restore retained old incarnation")
				}
				wantHead, floor := "5", "0"
				if mode == driver.RestoreHistoryModePartial {
					wantHead, floor = "3", "1"
				}
				if mode == driver.RestoreHistoryModeSnapshot {
					wantHead, floor = "1", "1"
				}
				if report.Restored.Head.String() != wantHead || report.Restored.HistoryFloor.String() != floor {
					t.Fatalf("meta: %+v", report.Restored)
				}
				r, err := s.Read(ctx, driver.View{Incarnation: report.Restored.Incarnation})
				if err != nil {
					t.Fatal(err)
				}
				defer r.Close()
				ref := model.RefByID("demo-7", model.Natural64(0))
				rows, err := r.Get(ctx, []model.Ref{ref})
				if err != nil || len(rows) != 1 {
					t.Fatalf("head: %v", err)
				}
				var doc pipeline.Document
				if err = json.Unmarshal(rows[0].Body, &doc); err != nil {
					t.Fatal(err)
				}
				archive, err := dumpformat.Read(bytes.NewReader(data))
				if err != nil {
					t.Fatal(err)
				}
				var source dumpformat.Record
				for _, row := range archive.Records("bead") {
					if row.ID == "demo-7" {
						source = row
					}
				}
				for _, field := range dumpformat.OpaqueIssueFields() {
					want, _ := json.Marshal(source.Source.Fields[field])
					if !bytes.Equal(doc.Metadata["beads."+field], want) {
						t.Fatalf("opaque %s changed: %s != %s", field, doc.Metadata["beads."+field], want)
					}
				}
				if mode != driver.RestoreHistoryModeSnapshot {
					marker, err := r.Get(ctx, []model.Ref{model.RefByID(driver.OfflineHistoryEvidenceID, model.Natural64(0))})
					if err != nil || len(marker) != 1 || !bytes.Contains(marker[0].Body, []byte(strings.Repeat("d", 32))) {
						t.Fatalf("provenance missing: %v", err)
					}
					at := model.Natural64(1)
					historical, err := s.Read(ctx, driver.View{Incarnation: report.Restored.Incarnation, At: &at})
					if err != nil {
						t.Fatal(err)
					}
					before, err := historical.Get(ctx, []model.Ref{ref})
					historical.Close()
					if err != nil || len(before) != 1 || !bytes.Contains(before[0].Body, []byte("Historical title")) {
						t.Fatalf("historical image: %v", err)
					}
				}
				if floor == "1" {
					at := model.Natural64(0)
					_, err = s.Read(ctx, driver.View{Incarnation: report.Restored.Incarnation, At: &at})
					var truncated driver.ErrTruncated
					if !errors.As(err, &truncated) {
						t.Fatalf("floor refusal: %T %v", err, err)
					}
				}
				_, err = converterroundtrip.RecoverImport(ctx, s, p, converterroundtrip.Verification{ArchiveSHA256: p.ArchiveDigest, DerivedSHA256: p.DerivedDigest})
				if err != nil {
					t.Fatalf("receipt recovery: %v", err)
				}
			})
		}
	}
}
