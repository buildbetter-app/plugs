package s1relations_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"os"
	"path/filepath"
	"testing"

	"github.com/buildbetter-app/plugs/internal/integration/converterroundtrip"
)

var archivePath = flag.String("archive", "", "unchanged public exporter archive runfile")

func exporterArchive(t *testing.T) []byte {
	t.Helper()
	if *archivePath == "" || os.Getenv("TEST_SRCDIR") == "" {
		t.Fatal("Bazel must supply the retained public exporter archive")
	}
	data, err := os.ReadFile(filepath.Join(os.Getenv("TEST_SRCDIR"), *archivePath))
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(data)
	if hex.EncodeToString(digest[:]) != "c510f8e0b301086e1c9fdab6ed3dc60ea8da73597c4058a03d0abc478482ae3c" {
		t.Fatal("retained exporter archive changed without reviewed repinning")
	}
	return data
}

func TestB27PublicConverterRefusalsPreserveRelationalTargetsAcrossAllStores(t *testing.T) {
	data := exporterArchive(t)
	stores(t, func(t *testing.T, kind string) {
		for _, populated := range []bool{false, true} {
			name := "empty"
			if populated {
				name = "populated"
			}
			t.Run(name, func(t *testing.T) {
				f := newFixture(t, kind, []string{"s1-parent", "s1-target", "s1-child"})
				if populated {
					seedRelations(t, f)
				}
				before := f.image()
				currentReceipt := f.receipt("bead.close", "close")
				if populated && currentReceipt == nil {
					t.Fatal("populated target did not retain its real public close receipt")
				}
				report, err := converterroundtrip.RestoreWithLease(f.ctx, bytes.NewReader(data), f.store, f.lease)
				want := converterroundtrip.ErrRestoreUnavailable
				if populated {
					want = converterroundtrip.ErrTargetNotEmpty
				}
				if !errors.Is(err, want) || report.Archive == nil || report.Restored.StoreID != "" || report.IdempotencyKey != "" || report.Recovered {
					t.Fatalf("public converter refusal was represented as a restore: %+v err=%v", report, err)
				}
				manifest := report.Archive.Manifest()
				if manifest.History != "available-unverified" || manifest.Reference != "9d6fff760b7a314b476539198504d77ce0959656" {
					t.Fatalf("public converter lost the retained source disclosure: %+v", manifest)
				}
				if !bytes.Equal(before, f.image()) {
					t.Fatal("refused conversion changed target records, versions, events, head or reference indexes")
				}
				equalReceipt(t, currentReceipt, f.receipt("bead.close", "close"))
				f.reopen()
				if !bytes.Equal(before, f.image()) {
					t.Fatal("refused conversion changed durable state after SQL reopen/server reconstruction")
				}
				equalReceipt(t, currentReceipt, f.receipt("bead.close", "close"))
				if populated {
					got := f.call("bead.get", "retained-child", map[string]any{"id": ref("s1-child")})
					if got.Error != nil {
						t.Fatalf("retained relational target became unreadable: %+v", got)
					}
				}
				t.Logf("B27 %s/%s: public refusal %v conserved graph/history/reference indexes and current receipt", kind, name, want)
			})
		}
	})
}
