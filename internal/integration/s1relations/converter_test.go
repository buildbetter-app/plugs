package s1relations_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"os"
	"path/filepath"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
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
				f := newFixture(t, kind, []string{"s1-parent", "s1-target"})
				var child model.Ref
				if populated {
					child, _, _ = seedRelations(t, f)
					if child.ID != "s1-parent.1" {
						t.Fatalf("actual relational child identity: %+v", child)
					}
				}
				readChild := func(label string) {
					if !populated {
						return
					}
					got := f.call("bead.get", label, map[string]any{"id": child})
					var result struct {
						Bead struct {
							ID string `json:"id"`
						} `json:"bead"`
					}
					if got.Error != nil || json.Unmarshal(got.Result, &result) != nil || result.Bead.ID != child.ID {
						t.Fatalf("retained actual relational child became unreadable: %+v", got)
					}
				}
				readChild("before-refusal-child")
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
				readChild("after-refusal-child")
				f.reopen()
				if !bytes.Equal(before, f.image()) {
					t.Fatal("refused conversion changed durable state after SQL reopen/server reconstruction")
				}
				equalReceipt(t, currentReceipt, f.receipt("bead.close", "close"))
				readChild("reopened-retained-child")
				t.Logf("B27 %s/%s: public refusal %v conserved graph/history/reference indexes and current receipt", kind, name, want)
			})
		}
	})
}
