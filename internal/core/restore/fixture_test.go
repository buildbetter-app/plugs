package restore_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/restore"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

// The BUILD-13 converter owns conversion of this Beads archive. Until a
// driver seam can retain its allocator and disclosure metadata, restore must
// reject it before the destination publication transaction.
func TestOfflineConverterArchiveRefusesBeforeMutation(t *testing.T) {
	p := filepath.Join("..", "..", "integration", "converterroundtrip", "testdata", "synthetic-plug-dump-1.tar")
	if root := os.Getenv("TEST_SRCDIR"); root != "" {
		p = filepath.Join(root, os.Getenv("TEST_WORKSPACE"), "internal/integration/converterroundtrip/testdata/synthetic-plug-dump-1.tar")
	}
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	a, err := dumpformat.Read(bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	manifestBytes, ok := a.Member("manifest.json")
	if !ok {
		t.Fatal("converter archive has no manifest")
	}
	var manifest map[string]any
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		t.Fatal(err)
	}
	canonical, err := json.Marshal(a.Manifest())
	if err != nil {
		t.Fatal(err)
	}
	for _, driverName := range []string{"memory", "sqlite"} {
		t.Run(driverName, func(t *testing.T) {
			ctx := context.Background()
			var store driver.Store
			if driverName == "memory" {
				store = memory.New()
			} else {
				store, err = (sqlite.Driver{}).Open(ctx, driver.Config{DSN: filepath.Join(t.TempDir(), "fixture.db")})
				if err != nil {
					t.Fatal(err)
				}
			}
			defer store.Close()
			lease, err := store.Lease(ctx, "fixture-"+driverName, driver.LeaseTTL)
			if err != nil {
				t.Fatal(err)
			}
			defer lease.Release(ctx)
			mgr := restore.NewManager(store, lease, trustedPresenceVerifier())
			presence := restore.Presence{"mode": "presence_check", "challenge": "restore-test", "proof": "restore-test"}
			status, err := mgr.Begin(ctx, manifest, "head", presence)
			if err != nil {
				t.Fatal(err)
			}
			for offset, page := 0, uint64(0); offset < len(b); offset, page = offset+512, page+1 {
				end := offset + 512
				if end > len(b) {
					end = len(b)
				}
				chunk := b[offset:end]
				status, err = mgr.Chunk(ctx, status.RestoreID, page, chunk, sha(chunk), presence)
				if err != nil {
					t.Fatal(err)
				}
			}
			_, err = mgr.Finish(ctx, status.RestoreID, sha(canonical), model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}, model.WriteMeta{Method: "admin.restore.finish", IdempotencyKey: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}, presence)
			if err == nil {
				t.Fatal("unsupported converter archive was accepted")
			}
			meta, err := store.Meta(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if !meta.Head.IsZero() || !meta.HistoryFloor.IsZero() {
				t.Fatalf("refusal mutated target metadata: %+v", meta)
			}
			reader, err := store.Read(ctx, driver.View{Incarnation: meta.Incarnation})
			if err != nil {
				t.Fatal(err)
			}
			page, err := reader.Query(ctx, driver.Query{Limit: 1})
			_ = reader.Close()
			if err != nil {
				t.Fatal(err)
			}
			if len(page.Items) != 0 {
				t.Fatalf("refusal wrote %d records", len(page.Items))
			}
		})
	}
}

func sha(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}
