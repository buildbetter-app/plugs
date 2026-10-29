package restore_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/restore"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

func TestOfflineFixtureRestoreRetainsKindsAndSecrets(t *testing.T) {
	p := filepath.Join("..", "..", "integration", "converterroundtrip", "testdata", "synthetic-plug-dump-1.tar")
	if root := os.Getenv("TEST_SRCDIR"); root != "" {
		p = filepath.Join(root, os.Getenv("TEST_WORKSPACE"), "internal/integration/converterroundtrip/testdata/synthetic-plug-dump-1.tar")
	}
	b, e := os.ReadFile(p)
	if e != nil {
		t.Fatal(e)
	}
	a, e := dumpformat.Read(bytes.NewReader(b))
	if e != nil {
		t.Fatal(e)
	}
	mb, _ := a.Member("manifest.json")
	for _, driverName := range []string{"memory", "sqlite"} {
		t.Run(driverName, func(t *testing.T) {
			ctx := context.Background()
			var store driver.Store
			var err error
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
			mgr := restore.NewManager(store, lease)
			var manifest map[string]any
			if err := json.Unmarshal(mb, &manifest); err != nil {
				t.Fatal(err)
			}
			presence := restore.Presence{"mode": "presence_check", "challenge": "c", "proof": "p"}
			st, err := mgr.Begin(ctx, manifest, "full", presence)
			if err != nil {
				t.Fatal(err)
			}
			for i := 0; i < len(b); i += 512 {
				end := i + 512
				if end > len(b) {
					end = len(b)
				}
				chunk := b[i:end]
				st, err = mgr.Chunk(ctx, st.RestoreID, uint64(i/512), chunk, sha(chunk), presence)
				if err != nil {
					t.Fatal(err)
				}
			}
			if _, err = mgr.Finish(ctx, st.RestoreID, sha(mb), model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}, model.WriteMeta{Method: "admin.restore.finish", IdempotencyKey: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}, presence); err != nil {
				t.Fatal(err)
			}
			meta, err := store.Meta(ctx)
			if err != nil {
				t.Fatal(err)
			}
			r, err := store.Read(ctx, driver.View{Incarnation: meta.Incarnation})
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			pg, err := r.Query(ctx, driver.Query{Limit: 10000})
			if err != nil {
				t.Fatal(err)
			}
			if len(pg.Items) != 113 {
				t.Fatalf("records=%d", len(pg.Items))
			}
			counts := map[string]int{}
			secretReference := false
			beadIDs := map[string]bool{}
			for _, record := range pg.Items {
				counts[record.Kind]++
				if record.Kind == "bead" {
					beadIDs[record.Ref.ID] = true
				}
				if record.Kind == "config" && strings.Contains(string(record.Body), "$secret") {
					secretReference = true
				}
			}
			if !secretReference {
				t.Fatal("secret reference was not retained")
			}
			for _, id := range []string{"demo-7", "demo-7.4", "demo-8"} {
				if !beadIDs[id] {
					t.Fatalf("restored source ID %s missing", id)
				}
			}
			for kind, want := range map[string]int{"bead": 3, "edge": 1, "comment": 1, "memory": 1, "config": 2, "beads-archive": 105} {
				if counts[kind] != want {
					t.Fatalf("%s=%d want %d", kind, counts[kind], want)
				}
			}
		})
	}
}
func sha(b []byte) string { var h [32]byte; h = sha256.Sum256(b); return hex.EncodeToString(h[:]) }
