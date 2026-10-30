package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/api/wire"
	"github.com/buildbetter-app/plugs/internal/core/restore"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

var testPresence = map[string]any{"mode": "presence_check", "challenge": "cli-test", "proof": "cli-test"}

func testPresenceVerifier() restore.PresenceVerifier {
	return restore.PresenceVerifierFunc(func(_ context.Context, method, digest string, presence restore.Presence) error {
		if method == "" || digest == "" || presence["proof"] != "cli-test" {
			return fmt.Errorf("unbound test presence")
		}
		return nil
	})
}

func TestDumpAndRestoreCommandsUsePagedRPC(t *testing.T) {
	ctx := context.Background()
	source, err := OpenLocal(ctx, LocalConfig{Project: "cli-fixture", Agent: "source", Storage: StorageConfig{Driver: "memory"}, PresenceVerifier: testPresenceVerifier()})
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	if _, err := source.Client.Call(ctx, "bead.create", map[string]any{"type": "issue", "fields": map[string]any{"title": "CLI archive"}}, ""); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "fixture.tar")
	var out, errOut bytes.Buffer
	if err := (&Session{Client: source.Client, Out: &out, Err: &errOut, HumanPresence: testPresence}).Run(ctx, []string{"dump", "--history", "head", "--output", path, "--page-size", "512"}); err != nil {
		t.Fatalf("dump: %v; stderr=%s", err, errOut.String())
	}
	if !strings.Contains(out.String(), "wrote ") {
		t.Fatalf("dump output: %s", out.String())
	}
	target, err := OpenLocal(ctx, LocalConfig{Project: "cli-fixture", Agent: "target", Storage: StorageConfig{Driver: "memory"}, PresenceVerifier: testPresenceVerifier()})
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	out.Reset()
	if err := (&Session{Client: target.Client, Out: &out, Err: &errOut, HumanPresence: testPresence}).Run(ctx, []string{"restore", path, "--history", "head"}); err != nil {
		t.Fatalf("restore: %v; stderr=%s", err, errOut.String())
	}
	if !strings.Contains(out.String(), "restored ") {
		t.Fatalf("restore output: %s", out.String())
	}
	ready, err := target.Client.Call(ctx, "bead.ready", map[string]any{"limit": 10}, "")
	if err != nil || !bytes.Contains(ready.JSON, []byte("CLI archive")) {
		t.Fatalf("restored CLI fixture not readable: %s %v", ready.JSON, err)
	}
}

func TestAdminCLIRefusesWithoutTrustedPresence(t *testing.T) {
	ctx := context.Background()
	local, err := OpenLocal(ctx, LocalConfig{Project: "cli-presence", Agent: "source", Storage: StorageConfig{Driver: "memory"}})
	if err != nil {
		t.Fatal(err)
	}
	defer local.Close()
	path := filepath.Join(t.TempDir(), "refused.tar")
	var out, errOut bytes.Buffer
	err = (&Session{Client: local.Client, Out: &out, Err: &errOut}).Run(ctx, []string{"dump", "--history", "head", "--output", path})
	if err == nil || !strings.Contains(err.Error(), "human presence") {
		t.Fatalf("dump without trusted presence was accepted: err=%v stderr=%q", err, errOut.String())
	}
	if _, statErr := os.Stat(path); !os.IsNotExist(statErr) {
		t.Fatalf("refused dump created %s: %v", path, statErr)
	}
}

func TestNativeRestoreSameSessionPagesHelloAndSQLiteNewSession(t *testing.T) {
	ctx := context.Background()
	source, err := OpenLocal(ctx, LocalConfig{Project: "native-pages", Agent: "source", Storage: StorageConfig{Driver: "memory"}, PresenceVerifier: testPresenceVerifier()})
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	for i := range 3 {
		if _, err := source.Client.Call(ctx, "bead.create", map[string]any{"type": "issue", "fields": map[string]any{"title": fmt.Sprintf("native page %d", i)}}, ""); err != nil {
			t.Fatal(err)
		}
	}
	archivePath := filepath.Join(t.TempDir(), "pages.tar")
	var out, errOut bytes.Buffer
	if err := (&Session{Client: source.Client, Out: &out, Err: &errOut, HumanPresence: testPresence}).Run(ctx, []string{"dump", "--history", "head", "--output", archivePath, "--page-size", "512"}); err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"memory", "sqlite"} {
		t.Run(kind, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "pages.db")
			var store driver.Store = memory.New()
			if kind == "sqlite" {
				store.Close()
				store, err = (sqlite.Driver{}).Open(ctx, driver.Config{DSN: path})
				if err != nil {
					t.Fatal(err)
				}
			}
			before, err := store.Meta(ctx)
			if err != nil {
				t.Fatal(err)
			}
			open := func(store driver.Store) *Local {
				t.Helper()
				local, err := OpenLocal(ctx, LocalConfig{Project: "native-pages", Agent: "target", Storage: StorageConfig{Driver: "provided"}, PresenceVerifier: testPresenceVerifier(), Factory: func(context.Context, StorageConfig) (driver.Store, func() error, error) {
					return store, store.Close, nil
				}})
				if err != nil {
					store.Close()
					t.Fatal(err)
				}
				return local
			}
			target := open(store)
			defer target.Close()
			out.Reset()
			if err := (&Session{Client: target.Client, Out: &out, Err: &errOut, HumanPresence: testPresence}).Run(ctx, []string{"restore", archivePath, "--history", "head"}); err != nil {
				t.Fatal(err)
			}
			after, err := store.Meta(ctx)
			if err != nil || after.Incarnation == before.Incarnation || after.Head.String() != "1" || after.HistoryFloor.String() != "1" {
				t.Fatalf("native publication=%+v %v", after, err)
			}
			checkPages := func(local *Local, currentStore driver.Store) {
				t.Helper()
				for _, method := range []string{"bead.ready", "history.query"} {
					params := map[string]any{"limit": 1}
					if method == "history.query" {
						params["asOf"] = map[string]any{"revision": after.Head}
					}
					seen := map[string]bool{}
					for i := range 3 {
						reply, err := local.Client.Call(ctx, method, params, "")
						if err != nil {
							t.Fatal(method, err)
						}
						var page struct {
							Items []struct {
								ID string `json:"id"`
							} `json:"items"`
							AsOf struct {
								Incarnation string      `json:"incarnation"`
								Revision    json.Number `json:"revision"`
							} `json:"asOf"`
							Next string `json:"nextCursor"`
						}
						if err := json.Unmarshal(reply.JSON, &page); err != nil || len(page.Items) != 1 || page.AsOf.Incarnation != after.Incarnation || page.AsOf.Revision.String() != "1" {
							t.Fatalf("%s page=%s %v", method, reply.JSON, err)
						}
						if seen[page.Items[0].ID] {
							t.Fatal("continuation repeated native record", reply.JSON)
						}
						seen[page.Items[0].ID] = true
						if (i < 2) != (page.Next != "") {
							t.Fatal("native cursor boundary", string(reply.JSON))
						}
						params["cursor"] = page.Next
					}
				}
				// Hello has no _meta parameter. Use its canonical raw request
				// against this same local server; Client.Call stamps _meta.
				hello := local.server.Dispatch(ctx, []byte(`{"jsonrpc":"2.0","id":"hello","method":"server.hello","params":{}}`))
				var metadata struct {
					Result struct {
						Incarnation string `json:"incarnation"`
					} `json:"result"`
					Error *wire.Error `json:"error"`
				}
				if err := json.Unmarshal(hello, &metadata); err != nil || metadata.Error != nil || metadata.Result.Incarnation != after.Incarnation {
					t.Fatalf("hello retained startup incarnation: %s %v", hello, err)
				}
				_, err = local.Client.Call(ctx, "history.query", map[string]any{"asOf": map[string]any{"revision": after.Head, "incarnation": before.Incarnation}}, "")
				var typed *wire.Error
				if !errors.As(err, &typed) || typed.Data.Code != "incarnation_changed" {
					t.Fatalf("explicit old history generation accepted: %v", err)
				}
				meta, err := currentStore.Meta(ctx)
				if err != nil || !reflect.DeepEqual(meta, after) {
					t.Fatalf("consumer requests mutated store: %+v %v", meta, err)
				}
			}
			checkPages(target, store)
			if kind == "sqlite" {
				if err := target.Close(); err != nil {
					t.Fatal(err)
				}
				reopened, err := (sqlite.Driver{}).Open(ctx, driver.Config{DSN: path})
				if err != nil {
					t.Fatal(err)
				}
				fresh := open(reopened)
				defer fresh.Close()
				checkPages(fresh, reopened)
			}
		})
	}
}

type failedCurrentMetadataStore struct {
	driver.Store
	fail bool
}

func (s *failedCurrentMetadataStore) Meta(ctx context.Context) (driver.Meta, error) {
	if s.fail {
		return driver.Meta{}, fmt.Errorf("injected current metadata failure")
	}
	return s.Store.Meta(ctx)
}

func TestLocalCurrentMetadataFailureRefusesWithoutWrites(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	before, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	target := &failedCurrentMetadataStore{Store: store}
	local, err := OpenLocal(ctx, LocalConfig{Project: "metadata", Storage: StorageConfig{Driver: "provided"}, Factory: func(context.Context, StorageConfig) (driver.Store, func() error, error) {
		return target, store.Close, nil
	}})
	if err != nil {
		store.Close()
		t.Fatal(err)
	}
	defer local.Close()
	target.fail = true
	for _, method := range []string{"bead.ready", "history.query", "server.hello"} {
		params := map[string]any{}
		if method == "history.query" {
			params["asOf"] = map[string]any{"revision": 0}
		}
		var err error
		if method == "server.hello" {
			raw := local.server.Dispatch(ctx, []byte(`{"jsonrpc":"2.0","id":"hello","method":"server.hello","params":{}}`))
			var response struct {
				Error *wire.Error `json:"error"`
			}
			if err := json.Unmarshal(raw, &response); err != nil {
				t.Fatal(err)
			}
			if response.Error != nil {
				err = response.Error
			}
		} else {
			_, err = local.Client.Call(ctx, method, params, "")
		}
		var typed *wire.Error
		if !errors.As(err, &typed) || typed.Data.Code != "internal_error" {
			t.Fatalf("metadata failure fell back: %s %v", method, err)
		}
	}
	after, err := store.Meta(ctx)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("metadata refusal mutated empty target: %+v %v", after, err)
	}
	reader, err := store.Read(ctx, driver.View{Incarnation: before.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	entries, err := reader.Changesets(ctx, nil, 10)
	if err != nil || len(entries) != 0 {
		t.Fatalf("metadata refusal published: %+v %v", entries, err)
	}
}
