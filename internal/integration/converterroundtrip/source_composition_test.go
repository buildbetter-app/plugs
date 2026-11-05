package converterroundtrip_test

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/restore"
	"github.com/buildbetter-app/plugs/internal/integration/converterroundtrip"
	"github.com/buildbetter-app/plugs/internal/server/composition"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestJointSourceCompositionImportInstallAndStartupRecovery(t *testing.T) {
	data := converterroundtrip.JointFixtureForTest(t)
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			ctx := context.Background()
			dsn := ""
			if engine == "sqlite" {
				dsn = filepath.Join(t.TempDir(), "joint-composed.db")
			}
			if engine == "postgres" {
				dsn = converterroundtrip.JointPostgresDSNForTest(t)
			}
			presence := restore.Presence{"proof": "joint-composition"}
			config := composition.Config{Driver: engine, DSN: dsn, Project: "demo", AuthMode: "anonymous", RestorePresence: restore.PresenceVerifierFunc(func(ctx context.Context, method, digest string, p restore.Presence) error {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				if method != "admin.restore.begin" || len(digest) != 64 || p["proof"] != "joint-composition" {
					return errors.New("unbound presence")
				}
				return nil
			})}
			runtime, err := composition.Open(ctx, config)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if runtime != nil {
					if err := runtime.Close(ctx); err != nil {
						t.Error(err)
					}
				}
			})
			owner := model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}
			profile := converterroundtrip.JointProfileForTest(t, data, runtime.Store, runtime.Lease, owner)
			plan, err := profile.SourcePlanes.Plan()
			if err != nil {
				t.Fatal(err)
			}
			request := composition.RestoreImportRequest{IdempotencyKey: strings.Repeat("c", 32), HistoryMode: driver.RestoreHistoryModeSnapshot, Presence: presence, SourcePlanes: &plan}
			report, err := runtime.RestoreImport(ctx, owner, bytes.NewReader(data), request)
			if err != nil {
				t.Fatal("composed joint import", err)
			}
			if !runtime.Service.SourceRuntimeInstalled() {
				t.Fatal("import omitted live installation")
			}
			ref := model.RefByID("demo-7", model.Natural64(0))
			got := converterroundtrip.JointHTTPForTest(t, runtime.HTTPHandler, "bead.get", 0, map[string]any{"id": ref})
			if !bytes.Contains(got, []byte("Exact active collision")) {
				t.Fatal("bound HTTP handler missed newly installed provider", string(got))
			}
			converterroundtrip.JointHTTPForTest(t, runtime.HTTPHandler, "bead.update", 701, map[string]any{"id": ref, "fields": map[string]any{"title": "composed current source title"}})
			request.SourcePlanes = nil
			recovered, err := runtime.RestoreImport(ctx, owner, bytes.NewReader(data), request)
			if err != nil || !reflect.DeepEqual(report.Receipt, recovered.Receipt) {
				t.Fatal("composed import did not recover original receipt", err)
			}
			if engine != "memory" {
				if err := runtime.Close(ctx); err != nil {
					t.Fatal(err)
				}
				runtime = nil
				runtime, err = composition.Open(ctx, config)
				if err != nil {
					t.Fatal("startup recovery", err)
				}
			}
			if !runtime.Service.SourceRuntimeInstalled() {
				t.Fatal("source provider absent after lifecycle boundary")
			}
			current := converterroundtrip.JointHTTPForTest(t, runtime.HTTPHandler, "bead.get", 0, map[string]any{"id": ref})
			if !bytes.Contains(current, []byte("composed current source title")) {
				t.Fatal("recovery substituted initial receipt payload for current namespace", string(current))
			}
			replay := converterroundtrip.JointHTTPForTest(t, runtime.HTTPHandler, "bead.update", 701, map[string]any{"id": ref, "fields": map[string]any{"title": "composed current source title"}})
			if !bytes.Contains(replay, []byte("composed current source title")) {
				t.Fatal("recovery replay changed result", string(replay))
			}
			// Memory retained this one process-local store; only persistent stores were
			// closed and reopened. No restart persistence is claimed for memory.
		})
	}
}
