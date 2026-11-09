package converterroundtrip_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/restore"
	"github.com/buildbetter-app/plugs/internal/core/service"
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
			admitted, original, found, err := converterroundtrip.RecoverBoundSourceImport(ctx, runtime.Store)
			if err != nil || !found || !reflect.DeepEqual(original.Receipt, report.Receipt) || !reflect.DeepEqual(admitted.Admission.Actor, owner) {
				t.Fatal("composition did not retain the actual admitted importer and original receipt", err)
			}
			ref := model.RefByID("demo-7", model.Natural64(0))
			got := converterroundtrip.JointHTTPForTest(t, runtime.HTTPHandler, "bead.get", 0, map[string]any{"id": ref})
			if !bytes.Contains(got, []byte("Exact active collision")) {
				t.Fatal("bound HTTP handler missed newly installed provider", string(got))
			}
			converterroundtrip.JointAssertSourceCommentForTest(t, got, admitted, report)
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
			converterroundtrip.JointAssertSourceCommentForTest(t, current, admitted, report)
			replay := converterroundtrip.JointHTTPForTest(t, runtime.HTTPHandler, "bead.update", 701, map[string]any{"id": ref, "fields": map[string]any{"title": "composed current source title"}})
			if !bytes.Contains(replay, []byte("composed current source title")) {
				t.Fatal("recovery replay changed result", string(replay))
			}
			// Memory retained this one process-local store; only persistent stores were
			// closed and reopened. No restart persistence is claimed for memory.
		})
	}
}

func TestNativeCatalogFullHistoryInstalledRPCReadyAndRestart(t *testing.T) {
	data := converterroundtrip.NativeHistoryFixtureForTest(t)
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			ctx := context.Background()
			dsn := ""
			if engine == "sqlite" {
				dsn = filepath.Join(t.TempDir(), "native-catalog.db")
			}
			if engine == "postgres" {
				dsn = converterroundtrip.JointPostgresDSNForTest(t)
			}
			config := composition.Config{Driver: engine, DSN: dsn, Project: "pl", AuthMode: "anonymous", NativeImportOntology: true, RestorePresence: restore.PresenceVerifierFunc(func(ctx context.Context, method, digest string, p restore.Presence) error {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				if method != "admin.restore.begin" || len(digest) != 64 || p["proof"] != "public-native-catalog" {
					return errors.New("unbound public restore presence")
				}
				return nil
			})}
			runtime, err := composition.Open(ctx, config)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if runtime != nil {
					if err := runtime.Close(ctx); err != nil {
						t.Error(err)
					}
				}
			}()
			plan := converterroundtrip.NativeHistoryPlanForTest(t, data, runtime.Store, runtime.Lease)
			owner := model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}
			request := composition.RestoreImportRequest{IdempotencyKey: strings.Repeat("c", 32), HistoryMode: driver.RestoreHistoryModeFull, Presence: restore.Presence{"proof": "public-native-catalog"}, SourcePlanes: &plan}
			report, err := runtime.RestoreImport(ctx, owner, bytes.NewReader(data), request)
			if err != nil {
				t.Fatal("actual composed full native import", err)
			}
			for pass := 0; pass < 2; pass++ {
				for project, category := range map[string]string{"pl": "active", "qa": "done"} {
					raw := converterroundtrip.JointHTTPForTest(t, runtime.HTTPHandler, "ontology.get", 0, map[string]any{"project": project})
					var result struct {
						Document struct {
							Statuses map[string]struct {
								Category string `json:"category"`
							} `json:"statuses"`
						} `json:"document"`
					}
					if err := json.Unmarshal(raw, &result); err != nil || result.Document.Statuses["shared"].Category != category {
						t.Fatalf("installed %s ontology.get: %s: %v", project, raw, err)
					}
					ready, err := runtime.Service.Ready(ctx, owner, service.ReadyRequest{Project: project})
					want := 0
					if category == "active" {
						want = 1
					}
					if err != nil || len(ready.Items) != want {
						t.Fatalf("installed %s live category Ready: %d, want %d: %v", project, len(ready.Items), want, err)
					}
				}
				request.SourcePlanes = nil
				recovered, err := runtime.RestoreImport(ctx, owner, bytes.NewReader(data), request)
				if err != nil || !reflect.DeepEqual(report.Receipt, recovered.Receipt) {
					t.Fatal("composed original full receipt recovery", err)
				}
				if pass == 0 && engine != "memory" {
					if err := runtime.Close(ctx); err != nil {
						t.Fatal(err)
					}
					runtime = nil
					runtime, err = composition.Open(ctx, config)
					if err != nil {
						t.Fatal("native catalog full history restart", err)
					}
				}
			}
			// Memory repeats within its process lifetime; durable drivers reopen.
		})
	}
}
