package heartbeads

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/restore"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

// One public standalone Plug process reads one imported SQLite store containing
// both projects. The offline native source and presence verifier are synthetic;
// no production import, human-presence or live ontology credit is claimed.
func TestI8HeartbeadsImportedSQLiteBrowser(t *testing.T) {
	ctx := context.Background()
	t.Setenv("PATH", "/tmp/plug-swarm/bin/worker-safe:"+os.Getenv("PATH"))
	fixtureRoot := filepath.Join(os.Getenv("PLUG_SOURCE_MODULE_ROOT"), "internal/integration/heartbeads/testdata")
	raw, err := os.ReadFile(filepath.Join(fixtureRoot, "native-ontology-source.json"))
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(raw)
	if hex.EncodeToString(digest[:]) != "801b32a7548eaa98ba86e6d88dfac8355dd5aa33483203e5eb7f470b86ca78ad" {
		t.Fatal("synthetic per-project ontology source drift")
	}
	installed, err := service.DecodeOntologySource(raw)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, problems := ontology.Compile(installed.Base, installed.Overlays, installed.Plugins)
	if len(problems) != 0 {
		t.Fatal(problems)
	}
	source := memory.New()
	defer source.Close()
	lease, err := source.Lease(ctx, "i8-synthetic-source", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release(ctx)
	body, err := service.MarshalOntologySource(installed)
	if err != nil {
		t.Fatal(err)
	}
	_, err = conformance.Seed(ctx, source, lease, driver.ChangesetHeader{Principal: model.PrincipalRef{Kind: "system", ID: "i8-synthetic-source"}}, []conformance.FixtureChange{{Ref: model.RefByID("ontology/i8-host", model.Natural64(0)), Kind: "ontology", Op: model.OpCreate, Body: body}})
	if err != nil {
		t.Fatal(err)
	}
	rules := service.NewOntology(snapshot.Effective)
	writer := pipeline.New(source, lease, pipeline.WithOntology(rules))
	domain := service.New(source, lease, writer, service.WithOntology(rules))
	principal := model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}
	roots := map[string]string{}
	for _, project := range []string{"cody-ops", "signals-v2"} {
		commit, err := domain.Create(ctx, principal, service.CreateRequest{Type: "task", Fields: map[string]any{"title": "SHARED root " + project, "description": "before", "priority": 1}}, model.WriteMeta{Project: project, AgentLabel: "i8-heartbeads-fixture", Method: "bead.create"})
		if err != nil {
			t.Fatal(err)
		}
		roots[project] = commit.Results[0].Ref.ID
	}
	archive, manifest, err := restore.Dump(ctx, source, "head")
	if err != nil {
		t.Fatal(err)
	}
	manifestBytes, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	var manifestMap map[string]any
	if err := json.Unmarshal(manifestBytes, &manifestMap); err != nil {
		t.Fatal(err)
	}
	manifestSum := sha256.Sum256(manifestBytes)
	manifestHash := hex.EncodeToString(manifestSum[:])
	dir := t.TempDir()
	store, err := (sqlite.Driver{}).Open(ctx, driver.Config{DSN: filepath.Join(dir, "server.db")})
	if err != nil {
		t.Fatal(err)
	}
	targetLease, err := store.Lease(ctx, "i8-restore", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	verifier := restore.PresenceVerifierFunc(func(_ context.Context, _ string, _ string, p restore.Presence) error { return nil })
	mgr := restore.NewManager(store, targetLease, verifier)
	presence := restore.Presence{"mode": "presence_check", "challenge": "synthetic-i8", "proof": "synthetic-only"}
	begun, err := mgr.Begin(ctx, manifestMap, "head", presence)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(archive)
	if _, err := mgr.Chunk(ctx, begun.RestoreID, 0, archive, hex.EncodeToString(sum[:]), presence); err != nil {
		t.Fatal(err)
	}
	imported, err := mgr.Finish(ctx, begun.RestoreID, manifestHash, principal, model.WriteMeta{AgentLabel: "i8-heartbeads-fixture", Method: "admin.restore.finish"}, presence)
	if err != nil || imported.State != "complete" {
		t.Fatalf("restore=%+v err=%v", imported, err)
	}
	if err := targetLease.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(dir, ".plug"), 0700); err != nil {
		t.Fatal(err)
	}
	config := "driver: sqlite\ndsn: server.db\naddress: 127.0.0.1:0\nproject: wrong-ambient-default\nui: false\nontology:\n  source: ontology/i8-host\n"
	if err := os.WriteFile(filepath.Join(dir, ".plug/config.yaml"), []byte(config), 0600); err != nil {
		t.Fatal(err)
	}
	server := startPlugServer(t, dir, false)
	defer server.stop(t)
	hello := i8FixtureRPC(t, server.endpoint, "", "server.hello", map[string]any{}, "")
	var helloValue map[string]any
	if err := json.Unmarshal(hello.Result, &helloValue); err != nil {
		t.Fatal(err)
	}
	binding := map[string]any{"schema": "plug-host-binding/1", "endpoint": server.endpoint, "incarnation": helloValue["incarnation"], "principal": principal, "agent": "i8-heartbeads-fixture", "projects": map[string]any{}}
	projects := binding["projects"].(map[string]any)
	for project, id := range roots {
		read := i8FixtureRPC(t, server.endpoint, project, "bead.get", map[string]any{"id": map[string]any{"id": id}}, "")
		if read.Error != nil {
			t.Fatalf("import readback=%+v", read.Error)
		}
		workspace := filepath.Join(dir, project)
		if err := os.Mkdir(workspace, 0700); err != nil {
			t.Fatal(err)
		}
		projects[project] = map[string]any{"workspace": workspace, "legacy_ids": map[string]any{"SHARED": map[string]any{"id": id}}}
	}
	bindingBytes, _ := json.MarshalIndent(binding, "", "  ")
	bindingPath := filepath.Join(dir, "binding.json")
	if err := os.WriteFile(bindingPath, bindingBytes, 0600); err != nil {
		t.Fatal(err)
	}
	outputs := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR")
	if outputs == "" {
		outputs = t.TempDir()
	}
	if err := os.WriteFile(filepath.Join(outputs, "import-receipt.json"), []byte(`{"archive_sha256":"`+hex.EncodeToString(sum[:])+`","manifest_sha256":"`+manifestHash+`","state":"complete","projects":["cody-ops","signals-v2"],"colliding_legacy_id":"SHARED","ontology_sha256":"`+hex.EncodeToString(digest[:])+`"}`), 0600); err != nil {
		t.Fatal(err)
	}
	runCtx, cancel := context.WithTimeout(ctx, 13*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(runCtx, "node", "--experimental-vm-modules", filepath.Join(fixtureRoot, "e2e.mjs"), bindingPath, fixtureRoot, outputs)
	cmd.Env = append(os.Environ(), "PATH=/tmp/plug-swarm/bin/worker-safe:"+os.Getenv("PATH"))
	output, err := cmd.CombinedOutput()
	t.Logf("I8_ACTUAL_IMPORTED_BROWSER\n%s", output)
	if err != nil {
		t.Fatalf("fixture failed: %v", err)
	}
	t.Logf("I8_IMPORT_HTTP_UI_SSE_PASS SQLite import=%s projects=cody-ops,signals-v2 endpoint=%s", imported.State, server.endpoint)
}
