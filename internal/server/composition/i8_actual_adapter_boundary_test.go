package composition

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/client"
	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/plugins/authnapikey"
	"github.com/buildbetter-app/plugs/internal/server/authn"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// Supplemental, disposable proof: production restore + HTTP RPC + actual Python
// adapter processes. Only synthetic public archives enter the remote gate.
func TestI8ActualTwoProjectAdapterBoundary(t *testing.T) {
	ctx := context.Background()
	root := os.Getenv("PLUG_SOURCE_MODULE_ROOT")
	if root == "" {
		t.Fatal("runner source root is required")
	}
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatal(err)
	}
	harness := filepath.Join(root, "tools/agentcallers/runtime_boundary.py")
	tmp := t.TempDir()
	archivePath := filepath.Join(tmp, "two-projects.tar")
	cmd := exec.Command(python, "-B", harness, "build", filepath.Join(root, "internal/integration/converterroundtrip/testdata/synthetic-plug-dump-1.tar"), archivePath)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("fixture builder: %v %s", err, out)
	}
	data, err := os.ReadFile(archivePath)
	if err != nil {
		t.Fatal(err)
	}
	archive, err := dumpformat.Read(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("validate two-project synthetic archive: %v", err)
	}
	manifest, _ := archive.Member("manifest.json")
	var object map[string]any
	if err := json.Unmarshal(manifest, &object); err != nil {
		t.Fatal(err)
	}
	canonical, err := json.Marshal(archive.Manifest())
	if err != nil {
		t.Fatal(err)
	}
	manifestHash := sha256.Sum256(canonical)
	pageHash := sha256.Sum256(data)
	dsn := filepath.Join(tmp, "one-instance.db")
	f := startRuntime(t, Config{Driver: "sqlite", DSN: dsn, Project: "cody-ops", Address: DefaultAddress, RestorePresence: compositionRestoreVerifier()})
	begin, err := f.client.Call(ctx, "admin.restore.begin", map[string]any{"manifest": object, "history": "head", "humanPresence": compositionRestorePresence()}, strings.Repeat("a", 32))
	if err != nil {
		t.Fatal(err)
	}
	restoreID := compositionRestoreStatus(t, begin)["restoreId"]
	if _, err := f.client.Call(ctx, "admin.restore.chunk", map[string]any{"restoreId": restoreID, "page": 0, "data": base64.StdEncoding.EncodeToString(data), "sha256": hex.EncodeToString(pageHash[:]), "humanPresence": compositionRestorePresence()}, strings.Repeat("b", 32)); err != nil {
		t.Fatal(err)
	}
	if _, err := f.client.Call(ctx, "admin.restore.finish", map[string]any{"restoreId": restoreID, "manifestSha256": hex.EncodeToString(manifestHash[:]), "humanPresence": compositionRestorePresence()}, strings.Repeat("c", 32)); err != nil {
		t.Fatal(err)
	}
	meta, err := f.r.Store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// Install an explicit PUBLIC SYNTHETIC native-category ontology through the
	// existing trusted fixture seam. This boundary requires the separately owned
	// B21 category successor; it does not prove importer catalog installation.
	base := ontology.DefaultDocument()
	for name, category := range map[string]ontology.Category{
		"todo": "active", "in_progress": "wip", "done": "done", "backlog": "frozen",
	} {
		status := base.Statuses[name]
		status.Category = category
		base.Statuses[name] = status
	}
	typ := base.Types["issue"]
	typ.Fields["assignee"] = ontology.Field{Type: "string"}
	base.Types["issue"] = typ
	base.Statuses["caller-active"] = ontology.Status{Name: "Caller selected active", Category: ontology.Category("active")}
	base.Statuses["caller-done"] = ontology.Status{Name: "Caller selected done", Category: ontology.Category("done"), Outcome: ontology.Success}
	overlays := map[string]ontology.Document{}
	for _, project := range []string{"cody-ops", "signals-v2"} {
		status := ontology.Status{Name: "Shared display name", Category: ontology.Category("active")}
		if project == "signals-v2" {
			status.Category, status.Outcome = ontology.Category("done"), ontology.Success
		}
		workflow := ontology.DefaultDocument().Workflows["issue"]
		workflow.Statuses = append(workflow.Statuses, "shared", "caller-active", "caller-done")
		// Exact targets are deliberately different from every selected slot.
		// Copy the ordinary transition guards instead of installing a shortcut.
		for _, rule := range append([]ontology.Rule(nil), workflow.Transitions...) {
			switch rule.To {
			case "todo":
				rule.To = "caller-active"
			case "done":
				rule.To = "caller-done"
			default:
				continue
			}
			workflow.Transitions = append(workflow.Transitions, rule)
		}
		slot := ontology.Reopen
		if project == "signals-v2" {
			slot = ontology.Close
		}
		// Retain every guard on the canonical slot while changing only its
		// synthetic target name. Direct status patches are correctly refused.
		previous := workflow.Slots[slot]
		for _, rule := range append([]ontology.Rule(nil), workflow.Transitions...) {
			if rule.To == previous {
				rule.To = "shared"
				workflow.Transitions = append(workflow.Transitions, rule)
			}
		}
		workflow.Slots[slot] = "shared"
		overlays[project] = ontology.Document{Statuses: map[string]ontology.Status{"shared": status}, Workflows: map[string]ontology.Workflow{"issue": workflow}}
	}
	// Preserve the default completed/failure status and Fail slot. Native done
	// deliberately permits success only; it cannot replace that separate plane.
	if _, problems := ontology.Compile(base, overlays, nil); len(problems) != 0 {
		t.Fatalf("compile public native ontology: %+v", problems)
	}
	ref := model.Ref{ID: "ontology/i8-public-fixture"}
	raw, err := service.MarshalOntologySource(service.OntologySource{Base: base, Overlays: overlays})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conformance.Seed(ctx, f.r.Store, f.r.Lease, driver.ChangesetHeader{Principal: model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}, Time: time.Now().UTC()}, []conformance.FixtureChange{{Ref: ref, Kind: "ontology", Op: model.OpCreate, Body: raw}}); err != nil {
		t.Fatal(err)
	}
	f.stop(t)
	// Reopen the SAME SQLite instance with a real ephemeral API-key principal.
	keys := authnapikey.New(authnapikey.Config{})
	principal := model.PrincipalRef{Kind: "user", ID: "user:nikhil"}
	issued, err := keys.Issue(principal, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	f = startRuntime(t, Config{Driver: "sqlite", DSN: dsn, Project: "cody-ops", Address: DefaultAddress, AuthMode: "authenticated", Auth: authn.Config{Provider: keys}, Ontology: &OntologyConfig{Source: service.StaticOntologySource(ref)}})
	after, err := f.r.Store.Meta(ctx)
	if err != nil || after.Incarnation != meta.Incarnation {
		t.Fatalf("same-instance reopen: %v", err)
	}
	transport, err := client.NewHTTP(client.HTTPConfig{Server: "http://" + f.server.Addr().String(), Token: issued.Token})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(transport.Close)
	projects, nativeIDs := map[string]any{}, map[string]map[string]string{}
	for _, project := range []string{"cody-ops", "signals-v2"} {
		c, err := client.New(transport, project, "i8-boundary")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := c.Call(ctx, "bead.update", map[string]any{"id": map[string]any{"id": project + "-7"}, "fields": map[string]any{"assignee": "nikhil"}}, ""); err != nil {
			t.Fatal(err)
		}
		method := "bead.reopen"
		if project == "signals-v2" {
			// The imported dependent retains its real blocks edge. Satisfy it
			// through close before closing the dependent; never bypass guards.
			if _, err := c.Call(ctx, "bead.close", map[string]any{"id": map[string]any{"id": project + "-7"}}, ""); err != nil {
				t.Fatal(err)
			}
			method = "bead.close"
		}
		if _, err := c.Call(ctx, method, map[string]any{"id": map[string]any{"id": project + "-7.4"}}, ""); err != nil {
			t.Fatal(err)
		}
		if project == "signals-v2" {
			if _, err := c.Call(ctx, "bead.reopen", map[string]any{"id": map[string]any{"id": project + "-7"}}, ""); err != nil {
				t.Fatal(err)
			}
		}
		lifecycleRef := map[string]any{"id": project + "-8"}
		if _, err := c.Call(ctx, "bead.reopen", map[string]any{"id": lifecycleRef, "targetStatus": "caller-active"}, ""); err != nil {
			t.Fatal(err)
		}
		got, err := c.Call(ctx, "bead.get", map[string]any{"id": lifecycleRef}, "")
		if err != nil {
			t.Fatal(err)
		}
		var detail struct {
			Bead struct {
				Revision model.Revision `json:"revision"`
			} `json:"bead"`
		}
		if err := json.Unmarshal(got.JSON, &detail); err != nil {
			t.Fatal(err)
		}
		_, err = c.Call(ctx, "tx.apply", map[string]any{"changeSet": map[string]any{
			"ops":    []map[string]any{{"kind": "close", "target": lifecycleRef, "targetStatus": "caller-done", "reason": "new typed reason"}, {"kind": "setMeta", "target": lifecycleRef, "key": "i8.host.close_reason", "value": map[string]any{"schema": "plug-host-close-reason/1", "reason": "new typed reason"}}},
			"expect": []model.Precondition{model.PreconditionByRevision(model.Ref{ID: project + "-8"}, detail.Bead.Revision)},
		}}, "")
		if err != nil {
			t.Fatal(err)
		}
		created, err := c.Call(ctx, "bead.create", map[string]any{"type": "issue", "fields": map[string]any{"title": "Native synthetic " + project}}, "")
		if err != nil {
			t.Fatal(err)
		}
		id := resultObject(t, created)["results"].([]any)[0].(map[string]any)["ref"].(map[string]any)["id"].(string)
		nativeIDs[project] = map[string]string{"claimed": id}
		if _, err := c.Call(ctx, "bead.claim", map[string]any{"id": map[string]any{"id": id}}, ""); err != nil {
			t.Fatal(err)
		}
		for _, state := range []string{"frozen", "ready"} {
			params := map[string]any{"type": "issue", "fields": map[string]any{"title": "Native synthetic " + project + " " + state, "assignee": "nikhil"}}
			if state == "ready" {
				params["targetStatus"] = "caller-active"
			}
			created, err := c.Call(ctx, "bead.create", params, "")
			if err != nil {
				t.Fatal(err)
			}
			stateID := resultObject(t, created)["results"].([]any)[0].(map[string]any)["ref"].(map[string]any)["id"].(string)
			nativeIDs[project][state] = stateID
			if state == "frozen" {
				if _, err := c.Call(ctx, "bead.defer", map[string]any{"id": map[string]any{"id": stateID}}, ""); err != nil {
					t.Fatal(err)
				}
			}
		}
		workspace := filepath.Join(tmp, project)
		if err := os.Mkdir(workspace, 0o700); err != nil {
			t.Fatal(err)
		}
		ids := map[string]any{}
		for _, old := range []string{project + "-7", project + "-7.4", project + "-8", id} {
			ids[old] = map[string]any{"id": old}
		}
		for _, nativeID := range nativeIDs[project] {
			ids[nativeID] = map[string]any{"id": nativeID}
		}
		projects[project] = map[string]any{"workspace": workspace, "legacy_ids": ids}
	}
	binding, err := json.Marshal(map[string]any{"schema": "plug-host-binding/1", "endpoint": "http://" + f.server.Addr().String(), "incarnation": meta.Incarnation, "principal": principal, "agent": "i8-boundary", "token_env": "PLUG_I8_SYNTHETIC_TOKEN", "projects": projects})
	if err != nil {
		t.Fatal(err)
	}
	bindingPath := filepath.Join(tmp, "host-binding.json")
	if err := os.WriteFile(bindingPath, binding, 0o600); err != nil {
		t.Fatal(err)
	}
	nativeJSON, _ := json.Marshal(nativeIDs)
	runCallers := func(phase string) {
		t.Helper()
		cmd := exec.Command(python, "-B", harness, "run", bindingPath, string(nativeJSON), phase)
		cmd.Env = append(os.Environ(), "PLUG_I8_SYNTHETIC_TOKEN="+issued.Token)
		out, err := cmd.CombinedOutput()
		t.Logf("I8_ACTUAL_TWO_PROJECT_ADAPTER_BOUNDARY %s", out)
		if err != nil {
			t.Fatalf("actual adapter boundary %s failed: %v", phase, err)
		}
	}
	// Read the actual selected terminal state through every emitted command,
	// then reopen to the noncanonical active target in the same incarnation.
	runCallers("closed")
	for _, project := range []string{"cody-ops", "signals-v2"} {
		c, err := client.New(transport, project, "i8-boundary")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := c.Call(ctx, "bead.reopen", map[string]any{"id": map[string]any{"id": project + "-8"}, "targetStatus": "caller-active"}, ""); err != nil {
			t.Fatal(err)
		}
	}
	runCallers("reopened")
}

// This supplements the real Plug boundary with exact patch application and
// subprocess error-envelope cases for commands extracted from rendered files.
func TestI8RenderedPublicCallerCommands(t *testing.T) {
	root := os.Getenv("PLUG_SOURCE_MODULE_ROOT")
	if root == "" {
		t.Fatal("runner source root is required")
	}
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(python, "-B", filepath.Join(root, "tools/agentcallers/render_fixture.py"))
	out, err := cmd.CombinedOutput()
	t.Logf("I8_RENDERED_PUBLIC_CALLER_COMMANDS %s", out)
	if err != nil {
		t.Fatalf("rendered caller commands: %v", err)
	}
}
