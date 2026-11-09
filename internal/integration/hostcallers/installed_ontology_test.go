package hostcallers_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/restore"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
)

// This is a public synthetic native Plug head-archive boundary, not a Beads
// converter fixture. Literal native categories use the exact delivered B21 core;
// this does not qualify the Beads converter, private ontology or source history.
func prepareInstalledHostOntology(t *testing.T, root string, target driver.Store, targetLease driver.ActiveLease, imported bool) (model.Ref, ontology.Snapshot) {
	t.Helper()
	ctx := context.Background()
	raw, err := os.ReadFile(filepath.Join(root, "tools/hostcallers/host-ontology.json"))
	if err != nil {
		t.Fatal(err)
	}
	base, err := ontology.Decode(raw)
	if err != nil {
		t.Fatal(err)
	}
	source := service.OntologySource{Base: base}
	ref := model.Ref{ID: "ontology/i8-host"}
	if imported {
		source.Overlays = map[string]ontology.Document{}
		for _, project := range []string{"cody-ops", "signals-v2"} {
			source.Overlays[project] = categoryProjectFixture(t, base, project)
		}
	}
	body, err := service.MarshalOntologySource(source)
	if err != nil {
		t.Fatal(err)
	}
	changes := []conformance.FixtureChange{{Ref: ref, Kind: "ontology", Op: model.OpCreate, Body: body}}
	if imported {
		for _, project := range []string{"cody-ops", "signals-v2"} {
			status := "todo"
			id := "fixture_import_ops"
			if project == "signals-v2" {
				id = "fixture_import_signals"
			}
			body, err := json.Marshal(pipeline.Document{ID: id, Project: project, Type: "task", Fields: map[string]any{"title": "Public synthetic imported row", "status": status, "priority": 2}})
			if err != nil {
				t.Fatal(err)
			}
			changes = append(changes, conformance.FixtureChange{Ref: model.Ref{ID: id}, Kind: "bead", Op: model.OpCreate, Body: body, Graph: &conformance.FixtureGraphSource{}})
		}
		publicSource := memory.New()
		defer publicSource.Close()
		lease, err := publicSource.Lease(ctx, "i8-public-native-source", driver.LeaseTTL)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := conformance.Seed(ctx, publicSource, lease, driver.ChangesetHeader{}, changes); err != nil {
			t.Fatal(err)
		}
		data, manifest, err := restore.Dump(ctx, publicSource, "head")
		if err != nil {
			t.Fatal(err)
		}
		if err := lease.Release(ctx); err != nil {
			t.Fatal(err)
		}
		manifestBytes, err := json.Marshal(manifest)
		if err != nil {
			t.Fatal(err)
		}
		var manifestObject map[string]any
		if err := json.Unmarshal(manifestBytes, &manifestObject); err != nil {
			t.Fatal(err)
		}
		proof := restore.Presence{"mode": "presence_check", "challenge": "i8-public", "proof": "i8-public"}
		verifier := restore.PresenceVerifierFunc(func(ctx context.Context, method, digest string, got restore.Presence) error {
			if method == "" || digest == "" || got["proof"] != proof["proof"] {
				return fmt.Errorf("unbound public fixture presence")
			}
			return ctx.Err()
		})
		manager := restore.NewManager(target, targetLease, verifier)
		status, err := manager.Begin(ctx, manifestObject, "head", proof)
		if err != nil {
			t.Fatal(err)
		}
		for offset, page := 0, uint64(0); offset < len(data); page++ {
			end := offset + 16384
			if end > len(data) {
				end = len(data)
			}
			chunk := data[offset:end]
			sum := sha256.Sum256(chunk)
			if _, err := manager.Chunk(ctx, status.RestoreID, page, chunk, hex.EncodeToString(sum[:]), proof); err != nil {
				t.Fatal(err)
			}
			offset = end
		}
		sum := sha256.Sum256(manifestBytes)
		status, err = manager.Finish(ctx, status.RestoreID, hex.EncodeToString(sum[:]), model.PrincipalRef{Kind: "user", ID: "i8-public-owner"}, model.WriteMeta{Method: "admin.restore.finish", IdempotencyKey: "i8-category-native-head"}, proof)
		if err != nil || status.State != "complete" {
			t.Fatalf("public two-project native head restore: %+v %v", status, err)
		}
	} else if _, err := conformance.Seed(ctx, target, targetLease, driver.ChangesetHeader{}, changes); err != nil {
		t.Fatal(err)
	}
	meta, err := target.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	r, err := target.Read(ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	rows, err := r.Get(ctx, []model.Ref{ref})
	if err != nil || len(rows) != 1 || rows[0].Kind != "ontology" {
		t.Fatalf("stored source missing after preparation/restore: %+v %v", rows, err)
	}
	installed, err := service.DecodeOntologySource(rows[0].Body)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, problems := ontology.Compile(installed.Base, installed.Overlays, installed.Plugins)
	if len(problems) != 0 {
		t.Fatalf("installed project ontology: %+v", problems)
	}
	return ref, snapshot
}

func categoryProjectFixture(t *testing.T, base ontology.Document, project string) ontology.Document {
	t.Helper()
	statuses := map[string]ontology.Status{
		"todo":     {Name: "Todo", Category: ontology.Active},
		"working":  {Name: "Working", Category: ontology.WIP},
		"shipped":  {Name: "Shipped", Category: ontology.Done, Outcome: ontology.Success},
		"paused":   {Name: "Paused", Category: ontology.Frozen},
		"closed":   {Name: "Name does not imply terminal", Category: ontology.Active},
		"accepted": {Name: "Another terminal name", Category: ontology.Done, Outcome: ontology.Success},
	}
	initial, terminal := "todo", "shipped"
	if project == "cody-ops" {
		statuses["todo"] = ontology.Status{Name: "Todo is terminal in this project", Category: ontology.Done, Outcome: ontology.Success}
		statuses["queued"] = ontology.Status{Name: "Queued", Category: ontology.Active}
		initial, terminal = "queued", "todo"
	}
	workflow := base.Workflows["host"]
	workflow.Statuses = []string{}
	workflow.Transitions = nil
	for _, status := range []string{"todo", "working", "shipped", "paused", "closed", "accepted", "queued"} {
		if _, ok := statuses[status]; !ok {
			continue
		}
		workflow.Statuses = append(workflow.Statuses, status)
		guards := []ontology.Guard{{Kind: ontology.Permission}}
		if statuses[status].Category.Terminal() {
			guards = append(guards, ontology.Guard{Kind: ontology.NotTemplate}, ontology.Guard{Kind: ontology.NoOpenChildren}, ontology.Guard{Kind: ontology.NoOpenBlockers})
		}
		workflow.Transitions = append(workflow.Transitions, ontology.Rule{To: status, Guards: guards})
	}
	workflow.Initial = initial
	workflow.Slots = map[ontology.Slot]string{ontology.Claim: "working", ontology.Close: terminal, ontology.Reopen: initial, ontology.Defer: "paused"}
	return ontology.Document{Statuses: statuses, Workflows: map[string]ontology.Workflow{"host": workflow}}
}
