package b03catalog_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/ontologyops"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/server/composition"
	"github.com/buildbetter-app/plugs/internal/server/transport/inprocess"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func composedRuntimeCall(t *testing.T, transport *inprocess.Transport, method string, params map[string]any, project, key string) json.RawMessage {
	t.Helper()
	if params == nil {
		params = map[string]any{}
	}
	params["_meta"] = map[string]any{"project": project, "idempotencyKey": key}
	raw, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": key, "method": method, "params": params})
	if err != nil {
		t.Fatal(err)
	}
	response, err := transport.RoundTrip(context.Background(), raw)
	if err != nil {
		t.Fatalf("%s transport: %v", method, err)
	}
	var envelope struct {
		Result json.RawMessage `json:"result"`
		Error  *struct {
			Data struct {
				Code string `json:"code"`
				Hint string `json:"hint"`
			} `json:"data"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(response, &envelope); err != nil {
		t.Fatalf("%s response=%s: %v", method, response, err)
	}
	if envelope.Error != nil {
		t.Fatalf("%s refused: %s (%s; hint=%s)", method, envelope.Error.Data.Code, envelope.Error.Message, envelope.Error.Data.Hint)
	}
	return envelope.Result
}

func composedEventListContains(t *testing.T, transport *inprocess.Transport, project, eventType, resource string) bool {
	t.Helper()
	var cursor string
	for i := 0; i < 8; i++ {
		params := map[string]any{"filter": map[string]any{"events": []string{eventType}}, "limit": 100}
		if cursor != "" {
			params["after"] = cursor
		}
		raw := composedRuntimeCall(t, transport, "event.list", params, project, fmt.Sprintf("%032x", 0x70+i))
		var page struct {
			Events []json.RawMessage `json:"events"`
			Cursor string            `json:"cursor"`
		}
		if err := json.Unmarshal(raw, &page); err != nil {
			t.Fatal(err)
		}
		for _, event := range page.Events {
			if strings.Contains(string(event), eventType) && strings.Contains(string(event), resource) {
				return true
			}
		}
		if page.Cursor == "" || page.Cursor == cursor {
			return false
		}
		cursor = page.Cursor
	}
	return false
}

func composedRuntimeStore(t *testing.T, e engine, source model.Ref, body []byte) driver.Store {
	t.Helper()
	store, err := e.storage.Open(context.Background(), driver.Config{DSN: e.dsn})
	if err != nil {
		t.Fatalf("open %s: %v", e.name, err)
	}
	lease, err := store.Lease(context.Background(), "b03-composition-seed", driver.LeaseTTL)
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	_, err = conformance.Seed(context.Background(), store, lease, driver.ChangesetHeader{
		Principal: model.PrincipalRef{Kind: "system", ID: "b03-composition-seed"},
	}, []conformance.FixtureChange{{Ref: source, Kind: "ontology", Op: model.OpCreate, Body: body}})
	if err != nil {
		_ = lease.Release(context.Background())
		_ = store.Close()
		t.Fatalf("seed %s: %v", e.name, err)
	}
	if err := lease.Release(context.Background()); err != nil {
		_ = store.Close()
		t.Fatalf("release seed lease %s: %v", e.name, err)
	}
	if e.name != "memory" {
		// The production composition must reopen the same durable database through
		// openStore. A memory store cannot be preseeded before Open, so the
		// in-memory case intentionally retains the narrow StoreFactory seam.
		if err := store.Close(); err != nil {
			t.Fatalf("close %s seed store: %v", e.name, err)
		}
		return nil
	}
	return store
}

// This target intentionally uses composition.Open and its public byte RPC for
// every engine. The lower-level catalog tests retain their separate source
// reload and SQL fault coverage; this test proves the installed source is what
// the production runtime actually admits and shares with ordinary writes.
func TestConfiguredRuntimeLoadsCatalogAndAuditsOnEveryEngine(t *testing.T) {
	eachEngine(t, allEngines, func(t *testing.T, e engine) {
		sourceRef := model.Ref{ID: "ontology/b03-composition"}
		body, err := service.MarshalOntologySource(service.OntologySource{Base: ontology.DefaultDocument()})
		if err != nil {
			t.Fatal(err)
		}
		store := composedRuntimeStore(t, e, sourceRef, body)
		configPath := filepath.Join(t.TempDir(), "config.yaml")
		configBody := fmt.Sprintf("driver: %s\ndsn: %q\nproject: b03-composition\naddress: 127.0.0.1:0\nui: false\nontology:\n  source: %s\n", e.name, e.dsn, sourceRef.ID)
		if err := os.WriteFile(configPath, []byte(configBody), 0o600); err != nil {
			t.Fatal(err)
		}
		config, err := composition.LoadConfigFile(configPath)
		if err != nil {
			t.Fatalf("load configured %s ontology source: %v", e.name, err)
		}
		if store != nil {
			config.StoreFactory = func(context.Context, string, driver.Config) (driver.Store, error) {
				return store, nil
			}
		}
		runtime, err := composition.Open(context.Background(), config)
		if err != nil {
			if store != nil {
				_ = store.Close()
			}
			t.Fatalf("configured %s runtime refused installed source: %v", e.name, err)
		}
		defer func() {
			if err := runtime.Close(context.Background()); err != nil {
				t.Error(err)
			}
		}()

		transport := inprocess.New(runtime.RPC)
		got := composedRuntimeCall(t, transport, "ontology.get", nil, "b03-composition", "00000000000000000000000000000001")
		var initial ontologyops.Result
		if err := json.Unmarshal(got, &initial); err != nil || initial.Revision != "1" || initial.Document == nil {
			t.Fatalf("%s configured ontology.get did not expose installed document: %s", e.name, got)
		}
		// Create an ordinary row before planning so migration must evaluate the
		// real default workflow's Permission guard without inventing a policy.
		existingRaw := composedRuntimeCall(t, transport, "bead.create", map[string]any{
			"type": "issue", "fields": map[string]any{"title": "existing before catalog migration"},
		}, "b03-composition", "00000000000000000000000000000030")
		var existing model.Commit
		if err := json.Unmarshal(existingRaw, &existing); err != nil || len(existing.Results) != 1 {
			t.Fatalf("%s pre-migration create=%s err=%v", e.name, existingRaw, err)
		}
		updated := ontology.DefaultDocument()
		issue := updated.Types["issue"]
		issue.Fields["reviewed"] = ontology.Field{Type: "boolean", Default: false}
		updated.Types["issue"] = issue
		// The request carries the b03-composition project in _meta, so the
		// production planner installs this candidate as that project's overlay.
		updatedCompiled, problems := ontology.Compile(ontology.DefaultDocument(), map[string]ontology.Document{"b03-composition": updated}, nil)
		if len(problems) != 0 {
			t.Fatalf("%s updated ontology compile: %+v", e.name, problems)
		}
		planned := composedRuntimeCall(t, transport, "ontology.plan", map[string]any{"document": updated}, "b03-composition", "0000000000000000000000000000002a")
		var plan ontologyops.PlanResult
		if err := json.Unmarshal(planned, &plan); err != nil || !plan.Changes.Ready || plan.PlanDigest == "" {
			t.Fatalf("%s configured ontology.plan=%s err=%v", e.name, planned, err)
		}
		appliedRaw := composedRuntimeCall(t, transport, "ontology.apply", map[string]any{
			"planDigest": plan.PlanDigest,
			"expect":     plan.Changes.Expect,
			"humanPresence": map[string]any{
				"mode": "presence_check", "challenge": plan.PlanDigest, "proof": plan.PlanDigest,
			},
		}, "b03-composition", "0000000000000000000000000000002b")
		var applied model.Commit
		if err := json.Unmarshal(appliedRaw, &applied); err != nil || applied.Revision.IsZero() {
			t.Fatalf("%s configured apply=%s err=%v", e.name, appliedRaw, err)
		}
		{
			meta, err := runtime.Store.Meta(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			reader, err := runtime.Store.Read(context.Background(), driver.View{Incarnation: meta.Incarnation, At: &applied.Revision})
			if err != nil {
				t.Fatal(err)
			}
			sets, readErr := reader.Changesets(context.Background(), nil, 100)
			closeErr := reader.Close()
			if readErr != nil || closeErr != nil || len(sets) == 0 {
				t.Fatalf("%s read migration audit: %v %v", e.name, readErr, closeErr)
			}
			migration := sets[len(sets)-1]
			if migration.Revision != applied.Revision || migration.Kind != model.ChangesetMigration || len(migration.Changes) != 2 || len(migration.Events) != len(migration.Changes) || migration.ValidateEvents() != nil {
				t.Fatalf("%s migration did not atomically retain complete events: %+v", e.name, migration)
			}
			for i, event := range migration.Events {
				want := model.EventBeadUpdated
				if migration.Changes[i].Ref.ID == sourceRef.ID {
					want = model.EventOntologyApplied
				}
				if event.Type != want || event.Provenance.Method != "ontology.apply" || event.Provenance.Slot != "" || event.Provenance.Cause.Root != "0000000000000000000000000000002b" || len(event.Provenance.Chain) != 0 || len(event.Provenance.SetBy) == 0 {
					t.Fatalf("%s migration event lost trusted provenance: %+v", e.name, event)
				}
			}
		}
		migratedRaw := composedRuntimeCall(t, transport, "bead.get", map[string]any{"id": existing.Results[0].Ref}, "b03-composition", "00000000000000000000000000000031")
		var migrated struct {
			Bead struct {
				Fields map[string]any `json:"fields"`
			} `json:"bead"`
		}
		if err := json.Unmarshal(migratedRaw, &migrated); err != nil || migrated.Bead.Fields["reviewed"] != false || migrated.Bead.Fields["title"] != "existing before catalog migration" {
			t.Fatalf("%s existing ordinary row was not atomically migrated: %s err=%v", e.name, migratedRaw, err)
		}
		refreshed := composedRuntimeCall(t, transport, "ontology.get", nil, "b03-composition", "0000000000000000000000000000002c")
		var refreshedResult ontologyops.Result
		if err := json.Unmarshal(refreshed, &refreshedResult); err != nil || refreshedResult.Revision != applied.Revision || refreshedResult.Document == nil || refreshedResult.Document.Types["issue"].Fields["reviewed"].Default != false {
			t.Fatalf("%s ontology.apply did not refresh configured runtime: %s err=%v", e.name, refreshed, err)
		}
		created := composedRuntimeCall(t, transport, "bead.create", map[string]any{
			"type": "issue", "fields": map[string]any{"title": "configured blocker"},
		}, "b03-composition", "00000000000000000000000000000003")
		var blockerCommit model.Commit
		if err := json.Unmarshal(created, &blockerCommit); err != nil || len(blockerCommit.Results) != 1 {
			t.Fatalf("%s blocker create=%s err=%v", e.name, created, err)
		}
		createdDoc, err := pipeline.DecodeDocument(blockerCommit.Results[0].Record)
		if err != nil || createdDoc.Fields["reviewed"] != false {
			t.Fatalf("%s ordinary create did not use refreshed ontology: %+v err=%v", e.name, createdDoc, err)
		}
		blocker := blockerCommit.Results[0].Ref
		dependentRaw := composedRuntimeCall(t, transport, "bead.create", map[string]any{
			"type": "issue", "fields": map[string]any{"title": "configured dependent"},
		}, "b03-composition", "00000000000000000000000000000004")
		var dependentCommit model.Commit
		if err := json.Unmarshal(dependentRaw, &dependentCommit); err != nil || len(dependentCommit.Results) != 1 {
			t.Fatalf("%s dependent create=%s err=%v", e.name, dependentRaw, err)
		}
		dependent := dependentCommit.Results[0].Ref
		composedRuntimeCall(t, transport, "bead.link", map[string]any{"from": dependent, "to": blocker, "kind": "conditional-blocks"}, "b03-composition", "00000000000000000000000000000005")
		composedRuntimeCall(t, transport, "bead.close", map[string]any{"id": blocker, "reason": "configured audit"}, "b03-composition", "00000000000000000000000000000006")

		meta, err := runtime.Store.Meta(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		at := meta.Head
		reader, err := runtime.Store.Read(context.Background(), driver.View{Incarnation: meta.Incarnation, At: &at})
		if err != nil {
			t.Fatal(err)
		}
		sets, err := reader.Changesets(context.Background(), nil, 100)
		_ = reader.Close()
		if err != nil || len(sets) == 0 {
			t.Fatalf("%s changesets=%v err=%v", e.name, sets, err)
		}
		last := sets[len(sets)-1]
		if len(last.Events) < 2 {
			t.Fatalf("%s configured close did not publish conditional audit event: %+v", e.name, last)
		}
		var pinned bool
		for _, event := range last.Events {
			if event.Details == nil || event.Details.Ontology.Ref.ID != sourceRef.ID || event.Details.Ontology.Ref.Branch.String() != sourceRef.Branch.String() {
				continue
			}
			if event.Details.Ontology.Revision == applied.Revision && event.Details.Ontology.Digest == updatedCompiled.Digest() {
				pinned = true
			}
		}
		if !pinned {
			t.Fatalf("%s configured lifecycle lost ontology pin: %+v want ref=%s revision=%s digest=%s", e.name, last.Events, sourceRef.ID, applied.Revision, updatedCompiled.Digest())
		}
		// Traverse the actual migration boundary and its row patch before the
		// later lifecycle notice. A legacy broad /fields patch has insufficient
		// slot provenance and must not be guessed by the history materializer.
		for _, want := range []struct {
			kind model.DurableEventType
			ref  string
		}{{model.EventOntologyApplied, sourceRef.ID}, {model.EventBeadUpdated, existing.Results[0].Ref.ID}} {
			if !composedEventListContains(t, transport, "b03-composition", string(want.kind), want.ref) {
				t.Fatalf("%s event.list omitted committed migration event %s for %s", e.name, want.kind, want.ref)
			}
		}
		if !composedEventListContains(t, transport, "b03-composition", string(model.EventDependencyConditionUnmet), sourceRef.ID) {
			t.Fatalf("%s event.list omitted persisted installed ontology audit", e.name)
		}
	})
}
