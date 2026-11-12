package cardb20

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/integration/converterroundtrip"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/testclock"
	workflowplugin "github.com/buildbetter-app/plugs/plugins/workflow"
)

// These expectations are authored independently of the captured archive.
// SQL NULL is different from both an absent field and a timestamp string.
type b24TimerOracle struct {
	ID, Project, Title, Status string
	Due, Defer                 *time.Time
}

func b24TimerValue(t *testing.T, field string, value any, want *time.Time) {
	t.Helper()
	if want == nil {
		if value != nil {
			t.Fatalf("%s lost SQL NULL: %#v", field, value)
		}
		return
	}
	text, ok := value.(string)
	if !ok {
		t.Fatalf("%s timestamp is not text: %#v", field, value)
	}
	var observed time.Time
	var err error
	for _, layout := range []string{time.RFC3339Nano, "2006-01-02 15:04:05.999999999"} {
		observed, err = time.Parse(layout, text)
		if err == nil {
			break
		}
	}
	if err != nil || !observed.Equal(*want) {
		t.Fatalf("independent %s oracle: raw=%q actual=%s want=%s err=%v", field, text, observed, want, err)
	}
}

func b24TimerSource(t *testing.T, data []byte, want b24TimerOracle) dumpformat.Record {
	t.Helper()
	a, err := dumpformat.Read(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range a.Records("bead") {
		if row.ID != want.ID || row.Project != want.Project {
			continue
		}
		if row.Body["title"] != want.Title {
			t.Fatalf("native title update lost: %#v", row.Body["title"])
		}
		for name, expected := range map[string]*time.Time{"due_at": want.Due, "defer_until": want.Defer} {
			physical, present := row.Source.Fields[name]
			portable, bodyPresent := row.Body[name]
			if !present || !bodyPresent || !reflect.DeepEqual(physical, portable) {
				t.Fatalf("actual physical/portable %s missing or unequal: %#v/%#v", name, physical, portable)
			}
			b24TimerValue(t, name, physical, expected)
		}
		return row
	}
	t.Fatalf("actual timer source omitted %s/%s", want.Project, want.ID)
	return dumpformat.Record{}
}

// This is the explicit disposable host plan for the single canonical timer
// fixture, following converterroundtrip's retained nativeHistoryPlanFromArchive.
// It maps that one independently declared durable bead and retains every other
// captured record/sidecar as an origin; it grants no source-driven authority.
func b24BindTimerSource(t *testing.T, f *fixture, data []byte, profile driver.RestoreImportProfile, want b24TimerOracle) driver.RestoreImportProfile {
	t.Helper()
	a, err := dumpformat.Read(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	if a.Manifest().SourcePlaneAddressContract == "" {
		return profile
	}
	if a.HasNativeSourceHistories() {
		t.Fatal("this canonical snapshot fixture requires a separate full-history admission plan for native table histories")
	}
	meta, err := f.store.Meta(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	capability, err := driver.RequireWorkflowCheckpointStore(f.store, f.lease)
	if err != nil {
		t.Fatal(err)
	}
	lifetime := driver.CheckpointLifetimeRestart
	if f.engine == "memory" {
		lifetime = driver.CheckpointLifetimeProcess
	}
	// Process lifetime is an explicit workflow capability. Restart persistence
	// is a separate, stronger optional driver capability; direct memory stores
	// do not promise it. Observe both before asserting either contract.
	actualLifetime := capability.WorkflowCheckpointLifetime()
	persistence, hasPersistence := capability.(driver.PersistentCheckpointStore)
	persistent := hasPersistence && persistence.Persistent()
	b24Retain(t, want.ID+"-"+f.engine+"-checkpoint-capability.json", map[string]any{
		"workflow_lifetime": actualLifetime, "persistent_capability_present": hasPersistence,
		"restart_persistent": persistent, "persistent_basis": "optional-driver-capability",
	})
	if actualLifetime != lifetime || persistent != (f.engine != "memory") {
		t.Fatalf("actual import checkpoint capability: lifetime=%q persistent_capability=%t restart_persistent=%t", actualLifetime, hasPersistence, persistent)
	}
	principal, err := json.Marshal(profile.Admission.Actor)
	if err != nil {
		t.Fatal(err)
	}
	plan := driver.SourcePlanePlanV1{
		Version: driver.SourcePlanePlanVersionV1, AddressContract: driver.SourcePlaneAddressContractV1,
		SourceStoreID: a.Manifest().StoreID, ArchiveSHA256: a.SHA256(),
		Target:     driver.SourcePlaneTargetV1{StoreID: string(meta.StoreID), Incarnation: meta.Incarnation, RequireEmpty: true, Lifetime: lifetime, RuntimeContract: driver.SourcePlaneRuntimeContractV1},
		Principals: []driver.SourcePlanePrincipalV1{{Key: "timer-admin", Source: "explicit disposable B24 fixture host", Authority: "trusted-explicit/1", PrincipalJSON: principal}},
		Providers:  []driver.SourcePlaneProviderV1{{Alias: "workflow", Instance: "default", Namespace: "plugin/workflow/default/durable", Schema: workflowplugin.SourceStateSchema, Codec: "workflow/json/1", RuntimeContract: driver.SourcePlaneRuntimeContractV1, Lifetime: lifetime, OriginAdapter: "beads-workflow-origins/1"}},
	}
	disposition := driver.SourcePlaneDispositionV1{
		Key: "timer-durable", SourceType: "task", StorageClass: "", SinkKind: "bead", Schema: "bead/1", Lifetime: "core",
		Statuses: []driver.SourcePlaneStringPairV1{{From: "deferred", To: want.Status}}, RefusalPolicy: "exact-only",
	}
	commitment := func(data []byte, presence string) driver.SourcePlaneBytesV1 {
		return driver.SourcePlaneBytesV1{Presence: presence, Size: uint64(len(data)), SHA256: fmt.Sprintf("%x", sha256.Sum256(data))}
	}
	records, err := a.SourcePlaneRecords()
	if err != nil {
		t.Fatal(err)
	}
	for _, record := range records {
		var row dumpformat.Record
		var wire struct {
			Source struct {
				Fields map[string]json.RawMessage `json:"fields"`
			} `json:"source"`
		}
		decoder := json.NewDecoder(bytes.NewReader(record.Raw))
		decoder.UseNumber()
		if decoder.Decode(&row) != nil || json.Unmarshal(record.Raw, &wire) != nil {
			t.Fatal("captured source record cannot be decoded losslessly")
		}
		var census []driver.SourcePlaneFieldV1
		for name, raw := range wire.Source.Fields {
			presence := "number"
			switch bytes.TrimSpace(raw)[0] {
			case 'n':
				presence = "null"
			case '{':
				presence = "object"
			case '[':
				presence = "array"
			case 't', 'f':
				presence = "boolean"
			case '"':
				presence = "text"
				var text string
				if err := json.Unmarshal(raw, &text); err != nil {
					t.Fatal(err)
				}
				raw = []byte(text)
			}
			census = append(census, driver.SourcePlaneFieldV1{Name: name, Value: commitment(raw, presence)})
		}
		sort.Slice(census, func(i, j int) bool { return census[i].Name < census[j].Name })
		origin := commitment(record.Raw, "object")
		if row.Kind == "bead" {
			if row.ID != want.ID || row.Project != want.Project || row.Source.Table != dumpformat.DurableEntityTable || len(plan.Entities) != 0 {
				t.Fatal("canonical timer admission observed an undeclared entity")
			}
			for _, field := range census {
				rule := driver.SourcePlaneFieldRuleV1{Source: field.Name, Disposition: "origin-only"}
				switch field.Name {
				case "id", "title", "status", "metadata", "created_at", "priority", "description":
					rule.Target, rule.Disposition = field.Name, "exact-map"
				case "issue_type":
					rule.Target, rule.Disposition = "type", "exact-map"
				}
				disposition.Fields = append(disposition.Fields, rule)
			}
			key := driver.SourceEntityKey{Project: row.Project, Plane: row.Source.Table, ID: row.ID}
			plan.Entities = append(plan.Entities, driver.SourcePlaneEntityV1{Source: key, TargetKind: "bead", TargetID: row.ID, DispositionKey: disposition.Key, PrincipalKey: "timer-admin", Route: driver.SourcePlaneRouteV1{Project: row.Project, SourceID: row.ID, DurableTargetID: row.ID, Preference: "active-first"}, Origin: origin, Fields: census})
			continue
		}
		project := row.Project
		if row.Source.Identity != nil {
			project = row.Source.Identity.Project
		}
		plan.Origins = append(plan.Origins, driver.SourcePlaneOriginV1{Project: project, Table: row.Source.Table, ID: row.ID, Action: "inert-origin", Raw: origin, Fields: census, Parsed: driver.SourcePlaneBytesV1{Presence: "absent"}})
	}
	if len(plan.Entities) != 1 {
		t.Fatal("canonical timer admission omitted the declared entity")
	}
	plan.Dispositions = []driver.SourcePlaneDispositionV1{disposition}
	sidecars, err := a.SourcePlaneSidecarOrigins()
	if err != nil {
		t.Fatal(err)
	}
	plan.Origins = append(plan.Origins, sidecars...)
	sort.Slice(plan.Origins, func(i, j int) bool {
		a, b := plan.Origins[i], plan.Origins[j]
		left, right := []string{a.Project, a.Table, a.ID, a.Path, a.Layer}, []string{b.Project, b.Table, b.ID, b.Path, b.Layer}
		for k := range left {
			if left[k] != right[k] {
				return left[k] < right[k]
			}
		}
		return false
	})
	profile.DerivedDigest = "" // The existing adapter binds the exact joint image.
	profile, err = converterroundtrip.BindSourceImport(f.ctx, bytes.NewReader(data), profile, plan)
	if err != nil {
		t.Fatal("bind actual canonical timer source plan", err)
	}
	b24Retain(t, want.ID+"-"+f.engine+"-joint-plan.json", map[string]any{"profile": profile, "plan": plan, "lifetime": lifetime, "persistent": persistent, "persistent_capability_present": hasPersistence})
	return profile
}

func b24TimerReopen(t *testing.T, f *fixture, application *testclock.Clock, id, phase string) {
	t.Helper()
	before := b24ReadSnapshot(t, f)
	epoch := f.lease.Epoch()
	b24ReopenClocks(t, f, application)
	after := b24ReadSnapshot(t, f)
	sameState(t, before.Logical, after.Logical)
	if f.engine == "memory" {
		if f.lease.Epoch() != epoch || !reflect.DeepEqual(before, after) {
			t.Fatal("same-store memory readback changed state or lease")
		}
		return
	}
	// An explicit release/close/open reacquires this fixture's owned lease.
	// That row must change; every content/history/receipt/derived row must not.
	leaseTable := "active_lease"
	if f.engine == "postgres" {
		leaseTable = "lease"
	}
	if f.lease.Epoch() != epoch+1 || len(before.Physical[leaseTable]) != 1 || len(after.Physical[leaseTable]) != 1 || reflect.DeepEqual(before.Physical[leaseTable], after.Physical[leaseTable]) {
		t.Fatal("SQL reopen did not acquire exactly the next owned lease epoch")
	}
	if len(before.Physical) != len(after.Physical) {
		t.Fatal("SQL reopen changed physical table set")
	}
	for table, rows := range before.Physical {
		if table != leaseTable && !reflect.DeepEqual(rows, after.Physical[table]) {
			t.Fatalf("SQL reopen changed physical content table %s", table)
		}
	}
	b24Retain(t, fmt.Sprintf("%s-%s-%s-reopen.json", id, f.engine, phase), map[string]any{
		"before": before, "after": after, "owned_lease_table": leaseTable,
		"prior_lease_epoch": epoch, "new_lease_epoch": f.lease.Epoch(),
	})
	t.Logf("B24_TIMER_SQL_REOPEN engine=%s phase=%s prior_epoch=%d new_epoch=%d conserved_tables=%d", f.engine, phase, epoch, f.lease.Epoch(), len(before.Physical)-1)
}

func b24RunTimerImport(t *testing.T, engine string, data []byte, want b24TimerOracle) {
	t.Helper()
	source := b24TimerSource(t, data, want)
	f := newFixture(t, engine)
	inspection, err := converterroundtrip.InspectImport(f.ctx, bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	// Trusted disposable test-host admission; the archive supplies no authority.
	profile := driver.RestoreImportProfile{
		Admission:     driver.RestoreImportAdmission{Mode: "authenticated", Actor: model.PrincipalRef{Kind: "user", ID: "b24-timer-import-admin"}, Authority: "restore-admin", AdmissionDigest: key("b24-timer-import-admission")},
		SourceStoreID: inspection.SourceStoreID, Method: "offline.restore.import", IdempotencyKey: key("b24-timer-import"),
		ArchiveDigest: inspection.ArchiveSHA256, DerivedDigest: inspection.DerivedSHA256, HistoryMode: driver.RestoreHistoryModeSnapshot,
	}
	profile = b24BindTimerSource(t, f, data, profile, want)
	restored, err := converterroundtrip.RestoreImportWithLease(f.ctx, bytes.NewReader(data), f.store, f.lease, profile)
	if err != nil || restored.Restored.Head != model.Natural64(1) || restored.Restored.HistoryFloor != model.Natural64(1) {
		t.Fatalf("actual snapshot import: %+v %v", restored, err)
	}
	// Bind callers to the new incarnation using the existing owned lease.
	if err := f.lease.Release(f.ctx); err != nil {
		t.Fatal(err)
	}
	f.bind(t)
	ref := model.RefByID(want.ID, model.Natural64(0))
	importReceipt := func() []byte {
		t.Helper()
		meta, err := f.store.Meta(f.ctx)
		if err != nil {
			t.Fatal(err)
		}
		r, err := f.store.Read(f.ctx, driver.View{Incarnation: meta.Incarnation})
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()
		principal, err := driver.PrincipalKey(profile.Admission.Actor)
		if err != nil {
			t.Fatal(err)
		}
		receipt, err := r.IdempotencyByKey(f.ctx, principal, profile.AgentLabel(), profile.Method, profile.IdempotencyKey)
		if err != nil || receipt == nil {
			t.Fatalf("actual import receipt disappeared: %v", err)
		}
		encoded, err := json.Marshal(receipt)
		if err != nil {
			t.Fatal(err)
		}
		return encoded
	}
	conserve := func(phase string, operation func()) {
		t.Helper()
		before := importReceipt()
		b24ConservedRead(t, f, phase, operation)
		if !bytes.Equal(before, importReceipt()) {
			t.Fatalf("%s changed the original import receipt", phase)
		}
	}
	params := func(extra map[string]any) map[string]any {
		out := map[string]any{"id": ref, "_meta": map[string]any{"project": want.Project}}
		for k, v := range extra {
			out[k] = v
		}
		return out
	}
	read := func(phase, title, status string, comments int) {
		t.Helper()
		conserve(phase, func() {
			got := f.call("bead.get", phase, params(nil))
			if got.Error != nil {
				t.Fatalf("public imported get: %+v", got.Error)
			}
			var result struct {
				Bead struct {
					Status   string                     `json:"status"`
					Fields   map[string]any             `json:"fields"`
					Metadata map[string]json.RawMessage `json:"metadata"`
				} `json:"bead"`
				Comments []struct {
					Body string `json:"body"`
				} `json:"comments"`
			}
			if err := json.Unmarshal(got.Result, &result); err != nil {
				t.Fatal(err)
			}
			if result.Bead.Fields["title"] != title || result.Bead.Status != status || len(result.Comments) != comments {
				t.Fatalf("public imported state mismatch: %s", got.Result)
			}
			for name, expected := range map[string]any{"due_at": source.Body["due_at"], "defer_until": source.Body["defer_until"], "source_fields": source.Source.Fields} {
				metadataKey := "beads.source." + name
				if name == "source_fields" {
					metadataKey = "beads.source_fields"
				}
				encoded, err := json.Marshal(expected)
				var observed, expectedJSON any
				decoder := json.NewDecoder(bytes.NewReader(result.Bead.Metadata[metadataKey]))
				decoder.UseNumber()
				if err != nil || decoder.Decode(&observed) != nil {
					t.Fatalf("missing exact imported %s", metadataKey)
				}
				decoder = json.NewDecoder(bytes.NewReader(encoded))
				decoder.UseNumber()
				if decoder.Decode(&expectedJSON) != nil || !reflect.DeepEqual(observed, expectedJSON) {
					t.Fatalf("imported %s changed: %s want=%s", metadataKey, result.Bead.Metadata[metadataKey], encoded)
				}
				if _, installed := result.Bead.Fields[name]; installed {
					t.Fatalf("source %s silently became a live timer field", name)
				}
			}
			if comments != 0 && result.Comments[0].Body != "waiting for timer input" {
				t.Fatal("imported defer reason changed")
			}
		})
	}
	// The independent native date stays opaque. The application clock controls
	// stale reads; the durable clock stays in 2026 throughout this matrix.
	target := time.Date(2099, 6, 15, 0, 0, 0, 0, time.UTC)
	application := testclock.New(t, target.Add(-24*time.Hour))
	b24BindApplicationClock(t, f, application)
	read("imported-initial", want.Title, want.Status, 0)
	check := converterroundtrip.Verification{ArchiveSHA256: profile.ArchiveDigest, DerivedSHA256: profile.DerivedDigest}
	recover := func(phase string) {
		t.Helper()
		if profile.SourcePlanes != nil {
			bound, recovered, found, err := converterroundtrip.RecoverBoundSourceImport(f.ctx, f.store)
			if err != nil || !found || !reflect.DeepEqual(bound, profile) || !reflect.DeepEqual(recovered.Receipt, restored.Receipt) || !reflect.DeepEqual(recovered.SourcePlanes, restored.SourcePlanes) || recovered.Restored.Incarnation != restored.Restored.Incarnation {
				t.Fatalf("%s joint import receipt recovery: found=%t error=%v", phase, found, err)
			}
			return
		}
		recovered, err := converterroundtrip.RecoverImport(f.ctx, f.store, profile, check)
		if err != nil || recovered.Restored.Incarnation != restored.Restored.Incarnation {
			t.Fatalf("%s import receipt recovery: %+v %v", phase, recovered, err)
		}
	}
	recover("initial")
	r, err := f.store.Read(f.ctx, driver.View{Incarnation: restored.Restored.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	_, err = r.Changesets(f.ctx, nil, 10000)
	if closeErr := r.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	var truncated driver.ErrTruncated
	if !errors.As(err, &truncated) || truncated.Floor != model.Natural64(1) {
		t.Fatalf("snapshot floor was hidden: %v", err)
	}
	b24TimerReopen(t, f, application, want.ID, "initial")
	recover("reopened")
	read("imported-reopened", want.Title, want.Status, 0)
	conserve("imported-timed-request-refused", func() {
		got := f.call("bead.defer", "imported-timed-request-refused", params(map[string]any{"reason": "waiting for timer input", "until": target.Format(time.RFC3339Nano)}))
		if got.Error == nil || got.Error.Data.Code != "invalid_params" {
			t.Fatalf("imported timestamp enabled an unsupported timed Plug request: %+v", got)
		}
	})
	updated := "timer fields survive unrelated Plug update"
	commit(t, f.call("bead.update", "imported-title-update", params(map[string]any{"fields": map[string]any{"title": updated}})))
	read("imported-title-updated", updated, want.Status, 0)
	conserve("imported-empty-reason", func() {
		got := f.call("bead.defer", "imported-empty-reason", params(map[string]any{"reason": " "}))
		if got.Error == nil || got.Error.Data.Code != "invalid_params" {
			t.Fatalf("blank defer reason accepted: %+v", got)
		}
	})
	if want.Status == "backlog" {
		// The imported mapping is already in the adopted backlog slot. A second
		// defer must refuse without appending a comment or changing any image.
		// Enter ReadyEligible explicitly before exercising a positive defer.
		conserve("imported-already-backlog-defer", func() {
			got := f.call("bead.defer", "imported-already-backlog-defer", params(map[string]any{"reason": "waiting for timer input"}))
			if got.Error == nil || got.Error.Data.Code != "invalid_params" {
				t.Fatalf("already-backlog defer lost its typed refusal: %+v", got)
			}
			violations, ok := got.Error.Data.Details["violations"].([]any)
			if !ok || len(violations) != 1 {
				t.Fatalf("already-backlog defer omitted its status violation: %+v", got.Error)
			}
			violation, ok := violations[0].(map[string]any)
			if !ok || violation["code"] != "invalid_params" || violation["field"] != "status" {
				t.Fatalf("already-backlog defer changed its status refusal: %+v", got.Error)
			}
		})
		commit(t, f.call("bead.reopen", "imported-prepare-defer", params(nil)))
		read("imported-ready-before-defer", updated, "todo", 0)
	} else if want.Status != "todo" {
		t.Fatalf("unrecognized independently declared import slot %q", want.Status)
	}
	prior := f.state(t)
	commit(t, f.call("bead.defer", "imported-defer", params(map[string]any{"reason": "waiting for timer input"})))
	deferred := f.state(t)
	if len(deferred.Versions[ref.ID]) != len(prior.Versions[ref.ID])+1 || len(deferred.Changesets) != len(prior.Changesets)+1 {
		t.Fatal("Plug defer did not use one ordinary version/ChangeSet")
	}
	read("imported-deferred", updated, "backlog", 1)
	tick := time.Nanosecond
	if engine == "postgres" {
		tick = time.Microsecond
	}
	for _, boundary := range []struct {
		name string
		at   time.Time
	}{
		{"before", target.Add(-tick)}, {"at", target}, {"after", target.Add(tick)},
	} {
		b24StepClocks(t, f, application, boundary.at.Sub(application.Now()), 0)
		b24ObserveClocks(t, f, application, "imported-target-"+boundary.name)
		read("imported-target-"+boundary.name, updated, "backlog", 1)
		conserve("imported-stale-"+boundary.name, func() {
			query := map[string]any{
				"definition": staleDefinition("status=backlog"), "stale": map[string]any{"days": 1, "status": "backlog"},
				"_meta": map[string]any{"project": want.Project},
			}
			ids, next := staleIDs(t, f.call("view.run", "imported-stale-"+boundary.name, query))
			query["countOnly"] = true
			count := staleCount(t, f.call("view.run", "imported-stale-count-"+boundary.name, query))
			wanted := []string{}
			if boundary.name == "after" {
				wanted = []string{want.ID}
			}
			if next != "" || len(ids) != len(wanted) || count != len(wanted) || len(wanted) != 0 && ids[0] != want.ID {
				t.Fatalf("imported strict stale boundary %s ids=%v count=%d next=%q want=%v", boundary.name, ids, count, next, wanted)
			}
		})
		conserve("imported-ready-"+boundary.name, func() {
			got := f.call("bead.ready", "imported-ready-"+boundary.name, map[string]any{"limit": 100, "_meta": map[string]any{"project": want.Project}})
			if got.Error != nil {
				t.Fatalf("imported ready: %+v", got.Error)
			}
			var page struct {
				Items []struct{ ID string } `json:"items"`
			}
			if json.Unmarshal(got.Result, &page) != nil {
				t.Fatal("invalid ready page")
			}
			for _, item := range page.Items {
				if item.ID == want.ID {
					t.Fatal("opaque imported timestamp automatically woke untimed Plug defer")
				}
			}
		})
		b24TimerReopen(t, f, application, want.ID, boundary.name)
		read("imported-target-"+boundary.name+"-reopened", updated, "backlog", 1)
	}
	commit(t, f.call("bead.reopen", "imported-explicit-reopen", params(nil)))
	read("imported-explicit-reopened", updated, "todo", 1)
	b24TimerReopen(t, f, application, want.ID, "final")
	read("imported-final-durable", updated, "todo", 1)
	conserve("imported-final-ready", func() {
		got := f.call("bead.ready", "imported-final-ready", map[string]any{"limit": 100, "_meta": map[string]any{"project": want.Project}})
		if got.Error != nil {
			t.Fatalf("reopened ready: %+v", got.Error)
		}
		var page struct {
			Items []struct{ ID string } `json:"items"`
		}
		if json.Unmarshal(got.Result, &page) != nil {
			t.Fatal("invalid reopened ready page")
		}
		found := false
		for _, item := range page.Items {
			found = found || item.ID == want.ID
		}
		if !found {
			t.Fatal("explicit reopen did not restore actual imported derived readiness")
		}
	})
	persistence := "SAME_STORE_MEMORY_ONLY"
	if engine != "memory" {
		persistence = "ACTUAL_SQL_CLOSE_OPEN"
	}
	b24Retain(t, fmt.Sprintf("%s-%s-timer-import.json", want.ID, engine), map[string]any{
		"oracle": want, "archive_sha256": inspection.ArchiveSHA256, "source_fields": source.Source.Fields,
		"restore": restored.Restored, "persistence": persistence, "target": target,
		"import_mapping":              "beads.source.due_at / beads.source.defer_until / beads.source_fields",
		"adopted_semantics":           "UNTIMED_DEFER_EXPLICIT_REOPEN_ALWAYS_ON_CHANGESETS",
		"native_exact_clock_equality": "NOT_MEASURED", "native_version_read": "NOT_MEASURED",
	})
	t.Logf("B24_TIMER_IMPORT engine=%s id=%s archive=%s persistence=%s source_due=%v source_defer=%v", engine, want.ID, inspection.ArchiveSHA256, persistence, source.Body["due_at"], source.Body["defer_until"])
}

func TestB24PublicImportedNullTimersDurable(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "internal", "integration", "converterroundtrip", "testdata", "public-complete-root-990eb800.tar"))
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprintf("%x", sha256.Sum256(data)) != "a2d933a7a54b53303d33a20d5b96eaac0a85c079c5b08cae553aa2a654d820b5" {
		t.Fatal("actual NULL-timer archive changed")
	}
	// This public source has no due/defer values, distinct from the native
	// non-NULL fixture created by the separately selected manual target.
	want := b24TimerOracle{ID: "demo-42v", Project: "demo", Title: "Public source updated through Beads", Status: "todo"}
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) { b24RunTimerImport(t, engine, data, want) })
	}
}
