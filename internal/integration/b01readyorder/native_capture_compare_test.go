package b01readyorder_test

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/integration/converterroundtrip"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/testclock"
	workflowplugin "github.com/buildbetter-app/plugs/plugins/workflow"
)

type b01CapturedConsumer struct {
	p        *publicAcceptance
	input    *b01CapturedReadyInput
	profile  driver.RestoreImportProfile
	report   converterroundtrip.Report
	plan     driver.SourcePlanePlanV1
	targets  map[string]string
	statuses map[string]string
}

type b01CapturedConsumerState struct {
	Durable durableState
	All     []driver.Record
	Plugin  driver.Checkpoint
}

func (c *b01CapturedConsumer) state(t *testing.T) b01CapturedConsumerState {
	t.Helper()
	state := b01CapturedConsumerState{Durable: c.p.state(t), Plugin: c.p.checkpoint(t)}
	r := c.p.reader(t)
	defer r.Close()
	var cursor *driver.Cursor
	for {
		page, err := r.Query(c.p.ctx, driver.Query{Limit: 1000, Cursor: cursor})
		if err != nil {
			t.Fatal(err)
		}
		state.All = append(state.All, page.Items...)
		if page.NextCursor == nil {
			break
		}
		if len(page.Items) == 0 {
			t.Fatal("unfiltered capture conservation query failed to progress")
		}
		cursor = page.NextCursor
	}
	return state
}

func (c *b01CapturedConsumer) ids(t *testing.T, roles []string) []string {
	t.Helper()
	out := make([]string, len(roles))
	for i, role := range roles {
		out[i] = c.targets[c.input.Roles[role]]
		if out[i] == "" {
			t.Fatal("source role lacks an explicit plane-preserving target", role)
		}
	}
	return out
}

func (c *b01CapturedConsumer) pages(t *testing.T, filter map[string]any, roles ...string) {
	t.Helper()
	want := c.ids(t, roles)
	for _, limit := range []int{1, 2, 1000} {
		params := map[string]any{"limit": limit, "filter": filter, "_meta": map[string]any{"project": b01CaptureProject}}
		actual := []string{}
		seen := map[string]bool{}
		for {
			page := decodePage(t, c.p.call(t, "bead.ready", params))
			if len(page.Items) > limit {
				t.Fatal("captured comparison exceeded the public page limit")
			}
			for _, item := range page.Items {
				if seen[item.ID] || item.Revision.Validate() != nil {
					t.Fatal("captured comparison repeated or invented a public row", item.ID)
				}
				seen[item.ID] = true
				actual = append(actual, item.ID)
			}
			if len(actual) > len(want) {
				t.Fatalf("public captured membership exceeded literal roles: %v want %v", actual, want)
			}
			if page.NextCursor == nil {
				break
			}
			if len(page.Items) != limit || *page.NextCursor == "" {
				t.Fatal("captured comparison has an empty/short intermediate page")
			}
			params["cursor"] = *page.NextCursor
		}
		if !reflect.DeepEqual(actual, want) {
			t.Fatalf("captured native/Plug comparison limit=%d got=%v want=%v", limit, actual, want)
		}
		t.Logf("B01_CAPTURE_PUBLIC_PAGE engine=%s limit=%d filter=%s actual=%v", c.p.engine, limit, jsonBytes(t, filter), actual)
	}
}

func (c *b01CapturedConsumer) readback(t *testing.T, changedTitles map[string]string) {
	t.Helper()
	for _, entity := range c.plan.Entities {
		var source dumpformat.Row
		for _, row := range c.input.Tables[entity.Source.Plane].Rows {
			if row["id"] == entity.Source.ID {
				source = row
			}
		}
		if source == nil {
			t.Fatal("projected target lacks its authenticated SQL input", entity.Source)
		}
		// The public native source address must resolve to the same real target
		// as its explicit target ID, including every active workflow item.
		for _, address := range []string{entity.Source.ID, entity.TargetID} {
			response := c.p.call(t, "bead.get", map[string]any{"id": model.RefByID(address, model.Natural64(0)), "_meta": map[string]any{"project": b01CaptureProject}})
			var got struct{ Bead service.Bead }
			if response.Error != nil {
				t.Fatalf("public captured source get %s refused: %+v", address, response.Error)
			}
			b01CaptureDecode(t, response.Result, &got)
			wantStatus := b01CaptureText(t, source, "status")
			if entity.TargetKind == "bead" {
				wantStatus = b01CaptureStatus(t, c.input.Archive, wantStatus)
			}
			if changed, ok := c.statuses[entity.Source.ID]; ok {
				wantStatus = changed
			}
			created, err := time.Parse(time.RFC3339Nano, b01CaptureText(t, source, "created_at"))
			if err != nil || got.Bead.ID != entity.TargetID || got.Bead.Project != b01CaptureProject || got.Bead.Type != source["issue_type"] || got.Bead.Status != wantStatus || !got.Bead.CreatedAt.Equal(created) {
				t.Fatalf("native/public identity, status or creation differs: source=%s got=%s", entity.Source.ID, response.Result)
			}
			for _, field := range []string{"title", "description", "priority"} {
				want, present := source[field]
				if title, changed := changedTitles[entity.Source.ID]; field == "title" && changed {
					want = title
				}
				actual, found := got.Bead.Fields[field]
				if !present || !found || !reflect.DeepEqual(jsonBytes(t, actual), jsonBytes(t, want)) {
					t.Fatalf("native/public typed field differs: %s.%s got=%#v present=%t want=%#v", entity.Source.ID, field, actual, found, want)
				}
			}
			if entity.TargetKind == "workflow:item" && (got.Bead.Plane != "ephemeral" || got.Bead.Kind != workflowplugin.ItemKind || got.Bead.WorkflowRunID != c.plan.Runs[0].TargetID) {
				t.Fatal("genuine active native item was replaced by a durable counterpart")
			}
			original := c.input.CrossPlane
			if entity.Source.ID == original.ChildID && (entity.Source.Plane != dumpformat.DurableEntityTable || entity.TargetKind != "bead" || entity.TargetID != original.ChildID || got.Bead.ID != original.ChildID || got.Bead.Plane == "ephemeral") {
				t.Fatal("public import/readback renumbered or replaced the original durable child", original.ChildID)
			}
			if entity.Source.ID == original.ParentID {
				var wire struct {
					Bead map[string]json.RawMessage `json:"bead"`
				}
				b01CaptureDecode(t, response.Result, &wire)
				rawCounter, present := wire.Bead["childCounter"]
				if !present {
					t.Fatal("public active-parent readback omitted the original allocation counter")
				}
				var counter model.Natural
				b01CaptureDecode(t, rawCounter, &counter)
				value, err := counter.Uint64()
				if err != nil || value != original.HighWater || got.Bead.ChildCounter != original.HighWater {
					t.Fatal("public active-parent counter differs from original native SQL high-water mark", original.HighWater)
				}
				t.Logf("B01_CAPTURE_PUBLIC_CHILD_COUNTER engine=%s address=%s parent=%s child=%s child_number=%d actual_high_water=%d native_high_water=%d", c.p.engine, address, got.Bead.ID, original.ChildID, original.ChildNumber, value, original.HighWater)
			}
		}
	}
}

func (c *b01CapturedConsumer) reopen(t *testing.T) {
	t.Helper()
	if c.p.engine == "memory" {
		return // Its process lifetime and Persistent=false were asserted at mount.
	}
	if err := c.p.lease.Release(c.p.ctx); err != nil {
		t.Fatal(err)
	}
	c.p.lease = nil
	if err := c.p.store.Close(); err != nil {
		t.Fatal(err)
	}
	c.p.open(t)
	profile, recovered, found, err := converterroundtrip.RecoverBoundSourceImport(c.p.ctx, c.p.store)
	if recovered.Archive != nil {
		defer recovered.Archive.ReleaseNativeDecoded()
	}
	if err != nil || !found || !reflect.DeepEqual(jsonBytes(t, profile), jsonBytes(t, c.profile)) || !reflect.DeepEqual(recovered.SourcePlanes, c.report.SourcePlanes) || !reflect.DeepEqual(recovered.Receipt, c.report.Receipt) || !reflect.DeepEqual(recovered.NativeDeltaHistory, c.report.NativeDeltaHistory) {
		t.Fatal("actual SQL reopen lost authenticated capture, origin, lifetime or original receipt", err)
	}
	if err := converterroundtrip.VerifyNativeCaptureSetReadback(c.p.ctx, c.p.store, c.input.Archive, recovered); err != nil {
		t.Fatal("actual reopened native SQL rows differ", err)
	}
	b01MountCapturedWorkflow(t, c.p, profile, recovered)
}

func (c *b01CapturedConsumer) point(t *testing.T, name string, titles map[string]string, filter map[string]any, want ...string) {
	t.Helper()
	t.Run(name, func(t *testing.T) {
		before := c.state(t)
		c.pages(t, filter, want...)
		c.readback(t, titles)
		if !reflect.DeepEqual(before, c.state(t)) {
			t.Fatal("native comparison read changed source, ranks, history, receipt or workflow bytes")
		}
		c.reopen(t)
		if !reflect.DeepEqual(before, c.state(t)) {
			t.Fatal("actual SQL reopen changed complete imported state or stored rank")
		}
		c.pages(t, filter, want...)
		c.readback(t, titles)
		if !reflect.DeepEqual(before, c.state(t)) {
			t.Fatal("reopened native comparison repaired or rewrote state")
		}
		if c.p.engine == "postgres" && !testclock.PostgresNow(t, c.p.dsn).Equal(c.p.storage.Now()) {
			t.Fatal("actual PostgreSQL clock differs after comparison/reopen")
		}
		t.Logf("B01_CAPTURE_COMPARISON engine=%s point=%s application=%s driver=%s reopened=%t roles=%v", c.p.engine, name, c.p.clock.Now().Format(time.RFC3339Nano), c.p.storage.Now().Format(time.RFC3339Nano), c.p.engine != "memory", want)
	})
}

func (in *b01CapturedReadyInput) nativeIDs(t *testing.T, command string, roles []string, limit int) []string {
	t.Helper()
	var rows []struct {
		ID string `json:"id"`
	}
	b01CaptureDecode(t, in.command(t, command), &rows)
	want := []string{}
	for _, role := range roles {
		want = append(want, in.Roles[role])
	}
	if limit > 0 && len(want) > limit {
		want = want[:limit]
	}
	actual := make([]string, len(rows))
	for i := range rows {
		actual[i] = rows[i].ID
	}
	if !reflect.DeepEqual(actual, want) {
		t.Fatalf("original native ready %s differs from independent literal oracle: %v want=%v", command, actual, want)
	}
	return actual
}

func b01CompareCapturedReady(t *testing.T, fixturePath, wantSHA string) {
	t.Helper()
	in := b01ReadCapturedReady(t, fixturePath, wantSHA)
	boundary := time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC)
	for _, engine := range []string{"memory", "sqlite_wal", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			p := newPublicAcceptanceWithClocks(t, engine, false, boundary.Add(-time.Microsecond), boundary.Add(7*time.Hour))
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
			t.Cleanup(cancel)
			p.ctx = ctx
			plan := b01CapturePlan(t, p, in.Archive)
			profile, err := converterroundtrip.BindNativeCaptureSetImport(p.ctx, in.Archive, b01CaptureProfile(in.Archive, p.clock.Now()), plan)
			if err != nil {
				t.Fatal("bind authentic B01 source-plane correspondence", err)
			}
			report, err := converterroundtrip.RestoreNativeCaptureSetImportWithLease(p.ctx, in.Archive, p.store, p.lease, profile)
			if err != nil {
				t.Fatal("import genuine native B01 capture", err)
			}
			if err := converterroundtrip.VerifyNativeCaptureSetReadback(p.ctx, p.store, in.Archive, report); err != nil {
				t.Fatal("actual initial native SQL readback differs", err)
			}
			b01MountCapturedWorkflow(t, p, profile, report)
			c := &b01CapturedConsumer{p: p, input: in, profile: profile, report: report, plan: plan, targets: map[string]string{}, statuses: map[string]string{}}
			for _, entity := range plan.Entities {
				if c.targets[entity.Source.ID] != "" {
					t.Fatal("this recorded fixture unexpectedly contains an ambiguous source ID")
				}
				c.targets[entity.Source.ID] = entity.TargetID
			}
			mixed := c.ids(t, []string{"aging", "wisp", "blocked", "control"})
			filter := map[string]any{"resources": mixed, "includeEphemeral": true}
			fiveFilter := map[string]any{"resources": c.ids(t, []string{"old", "aging", "tie-z", "tie-a", "a-later"}), "includeEphemeral": false}
			titles := map[string]string{}
			c.point(t, "before-inclusive-cutoff", titles, filter, "aging", "wisp", "control")
			c.point(t, "five-role-before-inclusive-cutoff", titles, fiveFilter, "aging", "tie-a", "tie-z", "a-later", "old")
			p.clock.Advance(t, time.Microsecond)
			aging := model.RefByID(in.Roles["aging"], model.Natural64(0))
			titles[aging.ID] = "ranked at inclusive captured cutoff"
			updateParams := map[string]any{"id": aging, "fields": map[string]any{"title": titles[aging.ID]}, "_meta": map[string]any{"project": b01CaptureProject, "idempotencyKey": fixtureKey("captured-real-rank-at-cutoff")}}
			updated := p.call(t, "bead.update", updateParams)
			var commit model.Commit
			if updated.Error != nil || json.Unmarshal(updated.Result, &commit) != nil || commit.Revision.IsZero() || len(commit.Results) != 1 || commit.Results[0].Ref != aging {
				t.Fatalf("public Writer failed to establish real stored cutoff rank: %+v %s", updated.Error, updated.Result)
			}
			stored := connectedFact(t, p.fixture, aging, commit.Revision, boundary.Add(-48*time.Hour), boundary, 0, 0)
			for i, point := range in.Points {
				if point.Name == "mixed-before" {
					continue
				}
				if point.GoTime.Before(p.clock.Now()) || point.SQLTime.Before(p.storage.Now()) {
					t.Fatal("recorded mixed clocks cannot move backward in this counterpart")
				}
				p.clock.Advance(t, point.GoTime.Sub(p.clock.Now()))
				if point.SQLTime.After(p.storage.Now()) {
					before := c.state(t)
					if err := p.lease.Release(p.ctx); err != nil {
						t.Fatal(err)
					}
					p.lease = nil
					p.storage.Advance(t, point.SQLTime.Sub(p.storage.Now()))
					p.bind(t)
					b01MountCapturedWorkflow(t, p, profile, report)
					if !reflect.DeepEqual(before, c.state(t)) {
						t.Fatal("independent SQL-only movement rewrote imported state")
					}
				}
				nativeDefault := []string{"aging", "wisp", "control"}
				nativeHybrid := []string{"aging", "wisp", "control"}
				if point.GoTime.After(boundary) {
					nativeHybrid = []string{"wisp", "control", "aging"}
				}
				plug := []string{"aging", "wisp", "control"}
				if point.GoTime.After(boundary.Add(time.Minute)) {
					plug = []string{"wisp", "control", "aging"}
				}
				for _, limit := range []int{0, 1, 2, 1000} {
					for _, policy := range []struct {
						name string
						want []string
					}{{"default", nativeDefault}, {"hybrid", nativeHybrid}} {
						key := policy.name + "-" + strconv.Itoa(limit)
						actual := in.nativeIDs(t, point.Name+"-"+key, policy.want, limit)
						if !reflect.DeepEqual(actual, point.Observations[key]) {
							t.Fatal("native observations differ from command originals")
						}
					}
				}
				c.point(t, fmt.Sprintf("%02d-%s-adopted-P74", i, point.Name), titles, filter, plug...)
				fiveWant := []string{"aging", "tie-a", "tie-z", "a-later", "old"}
				if point.GoTime.After(boundary.Add(time.Minute)) {
					fiveWant = []string{"tie-a", "tie-z", "a-later", "old", "aging"}
				}
				c.point(t, "five-role-"+point.Name, titles, fiveFilter, fiveWant...)
				t.Logf("B01_CAPTURE_ORDER native_default=%v native_hybrid=%v Plug=%v stored_rank_cutoff=%s P74_inclusive_seconds=60", nativeDefault, nativeHybrid, plug, boundary.Format(time.RFC3339Nano))
			}
			c.point(t, "priority-two-before-paging", titles, map[string]any{"resources": mixed, "priority": 2}, "wisp")
			c.point(t, "durable-only-before-paging", titles, map[string]any{"resources": mixed, "includeEphemeral": false}, "control", "aging")
			for _, entry := range []struct {
				name, parent, child string
				include             bool
			}{
				{"durable-child-of-genuine-active-parent", "wisp", "ordinary_child", false},
				{"genuine-active-child-of-durable", "control", "active_child", true},
			} {
				c.point(t, entry.name, titles, map[string]any{"parent": c.targets[in.Roles[entry.parent]], "resources": c.ids(t, []string{entry.child}), "includeEphemeral": entry.include}, entry.child)
			}
			c.point(t, "unknown-parent-empty", titles, map[string]any{"parent": "clockready-missing", "includeEphemeral": true})
			blocker := model.RefByID(in.Roles["blocker"], model.Natural64(0))
			for _, transition := range []struct {
				method, status, point string
				want                  []string
			}{
				{"bead.close", "closed", "public-durable-close-releases-actual-active-item", []string{"blocked", "wisp", "control", "aging"}},
				{"bead.reopen", "open", "public-durable-reopen-restores-active-blocker", []string{"wisp", "control", "aging"}},
			} {
				response := p.call(t, transition.method, map[string]any{"id": blocker, "_meta": map[string]any{"project": b01CaptureProject, "idempotencyKey": fixtureKey("captured-" + transition.method)}})
				var commit model.Commit
				if response.Error != nil || json.Unmarshal(response.Result, &commit) != nil || commit.Revision.IsZero() || len(commit.Results) != 1 || commit.Results[0].Ref != blocker {
					t.Fatalf("captured public blocker lifecycle refused: %+v %s", response.Error, response.Result)
				}
				c.statuses[blocker.ID] = b01CaptureStatus(t, in.Archive, transition.status)
				c.point(t, transition.point, titles, filter, transition.want...)
			}
			if !reflect.DeepEqual(stored, connectedFact(t, p.fixture, aging, commit.Revision, boundary.Add(-48*time.Hour), boundary, 0, 0)) {
				t.Fatal("native comparison/read fallback repaired the stored rank")
			}
			beforeReplay := c.state(t)
			replayed := p.call(t, "bead.update", updateParams)
			if replayed.Error != nil || !reflect.DeepEqual(replayed.Result, updated.Result) || !reflect.DeepEqual(beforeReplay, c.state(t)) {
				t.Fatal("reopened real rank update lost its exact idempotent receipt")
			}
		})
	}
}
