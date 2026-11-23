package b01readyorder_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestB01PublicHybridOrderMembershipAndEveryPage(t *testing.T) {
	eachStore(t, func(t *testing.T, f *fixture) {
		now := f.clock.Now()
		// Priority reversals distinguish both buckets from priority-only order;
		// reverse lexical IDs distinguish creation ordering from ID ordering.
		f.seed(t,
			document("pl-old-ancient-p4", now.Add(-96*time.Hour), 4),
			document("pl-old-newer-p0", now.Add(-72*time.Hour), 0),
			document("pl-boundary-p2", now.Add(-48*time.Hour), 2),
			document("pl-just-old-p0", now.Add(-48*time.Hour-time.Nanosecond), 0),
			document("pl-just-new-p4", now.Add(-48*time.Hour+time.Nanosecond), 4),
			document("pl-recent-z-earlier", now.Add(-3*time.Hour), 1),
			document("pl-recent-a-later", now.Add(-2*time.Hour), 1),
			document("pl-tie-z", now.Add(-time.Hour).In(time.FixedZone("offset", -12600)), 1),
			document("pl-tie-a", now.Add(-time.Hour), 1),
			document("pl-recent-p0", now.Add(-30*time.Minute), 0),
		)
		f.unchanged(t, func() {
			f.pages(t, nil, "pl-recent-p0", "pl-recent-z-earlier", "pl-recent-a-later", "pl-tie-a", "pl-tie-z", "pl-boundary-p2", "pl-just-new-p4", "pl-old-ancient-p4", "pl-old-newer-p0", "pl-just-old-p0")
		})
	})
}

func TestB01ExactMembershipAndFiltersPrecedeThePage(t *testing.T) {
	eachStore(t, func(t *testing.T, f *fixture) {
		at := f.clock.Now().Add(-time.Hour)
		parent := document("pl-open-parent", at, 3)
		child := document("pl-open-child", at, 1)
		child["parent"] = model.Ref{ID: "pl-open-parent"}
		deferred := document("pl-deferred", at, 0)
		deferred["fields"].(map[string]any)["status"] = "backlog"
		deferredChild := document("pl-deferred-child", at, 0)
		deferredChild["parent"] = model.Ref{ID: "pl-deferred"}
		grandchild := document("pl-deferred-grandchild", at, 0)
		grandchild["parent"] = model.Ref{ID: "pl-deferred-child"}
		blocker := document("pl-blocker", at, 0)
		blocker["fields"].(map[string]any)["status"] = "in_progress"
		blocked := document("pl-blocked", at, 0)
		blocked["links"] = []map[string]any{{"kind": "blocks", "to": model.Ref{ID: "pl-blocker"}}}
		closed := document("pl-closed", at, 0)
		closed["fields"].(map[string]any)["status"] = "done"
		template := document("pl-template", at, 0)
		template["fields"].(map[string]any)["template"] = true
		infra := document("pl-infrastructure", at, 0)
		infra["type"] = "infrastructure"
		other := document("pl-other-type", at, 0)
		other["type"] = "other"
		foreign := document("pl-other-project", at, 0)
		foreign["project"] = "another-project"
		active := document("pl-custom-active", at, 2)
		active["fields"].(map[string]any)["status"] = "custom_active"
		claimed := document("pl-leased", at, 0)
		claimed["lease"] = map[string]any{"principal": anonymous, "agent": "holder", "expiresAt": f.clock.Now().Add(5 * time.Minute), "ttlMs": model.Natural64(300000), "epoch": model.Natural64(f.lease.Epoch())}
		f.seed(t, parent, child, deferred, deferredChild, grandchild, blocker, blocked, closed, template, infra, other, foreign, active, claimed)
		// A stored front row, and even a rank for an absent row, cannot create
		// membership. This is a literal persisted projection, not a predicate.
		f.ranks(t,
			rankRow(t, "pl-blocked", at, f.clock.Now(), model.Natural64(1), 0, 0),
			rankRow(t, "pl-absent", at, f.clock.Now(), model.Natural64(1), 0, 0),
		)
		f.unchanged(t, func() {
			f.pages(t, nil, "pl-other-type", "pl-open-child", "pl-custom-active", "pl-open-parent")
			f.pages(t, map[string]any{"types": []string{"issue"}}, "pl-open-child", "pl-custom-active", "pl-open-parent")
			f.pages(t, map[string]any{"resources": []string{"pl-blocked", "pl-open-parent", "pl-custom-active"}}, "pl-custom-active", "pl-open-parent")
			f.pages(t, map[string]any{"types": []string{"other"}, "resources": []string{"pl-open-parent"}})
		})
	})
}

func TestB01DefaultReadyExcludesBeadsInfrastructureTypesAndAllowsExplicitOptIn(t *testing.T) {
	eachStore(t, func(t *testing.T, f *fixture) {
		now := f.clock.Now().Add(-time.Hour)
		controls := []string{"pl-ready-control", "pl-ready-epic"}
		control := document(controls[0], now, 1)
		epicControl := document(controls[1], now, 1)
		epicControl["type"] = "epic"
		f.seed(t, control, epicControl)
		for _, typ := range []string{"merge-request", "gate", "mole" + "cule", "r" + "ig", "agent", "role", "message"} {
			row := document("pl-excluded-"+typ, now, 1)
			row["type"] = typ
			f.seed(t, row)
		}
		f.pages(t, nil, controls[0], controls[1])
		for _, typ := range []string{"merge-request", "gate", "mole" + "cule", "r" + "ig", "agent", "role", "message"} {
			f.pages(t, map[string]any{"types": []string{typ}}, "pl-excluded-"+typ)
		}

		imported := document("pl-imported-gate", now, 1)
		imported["metadata"] = map[string]any{"_plug_native_type": "gate"}
		f.seed(t, imported)
		f.pages(t, nil, controls[0], controls[1])
		f.pages(t, map[string]any{"types": []string{"gate"}}, "pl-excluded-gate", "pl-imported-gate")
	})
}

func TestB01EpicParentReadyMembershipWithOpenClosedAndNoChildren(t *testing.T) {
	eachStore(t, func(t *testing.T, f *fixture) {
		now := f.clock.Now().Add(-time.Hour)
		openParent := document("pl-epic-open-parent", now, 1)
		openParent["type"] = "epic"
		openChild := document("pl-epic-open-child", now, 2)
		openChild["parent"] = model.Ref{ID: openParent["id"].(string)}
		closedParent := document("pl-epic-closed-parent", now, 1)
		closedParent["type"] = "epic"
		closedChild := document("pl-epic-closed-child", now, 2)
		closedChild["parent"] = model.Ref{ID: closedParent["id"].(string)}
		closedChild["fields"].(map[string]any)["status"] = "done"
		noChildren := document("pl-epic-no-children", now, 1)
		noChildren["type"] = "epic"
		f.seed(t, openParent, openChild, closedParent, closedChild, noChildren)
		f.pages(t, nil, "pl-epic-closed-parent", "pl-epic-no-children", "pl-epic-open-parent", "pl-epic-open-child")
		f.pages(t, map[string]any{"resources": []string{"pl-epic-open-parent"}}, "pl-epic-open-parent")
		f.pages(t, map[string]any{"resources": []string{"pl-epic-closed-parent"}}, "pl-epic-closed-parent")
		f.pages(t, map[string]any{"resources": []string{"pl-epic-no-children"}}, "pl-epic-no-children")
	})
}

func TestP39ReadyUsesLiveGateLinksOnEpicAncestors(t *testing.T) {
	eachStore(t, func(t *testing.T, f *fixture) {
		at := f.clock.Now().Add(-time.Hour)
		for _, tc := range []struct {
			name          string
			parentType    string
			parentStatus  string
			gateStatus    string
			explicitBlock bool
			childBlock    bool
			ready         bool
		}{
			{name: "closed-epic-with-open-gate", parentType: "epic", parentStatus: "done", gateStatus: "todo", ready: true},
			{name: "closed-gate-parent-with-open-gate", parentType: "gate", parentStatus: "done", gateStatus: "todo", ready: true},
			{name: "open-epic-with-open-gate", parentType: "epic", parentStatus: "todo", gateStatus: "todo"},
			{name: "open-epic-with-closed-gate", parentType: "epic", parentStatus: "todo", gateStatus: "done", ready: true},
			{name: "deferred-epic-with-closed-gate", parentType: "epic", parentStatus: "backlog", gateStatus: "done"},
			{name: "explicitly-blocked-closed-epic", parentType: "epic", parentStatus: "done", gateStatus: "done", explicitBlock: true},
			{name: "closed-epic-with-child-blocker", parentType: "epic", parentStatus: "done", gateStatus: "todo", childBlock: true},
		} {
			t.Run(tc.name, func(t *testing.T) {
				prefix := "pl-" + tc.name
				parentID, gateID := prefix+"-parent", prefix+"-blocker"
				childID, grandchildID := prefix+"-child", prefix+"-grandchild"
				blocker := document(gateID, at, 1)
				blocker["type"] = "gate"
				blocker["fields"].(map[string]any)["status"] = tc.gateStatus
				parent := document(parentID, at, 1)
				parent["type"] = tc.parentType
				fields := parent["fields"].(map[string]any)
				fields["status"] = tc.parentStatus
				if tc.explicitBlock {
					fields["blocked"] = true
				}
				blockingLinks := []map[string]any{{"kind": "blocks", "to": model.Ref{ID: gateID}}}
				parent["links"] = blockingLinks
				child := document(childID, at, 1)
				child["parent"] = model.Ref{ID: parentID}
				if tc.childBlock {
					child["links"] = blockingLinks
				}
				grandchild := document(grandchildID, at, 1)
				grandchild["parent"] = model.Ref{ID: childID}
				docs := []map[string]any{parent, blocker, child, grandchild}
				if tc.parentType == "gate" {
					epic := document(prefix+"-epic", at, 1)
					epic["type"] = "epic"
					parent["parent"] = model.Ref{ID: prefix + "-epic"}
					docs = append(docs, epic)
				}
				f.seed(t, docs...)
				filter := map[string]any{"resources": []string{childID, grandchildID}}
				if tc.ready {
					f.pages(t, filter, childID, grandchildID)
				} else {
					f.pages(t, filter)
				}
			})
		}
	})
}

func TestB01StoredRankLagIsBoundedAtSixtySeconds(t *testing.T) {
	eachStore(t, func(t *testing.T, f *fixture) {
		now := f.clock.Now()
		created := now.Add(-48 * time.Hour)
		f.seed(t, document("pl-aging", created, 0), document("pl-recent", now.Add(-time.Hour), 4))
		f.ranks(t, rankRow(t, "pl-aging", created, now, model.Natural64(1), 0, 0))
		f.pages(t, nil, "pl-aging", "pl-recent")
		for _, delta := range []time.Duration{time.Nanosecond, time.Minute - time.Nanosecond} {
			f.advance(t, delta)
			f.unchanged(t, func() { f.pages(t, nil, "pl-aging", "pl-recent") })
		}
		// The process was recomposed on every advance: the lag comes from the
		// real stored rank. At 60s+1ns it must no longer govern any page.
		f.advance(t, time.Nanosecond)
		f.unchanged(t, func() {
			f.pages(t, nil, "pl-recent", "pl-aging")
			page := decodePage(t, f.call(t, "bead.ready", map[string]any{"limit": 1}))
			if page.AsOf.Revision.String() != "2" || page.Items[0].Revision.String() != "1" {
				t.Fatalf("rank fallback rewrote head/row revision: %+v", page)
			}
		})
		// The answer's repair did not silently persist new rank bytes.
		r := f.reader(t)
		defer r.Close()
		rows, err := r.DerivedRows(f.ctx, driver.DerivedSortKey, "ready/")
		if err != nil || len(rows) != 1 || !reflect.DeepEqual(rows[0], rankRow(t, "pl-aging", created, now, model.Natural64(1), 0, 0)) {
			t.Fatalf("read changed persisted projection: %+v %v", rows, err)
		}
	})
}

func TestB01RankLagNeverDelaysMembership(t *testing.T) {
	eachStore(t, func(t *testing.T, f *fixture) {
		now := f.clock.Now()
		aging := document("pl-aging", now.Add(-48*time.Hour), 0)
		f.seed(t, aging, document("pl-recent", now.Add(-time.Hour), 4))
		f.ranks(t, rankRow(t, "pl-aging", now.Add(-48*time.Hour), now, model.Natural64(1), 0, 0))
		f.advance(t, time.Second)
		aging["fields"].(map[string]any)["status"] = "backlog"
		if _, err := conformance.Seed(f.ctx, f.store, f.lease, driver.ChangesetHeader{Time: f.clock.Now()}, []conformance.FixtureChange{{Ref: model.Ref{ID: "pl-aging"}, Kind: "bead", Op: model.OpPatch, Body: jsonBytes(t, aging), ChangedPaths: []string{"/fields/status"}}}); err != nil {
			t.Fatal(err)
		}
		f.unchanged(t, func() { f.pages(t, nil, "pl-recent") })
	})
}

func TestB01OrdinaryPriorityUpdateInvalidatesStoredRankImmediately(t *testing.T) {
	eachStore(t, func(t *testing.T, f *fixture) {
		now := f.clock.Now()
		created := now.Add(-time.Hour)
		f.seed(t, document("pl-changing", created, 0), document("pl-steady", created, 1))
		f.ranks(t, rankRow(t, "pl-changing", created, now, model.Natural64(1), 0, 0))
		f.pages(t, nil, "pl-changing", "pl-steady")
		if _, err := f.svc.Update(f.ctx, anonymous, service.UpdateRequest{ID: model.Ref{ID: "pl-changing"}, Fields: map[string]any{"priority": 4}}, model.WriteMeta{Project: project, Agent: "author", AgentLabel: "author", IdempotencyKey: fixtureKey("b01-update")}); err != nil {
			t.Fatal(err)
		}
		f.unchanged(t, func() { f.pages(t, nil, "pl-steady", "pl-changing") })
	})
}

func TestB01StaleSourceAndFutureRanksCannotOverrideCurrentOrder(t *testing.T) {
	for _, reason := range []string{"record-revision", "creation-time", "future-clock"} {
		t.Run(reason, func(t *testing.T) {
			eachStore(t, func(t *testing.T, f *fixture) {
				now := f.clock.Now()
				created := now.Add(-49 * time.Hour)
				f.seed(t, document("pl-old", created, 0), document("pl-recent", now.Add(-time.Hour), 4))
				ranked, sourceCreated, revision := now, created, model.Natural64(1)
				switch reason {
				case "record-revision":
					revision = model.Natural64(2)
				case "creation-time":
					sourceCreated = now
				case "future-clock":
					ranked = now.Add(time.Second)
				}
				f.ranks(t, rankRow(t, "pl-old", sourceCreated, ranked, revision, 0, 0))
				f.unchanged(t, func() { f.pages(t, nil, "pl-recent", "pl-old") })
			})
		})
	}
}

func TestB01MalformedStoredRanksRefuseWithoutPublication(t *testing.T) {
	for _, shape := range []string{"wrong-bucket", "wrong-priority", "missing-member", "null-member", "unknown-member", "trailing-value", "bad-json"} {
		t.Run(shape, func(t *testing.T) {
			eachStore(t, func(t *testing.T, f *fixture) {
				now := f.clock.Now()
				created := now.Add(-time.Hour)
				f.seed(t, document("pl-row", created, 2))
				value := map[string]any{"bucket": 0, "rank": 2, "createdAt": created, "rankedAt": now, "revision": model.Natural64(1)}
				switch shape {
				case "wrong-bucket":
					value["bucket"], value["rank"] = 1, 999
				case "wrong-priority":
					value["rank"] = 0
				case "missing-member":
					delete(value, "rank")
				case "null-member":
					value["bucket"] = nil
				case "unknown-member":
					value["membership"] = true
				}
				raw := jsonBytes(t, value)
				if shape == "trailing-value" {
					raw = append(raw, []byte(` {}`)...)
				}
				if shape == "bad-json" {
					raw = []byte(`{`)
				}
				f.ranks(t, driver.DerivedRow{Key: "ready/pl-row", Value: raw})
				f.unchanged(t, func() {
					response := f.call(t, "bead.ready", map[string]any{"limit": 1})
					if response.Error == nil || response.Error.Data.Code != "internal_error" || len(response.Result) != 0 {
						t.Fatalf("corrupt rank became a partial page: %+v", response)
					}
				})
			})
		})
	}
}

func TestB01LegacyMissingCreationTimeHasAnAtomicExplicitBoundary(t *testing.T) {
	eachStore(t, func(t *testing.T, f *fixture) {
		legacy := document("pl-legacy", f.clock.Now(), 0)
		delete(legacy, "createdAt")
		f.seed(t, legacy, document("pl-known", f.clock.Now(), 1))
		f.unchanged(t, func() {
			_, err := f.svc.Ready(f.ctx, anonymous, service.ReadyRequest{Project: project, Limit: 1})
			var typed *model.Error
			if !errors.As(err, &typed) || typed.Data.Code != "internal_error" || typed.Data.Details["reason"] != "ready_creation_time_unavailable" {
				t.Fatalf("legacy row received a fabricated time/order or lost refusal: %v", err)
			}
			response := f.call(t, "bead.ready", map[string]any{"limit": 1})
			if response.Error == nil || response.Error.Data.Code != "internal_error" || len(response.Result) != 0 {
				t.Fatalf("legacy row produced a short success: %+v", response)
			}
			f.pages(t, map[string]any{"resources": []string{"pl-known"}}, "pl-known")
		})
	})
}

func TestB01StoreScanningDoesNotDiscardQualifyingRowsAfterTheFirstThousand(t *testing.T) {
	eachStore(t, func(t *testing.T, f *fixture) {
		docs := make([]map[string]any, 0, 1003)
		for i := 0; i < 1001; i++ {
			doc := document(fmt.Sprintf("pl-aaa-excluded-%04d", i), f.clock.Now(), 0)
			doc["fields"].(map[string]any)["status"] = "done"
			docs = append(docs, doc)
		}
		docs = append(docs, document("pl-zzz-first", f.clock.Now(), 1), document("pl-zzz-second", f.clock.Now(), 2))
		f.seed(t, docs...)
		f.pages(t, nil, "pl-zzz-first", "pl-zzz-second")
	})
}

func TestB01PublicCursorBindingsAndLiveRevisionContract(t *testing.T) {
	eachStore(t, func(t *testing.T, f *fixture) {
		at := f.clock.Now().Add(-time.Hour)
		f.seed(t, document("pl-a", at, 0), document("pl-b", at, 1), document("pl-c", at, 2))
		first := decodePage(t, f.call(t, "bead.ready", map[string]any{"limit": 1}))
		if first.NextCursor == nil || first.Items[0].ID != "pl-a" {
			t.Fatalf("first hybrid page: %+v", first)
		}
		f.unchanged(t, func() {
			response := f.call(t, "bead.ready", map[string]any{"limit": 1, "cursor": *first.NextCursor, "filter": map[string]any{"resources": []string{"pl-b"}}})
			if response.Error == nil || response.Error.Data.Code != "cursor_invalid" {
				t.Fatalf("filter mismatch accepted old cursor: %+v", response)
			}
		})
		b := document("pl-b", at, 1)
		b["fields"].(map[string]any)["title"] = "edited between live pages"
		if _, err := conformance.Seed(f.ctx, f.store, f.lease, driver.ChangesetHeader{Time: f.clock.Now()}, []conformance.FixtureChange{{Ref: model.Ref{ID: "pl-b"}, Kind: "bead", Op: model.OpPatch, Body: jsonBytes(t, b), ChangedPaths: []string{"/fields/title"}}}); err != nil {
			t.Fatal(err)
		}
		second := decodePage(t, f.call(t, "bead.ready", map[string]any{"limit": 1, "cursor": *first.NextCursor}))
		if second.Items[0].ID != "pl-b" || second.Items[0].Revision.String() != "2" || second.AsOf.Revision.String() != "2" || first.AsOf.Revision.String() != "1" {
			t.Fatalf("ready cursor became a historical or invalid revision page: first %+v second %+v", first, second)
		}
	})
}

func TestB01CreationClockSeamFeedsOrderWithoutChangingCreationOnUpdate(t *testing.T) {
	eachStore(t, func(t *testing.T, f *fixture) {
		createdAt := f.clock.Now()
		commit, err := f.svc.Create(f.ctx, anonymous, service.CreateRequest{Type: "issue", Fields: map[string]any{"title": "created by writer", "priority": 1}}, model.WriteMeta{Project: project, AgentLabel: "author", IdempotencyKey: fixtureKey("b01-create")})
		if err != nil || len(commit.Results) != 1 {
			t.Fatalf("ordinary create: %+v %v", commit, err)
		}
		id := commit.Results[0].Ref
		readCreation := func() time.Time {
			r := f.reader(t)
			defer r.Close()
			rows, err := r.Get(f.ctx, []model.Ref{id})
			if err != nil || len(rows) != 1 {
				t.Fatalf("persisted creation read: %+v %v", rows, err)
			}
			var envelope struct{ CreatedAt *time.Time }
			if err := json.Unmarshal(rows[0].Body, &envelope); err != nil || envelope.CreatedAt == nil {
				t.Fatalf("ordinary create omitted actual clock: %s %v", rows[0].Body, err)
			}
			return *envelope.CreatedAt
		}
		if got := readCreation(); !got.Equal(createdAt) {
			t.Fatalf("persisted creation %s, want actual writer time %s", got, createdAt)
		}
		f.advance(t, 49*time.Hour)
		if _, err := f.svc.Update(f.ctx, anonymous, service.UpdateRequest{ID: id, Fields: map[string]any{"title": "changed later"}}, model.WriteMeta{Project: project, AgentLabel: "author", IdempotencyKey: fixtureKey("b01-update")}); err != nil {
			t.Fatal(err)
		}
		if got := readCreation(); !got.Equal(createdAt) {
			t.Fatalf("edit rewrote creation: %s -> %s", createdAt, got)
		}
		// The preceding Writer created certified source coverage. This literal
		// fixture is an explicit root with no outgoing links, using the same
		// typed publication contract as the existing real driver fixture helper.
		recent := document("pl-recent-p4", f.clock.Now(), 4)
		if _, err := conformance.Seed(f.ctx, f.store, f.lease, driver.ChangesetHeader{Time: f.clock.Now()}, []conformance.FixtureChange{{
			Ref: model.Ref{ID: "pl-recent-p4"}, Kind: "bead", Op: model.OpCreate, Body: jsonBytes(t, recent), Graph: &conformance.FixtureGraphSource{},
		}}); err != nil {
			t.Fatal(err)
		}
		f.unchanged(t, func() { f.pages(t, nil, "pl-recent-p4", id.ID) })
	})
}

func TestB01PublicCutoffOrderRemainsLiveAcrossCursorContinuation(t *testing.T) {
	eachStore(t, func(t *testing.T, f *fixture) {
		old := f.clock.Now().Add(-47 * time.Hour)
		f.seed(t, document("pl-old", old, 0), document("pl-new", f.clock.Now(), 4))
		first := decodePage(t, f.call(t, "bead.ready", map[string]any{"limit": 1}))
		if len(first.Items) != 1 || first.Items[0].ID != "pl-old" || first.NextCursor == nil {
			t.Fatalf("initial cutoff page: %+v", first)
		}
		// This fixture binds the unguarded core service: its cursor is a live
		// offset, rather than the guarded authorized-set fingerprint. With a
		// stable signing key, both continuations expose that actual contract.
		stable := decodePage(t, f.call(t, "bead.ready", map[string]any{"limit": 1, "cursor": *first.NextCursor}))
		if len(stable.Items) != 1 || stable.Items[0].ID != "pl-new" {
			t.Fatalf("unchanged live continuation: %+v", stable)
		}
		f.advance(t, 2*time.Hour)
		f.unchanged(t, func() {
			f.pages(t, nil, "pl-new", "pl-old")
			live := decodePage(t, f.call(t, "bead.ready", map[string]any{"limit": 1, "cursor": *first.NextCursor}))
			if len(live.Items) != 1 || live.Items[0].ID != "pl-old" || live.AsOf.Revision.String() != first.AsOf.Revision.String() {
				t.Fatalf("plain core cursor did not retain its live offset at the unchanged revision: %+v", live)
			}
		})
	})
}

func TestB01QueueSeedDoesNotCrossHybridOrderBoundaries(t *testing.T) {
	eachStore(t, func(t *testing.T, f *fixture) {
		now := f.clock.Now()
		f.seed(t,
			document("pl-recent-first", now.Add(-time.Hour), 0),
			document("pl-recent-second", now.Add(-2*time.Hour), 4),
			document("pl-old-first", now.Add(-96*time.Hour), 4),
			document("pl-old-second", now.Add(-72*time.Hour), 0),
		)
		f.unchanged(t, func() {
			for i := 0; i < 32; i++ {
				page, err := f.svc.ClaimNextReadyPage(f.ctx, anonymous, service.ReadyRequest{Project: project, Limit: 1000, ClaimNextSeed: service.ClaimNextSelectionSeed("hybrid-selector", fmt.Sprintf("key-%d", i))})
				if err != nil {
					t.Fatal(err)
				}
				got := make([]string, len(page.Items))
				for j, bead := range page.Items {
					got[j] = bead.ID
				}
				want := []string{"pl-recent-first", "pl-recent-second", "pl-old-first", "pl-old-second"}
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("queue seed crossed a bucket/rank/creation boundary: got %v want %v", got, want)
				}
			}
		})
	})
}

type failedReadStore struct {
	driver.Store
	which string
}

func (s failedReadStore) Read(ctx context.Context, view driver.View) (driver.Reader, error) {
	r, err := s.Store.Read(ctx, view)
	if err != nil {
		return nil, err
	}
	return failedReader{Reader: r, which: s.which}, nil
}

type failedReader struct {
	driver.Reader
	which string
}

func (r failedReader) Query(ctx context.Context, q driver.Query) (driver.Page[driver.Record], error) {
	if r.which == "graph" {
		return driver.Page[driver.Record]{}, errors.New("injected real query failure")
	}
	return r.Reader.Query(ctx, q)
}

func (r failedReader) DerivedRows(ctx context.Context, table driver.DerivedTable, prefix string) ([]driver.DerivedRow, error) {
	if r.which == "rank" {
		return nil, errors.New("injected derived query failure")
	}
	return r.Reader.DerivedRows(ctx, table, prefix)
}

func TestB01QueryFailuresNeverBecomeEmptyOrPartialSuccess(t *testing.T) {
	for _, which := range []string{"graph", "rank"} {
		t.Run(which, func(t *testing.T) {
			eachStore(t, func(t *testing.T, f *fixture) {
				f.seed(t, document("pl-row", f.clock.Now(), 0))
				store := failedReadStore{Store: f.store, which: which}
				writer := pipeline.New(store, f.lease, pipeline.WithOntology(f.onto), pipeline.WithClock(f.clock.Now))
				svc := service.New(store, f.lease, writer, service.WithOntology(f.onto), service.WithClock(f.clock.Now))
				f.unchanged(t, func() {
					page, err := svc.Ready(f.ctx, anonymous, service.ReadyRequest{Project: project, Limit: 1})
					if err == nil || len(page.Items) != 0 || page.Next != nil {
						t.Fatalf("%s failure became a ready page: %+v %v", which, page, err)
					}
				})
			})
		})
	}
}
