package authzrpc_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/plugins/authzfixed"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// D26 authorizes old bead content using the bead's CURRENT assignment. Unlike
// domain Memory history.get/diff, bead history has no same-lifetime restriction.
// Delete/recreate is an explicit storage fixture, not a fixed-policy write API.
func TestFixedPermissionRecreatedBeadHistoryAcrossDrivers(t *testing.T) {
	countDrivers(t, func(t *testing.T, open storeFactory) {
		ctx := context.Background()
		f := newFixtureOn(t, open(t), nil)
		ref := fixtureRef(1)
		other := model.RefByID("other", model.Natural64(0))
		f.base.Projects = append(f.base.Projects, authzfixed.Project{Ref: other, Name: "other", InstallationID: "fixed", Active: true})
		f.base.Grants = append(f.base.Grants, authz.Grant{
			Subject: authz.SubjectRef{Kind: "user", ID: "user:operator"},
			Scope:   authz.Scope{Kind: authz.ProjectScope, Ref: other}, Role: "involved",
		})
		if err := f.facts.Replace(f.base); err != nil {
			t.Fatal(err)
		}
		oldWriter := worker("user:operator")
		const originalKey = 19100
		success(t, f.call(t, oldWriter, "bead.update", map[string]any{
			"id": ref, "fields": map[string]any{"title": "authorized first lifetime"},
		}, originalKey))
		original := f.receipt(t, oldWriter, "bead.update", originalKey)
		if original == nil || original.ResponsePending {
			t.Fatal("authorized first-lifetime write needs its final durable receipt")
		}
		old := f.reader(t)
		oldRows, err := old.Get(ctx, []model.Ref{ref})
		if err != nil || len(oldRows) != 1 {
			t.Fatalf("first-lifetime source: rows=%+v err=%v", oldRows, err)
		}
		view := old.Snapshot()
		asOf := map[string]any{"revision": *view.At, "incarnation": view.Incarnation}
		reader := func(project string) model.PrincipalRef {
			return model.PrincipalRef{Kind: "user", ID: "user:history", Caveats: map[string]any{
				"refs": []model.Ref{ref}, "projects": []model.Ref{model.RefByID(project, model.Natural64(0))},
			}}
		}
		point := func(t *testing.T, by model.PrincipalRef, at map[string]any, key int) reply {
			t.Helper()
			return f.call(t, by, "bead.get", map[string]any{"id": ref, "asOf": at}, key)
		}
		query := func(t *testing.T, by model.PrincipalRef, key int) rpcPage {
			t.Helper()
			p := page(t, f.call(t, by, "history.query", map[string]any{
				"asOf": asOf, "_meta": map[string]any{"project": "project"},
			}, key))
			if p.AsOf.Revision.String() != view.At.String() || p.AsOf.Incarnation != view.Incarnation || p.NextCursor != "" {
				t.Fatalf("history content snapshot changed: %+v", p)
			}
			return p
		}
		assertOldPoint := func(t *testing.T, got reply) {
			t.Helper()
			success(t, got)
			var result struct {
				Bead struct {
					ID       string         `json:"id"`
					Project  string         `json:"project"`
					Revision model.Revision `json:"revision"`
					Fields   map[string]any `json:"fields"`
				} `json:"bead"`
			}
			if err := json.Unmarshal(got.Result, &result); err != nil {
				t.Fatal(err)
			}
			if result.Bead.ID != ref.ID || result.Bead.Project != "project" || result.Bead.Fields["title"] != "authorized first lifetime" || result.Bead.Revision.String() != oldRows[0].Revision.String() {
				t.Fatalf("historical point read substituted current content: %s", got.Result)
			}
		}
		assertOldPage := func(t *testing.T, p rpcPage) {
			t.Helper()
			if len(p.Items) != 1 || p.Items[0].ID != ref.ID || p.Items[0].Fields["title"] != "authorized first lifetime" {
				t.Fatalf("historical query substituted current content: %+v", p)
			}
		}
		assertEmpty := func(t *testing.T, p rpcPage) {
			t.Helper()
			if len(p.Items) != 0 {
				t.Fatalf("current restrictions disclosed old content: %+v", p)
			}
		}

		t.Run("authorized_before_deletion", func(t *testing.T) {
			before := recreatedHistoryState(t, f)
			assertOldPoint(t, point(t, reader("project"), asOf, 19101))
			assertOldPage(t, query(t, reader("project"), 19102))
			recreatedHistoryRefusal(t, point(t, reader("other"), asOf, 19103), "not_found")
			assertEmpty(t, query(t, reader("other"), 19104))
			assertRecreatedHistoryUnchanged(t, f, before)
		})

		deleted, err := conformance.Seed(ctx, f.store, f.lease, driver.ChangesetHeader{}, []conformance.FixtureChange{{
			Ref: ref, Kind: "bead", Op: model.OpDelete,
		}})
		if err != nil {
			t.Fatal(err)
		}
		t.Run("deleted_current_assignment", func(t *testing.T) {
			before := recreatedHistoryState(t, f)
			if len(before.Rows) != 0 || len(before.Versions.Items) == 0 || !before.Versions.Items[len(before.Versions.Items)-1].Deleted {
				t.Fatal("delete fixture needs an actual persisted tombstone")
			}
			recreatedHistoryRefusal(t, point(t, reader("project"), asOf, 19110), "not_found")
			assertEmpty(t, query(t, reader("project"), 19111))
			assertRecreatedHistoryUnchanged(t, f, before)
		})

		_, err = conformance.Seed(ctx, f.store, f.lease, driver.ChangesetHeader{}, []conformance.FixtureChange{{
			Ref: ref, Kind: "bead", Op: model.OpCreate,
			Body: []byte(`{"id":"` + ref.ID + `","type":"issue","project":"other","fields":{"title":"replacement lifetime","status":"todo","priority":0}}`),
		}})
		if err != nil {
			t.Fatal(err)
		}
		t.Run("recreated_current_assignment", func(t *testing.T) {
			before := recreatedHistoryState(t, f)
			// The same caller now reads the old bytes only with its new-project
			// caveat. Involvement grants alone never restrict CP7 User reads.
			recreatedHistoryRefusal(t, point(t, reader("project"), asOf, 19120), "not_found")
			assertEmpty(t, query(t, reader("project"), 19121))
			assertOldPoint(t, point(t, reader("other"), asOf, 19122))
			assertOldPage(t, query(t, reader("other"), 19123))
			recreatedHistoryRefusal(t, point(t, oldWriter, asOf, 19124), "not_found")
			assertEmpty(t, query(t, oldWriter, 19125))
			recreatedHistoryRefusal(t, point(t, reader("other"), map[string]any{
				"revision": deleted.Revision, "incarnation": view.Incarnation,
			}, 19126), "not_found")
			recreatedHistoryRefusal(t, f.call(t, oldWriter, "bead.update", map[string]any{
				"id": ref, "fields": map[string]any{"title": "must not commit"},
			}, 19127), "forbidden")
			if f.receipt(t, oldWriter, "bead.update", 19127) != nil {
				t.Fatal("refused old-project write created a receipt")
			}
			assertRecreatedHistoryUnchanged(t, f, before)
		})

		currentWriter := worker("user:operator")
		currentWriter.Caveats["projects"] = []model.Ref{other}
		t.Run("stale_post_image", func(t *testing.T) {
			before := recreatedHistoryState(t, f)
			current := f.reader(t)
			if len(before.Rows) != 1 {
				t.Fatal("recreated bead is missing")
			}
			proposal := port.ProposedChange{
				Principal: currentWriter, Meta: model.WriteMeta{Method: "bead.update"},
				Changes: []port.Change{{Resource: model.ResourceRef{Kind: "bead", Ref: ref}, Before: before.Rows[0].Body, After: before.Rows[0].Body}},
			}
			// This exact caller must pass the current-image control first;
			// otherwise a policy denial could mask a missing stored-image check.
			if err := f.server.Checker.Authorize(ctx, port.AuthorizeAfter, proposal, current); err != nil {
				t.Fatalf("authorized current image: %v", err)
			}
			for _, both := range []bool{false, true} {
				stale := proposal
				stale.Changes = append([]port.Change(nil), proposal.Changes...)
				stale.Changes[0].Before = oldRows[0].Body
				if both {
					stale.Changes[0].After = oldRows[0].Body
				}
				err := f.server.Checker.Authorize(ctx, port.AuthorizeAfter, stale, current)
				var typed *model.Error
				if !errors.As(err, &typed) || typed.Data.Code != "forbidden" || typed.Data.Hint != "stored bead image changed during authorization" {
					t.Fatalf("stale lifetime was not rejected by stored-image validation: both=%t err=%v", both, err)
				}
			}
			assertRecreatedHistoryUnchanged(t, f, before)
		})

		t.Run("authorized_current_write", func(t *testing.T) {
			success(t, f.call(t, currentWriter, "bead.update", map[string]any{
				"id": ref, "fields": map[string]any{"title": "authorized replacement lifetime"},
			}, 19130))
			current := recreatedHistoryState(t, f)
			if len(current.Rows) != 1 || f.receipt(t, currentWriter, "bead.update", 19130) == nil {
				t.Fatal("current-project control did not persist a write and receipt")
			}
			var body struct {
				Project string         `json:"project"`
				Fields  map[string]any `json:"fields"`
			}
			if err := json.Unmarshal(current.Rows[0].Body, &body); err != nil || body.Project != "other" || body.Fields["title"] != "authorized replacement lifetime" {
				t.Fatalf("current-project control stored the wrong image: %+v err=%v", body, err)
			}
			assertOldPoint(t, point(t, reader("other"), asOf, 19131))
			assertRecreatedHistoryUnchanged(t, f, current)
		})
		// The first operation completed before deletion/recreation. Later
		// authority changes neither undo that commit nor rewrite its receipt.
		if !reflect.DeepEqual(original, f.receipt(t, oldWriter, "bead.update", originalKey)) {
			t.Fatal("later assignment change rewrote the legitimately committed receipt")
		}
	})
}

type recreatedBeadHistoryState struct {
	Meta       driver.Meta
	Rows       []driver.Record
	Versions   driver.Page[driver.Version]
	Changesets []model.Changeset
}

func recreatedHistoryState(t *testing.T, f *fixture) recreatedBeadHistoryState {
	t.Helper()
	ctx := context.Background()
	meta, err := f.store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	r := f.reader(t)
	rows, err := r.Get(ctx, []model.Ref{fixtureRef(1)})
	if err != nil {
		t.Fatal(err)
	}
	versions, err := r.Versions(ctx, fixtureRef(1), driver.PageReq{Limit: 100})
	if err != nil || versions.NextCursor != nil {
		t.Fatalf("complete durable version observation: %+v %v", versions, err)
	}
	changesets, err := r.Changesets(ctx, nil, 100)
	if err != nil || len(changesets) >= 100 || r.Snapshot().At.String() != meta.Head.String() {
		t.Fatalf("complete durable changeset observation: head=%s entries=%d err=%v", meta.Head, len(changesets), err)
	}
	return recreatedBeadHistoryState{meta, rows, versions, changesets}
}

func assertRecreatedHistoryUnchanged(t *testing.T, f *fixture, before recreatedBeadHistoryState) {
	t.Helper()
	if after := recreatedHistoryState(t, f); !reflect.DeepEqual(before, after) {
		t.Fatalf("historical reads or refusals wrote durable state: before=%+v after=%+v", before, after)
	}
}

func recreatedHistoryRefusal(t *testing.T, got reply, code model.ErrorCode) {
	t.Helper()
	if got.Error == nil || got.Error.Data.Code != code || got.Error.Data.Retryable || len(got.Error.Data.Details) != 0 || len(got.Result) != 0 {
		t.Fatalf("expected exact %s refusal without content or history details: %+v", code, got)
	}
}
