package batchsql_test

import (
	"bytes"
	"encoding/json"
	"reflect"
	"sort"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// Literal storage fixtures bypass Writer canonicalization. Reads use the real
// public no-plugin HTTP host so an authorization projection cannot replace the
// service decoder's parent. Installed fixed-policy permissions are unchanged.
func b01PersistedReadFixture(t *testing.T, engine string) (*b01Public, *b01PublicationProbe, string) {
	t.Helper()
	f := newFixture(t, engine)
	probe := &b01PublicationProbe{}
	b01InstallPublicationProbe(f, probe)
	h := newB01Public(t, f)
	reply, key := b01LocalWrite(t, h, "bead.create", b01Old, map[string]any{"type": "issue", "fields": map[string]any{"title": "receipt and publication observation"}})
	b01Committed(t, reply)
	if b01AnonymousReceipt(t, h, "bead.create", key) == nil || probe.calls.Load() != 1 {
		t.Fatal("public fixture did not establish its receipt and publication observation")
	}
	return h, probe, key
}

type b01PersistedReadState struct {
	Durable      durableState
	Closure      []driver.DerivedRow
	Facts        []driver.LockState
	Receipt      *driver.IdempotencyRecord
	Publications uint64
}

func b01PersistedReadSnapshot(t *testing.T, h *b01Public, probe *b01PublicationProbe, key string) b01PersistedReadState {
	t.Helper()
	f := h.f
	s := b01PersistedReadState{Durable: f.state(t), Receipt: b01AnonymousReceipt(t, h, "bead.create", key), Publications: probe.calls.Load()}
	r, err := f.store.Read(f.ctx, driver.View{Incarnation: s.Durable.Meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	s.Closure, err = r.DerivedRows(f.ctx, driver.DerivedAncestorClosure, "")
	if err != nil {
		t.Fatal(err)
	}
	locks := []driver.LockKey{driver.GraphBlockingSentinelLock}
	for id := range s.Durable.Records {
		bead, err := driver.NewBeadSentinel(id)
		if err != nil {
			t.Fatal(err)
		}
		closure, err := driver.NewClosureSentinel(id)
		if err != nil {
			t.Fatal(err)
		}
		locks = append(locks, driver.LockKey{Kind: driver.LockSentinel, Sentinel: bead}, driver.LockKey{Kind: driver.LockSentinel, Sentinel: closure})
	}
	sort.Slice(locks, func(i, j int) bool {
		a, _ := driver.EncodeLockKey(locks[i])
		b, _ := driver.EncodeLockKey(locks[j])
		return a < b
	})
	s.Facts, err = r.Observe(f.ctx, locks)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func b01PersistedRead(t *testing.T, h *b01Public, method string, params map[string]any) b01Reply {
	t.Helper()
	key := h.f.meta("persisted-read").IdempotencyKey
	params["_meta"] = map[string]any{"project": b01Child, "idempotencyKey": key}
	if b01AnonymousReceipt(t, h, method, key) != nil {
		t.Fatal("read key already had a receipt")
	}
	reply := h.call(t, h.slots, "", method, params)
	if reply.Meta.Replayed || b01AnonymousReceipt(t, h, method, key) != nil {
		t.Fatal("public read created or replayed a write receipt")
	}
	return reply
}

func b01PersistedParent(t *testing.T, bead map[string]any, title string) {
	t.Helper()
	fields, ok := bead["fields"].(map[string]any)
	if !ok || bead["id"] != "pl-persisted-child" || bead["parent"] != "pl-persisted-parent" || bead["project"] != b01Child || bead["status"] != "todo" || fields["title"] != title {
		t.Fatalf("literal persisted parent/title projection was lost: %v, want parent pl-persisted-parent and title %q", bead, title)
	}
}

func TestB01PublicPersistedHierarchyParentProjectionAcrossDrivers(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		for _, tc := range []struct{ name, members string }{
			{"canonical-only", `,"parent":{"id":"pl-persisted-parent"}`},
			{"legacy-only", `,"links":[{"to":{"id":"pl-persisted-parent"},"kind":"parent-child"}]`},
			{"matching-forms", `,"parent":{"id":"pl-persisted-parent"},"links":[{"to":{"id":"pl-persisted-parent"},"kind":"parent-child"}]`},
		} {
			t.Run(engine+"/"+tc.name, func(t *testing.T) {
				h, probe, key := b01PersistedReadFixture(t, engine)
				f := h.f
				// These literal rows have this fixed fixture creation instant;
				// ready ordering must not infer it from their later patch/history.
				original := []byte(`{"id":"pl-persisted-child","project":"b01-child-project","type":"issue","createdAt":"2026-11-03T00:00:00Z","fields":{"title":"then","status":"todo","priority":0}` + tc.members + `}`)
				parent := b01Ref("pl-persisted-parent")
				graph := &conformance.FixtureGraphSource{Parent: &parent}
				if tc.name != "canonical-only" {
					graph.Links = []driver.SourceLink{{Kind: "parent-child", To: parent}}
				}
				// The public create above certified source coverage. Supply the
				// exact typed facts without normalizing these literal test bytes.
				created, err := conformance.Seed(f.ctx, f.store, f.lease, driver.ChangesetHeader{}, []conformance.FixtureChange{
					{Ref: parent, Kind: "bead", Op: model.OpCreate, Body: []byte(`{"id":"pl-persisted-parent","project":"b01-epic-project","type":"issue","createdAt":"2026-11-03T00:00:00Z","fields":{"title":"parent","status":"todo"}}`), Graph: &conformance.FixtureGraphSource{}},
					{Ref: b01Ref("pl-persisted-child"), Kind: "bead", Op: model.OpCreate, Body: original, Graph: graph},
				})
				if err != nil {
					t.Fatal(err)
				}
				current := bytes.Replace(original, []byte(`"title":"then"`), []byte(`"title":"now"`), 1)
				if _, err := conformance.Seed(f.ctx, f.store, f.lease, driver.ChangesetHeader{}, []conformance.FixtureChange{{Ref: b01Ref("pl-persisted-child"), Kind: "bead", Op: model.OpPatch, Body: current, ChangedPaths: []string{"/fields/title"}, Graph: graph}}); err != nil {
					t.Fatal(err)
				}
				before := b01PersistedReadSnapshot(t, h, probe, key)
				if !bytes.Equal(before.Durable.Records["pl-persisted-child"].Body, current) || len(before.Durable.Versions["pl-persisted-child"]) != 2 {
					t.Fatal("fixture did not persist the literal current bytes and two real versions")
				}
				if tc.name == "legacy-only" {
					var envelope map[string]json.RawMessage
					if err := json.Unmarshal(before.Durable.Records["pl-persisted-child"].Body, &envelope); err != nil {
						t.Fatal(err)
					}
					if _, present := envelope["parent"]; present {
						t.Fatal("legacy-only fixture already had a canonical parent")
					}
				}
				asOf := map[string]any{"revision": created.Revision, "incarnation": before.Durable.Meta.Incarnation}
				observe := func(t *testing.T) {
					t.Helper()
					live := b01Result(t, b01PersistedRead(t, h, "bead.get", map[string]any{"id": b01Ref("pl-persisted-child")}))
					b01PersistedParent(t, live["bead"].(map[string]any), "now")
					past := b01Result(t, b01PersistedRead(t, h, "bead.get", map[string]any{"id": b01Ref("pl-persisted-child"), "asOf": asOf}))
					b01PersistedParent(t, past["bead"].(map[string]any), "then")
					for _, request := range []struct {
						method, title string
						params        map[string]any
					}{
						{"bead.ready", "now", map[string]any{"limit": 1000}},
						{"history.query", "then", map[string]any{"limit": 1000, "asOf": asOf}},
					} {
						page := b01Result(t, b01PersistedRead(t, h, request.method, request.params))
						items, ok := page["items"].([]any)
						if !ok || len(items) != 1 || page["nextCursor"] != nil {
							t.Fatalf("%s did not return the single complete child projection: %v", request.method, page)
						}
						b01PersistedParent(t, items[0].(map[string]any), request.title)
					}
					func() {
						r, err := f.store.Read(f.ctx, driver.View{Incarnation: before.Durable.Meta.Incarnation, At: &created.Revision})
						if err != nil {
							t.Fatal(err)
						}
						defer r.Close()
						rows, err := r.Get(f.ctx, []model.Ref{b01Ref("pl-persisted-child")})
						if err != nil || len(rows) != 1 || !bytes.Equal(rows[0].Body, original) {
							t.Fatalf("public reads changed literal historical bytes: %+v (%v)", rows, err)
						}
					}()
					if after := b01PersistedReadSnapshot(t, h, probe, key); !reflect.DeepEqual(before, after) {
						t.Fatal("public reads changed raw records, versions/history, closure/facts, receipt or publication")
					}
				}
				observe(t)
				if engine != "memory" {
					t.Run("reopened", func(t *testing.T) {
						h.api.Close()
						h.slots.Close()
						f.reopen(t)
						b01InstallPublicationProbe(f, probe)
						h.open(t)
						observe(t)
					})
				}
			})
		}
	}
}

func TestB01PublicPersistedHierarchyRefusalsAcrossDrivers(t *testing.T) {
	const legacy = `,"links":[{"to":{"id":"pl-persisted-parent"},"kind":"parent-child"}]`
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		for _, tc := range []struct{ name, members string }{
			{"conflicting-parent", `,"parent":{"id":"pl-other-parent"}` + legacy},
			{"multiple-legacy-parents", `,"links":[{"to":{"id":"pl-persisted-parent"},"kind":"parent-child"},{"to":{"id":"pl-other-parent"},"kind":"parent-child"}]`},
			{"duplicate-legacy-parent", `,"links":[{"to":{"id":"pl-persisted-parent"},"kind":"parent-child"},{"to":{"id":"pl-persisted-parent"},"kind":"parent-child"}]`},
			{"canonical-nonzero-branch", `,"parent":{"id":"pl-persisted-parent","branch":1}`},
			{"legacy-nonzero-branch", `,"links":[{"to":{"id":"pl-persisted-parent","branch":1},"kind":"parent-child"}]`},
			{"canonical-unresolved-key", `,"parent":{"key":"parent"}`},
			{"legacy-unresolved-key", `,"links":[{"to":{"key":"parent"},"kind":"parent-child"}]`},
			{"canonical-string", `,"parent":"pl-persisted-parent"`},
			{"legacy-null", `,"links":[{"to":null,"kind":"parent-child"}]`},
			{"unknown-envelope-member", legacy + `,"unexpected":true`},
			{"null-comments", legacy + `,"comments":null`},
			{"unknown-comment-member", legacy + `,"comments":[{"body":"old","principal":{"kind":"anonymous","id":"anonymous"},"time":"2026-10-29T16:00:00Z","unexpected":true}]`},
			{"incomplete-imported-comment", legacy + `,"comments":[{"body":"old","principal":{"kind":"anonymous","id":"anonymous"},"time":"2026-10-29T16:00:00Z","origin":"import"}]`},
			{"legacy-comment-source-provenance", legacy + `,"comments":[{"body":"old","principal":{"kind":"anonymous","id":"anonymous"},"time":"2026-10-29T16:00:00Z","sourceAuthor":"source"}]`},
		} {
			t.Run(engine+"/"+tc.name, func(t *testing.T) {
				h, probe, key := b01PersistedReadFixture(t, engine)
				f := h.f
				body := []byte(`{"id":"pl-persisted-child","project":"b01-child-project","type":"issue","fields":{"title":"must refuse","status":"todo"}` + tc.members + `}`)
				// These are deliberately invalid persisted documents, not product
				// writes. Declare a trusted root/no-links fixture solely to keep
				// certified source publication complete. The unchanged malformed
				// Body remains the public decoder's refusal subject; no hierarchy
				// from it is blessed or exposed, and every read must still fail.
				seeded, err := conformance.Seed(f.ctx, f.store, f.lease, driver.ChangesetHeader{}, []conformance.FixtureChange{
					{Ref: b01Ref("pl-persisted-parent"), Kind: "bead", Op: model.OpCreate, Body: []byte(`{"id":"pl-persisted-parent","project":"b01-epic-project","type":"issue","fields":{"title":"parent","status":"todo"}}`), Graph: &conformance.FixtureGraphSource{}},
					{Ref: b01Ref("pl-persisted-child"), Kind: "bead", Op: model.OpCreate, Body: body, Graph: &conformance.FixtureGraphSource{}},
				})
				if err != nil {
					t.Fatal(err)
				}
				before := b01PersistedReadSnapshot(t, h, probe, key)
				if !bytes.Equal(before.Durable.Records["pl-persisted-child"].Body, body) {
					t.Fatal("refusal fixture was normalized before the public read")
				}
				asOf := map[string]any{"revision": seeded.Revision, "incarnation": before.Durable.Meta.Incarnation}
				for _, request := range []struct {
					method string
					params map[string]any
				}{
					{"bead.get", map[string]any{"id": b01Ref("pl-persisted-child")}},
					{"bead.get", map[string]any{"id": b01Ref("pl-persisted-child"), "asOf": asOf}},
					{"bead.ready", map[string]any{"limit": 1000}},
					{"history.query", map[string]any{"limit": 1000, "asOf": asOf}},
				} {
					// Invalid persisted content is wrapped by the public service;
					// it never becomes a successful empty parent or partial list.
					b01Code(t, b01PersistedRead(t, h, request.method, request.params), "internal_error")
				}
				if after := b01PersistedReadSnapshot(t, h, probe, key); !reflect.DeepEqual(before, after) {
					t.Fatal("refused public reads changed graph/history, closure/facts, receipt or publication")
				}
			})
		}
	}
}
