package s1b17b20

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func instantAt(at time.Time) map[string]any {
	return map[string]any{"instant": at.UTC().Format(time.RFC3339Nano)}
}

func TestB18PairedStoreInstantSelectsCommittedRevision(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			p := newPairedStore(t, engine)
			r := p.rpc(t)
			firstTime := p.now
			created := success[model.Commit](t, "create", r.call(t, "", "bead.create", map[string]any{
				"type": "issue", "fields": map[string]any{"title": "original"},
			}, true))
			ref := created.Results[0].Ref
			p.advance(t)
			secondTime := p.now
			success[model.Commit](t, "update", r.call(t, "", "bead.update", map[string]any{
				"id": ref, "fields": map[string]any{"title": "second"},
			}, true))
			// Equal timestamps are ordered by committed revision. Neither
			// the point read nor a history query may return the first tie.
			tied := success[model.Commit](t, "update", r.call(t, "", "bead.update", map[string]any{
				"id": ref, "fields": map[string]any{"title": "equal-time-latest"},
			}, true))
			p.advance(t)
			head := success[model.Commit](t, "update", r.call(t, "", "bead.update", map[string]any{
				"id": ref, "fields": map[string]any{"title": "head"},
			}, true))
			check := func(t *testing.T) {
				t.Helper()
				r = p.rpc(t)
				for _, tc := range []struct {
					name     string
					at       time.Time
					revision model.Revision
					title    string
				}{
					{"at-create", firstTime, created.Revision, "original"},
					{"strictly-before-update", secondTime.Add(-time.Microsecond), created.Revision, "original"},
					{"equal-time-highest-revision", secondTime, tied.Revision, "equal-time-latest"},
					{"after-head", p.now.Add(time.Hour), head.Revision, "head"},
				} {
					t.Run(tc.name, func(t *testing.T) {
						get := success[map[string]any](t, "bead.get", r.call(t, "", "bead.get", map[string]any{"id": ref, "asOf": instantAt(tc.at)}, false))
						bead := get["bead"].(map[string]any)
						if bead["fields"].(map[string]any)["title"] != tc.title || natural(t, bead["revision"]).String() != tc.revision.String() {
							t.Fatalf("instant point read substituted a state: %+v, want %s/%s", bead, tc.revision, tc.title)
						}
						version := success[map[string]any](t, "history.get", r.call(t, "", "history.get", map[string]any{"ref": ref, "asOf": instantAt(tc.at)}, false))
						if natural(t, version["revision"]).String() != tc.revision.String() || version["body"].(map[string]any)["fields"].(map[string]any)["title"] != tc.title {
							t.Fatalf("instant version substituted a state: %+v", version)
						}
						page := success[map[string]any](t, "history.query", r.call(t, "", "history.query", map[string]any{"asOf": instantAt(tc.at)}, false))
						items := page["items"].([]any)
						if len(items) != 1 || items[0].(map[string]any)["fields"].(map[string]any)["title"] != tc.title || natural(t, page["asOf"].(map[string]any)["revision"]).String() != tc.revision.String() {
							t.Fatalf("instant query did not report its resolved snapshot: %+v", page)
						}
						if _, leaked := page["asOf"].(map[string]any)["instant"]; leaked {
							t.Fatal("result must report a concrete revision address")
						}
					})
				}
				for _, method := range []string{"bead.get", "history.get"} {
					params := map[string]any{"asOf": instantAt(firstTime.Add(-time.Microsecond))}
					if method == "bead.get" {
						params["id"] = ref
					} else {
						params["ref"] = ref
					}
					reply := r.call(t, "", method, params, false)
					if reply.Error == nil || reply.Error.Data.Code != "not_found" || len(reply.Result) != 0 {
						t.Fatalf("before-first instant must be unknown without a nearby state: %+v", reply)
					}
				}
				empty := success[map[string]any](t, "history.query", r.call(t, "", "history.query", map[string]any{"asOf": instantAt(firstTime.Add(-time.Microsecond))}, false))
				if len(empty["items"].([]any)) != 0 || natural(t, empty["asOf"].(map[string]any)["revision"]).String() != "0" {
					t.Fatalf("before-first query substituted head: %+v", empty)
				}
				before := b17StateOf(t, p.b17Harness, ref)
				for index, selector := range []map[string]any{
					{},
					{"instant": nil},
					{"instant": "not-a-time"},
					{"instant": "2037-03-29"},
					{"instant": firstTime.Format(time.RFC3339Nano), "revision": 0},
					{"instant": firstTime.Format(time.RFC3339Nano), "unexpected": true},
				} {
					reply := r.call(t, "", "history.get", map[string]any{"ref": ref, "asOf": selector}, false)
					if reply.Error == nil || reply.Error.Data.Code != "invalid_params" || len(reply.Result) != 0 {
						t.Fatalf("invalid selector %d admitted: %+v", index, reply)
					}
				}
				after := b17StateOf(t, p.b17Harness, ref)
				if string(before.Body) != string(after.Body) || len(before.Log) != len(after.Log) || len(before.Versions) != len(after.Versions) || before.Meta.Head.String() != after.Meta.Head.String() {
					t.Fatal("selector refusal mutated durable state/history")
				}
			}
			check(t)
			p.reopen(t)
			t.Run("after-persistent-reopen", check)
			state := b17StateOf(t, p.b17Harness, ref)
			if len(state.Log) != 4 || !state.Log[1].Time.Equal(state.Log[2].Time) || !state.Log[0].Time.Equal(firstTime) {
				t.Fatalf("test did not exercise the exact stored instant schedule: %+v", state.Log)
			}
			t.Logf("%s actual stored commit instants resolve through all three public RPC reads; equal-time ties, strict cutoff, unknown before-first, refusal and reopen checked", engine)
		})
	}
}

func TestB18InstantHistoryCursorPinsOriginalResolution(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			p := newPairedStore(t, engine)
			nextID := 0
			writer := pipeline.New(p.store, p.lease, pipeline.WithOntology(service.NewOntology(nil)), pipeline.WithClock(p.clock.Now), pipeline.WithIDGenerator(func() (string, error) {
				nextID++
				return fmt.Sprintf("pl-instant-%d", nextID), nil
			}))
			r := p.rpcWithWriter(t, writer)
			// Named create keys keep the public transaction on the ordinary
			// allocator while supplying a deterministic three-record order.
			ops := []map[string]any{}
			for index := 0; index < 3; index++ {
				ops = append(ops, map[string]any{"kind": "create", "key": fmt.Sprintf("item-%d", index), "type": "issue", "fields": map[string]any{"title": fmt.Sprintf("original-%d", index)}})
			}
			created := success[model.Commit](t, "tx.apply", r.call(t, "", "tx.apply", map[string]any{"changeSet": map[string]any{"ops": ops}}, true))
			selector := instantAt(p.now.Add(time.Hour))
			first := success[map[string]any](t, "history.query", r.call(t, "", "history.query", map[string]any{"asOf": selector, "limit": 1}, false))
			token, ok := first["nextCursor"].(string)
			if !ok || token == "" {
				t.Fatalf("first instant page omitted cursor: %+v", first)
			}
			p.advance(t)
			// This later write still precedes the original future selector.
			// Continuation must use the signed resolved revision, even though
			// resolving the selector anew would now observe the edit.
			changed := success[model.Commit](t, "update", r.call(t, "", "bead.update", map[string]any{"id": created.Results[1].Ref, "fields": map[string]any{"title": "later"}}, true))
			next := success[map[string]any](t, "history.query", r.call(t, "", "history.query", map[string]any{"asOf": selector, "limit": 2, "cursor": token}, false))
			if natural(t, next["asOf"].(map[string]any)["revision"]).String() != created.Revision.String() || len(next["items"].([]any)) != 2 {
				t.Fatalf("cursor lost its original instant resolution: %+v", next)
			}
			for _, item := range next["items"].([]any) {
				if item.(map[string]any)["fields"].(map[string]any)["title"] == "later" {
					t.Fatal("later write contaminated instant continuation")
				}
			}
			fresh := success[map[string]any](t, "history.query", r.call(t, "", "history.query", map[string]any{"asOf": selector, "limit": 3}, false))
			if natural(t, fresh["asOf"].(map[string]any)["revision"]).String() != changed.Revision.String() {
				t.Fatalf("fresh instant request failed to resolve current committed head: %+v", fresh)
			}
			for _, replacement := range []map[string]any{
				instantAt(p.now.Add(2 * time.Hour)),
				{"revision": json.Number(created.Revision.String())},
			} {
				reply := r.call(t, "", "history.query", map[string]any{"asOf": replacement, "limit": 2, "cursor": token}, false)
				if reply.Error == nil || reply.Error.Data.Code != "cursor_invalid" || len(reply.Result) != 0 {
					t.Fatalf("cursor admitted a changed original selector: %+v", reply)
				}
			}
		})
	}
}

// These wrappers keep the real driver snapshot and committed rows. Only the
// page size and explicit read faults are injected; this is resolver fault
// evidence, not a claim that production storage suffered or repaired a hole.
type instantLogStore struct {
	driver.Store
	mode  string
	pages int
}

func (s *instantLogStore) Read(ctx context.Context, v driver.View) (driver.Reader, error) {
	r, err := s.Store.Read(ctx, v)
	if err != nil {
		return nil, err
	}
	return &instantLogReader{Reader: r, owner: s}, nil
}

type instantLogReader struct {
	driver.Reader
	owner *instantLogStore
}

func (r *instantLogReader) Changesets(ctx context.Context, after *driver.Cursor, limit int) ([]model.Changeset, error) {
	r.owner.pages++
	switch r.owner.mode {
	case "truncated":
		return nil, driver.ErrTruncated{Floor: model.Natural64(1)}
	case "empty":
		return nil, nil
	case "hole":
		rows, err := r.Reader.Changesets(ctx, after, 2)
		if len(rows) > 1 {
			return rows[1:], err
		}
		return rows, err
	default:
		return r.Reader.Changesets(ctx, after, 1)
	}
}
func TestB18InstantResolverPagesAndFailsClosedOnLogFaults(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			p := newPairedStore(t, engine)
			rpc := p.rpc(t)
			created := success[model.Commit](t, "create", rpc.call(t, "", "bead.create", map[string]any{"type": "issue", "fields": map[string]any{"title": "one"}}, true))
			ref := created.Results[0].Ref
			p.advance(t)
			at := p.now
			middle := success[model.Commit](t, "update", rpc.call(t, "", "bead.update", map[string]any{"id": ref, "fields": map[string]any{"title": "two"}}, true))
			p.advance(t)
			success[model.Commit](t, "update", rpc.call(t, "", "bead.update", map[string]any{"id": ref, "fields": map[string]any{"title": "three"}}, true))
			before := b17StateOf(t, p.b17Harness, ref)
			for _, tc := range []struct {
				mode string
				code model.ErrorCode
			}{{"one-row-pages", ""}, {"truncated", "history_gone"}, {"empty", "internal_error"}, {"hole", "internal_error"}} {
				t.Run(tc.mode, func(t *testing.T) {
					wrapped := &instantLogStore{Store: p.store, mode: tc.mode}
					r, err := service.ReadAsOf(p.ctx, wrapped, &service.AsOf{Instant: &at})
					if tc.code != "" {
						var typed *model.Error
						if r != nil || !errors.As(err, &typed) || typed.Data.Code != tc.code {
							t.Fatalf("log fault substituted state: reader=%v err=%v", r, err)
						}
						if tc.code == "history_gone" && typed.Data.Details["floor"].(model.Revision).String() != "1" {
							t.Fatal("truncation omitted retained floor")
						}
						return
					}
					if err != nil {
						t.Fatal(err)
					}
					defer r.Close()
					rows, err := r.Get(p.ctx, []model.Ref{ref})
					if err != nil || len(rows) != 1 || rows[0].Revision.String() != middle.Revision.String() || r.Snapshot().At.String() != middle.Revision.String() || wrapped.pages != 3 {
						t.Fatalf("resolver did not exhaust pinned pages: rows=%+v err=%v pages=%d", rows, err, wrapped.pages)
					}
				})
			}
			after := b17StateOf(t, p.b17Harness, ref)
			if string(before.Body) != string(after.Body) || len(before.Log) != len(after.Log) || len(before.Versions) != len(after.Versions) || before.Meta.Head.String() != after.Meta.Head.String() {
				t.Fatal("read fault changed durable history")
			}
		})
	}
}
