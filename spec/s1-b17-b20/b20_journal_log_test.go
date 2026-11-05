package s1b17b20

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	coreevents "github.com/buildbetter-app/plugs/internal/core/events"
	"github.com/buildbetter-app/plugs/internal/core/model"
	serverevents "github.com/buildbetter-app/plugs/internal/server/events"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// These wrappers inject only read faults into real committed driver pages.
// They do not fabricate a valid journal, mutate storage, or claim that an
// engine corruption/repair or native producer was exercised.
type b20JournalReadStore struct {
	serverevents.ReadStore
	fault                      func([]model.Changeset) ([]model.Changeset, error)
	afterOpen                  func()
	opened, closed, faultCalls int
}

func (s *b20JournalReadStore) Read(ctx context.Context, view driver.View) (driver.Reader, error) {
	r, err := s.ReadStore.Read(ctx, view)
	if err != nil {
		return nil, err
	}
	s.opened++
	if s.afterOpen != nil {
		hook := s.afterOpen
		s.afterOpen = nil
		hook()
	}
	return &b20JournalRead{Reader: r, store: s}, nil
}

type b20JournalRead struct {
	driver.Reader
	store *b20JournalReadStore
}

func (r *b20JournalRead) Changesets(ctx context.Context, after *driver.Cursor, limit int) ([]model.Changeset, error) {
	sets, err := r.Reader.Changesets(ctx, after, limit)
	if err != nil || r.store.fault == nil {
		return sets, err
	}
	r.store.faultCalls++
	return r.store.fault(sets)
}
func (r *b20JournalRead) Close() error {
	r.store.closed++
	return r.Reader.Close()
}

func TestB20LogRefusesIncompleteCommittedPages(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			h := b20JournalHarness(t, engine)
			meta, err := h.store.Meta(h.ctx)
			if err != nil {
				t.Fatal(err)
			}
			initial := coreevents.Position{Incarnation: meta.Incarnation}
			t.Run("empty_snapshot", func(t *testing.T) {
				sets, err := (serverevents.Log{Store: h.store}).Scan(h.ctx, initial, 10)
				if err != nil || len(sets) != 0 {
					t.Fatalf("empty real store page=%v err=%v", sets, err)
				}
			})
			created := b17Commit(t, h, model.ChangeSet{Ops: []model.Op{b17Create()}}, 401)
			ref := created.Results[0].Ref
			b17Commit(t, h, model.ChangeSet{Ops: []model.Op{b17Patch(ref, "second committed title")}}, 402)
			b17Commit(t, h, model.ChangeSet{Ops: []model.Op{b17Patch(ref, "third committed title")}}, 403)
			baseline := b17StateOf(t, h, ref)
			if baseline.Meta.Head.String() != "3" || len(baseline.Log) != 3 {
				t.Fatal("three real committed revisions required")
			}
			sourceErr := errors.New("injected B20 partial-page source failure")
			faults := []string{"missing_first", "missing_middle", "missing_tail", "empty_before_head", "duplicate", "regression", "wrong_incarnation", "beyond_snapshot", "partial_source_error"}
			for _, name := range faults {
				t.Run(name, func(t *testing.T) {
					fault := func(sets []model.Changeset) ([]model.Changeset, error) {
						if len(sets) != 3 {
							return nil, fmt.Errorf("fault requires three real rows; got%d", len(sets))
						}
						switch name {
						case "missing_first":
							return sets[1:], nil
						case "missing_middle":
							return []model.Changeset{sets[0], sets[2]}, nil
						case "missing_tail":
							return sets[:2], nil
						case "empty_before_head":
							return nil, nil
						case "duplicate":
							return []model.Changeset{sets[0], sets[0], sets[2]}, nil
						case "regression":
							return []model.Changeset{sets[0], sets[2], sets[1]}, nil
						case "wrong_incarnation":
							sets[1].Incarnation = "injected-other-incarnation"
						case "beyond_snapshot":
							sets[2].Revision = model.Natural64(4)
						case "partial_source_error":
							return sets[:1], sourceErr
						}
						return sets, nil
					}
					t.Run("direct_scan_no_prefix", func(t *testing.T) {
						store := &b20JournalReadStore{ReadStore: h.store, fault: fault}
						sets, err := (serverevents.Log{Store: store}).Scan(h.ctx, initial, 10)
						if err == nil || len(sets) != 0 {
							t.Errorf("invalid log escaped: rows=%d err=%v; want error with no prefix", len(sets), err)
						}
						if name == "partial_source_error" && !errors.Is(err, sourceErr) {
							t.Errorf("lost source error: %v", err)
						}
						b20JournalClosed(t, store)
					})
					for _, filtered := range []bool{false, true} {
						t.Run(fmt.Sprintf("signed_list_filtered_%t", filtered), func(t *testing.T) {
							store := &b20JournalReadStore{ReadStore: h.store, fault: fault}
							svc := b20JournalService(t, store, h.store)
							req := serverevents.Request{Principal: b17Actor, Limit: 1, Ontology: model.Natural64(1), PolicyVersion: "b20-journal"}
							if filtered {
								req.Filter.Resources = []string{"bead/pl-no-matching-resource"}
							}
							page, err := svc.List(h.ctx, req)
							if err == nil || len(page.Events) != 0 || page.Cursor != "" || page.Reset {
								t.Errorf("invalid journal became a deliverable page: events=%d cursor=%q reset=%t err=%v", len(page.Events), page.Cursor, page.Reset, err)
							}
							if name == "partial_source_error" && !errors.Is(err, sourceErr) {
								t.Errorf("lost source error: %v", err)
							}
							b20JournalClosed(t, store)
						})
					}
				})
			}
			if got := b17StateOf(t, h, ref); !reflect.DeepEqual(got, baseline) {
				t.Fatal("read fault changed actual rows/versions/log/head")
			}
			t.Run("legal_limited_pages_and_inclusive_resume", func(t *testing.T) {
				log := serverevents.Log{Store: h.store}
				for _, tc := range []struct {
					after string
					limit int
					want  []string
				}{
					{"0", 1, []string{"1"}}, {"1", 2, []string{"1", "2"}}, {"2", 2, []string{"2", "3"}}, {"3", 1, []string{"3"}},
				} {
					after := initial
					after.Revision = model.Natural(tc.after)
					sets, err := log.Scan(h.ctx, after, tc.limit)
					got := []string{}
					for _, set := range sets {
						got = append(got, set.Revision.String())
					}
					if err != nil || !reflect.DeepEqual(got, tc.want) {
						t.Fatalf("inclusive page after%s limit%d=%v err=%v; want%v", tc.after, tc.limit, got, err, tc.want)
					}
				}
				svc := b20JournalService(t, h.store, h.store)
				req := serverevents.Request{Principal: b17Actor, Limit: 1, Ontology: model.Natural64(1), PolicyVersion: "b20-journal"}
				for revision := 1; revision <= 3; revision++ {
					page, err := svc.List(h.ctx, req)
					if err != nil || len(page.Events) != 1 || page.Events[0].Durable == nil || page.Events[0].Durable.Revision.String() != fmt.Sprint(revision) || page.Cursor == "" || page.Cursor == req.After {
						t.Fatalf("signed page%d=%+v err=%v", revision, page, err)
					}
					req.After = page.Cursor
				}
				page, err := svc.List(h.ctx, req)
				if err != nil || len(page.Events) != 0 || page.Cursor != req.After {
					t.Fatalf("caught-up signed page=%+v err=%v", page, err)
				}
			})
			t.Run("concurrent_append_uses_reader_snapshot", func(t *testing.T) {
				store := &b20JournalReadStore{ReadStore: h.store, afterOpen: func() {
					commit := b17Commit(t, h, model.ChangeSet{Ops: []model.Op{b17Patch(ref, "after pinned snapshot")}}, 404)
					if commit.Revision.String() != "4" {
						t.Fatalf("concurrent append revision=%s", commit.Revision)
					}
				}}
				sets, err := (serverevents.Log{Store: store}).Scan(h.ctx, initial, 10)
				if err != nil || len(sets) != 3 || sets[2].Revision.String() != "3" {
					t.Fatalf("valid pinned three-row page mistaken for a live-head hole: rows=%d err=%v", len(sets), err)
				}
				b20JournalClosed(t, store)
				fresh, err := (serverevents.Log{Store: h.store}).Scan(h.ctx, initial, 10)
				if err != nil || len(fresh) != 4 || fresh[3].Revision.String() != "4" {
					t.Fatalf("real concurrent append missing: rows=%d err=%v", len(fresh), err)
				}
			})
		})
	}
}

func b20JournalClosed(t *testing.T, store *b20JournalReadStore) {
	t.Helper()
	if store.opened != 1 || store.closed != 1 {
		t.Errorf("log readers opened=%d closed=%d, want1/1", store.opened, store.closed)
	}
	if store.fault != nil && store.faultCalls != 1 {
		t.Errorf("injected reader calls=%d, want1", store.faultCalls)
	}
}
func b20JournalService(t *testing.T, logStore, materializerStore serverevents.ReadStore) serverevents.Service {
	t.Helper()
	codec, err := serverevents.NewCursorCodec([]byte(b20CursorKey))
	if err != nil {
		t.Fatal(err)
	}
	return serverevents.Service{Reader: &coreevents.Reader{Log: serverevents.Log{Store: logStore}, Materializer: serverevents.HistoryMaterializer{Store: materializerStore}, Authorization: coreevents.AllowAll{}}, Cursors: codec}
}
