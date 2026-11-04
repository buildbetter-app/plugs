package viewops

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type w9SelectionReader struct {
	*syntheticReader
	selectFn func(context.Context, driver.SelectionPlan) (driver.SelectionResult, error)
}

func (r *w9SelectionReader) Select(ctx context.Context, p driver.SelectionPlan) (driver.SelectionResult, error) {
	return r.selectFn(ctx, p)
}

func w9SelectionConfig(t *testing.T, selectFn func(context.Context, driver.SelectionPlan) (driver.SelectionResult, error), fence func(context.Context) error) *SelectionConfig {
	t.Helper()
	return &SelectionConfig{AuthorizeRead: func(context.Context, model.PrincipalRef, model.ResourceRef, string) error { return nil }, Bind: func(_ context.Context, _ model.PrincipalRef, definition string) (*SelectionAuthority, error) {
		control := model.RefByKey("actual-test-control")
		view := driver.View{At: ptr(model.Natural64(1)), Incarnation: "inc"}
		var boundary *driver.SelectionBoundary
		var err error
		if definition != "" {
			boundary, err = driver.PrepareSelectionBoundary(driver.SelectionBoundarySpec{Version: 1, PreparedBinding: driver.SelectionDigest([]byte("viewops-fault-control/v1")), Incarnation: "inc", AuthorityRevision: model.Natural64(1), Authorization: driver.SelectionExpr{Op: driver.SelectionTrue}, Chain: driver.SelectionChainNone, Host: driver.SelectionHostAbsent, InlineViewBinding: definition, Sources: []driver.SelectionSource{{Key: driver.LockKey{Kind: driver.LockRecord, Ref: &control}, Revision: model.Natural64(0)}}})
			if err != nil {
				t.Fatal(err)
			}
		}
		return &SelectionAuthority{Reader: &w9SelectionReader{&syntheticReader{snapshot: view}, selectFn}, Boundary: boundary, Binding: "current-binding", Check: func(_ context.Context, refs []model.Ref) ([]bool, error) {
			out := make([]bool, len(refs))
			for i := range out {
				out[i] = true
			}
			return out, nil
		}, Fence: fence}, nil
	}}
}
func w9SyntheticService(t *testing.T, rows []driver.Record, cfg Config) *Service {
	t.Helper()
	store := &syntheticStore{rows: rows, meta: driver.Meta{StoreID: "store", Incarnation: "inc", Head: model.Natural64(1)}}
	s, err := New(store, &testLease{lost: make(chan struct{})}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	return s
}
func w9WantCode(t *testing.T, err error, want model.ErrorCode) {
	t.Helper()
	var typed *model.Error
	if !errors.As(err, &typed) || typed.Data.Code != want {
		t.Fatalf("error=%T %v want=%s", err, err, want)
	}
}

func TestW9SelectionErrorsCancellationAndFinalFenceEraseResults(t *testing.T) {
	for _, name := range []string{"invalid", "canceled", "deadline", "storage_error", "final_fence", "final_canceled", "final_lease"} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			storageErr := errors.New("literal storage failure")
			calls := 0
			cfg := Config{}
			var svc *Service
			cfg.Selection = w9SelectionConfig(t, func(_ context.Context, p driver.SelectionPlan) (driver.SelectionResult, error) {
				calls++
				if p.Mode != driver.SelectionCountMode || p.Page != nil || p.Cap != OrdinaryCandidateCap {
					t.Fatalf("count plan=%+v", p)
				}
				switch name {
				case "invalid":
					return driver.SelectionResult{}, driver.ErrSelectionInvalid
				case "canceled":
					return driver.SelectionResult{}, context.Canceled
				case "deadline":
					return driver.SelectionResult{}, context.DeadlineExceeded
				case "storage_error":
					return driver.SelectionResult{}, storageErr
				}
				return driver.SelectionResult{Total: 1}, nil
			}, func(context.Context) error {
				switch name {
				case "final_fence":
					return failure("authz_unavailable", "authority changed")
				case "final_canceled":
					cancel()
				case "final_lease":
					close(svc.lease.(*testLease).lost)
				}
				return nil
			})
			svc = w9SyntheticService(t, nil, cfg)
			got, err := svc.Run(ctx, model.PrincipalRef{Kind: "user", ID: "reader"}, RunRequest{CountOnly: true, Definition: &Definition{Filter: &Filter{}}})
			if calls != 1 || got.Count != nil || len(got.Items) != 0 {
				t.Fatalf("failure returned partial: %+v calls=%d err=%v", got, calls, err)
			}
			switch name {
			case "storage_error":
				if err != storageErr {
					t.Fatalf("literal error changed: %v", err)
				}
			case "invalid":
				if !errors.Is(err, driver.ErrSelectionInvalid) {
					t.Fatal(err)
				}
			case "final_fence":
				w9WantCode(t, err, "authz_unavailable")
			default:
				w9WantCode(t, err, "busy")
			}
		})
	}
}

func TestW9RejectsDuplicateKeysMismatchedPayloadAndInvalidTotals(t *testing.T) {
	record := driver.Record{Ref: model.RefByID("a", model.Natural64(0)), Kind: "bead", Revision: model.Natural64(1), Body: explainDocument(t, "a", "p")}
	key := driver.SelectionKey{Ref: record.Ref, Revision: record.Revision}
	for _, name := range []string{"duplicate", "wrong_revision", "missing", "partial", "duplicate_group", "incomplete_group", "summary_elision", "wrong_suffix_total"} {
		t.Run(name, func(t *testing.T) {
			cfg := Config{Selection: w9SelectionConfig(t, func(_ context.Context, p driver.SelectionPlan) (driver.SelectionResult, error) {
				switch name {
				case "duplicate":
					return driver.SelectionResult{Keys: []driver.SelectionKey{key, key}, Total: 2}, nil
				case "wrong_revision":
					bad := key
					bad.Revision = model.Natural64(2)
					return driver.SelectionResult{Keys: []driver.SelectionKey{bad}, Total: 1}, nil
				case "missing":
					bad := key
					bad.Ref = model.RefByID("missing", model.Natural64(0))
					return driver.SelectionResult{Keys: []driver.SelectionKey{bad}, Total: 1}, nil
				case "partial":
					return driver.SelectionResult{Keys: []driver.SelectionKey{key}, Total: 2}, nil
				case "duplicate_group":
					return driver.SelectionResult{Total: 2, Groups: []driver.SelectionTypeGroup{{Type: "issue", Count: 1}, {Type: "issue", Count: 1}}}, nil
				case "incomplete_group":
					return driver.SelectionResult{Total: 2, Groups: []driver.SelectionTypeGroup{{Type: "issue", Count: 1}}}, nil
				case "summary_elision":
					return driver.SelectionResult{Total: 1, Groups: []driver.SelectionTypeGroup{{Type: "issue", Count: 1, Elided: 1}}}, nil
				case "wrong_suffix_total":
					return driver.SelectionResult{Keys: []driver.SelectionKey{key}, Total: 2, More: true, Groups: []driver.SelectionTypeGroup{{Type: "issue", Count: 2}}}, nil
				}
				panic("unreachable")
			}, func(context.Context) error { return nil })}
			svc := w9SyntheticService(t, []driver.Record{record}, cfg)
			def := &Definition{Filter: &Filter{}}
			if name == "duplicate_group" || name == "incomplete_group" || name == "summary_elision" {
				def.Fields = []string{}
				def.GroupBy = []string{"type"}
			}
			if name == "wrong_suffix_total" {
				budget := model.Natural64(1)
				def.Fields, def.GroupBy, def.BudgetTokens = []string{"id"}, []string{"type"}, &budget
			}
			got, err := svc.Run(context.Background(), model.PrincipalRef{Kind: "user", ID: "reader"}, RunRequest{Definition: def})
			if err == nil || len(got.Items) != 0 || len(got.Groups) != 0 {
				t.Fatalf("invalid storage output escaped: %+v err=%v", got, err)
			}
		})
	}
}

func TestW9FallbackDeduplicatesMembershipAndRetainsOrdinarySummaryCap(t *testing.T) {
	rows := make([]driver.Record, 0, OrdinaryCandidateCap+2)
	for i := 0; i <= OrdinaryCandidateCap; i++ {
		id := fmt.Sprintf("item-%05d", i)
		rows = append(rows, driver.Record{Ref: model.RefByID(id, model.Natural64(0)), Kind: "bead", Revision: model.Natural64(1), Body: explainDocument(t, id, "p")})
	}
	rows = append(rows, rows[0])
	svc := w9SyntheticService(t, rows, Config{})
	_, err := svc.Run(context.Background(), model.PrincipalRef{Kind: "user", ID: "reader"}, RunRequest{Definition: &Definition{Filter: &Filter{}, Fields: []string{}, GroupBy: []string{"type"}}})
	w9WantCode(t, err, "scope_too_large")
	// A repeated physical image cannot increase the exact logical count.
	svc = w9SyntheticService(t, []driver.Record{rows[0], rows[0], rows[1]}, Config{})
	got, err := svc.Run(context.Background(), model.PrincipalRef{Kind: "user", ID: "reader"}, RunRequest{CountOnly: true, Definition: &Definition{Filter: &Filter{Resources: []string{rows[0].Ref.ID, rows[0].Ref.ID, rows[1].Ref.ID}}}})
	if err != nil || got.Count == nil || *got.Count != 2 {
		t.Fatalf("duplicate count=%+v err=%v", got, err)
	}
	conflicting := rows[0]
	conflicting.Body = explainDocument(t, conflicting.Ref.ID, "other")
	svc = w9SyntheticService(t, []driver.Record{rows[0], conflicting}, Config{})
	_, err = svc.Run(context.Background(), model.PrincipalRef{Kind: "user", ID: "reader"}, RunRequest{CountOnly: true, Definition: &Definition{Filter: &Filter{}}})
	w9WantCode(t, err, "internal_error")
}

func TestW9UnboundedLabelsSavedViewsAndAdditionalHostPolicyNeverSelect(t *testing.T) {
	for _, name := range []string{"unbounded_labels", "saved", "host_restriction", "over_budget_bounded_labels"} {
		t.Run(name, func(t *testing.T) {
			calls := 0
			cfg := Config{Selection: w9SelectionConfig(t, func(context.Context, driver.SelectionPlan) (driver.SelectionResult, error) {
				calls++
				return driver.SelectionResult{}, errors.New("unexpected incomplete selection")
			}, func(context.Context) error { return nil })}
			def := Definition{Filter: &Filter{}}
			if name == "unbounded_labels" {
				def.Filter.Query = "label=x"
			}
			if name == "over_budget_bounded_labels" {
				def.Filter.Query = "label=x"
				for i := 0; i < 1500; i++ {
					def.Filter.Resources = append(def.Filter.Resources, fmt.Sprintf("absent-%04d", i))
				}
				def.Filter.Resources = append(def.Filter.Resources, "a")
			}
			if name == "saved" {
				cfg.Views = savedViewMap{"saved": def}
			}
			if name == "host_restriction" {
				cfg.AuthorizeRead = func(context.Context, model.PrincipalRef, model.ResourceRef, string) error {
					return failure("forbidden", "extra host restriction")
				}
			}
			record := driver.Record{Ref: model.RefByID("a", model.Natural64(0)), Kind: "bead", Revision: model.Natural64(1), Body: explainDocument(t, "a", "p")}
			svc := w9SyntheticService(t, []driver.Record{record}, cfg)
			req := RunRequest{CountOnly: true, Definition: &def}
			if name == "saved" {
				req.Definition = nil
				req.ViewID = "saved"
			}
			got, err := svc.Run(context.Background(), model.PrincipalRef{Kind: "user", ID: "reader"}, req)
			want := 0
			if name == "saved" {
				want = 1
			}
			if err != nil || calls != 0 || got.Count == nil || *got.Count != want {
				t.Fatalf("incomplete membership/policy selected: %+v err=%v calls=%d", got, err, calls)
			}
		})
	}
}

func TestW9ContinuationRequiresSameCurrentAuthority(t *testing.T) {
	cfg := Config{Selection: w9SelectionConfig(t, func(context.Context, driver.SelectionPlan) (driver.SelectionResult, error) {
		t.Fatal("stale authority reached selection")
		return driver.SelectionResult{}, nil
	}, func(context.Context) error { return nil })}
	svc := w9SyntheticService(t, nil, cfg)
	def := Definition{Filter: &Filter{}}
	by := model.PrincipalRef{Kind: "user", ID: "reader"}
	identity, err := svc.effectiveDigest(def, "", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	cursor := svc.nextCursor(by, "", "", identity, AsOf{Revision: model.Natural64(1), Incarnation: "inc"}, 1, nil, "")
	cursor.AuthorityBinding = "old-binding"
	got, err := svc.Run(context.Background(), by, RunRequest{Definition: &def, Cursor: cursor})
	w9WantCode(t, err, "cursor_invalid")
	if !reflect.DeepEqual(got, Result{}) {
		t.Fatal(got)
	}
}

func TestW9EveryUnavailableSummaryPathKeepsAuthorized20000Cap(t *testing.T) {
	rows := make([]driver.Record, OrdinaryCandidateCap+1)
	for i := range rows {
		id := fmt.Sprintf("bounded-%05d", i)
		body := explainDocument(t, id, "p")
		body = append(body[:len(body)-1], []byte(`,"labels":["x"]}`)...)
		rows[i] = driver.Record{Ref: model.RefByID(id, model.Natural64(0)), Kind: "bead", Revision: model.Natural64(1), Body: body}
	}
	for _, name := range []string{"no_compiled_authority", "unbounded_labels", "saved_without_source_binding", "additional_host_policy", "storage_unavailable", "storage_check_only"} {
		t.Run(name, func(t *testing.T) {
			def := Definition{Filter: &Filter{}, Fields: []string{}, GroupBy: []string{"type"}}
			cfg := Config{}
			if name != "no_compiled_authority" {
				cfg.Selection = w9SelectionConfig(t, func(context.Context, driver.SelectionPlan) (driver.SelectionResult, error) {
					if name == "storage_unavailable" {
						return driver.SelectionResult{}, driver.ErrSelectionUnavailable
					}
					if name == "storage_check_only" {
						return driver.SelectionResult{}, driver.ErrSelectionCheckOnly
					}
					t.Fatal("unavailable summary reached storage")
					return driver.SelectionResult{}, nil
				}, func(context.Context) error { return nil })
			}
			if name == "unbounded_labels" {
				def.Filter.Query = "label=x"
			}
			if name == "additional_host_policy" {
				cfg.AuthorizeRead = func(context.Context, model.PrincipalRef, model.ResourceRef, string) error { return nil }
			}
			req := RunRequest{Definition: &def}
			if name == "saved_without_source_binding" {
				cfg.Views = savedViewMap{"saved": def}
				req.Definition = nil
				req.ViewID = "saved"
			}
			got, err := w9SyntheticService(t, rows, cfg).Run(context.Background(), model.PrincipalRef{Kind: "user", ID: "reader"}, req)
			w9WantCode(t, err, "scope_too_large")
			if got.Count != nil || len(got.Groups) != 0 || len(got.Items) != 0 {
				t.Fatal(got)
			}
		})
	}
}

func TestW9OrdinaryGroupsKeepExistingBoundAndAnonymousEpicRejectsSelection(t *testing.T) {
	rows := make([]driver.Record, MaxGroups+1)
	for i := range rows {
		id := fmt.Sprintf("group-%03d", i)
		body := []byte(fmt.Sprintf(`{"id":%q,"type":%q,"fields":{}}`, id, id))
		rows[i] = driver.Record{Ref: model.RefByID(id, model.Natural64(0)), Kind: "bead", Revision: model.Natural64(1), Body: body}
	}
	cfg := Config{Selection: w9SelectionConfig(t, func(context.Context, driver.SelectionPlan) (driver.SelectionResult, error) {
		return driver.SelectionResult{}, driver.ErrSelectionCap{Limit: MaxGroups, Groups: true}
	}, func(context.Context) error { return nil })}
	got, err := w9SyntheticService(t, rows, cfg).Run(context.Background(), model.PrincipalRef{Kind: "user", ID: "reader"}, RunRequest{Definition: &Definition{Filter: &Filter{}, Fields: []string{"id"}, GroupBy: []string{"type"}}})
	if err != nil || len(got.Groups) != MaxGroups+1 {
		t.Fatalf("ordinary group bound changed: groups=%d err=%v", len(got.Groups), err)
	}
	cfg.EpicProgress = &EpicProgressConfig{
		AnonymousOnly:  true,
		OntologySource: func(string) (model.Ref, error) { return model.RefByKey("ontology"), nil },
		AuthorizeScope: func(context.Context, model.PrincipalRef, driver.Reader, model.Ref, []model.Ref) (EpicProgressAuthority, error) {
			return EpicProgressAuthority{}, nil
		},
		ValidateAuthority: func(context.Context, model.PrincipalRef, string) error { return nil },
	}
	store := &syntheticStore{meta: driver.Meta{StoreID: "store", Incarnation: "inc", Head: model.Natural64(1)}}
	if _, err := New(store, &testLease{lost: make(chan struct{})}, cfg); err == nil {
		t.Fatal("anonymous epic accepted an installed current authorization adapter")
	}
	cfg.Selection = nil
	if _, err := New(store, &testLease{lost: make(chan struct{})}, cfg); err != nil {
		t.Fatalf("valid anonymous epic configuration refused: %v", err)
	}
}

func TestW3SelectionFallbackPreservesNaturalRevisionOrder(t *testing.T) {
	rows := []driver.Record{}
	for _, item := range []struct{ id, revision string }{{"b", "10"}, {"z", "2"}, {"a", "10.0"}} {
		rows = append(rows, driver.Record{Ref: model.RefByID(item.id, model.Natural64(0)), Kind: "bead", Revision: model.Revision(item.revision), Body: explainDocument(t, item.id, "p")})
	}
	for _, tc := range []struct {
		name, order string
		want        []string
	}{
		{"ascending", "revision", []string{"z", "a", "b"}},
		{"descending", "-revision", []string{"a", "b", "z"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			selects, fences := 0, 0
			selection := w9SelectionConfig(t, func(context.Context, driver.SelectionPlan) (driver.SelectionResult, error) {
				selects++
				return driver.SelectionResult{}, errors.New("lexical revision selector must not run")
			}, func(context.Context) error { fences++; return nil })
			svc := w9SyntheticService(t, rows, Config{Selection: selection})
			got, err := svc.Run(context.Background(), model.PrincipalRef{Kind: "user", ID: "reader"}, RunRequest{Definition: &Definition{Filter: &Filter{}, Fields: []string{"id", "revision"}, Sort: []string{tc.order}}})
			if err != nil {
				t.Fatal(err)
			}
			var ids []string
			for _, item := range got.Items {
				ids = append(ids, item.ID)
			}
			if !reflect.DeepEqual(ids, tc.want) || selects != 0 || fences != 1 {
				t.Fatalf("items=%v want=%v selects=%d fences=%d", ids, tc.want, selects, fences)
			}
		})
	}
}

func TestW3StaleFallbackKeepsBoundAuthorityBeforeProtectedReads(t *testing.T) {
	for _, failedFence := range []bool{false, true} {
		t.Run(fmt.Sprintf("failed_fence_%t", failedFence), func(t *testing.T) {
			selects, checks, fences, protectedReads := 0, 0, 0, 0
			selection := w9SelectionConfig(t, func(context.Context, driver.SelectionPlan) (driver.SelectionResult, error) {
				selects++
				return driver.SelectionResult{}, errors.New("selector cannot represent stale")
			}, func(context.Context) error {
				fences++
				if failedFence {
					return driver.ErrSelectionUnavailable
				}
				return nil
			})
			bind := selection.Bind
			selection.Bind = func(ctx context.Context, by model.PrincipalRef, definition string) (*SelectionAuthority, error) {
				authority, err := bind(ctx, by, definition)
				if err != nil {
					return nil, err
				}
				authority.Check = func(_ context.Context, refs []model.Ref) ([]bool, error) {
					checks++
					return make([]bool, len(refs)), nil
				}
				return authority, nil
			}
			row := driver.Record{Ref: model.RefByID("denied", model.Natural64(0)), Kind: "bead", Revision: model.Natural64(1), Body: explainDocument(t, "denied", "private")}
			svc := w9SyntheticService(t, []driver.Record{row}, Config{
				Selection: selection,
				Now:       func() time.Time { return time.Date(2026, 11, 4, 0, 0, 0, 0, time.UTC) },
				StaleOntologySource: func(string) (model.Ref, error) {
					protectedReads++
					return model.Ref{}, errors.New("denied row must not read protected ontology")
				},
			})
			got, err := svc.Run(context.Background(), model.PrincipalRef{Kind: "user", ID: "reader"}, RunRequest{Definition: &Definition{Filter: &Filter{}}, CountOnly: true, Stale: &StaleFilter{Days: 1}})
			if selects != 0 || checks != 1 || fences != 1 || protectedReads != 0 {
				t.Fatalf("selects=%d checks=%d fences=%d protected_reads=%d", selects, checks, fences, protectedReads)
			}
			if failedFence {
				if err == nil || !reflect.DeepEqual(got, Result{}) {
					t.Fatalf("failed fence returned output: %+v err=%v", got, err)
				}
			} else if err != nil || got.Count == nil || *got.Count != 0 || len(got.Items) != 0 || got.NextCursor != nil {
				t.Fatalf("denied stale count=%+v err=%v", got, err)
			}
		})
	}
}
