package labelcontract_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/core/viewops"
	"github.com/buildbetter-app/plugs/internal/plugins/authnapikey"
	"github.com/buildbetter-app/plugs/internal/plugins/authzfixed"
	"github.com/buildbetter-app/plugs/internal/server/authn"
	"github.com/buildbetter-app/plugs/internal/server/authzrpc"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/server/transport/httpjson"
	"github.com/buildbetter-app/plugs/internal/server/viewrpc"
)

// This adds the real public view binding to the existing public-write
// fixture. Records still enter through bead.create/update/close, never seeds.
func (f *fixture) bindLabelQueryRPC(cfg viewops.Config, guarded rpc.Service) {
	f.t.Helper()
	if guarded == nil {
		ontology := service.NewOntology(nil)
		writer := pipeline.New(f.store, f.lease, pipeline.WithOntology(ontology), pipeline.WithIDGenerator(func() (string, error) {
			f.nextID++
			return fmt.Sprintf("lc-%06d", f.nextID), nil
		}))
		guarded = service.New(f.store, f.lease, writer, service.WithOntology(ontology))
	}
	cfg.OntologyRevision = model.Natural64(1)
	views, err := viewops.New(f.store, f.lease, cfg)
	if err != nil {
		f.t.Fatal(err)
	}
	registry, err := rpc.NewRegistry()
	if err != nil {
		f.t.Fatal(err)
	}
	if err := rpc.BindService(registry, guarded); err != nil {
		f.t.Fatal(err)
	}
	key := []byte("label-query-cursor-key-0000000001")
	if err := viewrpc.BindWithConfig(registry, views, viewrpc.Config{CursorKey: key}); err != nil {
		f.t.Fatal(err)
	}
	f.server, err = rpc.New(registry, rpc.Config{DefaultProject: "local", Incarnation: f.meta().Incarnation, OntologyRevision: model.Natural64(1), CursorKey: key})
	if err != nil {
		f.t.Fatal(err)
	}
}

func labelQueryParams(query string, summary bool) map[string]any {
	definition := map[string]any{
		"filter": map[string]any{"query": query},
		"sort":   []string{"id"}, "budgetTokens": 100,
	}
	if summary {
		definition["fields"], definition["groupBy"] = []string{}, []string{"type"}
	}
	return map[string]any{"definition": definition}
}

func labelQueryIDs(t *testing.T, result map[string]any) []string {
	t.Helper()
	items, ok := result["items"].([]any)
	if !ok {
		t.Fatalf("view.run omitted its item array: %v", result)
	}
	ids := make([]string, 0, len(items))
	for _, item := range items {
		ids = append(ids, item.(map[string]any)["id"].(string))
	}
	return ids
}

func labelQueryCount(t *testing.T, result map[string]any) int {
	t.Helper()
	total := 0
	groups, _ := result["groups"].([]any)
	for _, raw := range groups {
		group := raw.(map[string]any)
		count, ok := group["count"].(float64)
		if !ok || count < 0 || count != float64(int(count)) {
			t.Fatalf("invalid type-summary count: %v", group)
		}
		total += int(count)
	}
	return total
}

func (f *fixture) queryIDs(query string, want ...string) {
	f.t.Helper()
	before := f.meta()
	result := f.call("view.run", labelQueryParams(query, false), false, "")
	expected := append([]string{}, want...)
	sort.Strings(expected)
	if got := labelQueryIDs(f.t, result); !reflect.DeepEqual(got, expected) || result["nextCursor"] != nil {
		f.t.Fatalf("query %q: ids=%v want=%v result=%v", query, got, expected, result)
	}
	// This exercises the existing complete fields=[] type-summary path.
	// It is bounded shared-matcher evidence, not a countOnly implementation.
	summary := f.call("view.run", labelQueryParams(query, true), false, "")
	if count := labelQueryCount(f.t, summary); count != len(expected) {
		f.t.Fatalf("query %q: summary=%v list=%v", query, summary, expected)
	}
	if !reflect.DeepEqual(before, f.meta()) {
		f.t.Fatal("read-only label predicate changed store metadata")
	}
}

func TestB16PublicLabelQueryAllAnyExcludePrecedenceAndGrouping(t *testing.T) {
	eachStore(t, func(t *testing.T, f *fixture) {
		f.bindLabelQueryRPC(viewops.Config{}, nil)
		create := func(title string, labels ...string) string {
			fields := map[string]any{"title": title}
			if len(labels) != 0 {
				fields["labels"] = labels
			}
			id, _ := f.create(fields, "")
			return id
		}
		red := create("red", "red")
		both := create("both", "red", "blue")
		blue := create("blue", "blue")
		redGreen := create("red green", "red", "green")
		green := create("green", "green")
		empty := create("empty")
		closed := create("closed", "red")
		f.call("bead.close", map[string]any{"id": closed}, true, "")
		for _, tc := range []struct{ name, id, status string }{
			{"red", red, "todo"},
			{"both", both, "todo"},
			{"blue", blue, "todo"},
			{"red_green", redGreen, "todo"},
			{"green", green, "todo"},
			{"empty", empty, "todo"},
			{"closed", closed, "done"},
		} {
			t.Run("stored_status_"+tc.name, func(t *testing.T) {
				prior := f.t
				f.t = t
				defer func() { f.t = prior }()
				bead := f.bead(tc.id)
				got, ok := bead["status"].(string)
				t.Logf("public bead.get engine=%s id=%s status=%#v want=%q", f.engine, tc.id, bead["status"], tc.status)
				if !ok || got != tc.status {
					t.Fatalf("public bead.get %s status=%#v want=%q", tc.id, bead["status"], tc.status)
				}
			})
		}
		cases := []struct {
			name, query string
			want        []string
		}{
			{"all_aliases", "label=red AND labels=blue", []string{both}},
			{"any", "(label=red OR labels=blue)", []string{red, both, blue, redGreen, closed}},
			{"exclude_includes_empty", "NOT label=red", []string{blue, green, empty}},
			{"scalar_conjunction", "status=open AND (label=red OR label=blue)", []string{}},
			{"scalar_conjunction_todo", "status=todo AND (label=red OR label=blue)", []string{red, both, blue, redGreen}},
			{"root_project_conjunction", "project=local AND label=red", []string{red, both, redGreen, closed}},
			{"independent_any_groups", "(label=red OR label=blue) AND (label=green OR label=blue)", []string{both, blue, redGreen}},
			{"and_before_or", "label=red OR label=blue AND label=green", []string{red, both, redGreen, closed}},
			{"not_before_and", "NOT label=red AND label=green", []string{green}},
			{"not_of_group", "NOT (label=red OR label=blue)", []string{green, empty}},
			{"nested_groups", "(label=red AND NOT (label=blue OR label=green)) OR label=blue", []string{red, both, blue, closed}},
			{"double_not", "NOT NOT label=red", []string{red, both, redGreen, closed}},
			{"separated_label_operator", "label = red AND labels = blue", []string{both}},
			{"lowercase_operators", "not label=red and (labels=green or label=blue)", []string{blue, green}},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				prior := f.t
				f.t = t
				defer func() { f.t = prior }()
				f.queryIDs(tc.query, tc.want...)
			})
		}
	})
}

func TestB16PublicLabelQueryExactLiteralsQuotesAndSQLiteReopen(t *testing.T) {
	eachStore(t, func(t *testing.T, f *fixture) {
		f.bindLabelQueryRPC(viewops.Config{}, nil)
		values := []string{"red", "Red", "none", "null", "日本語", "é", "e\u0301", " red ", "AND OR NOT (=)", "line\nbreak", "tab\tvalue", "path\\value", "single'quote", "double\"quote", "red*"}
		ids := make(map[string]string, len(values))
		for _, value := range values {
			id, _ := f.create(map[string]any{"title": value, "labels": []string{value}}, "")
			ids[value] = id
		}
		empty, _ := f.create(map[string]any{"title": "empty"}, "")
		cases := []struct{ name, query, value string }{
			{"exact_lower", "label=red", "red"},
			{"exact_upper", "labels=Red", "Red"},
			{"literal_none", "label=none", "none"},
			{"quoted_none", `label="none"`, "none"},
			{"literal_null", "label=null", "null"},
			{"quoted_null", "label='null'", "null"},
			{"utf8", `label="日本語"`, "日本語"},
			{"composed", "label='é'", "é"},
			{"decomposed", "label='e\u0301'", "e\u0301"},
			{"no_trim", `label=" red "`, " red "},
			{"operator_text", `label="AND OR NOT (=)"`, "AND OR NOT (=)"},
			{"newline_escape", `label="line\nbreak"`, "line\nbreak"},
			{"tab_escape", `label='tab\tvalue'`, "tab\tvalue"},
			{"backslash_escape", `label='path\\value'`, "path\\value"},
			{"single_quote_escape", `label='single\'quote'`, "single'quote"},
			{"double_quote_escape", `label="double\"quote"`, "double\"quote"},
			{"literal_pattern_text", `label="red*"`, "red*"},
		}
		check := func(t *testing.T) {
			prior := f.t
			f.t = t
			defer func() { f.t = prior }()
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					prior := f.t
					f.t = t
					defer func() { f.t = prior }()
					f.queryIDs(tc.query, ids[tc.value])
				})
			}
			all := []string{empty}
			for _, id := range ids {
				all = append(all, id)
			}
			f.queryIDs("NOT label=absent", all...)
			f.queryIDs("id="+empty+" AND NOT label=none", empty)
			f.queryIDs("id=" + empty + " AND label=none")
		}
		t.Run("initial", check)
		if f.engine == "sqlite" {
			before := f.meta()
			f.close()
			f.open()
			f.bindLabelQueryRPC(viewops.Config{}, nil)
			if !reflect.DeepEqual(before, f.meta()) {
				t.Fatal("SQLite reopen changed label-query content")
			}
			t.Run("reopen", check)
		}
	})
}

// These controls must remain GREEN on the unchanged bce source and on the
// changed candidate. They pin opaque scalar bytes, matching and refusals.
func TestB16PublicLabelQueryLegacyScalarControls(t *testing.T) {
	eachStore(t, func(t *testing.T, f *fixture) {
		f.bindLabelQueryRPC(viewops.Config{}, nil)
		ids := map[string]string{}
		for _, title := range []string{"OR", "(foo)", `"quoted"`, "'quoted'", "Alphabet"} {
			id, _ := f.create(map[string]any{"title": title}, "")
			ids[title] = id
		}
		for _, tc := range []struct{ query, title string }{
			{"title=OR", "OR"}, {"title=(foo)", "(foo)"},
			{`title="quoted"`, `"quoted"`}, {"title='quoted'", "'quoted'"},
			{"title=pha", "Alphabet"},
		} {
			f.queryIDs(tc.query, ids[tc.title])
		}
		f.queryIDs("id="+ids["OR"]+" AND type=issue AND project=local", ids["OR"])
		f.queryIDs("id=a=b")
		f.queryIDs("title=alphabet")
		f.queryIDs("title=*")
		for _, tc := range []struct{ query, code string }{
			{"title=\"two words\"", "invalid_params"}, {"id=", "invalid_params"},
			{"id=a&&type=issue", "invalid_params"}, {"type=issue OR status=open", "invalid_params"},
			{"NOT status=open", "invalid_params"}, {"status=open AND", "invalid_params"},
			{"status=open OR unknown=value", "invalid_params"}, {"NOT unknown=value", "invalid_params"},
			{"ID=x", "unknown_field"}, {"project=elsewhere", "invalid_params"},
		} {
			before := f.meta()
			f.call("view.run", labelQueryParams(tc.query, false), false, tc.code)
			if !reflect.DeepEqual(before, f.meta()) {
				t.Fatal("scalar query refusal changed content")
			}
		}
	})
}

func TestB16PublicLabelQueryMalformedAndMixedRefusals(t *testing.T) {
	eachStore(t, func(t *testing.T, f *fixture) {
		f.bindLabelQueryRPC(viewops.Config{}, nil)
		f.create(map[string]any{"title": "must remain", "labels": []string{"red"}}, "")
		queries := []string{
			"label", "label=", "label= AND label=red", `label=""`, "labels=''",
			`label="unterminated`, `label='unterminated`, `label="dangling\`,
			`label="unknown\q"`, `label="unknown\r"`, `label="red"garbage`,
			"label!=red", "label>red", "label=~red", "label=red*", "label=red label=blue",
			"label=red AND", "label=red OR", "()", "(label=red", "label=red)",
			"label=red OR status=open", "status=open OR label=red", "NOT status=open",
			"status=open AND label=red OR label=blue", "label=red OR (label=blue AND status=open)",
			"NOT (label=red AND project=local)", "(status=open AND label=red)",
			"project=elsewhere AND label=red", "title=\"two words\" AND label=red",
		}
		for i, query := range queries {
			t.Run(fmt.Sprintf("refusal_%02d", i), func(t *testing.T) {
				prior := f.t
				f.t = t
				defer func() { f.t = prior }()
				before := f.meta()
				f.call("view.run", labelQueryParams(query, false), false, "invalid_params")
				if !reflect.DeepEqual(before, f.meta()) {
					t.Fatalf("refused query %q wrote content", query)
				}
			})
		}
	})
}

type labelQuerySavedView struct{ query string }

func (v labelQuerySavedView) GetView(_ context.Context, _ model.PrincipalRef, id string) (viewops.Definition, error) {
	if id != "label-query" {
		return viewops.Definition{}, &model.Error{Data: model.ErrorData{Code: "not_found"}}
	}
	return viewops.Definition{Filter: &viewops.Filter{Query: v.query}, Sort: []string{"id"}}, nil
}

func TestB16PublicLabelQueryExplainSharesMembership(t *testing.T) {
	eachStore(t, func(t *testing.T, f *fixture) {
		query := "(label=red OR label=blue) AND NOT label=green"
		f.bindLabelQueryRPC(viewops.Config{Views: labelQuerySavedView{query}}, nil)
		included, _ := f.create(map[string]any{"title": "included", "labels": []string{"red"}}, "")
		excluded, _ := f.create(map[string]any{"title": "excluded", "labels": []string{"blue", "green"}}, "")
		f.queryIDs(query, included)
		before := f.meta()
		for _, tc := range []struct {
			id       string
			included bool
			reason   string
		}{{included, true, "included"}, {excluded, false, "labels"}} {
			result := f.call("view.explain", map[string]any{"viewId": "label-query", "itemId": tc.id}, false, "")
			if result["included"] != tc.included || !reflect.DeepEqual(result["reasons"], []any{tc.reason}) {
				t.Fatalf("list/explain disagree: %v", result)
			}
		}
		if !reflect.DeepEqual(before, f.meta()) {
			t.Fatal("label explanation changed content")
		}
	})
}

func labelQueryHTTP(t *testing.T, handler http.Handler, token, method string, params map[string]any, code string) map[string]any {
	t.Helper()
	raw, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "http://127.0.0.1/rpc/v1", bytes.NewReader(raw))
	req.RemoteAddr = "127.0.0.1:12345"
	req.Header.Set("Plug-RPC", "1")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	out := httptest.NewRecorder()
	handler.ServeHTTP(out, req)
	var reply map[string]any
	if err := json.Unmarshal(out.Body.Bytes(), &reply); err != nil {
		t.Fatal(err)
	}
	if code != "" {
		e, ok := reply["error"].(map[string]any)
		if !ok || e["data"].(map[string]any)["code"] != code || reply["result"] != nil {
			t.Fatalf("wanted %s without result: %s", code, out.Body.String())
		}
		return reply
	}
	if reply["error"] != nil {
		t.Fatalf("view RPC: %s", out.Body.String())
	}
	return reply["result"].(map[string]any)
}

func TestB16PublicLabelQueryFixedAuthorizationNonDisclosure(t *testing.T) {
	eachStore(t, func(t *testing.T, f *fixture) {
		const secret = "needle-secret"
		id, _ := f.create(map[string]any{"title": "hidden-title", "labels": []string{secret}}, "")
		m := f.meta()
		ns := authz.Namespace{StoreID: string(m.StoreID), Incarnation: m.Incarnation, InstallationID: "fixed", Authority: model.RefByID("authority", model.Natural64(0))}
		facts := authzfixed.Facts{Namespace: ns,
			Projects: []authzfixed.Project{{Ref: model.RefByID("local", model.Natural64(0)), Name: "local", InstallationID: "fixed", Active: true}},
			Grants:   []authz.Grant{{Subject: authz.SubjectRef{Kind: "user", ID: "user:reader"}, Scope: authz.Scope{Kind: authz.ProjectScope, Ref: model.RefByID("local", model.Natural64(0))}, Role: "involved"}},
		}
		keys := authnapikey.New(authnapikey.Config{AuthorizationInstalled: func() bool { return true }})
		app, err := authzrpc.New(context.Background(), authzrpc.Config{Store: f.store, Lease: f.lease, Provider: keys, Facts: authzrpc.StaticFacts{Facts: facts}, Namespace: ns, DefaultProject: "local"})
		if err != nil {
			t.Fatal(err)
		}
		query := "label=" + secret
		f.bindLabelQueryRPC(viewops.Config{
			Views: labelQuerySavedView{query},
			AuthorizeRead: func(ctx context.Context, by model.PrincipalRef, resource model.ResourceRef, _ string) error {
				_, err := app.Service.Get(ctx, by, service.GetRequest{ID: resource.Ref})
				return err
			},
		}, app.Service)
		admission, err := authn.New(authn.Config{Provider: keys, AuthorizationInstalled: func() bool { return true }})
		if err != nil {
			t.Fatal(err)
		}
		handler, err := httpjson.New(f.server, admission, httpjson.Config{})
		if err != nil {
			t.Fatal(err)
		}
		user := model.PrincipalRef{Kind: "user", ID: "user:reader"}
		reader, err := keys.Issue(user, time.Now().Add(time.Hour))
		if err != nil {
			t.Fatal(err)
		}
		agent, err := keys.Issue(model.PrincipalRef{Kind: "agent", ID: "agent:reader", Sponsor: &user}, time.Now().Add(time.Hour))
		if err != nil {
			t.Fatal(err)
		}
		before := f.meta()
		// CP7 permits User reads and refuses Agent reads even with a sponsor's
		// project grant. Labels must never widen that finite read policy.
		for _, q := range []string{"", query, "NOT label=absent", "id=" + id + " AND " + query} {
			visible := labelQueryHTTP(t, handler, reader.Token, "view.run", labelQueryParams(q, false), "")
			if !reflect.DeepEqual(labelQueryIDs(t, visible), []string{id}) {
				t.Fatalf("permitted User lost actual matching record: %v", visible)
			}
			hidden := labelQueryHTTP(t, handler, agent.Token, "view.run", labelQueryParams(q, false), "")
			summary := labelQueryHTTP(t, handler, agent.Token, "view.run", labelQueryParams(q, true), "")
			if len(labelQueryIDs(t, hidden)) != 0 || labelQueryCount(t, summary) != 0 {
				t.Fatalf("label predicate disclosed denied content/count: %v %v", hidden, summary)
			}
			raw, _ := json.Marshal([]any{hidden, summary})
			for _, value := range []string{id, secret, "hidden-title"} {
				if strings.Contains(string(raw), value) {
					t.Fatalf("denied view disclosed %q: %s", value, raw)
				}
			}
		}
		for _, target := range []string{id, "missing-id"} {
			reply := labelQueryHTTP(t, handler, agent.Token, "view.explain", map[string]any{"viewId": "label-query", "itemId": target}, "not_found")
			raw, _ := json.Marshal(reply)
			if strings.Contains(string(raw), id) || strings.Contains(string(raw), secret) || strings.Contains(string(raw), "hidden-title") {
				t.Fatalf("denied explanation disclosed content: %s", raw)
			}
		}
		if !reflect.DeepEqual(before, f.meta()) {
			t.Fatal("authorized or denied label views changed content")
		}
	})
}
