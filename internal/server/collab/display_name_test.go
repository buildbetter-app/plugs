package collab_test

import (
	"context"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/server/collab"
)

func presenceBySession(t *testing.T, rows []any) map[string]map[string]any {
	t.Helper()
	out := map[string]map[string]any{}
	for _, value := range rows {
		row := value.(map[string]any)
		id := row["sessionId"].(string)
		if out[id] != nil {
			t.Fatal("duplicate presence session")
		}
		out[id] = row
	}
	return out
}

func TestRealRPCDisplayNameNormalizationAndUnchangedSessionResult(t *testing.T) {
	for _, test := range []struct {
		name  string
		input map[string]any
		want  string
	}{
		{"omitted", map[string]any{}, ""},
		{"empty", map[string]any{"displayName": ""}, ""},
		{"whitespace only", map[string]any{"displayName": "\t\n\r\u0085\u00a0\u1680\u2000\u2001\u2002\u2003\u2004\u2005\u2006\u2007\u2008\u2009\u200a\u2028\u2029\u202f\u205f\u3000"}, ""},
		{"Unicode trim", map[string]any{"displayName": "\u2003\u00a0\t Worker \u03b1 \u3000\n"}, "Worker \u03b1"},
		{"internal space", map[string]any{"displayName": "worker\u00a0one"}, "worker\u00a0one"},
		{"64 ASCII scalars", map[string]any{"displayName": strings.Repeat("a", 64)}, strings.Repeat("a", 64)},
		{"64 multibyte scalars", map[string]any{"displayName": strings.Repeat("\u754c", 64)}, strings.Repeat("\u754c", 64)},
		{"64 supplementary scalars", map[string]any{"displayName": strings.Repeat("\U0001f600", 64)}, strings.Repeat("\U0001f600", 64)},
		{"64 combining scalars", map[string]any{"displayName": strings.Repeat("e\u0301", 32)}, strings.Repeat("e\u0301", 32)},
		{"padded 64 scalars", map[string]any{"displayName": strings.Repeat("\u2003", 80) + strings.Repeat("\U0001f600", 64) + "\u00a0\t"}, strings.Repeat("\U0001f600", 64)},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newFixture(t, nil)
			before := durableState(t, f)
			opened := result(t, f.call(t, alice, "session.open", test.input))
			if len(opened) != 3 || opened["displayName"] != nil || !reflect.DeepEqual(opened["principal"], map[string]any{"kind": "user", "id": alice.ID}) {
				t.Fatal("display name changed the closed Session result or principal")
			}
			id := opened["sessionId"].(string)
			row := result(t, f.call(t, alice, "presence.set", map[string]any{"sessionId": id}))
			if value, present := row["displayName"]; present != (test.want != "") || (present && value != test.want) {
				t.Fatal("presence.set did not return the normalized optional label")
			}
			rows := presenceBySession(t, f.rows(t, bob, "project-a"))
			if len(rows) != 1 || !reflect.DeepEqual(rows[id], row) {
				t.Fatal("presence.list did not retain the normalized member")
			}
			result(t, f.call(t, alice, "session.close", map[string]any{"sessionId": id}))
			if len(f.rows(t, bob, "project-a")) != 0 || string(before) != string(durableState(t, f)) {
				t.Fatal("display lifecycle retained a closed row or changed durable state")
			}
		})
	}
}

func TestRealRPCDisplayNameInvalidParamsNeverAdmitSessions(t *testing.T) {
	f := newFixture(t, func(c *collab.Config) { c.Limits.Sessions = 1 })
	before := durableState(t, f)
	for _, input := range []any{
		nil, 1, true, []any{"worker"}, map[string]any{"id": "worker"},
		"a\x00b", "a\x1fb", "a\x7fb", "a\u0085b",
		"a\u00adb", "a\u061cb", "a\u200bb", "a\u200db", "a\u202eb", "a\u2066b", "\ufeffworker", "a\U000e0001b",
		strings.Repeat("a", 65), strings.Repeat("\u754c", 65), strings.Repeat("\U0001f600", 65), strings.Repeat("e\u0301", 32) + "e",
		"\u3000" + strings.Repeat("a", 65) + "\u2003",
	} {
		code(t, f.call(t, alice, "session.open", map[string]any{"displayName": input}), "invalid_params")
		if len(f.rows(t, bob, "project-a")) != 0 {
			t.Fatal("invalid display name consumed session state")
		}
	}
	for _, input := range []map[string]any{
		{"displayName": "worker", "principal": map[string]any{"kind": "user", "id": bob.ID}},
		{"_meta": map[string]any{"displayName": "worker"}},
	} {
		code(t, f.call(t, alice, "session.open", input), "invalid_params")
	}
	id := f.open(t, alice, map[string]any{"displayName": "worker"})
	for _, method := range []string{"presence.set", "session.close", "presence.list"} {
		params := map[string]any{"displayName": "replacement"}
		if method == "presence.list" {
			params["project"] = "project-a"
		} else {
			params["sessionId"] = id
		}
		code(t, f.call(t, alice, method, params), "invalid_params")
	}
	if row := presenceBySession(t, f.rows(t, bob, "project-a"))[id]; row["displayName"] != "worker" {
		t.Fatal("foreign input mutated the display name or closed its session")
	}
	if string(before) != string(durableState(t, f)) {
		t.Fatal("display-name validation changed durable state")
	}
}

func TestRealRPCDisplayNamesKeepNamedGroupMembersSeparate(t *testing.T) {
	for _, mode := range []string{"ungrouped", "self grouped", "sponsored"} {
		t.Run(mode, func(t *testing.T) {
			f := newFixture(t, func(c *collab.Config) {
				if mode == "sponsored" {
					c.AuthorizationInstalled = func() bool { return true }
					c.Policy = controlledPolicy{}
				}
			})
			by := alice
			if mode == "sponsored" {
				by = model.PrincipalRef{Kind: "agent", ID: "agent:worker", Sponsor: &alice, Caveats: map[string]any{"resources": []any{"bead/private"}}}
			}
			expectedPrincipal := map[string]any{"kind": by.Kind, "id": by.ID}
			if by.Sponsor != nil {
				expectedPrincipal["sponsor"] = map[string]any{"kind": "user", "id": alice.ID}
			}
			ids := map[string]string{}
			for _, label := range []string{"worker A", "worker B", "worker A"} {
				params := map[string]any{"displayName": label}
				if mode == "self grouped" {
					params["onBehalfOf"] = alice.ID
				}
				id := f.open(t, by, params)
				if _, duplicate := ids[id]; duplicate {
					t.Fatal("display name deduplicated sessions")
				}
				ids[id] = label
			}
			rows := presenceBySession(t, f.rows(t, bob, "project-a"))
			if len(rows) != len(ids) {
				t.Fatal("compact grouping merged named members")
			}
			for id, label := range ids {
				row := rows[id]
				if row["displayName"] != label || !reflect.DeepEqual(row["principal"], expectedPrincipal) {
					t.Fatal("member label replaced trusted identity or exposed caveats")
				}
				if mode != "ungrouped" && row["onBehalfOf"] != alice.ID {
					t.Fatal("display name replaced trusted sponsorship")
				}
				code(t, f.call(t, bob, "session.close", map[string]any{"sessionId": id}), "not_found")
				code(t, f.call(t, by, "presence.set", map[string]any{"sessionId": id, "onBehalfOf": label}), "forbidden")
				updated := result(t, f.call(t, by, "presence.set", map[string]any{"sessionId": id, "onBehalfOf": alice.ID}))
				if updated["displayName"] != label || updated["onBehalfOf"] != alice.ID {
					t.Fatal("trusted grouping update changed display identity")
				}
			}
			// Omitted/trimmed-empty names retain legacy compact presentation;
			// they never absorb a named member, even with the same sponsor.
			f.open(t, by, map[string]any{"onBehalfOf": alice.ID})
			f.open(t, by, map[string]any{"onBehalfOf": alice.ID, "displayName": "\u2003"})
			rows = presenceBySession(t, f.rows(t, bob, "project-a"))
			if len(rows) != len(ids)+1 {
				t.Fatal("legacy compact rows absorbed named members or changed omitted behavior")
			}
			for id, label := range ids {
				if rows[id]["displayName"] != label {
					t.Fatal("mixed group lost a named member")
				}
			}
		})
	}
}

func TestRealRPCDisplayNameDoesNotOwnAnonymousSessionsOrClaims(t *testing.T) {
	f := newFixture(t, nil)
	anonymous := model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}
	bead := f.create(t, "project-a")
	resource := "bead/" + bead
	f.write(t, anonymous, "bead.claim", map[string]any{"id": map[string]any{"id": bead}, "_meta": map[string]any{"agent": "client-a"}})
	before := durableState(t, f)
	ids := []string{}
	for _, input := range []struct{ agent, label string }{{"client-a", bob.ID}, {"client-b", bob.ID}, {"client-a", "other display"}} {
		ids = append(ids, f.open(t, anonymous, map[string]any{"displayName": input.label, "_meta": map[string]any{"agent": input.agent}}))
	}
	code(t, f.call(t, anonymous, "presence.set", map[string]any{"sessionId": ids[0], "_meta": map[string]any{"agent": "client-b"}}), "not_found")
	code(t, f.call(t, anonymous, "session.close", map[string]any{"sessionId": ids[0], "_meta": map[string]any{"agent": bob.ID}}), "not_found")
	code(t, f.call(t, bob, "session.close", map[string]any{"sessionId": ids[0], "_meta": map[string]any{"agent": "client-a"}}), "not_found")
	code(t, f.call(t, anonymous, "presence.set", map[string]any{"sessionId": ids[1], "workingOn": []string{resource}, "_meta": map[string]any{"agent": "client-b"}}), "forbidden")
	code(t, f.call(t, anonymous, "session.open", map[string]any{"displayName": bob.ID, "onBehalfOf": bob.ID}), "forbidden")
	for _, id := range []string{ids[0], ids[2]} {
		result(t, f.call(t, anonymous, "presence.set", map[string]any{"sessionId": id, "workingOn": []string{resource}, "_meta": map[string]any{"agent": "client-a"}}))
	}
	rows := presenceBySession(t, f.rows(t, bob, "project-a"))
	if len(rows) != 3 {
		t.Fatal("anonymous display labels deduplicated sessions")
	}
	for i, id := range ids {
		row := rows[id]
		if !reflect.DeepEqual(row["principal"], map[string]any{"kind": "anonymous", "id": "anonymous"}) || row["onBehalfOf"] != nil || row["_meta"] != nil || row["agent"] != nil {
			t.Fatal("untrusted label changed principal or exposed ownership metadata")
		}
		if (i == 1 && row["workingOn"] != nil) || (i != 1 && !reflect.DeepEqual(row["workingOn"], []any{resource})) {
			t.Fatal("display name changed anonymous claim matching")
		}
	}
	if rows[ids[0]]["displayName"] != bob.ID || rows[ids[1]]["displayName"] != bob.ID || rows[ids[2]]["displayName"] != "other display" {
		t.Fatal("anonymous display labels were not retained independently")
	}
	if string(before) != string(durableState(t, f)) {
		t.Fatal("anonymous display presence changed graph, leases or log")
	}
}

func TestRealRPCDisplayNamesDoNotBypassPrincipalOrSponsorQuotas(t *testing.T) {
	for _, sponsored := range []bool{false, true} {
		t.Run(map[bool]string{false: "principal", true: "sponsor"}[sponsored], func(t *testing.T) {
			f := newFixture(t, func(c *collab.Config) {
				c.Limits.PerPrincipal, c.Limits.PerSponsor = 2, 2
				if sponsored {
					c.AuthorizationInstalled = func() bool { return true }
					c.Policy = controlledPolicy{}
				}
			})
			for i, label := range []string{"one", "two", "one"} {
				by := alice
				if sponsored {
					by = model.PrincipalRef{Kind: "agent", ID: []string{"agent:a", "agent:b", "agent:c"}[i], Sponsor: &alice}
				}
				response := f.call(t, by, "session.open", map[string]any{"displayName": label})
				if i == 2 {
					code(t, response, "too_many_sessions")
				} else {
					result(t, response)
				}
			}
			if len(f.rows(t, bob, "project-a")) != 2 {
				t.Fatal("display identity changed quota accounting")
			}
		})
	}
}

func TestRealRPCDisplayNameSurvivesCurrentResourceMaskingWithoutMarkers(t *testing.T) {
	var visible atomic.Bool
	visible.Store(true)
	f := newFixture(t, func(c *collab.Config) {
		c.AuthorizationInstalled = func() bool { return true }
		c.Policy = controlledPolicy{read: func(_ context.Context, by model.PrincipalRef, bead service.Bead) (bool, error) {
			if (by.ID != alice.ID && by.ID != bob.ID) || bead.Project != "project-a" || bead.ID == "" {
				t.Error("policy did not receive actual principal and service resource")
			}
			return visible.Load(), nil
		}}
	})
	bead := f.create(t, "project-a")
	resource := "bead/" + bead
	f.write(t, alice, "bead.claim", map[string]any{"id": map[string]any{"id": bead}})
	before := durableState(t, f)
	id := f.open(t, alice, map[string]any{"displayName": "reader"})
	baseline := f.rows(t, bob, "project-a")
	result(t, f.call(t, alice, "presence.set", map[string]any{"sessionId": id, "focus": resource, "workingOn": []string{resource}}))
	row := presenceBySession(t, f.rows(t, bob, "project-a"))[id]
	if row["focus"] != resource || !reflect.DeepEqual(row["workingOn"], []any{resource}) || row["displayName"] != "reader" {
		t.Fatal("named presence did not retain service-backed resources")
	}
	visible.Store(false)
	if !reflect.DeepEqual(f.rows(t, bob, "project-a"), baseline) {
		t.Fatal("current denial left a resource marker or removed display identity")
	}
	code(t, f.call(t, alice, "presence.set", map[string]any{"sessionId": id, "focus": "bead/missing"}), "not_found")
	code(t, f.call(t, bob, "presence.list", map[string]any{"project": "project-b"}), "forbidden")
	if string(before) != string(durableState(t, f)) {
		t.Fatal("display masking changed graph, head or log")
	}
}

func TestRealRPCDisplayNameExpiryCloseAndRefreshAreEphemeral(t *testing.T) {
	f := newFixture(t, nil)
	before := durableState(t, f)
	first := f.open(t, alice, map[string]any{"displayName": "first"})
	second := f.open(t, alice, map[string]any{"displayName": "second"})
	lifetime, err := f.manager.SessionContext(context.Background(), collab.Caller{Principal: alice}, first)
	if err != nil {
		t.Fatal(err)
	}
	f.clock.advance(4 * time.Minute)
	code(t, f.call(t, alice, "presence.set", map[string]any{"sessionId": second, "displayName": "replacement"}), "invalid_params")
	code(t, f.call(t, alice, "presence.set", map[string]any{"sessionId": second, "focus": "bead/missing"}), "not_found")
	row := result(t, f.call(t, alice, "presence.set", map[string]any{"sessionId": first}))
	if row["displayName"] != "first" {
		t.Fatal("successful refresh changed display identity")
	}
	f.clock.advance(time.Minute)
	rows := presenceBySession(t, f.rows(t, bob, "project-a"))
	if len(rows) != 1 || rows[first]["displayName"] != "first" || lifetime.Err() != nil {
		t.Fatal("list or refused updates renewed the wrong named session")
	}
	code(t, f.call(t, alice, "presence.set", map[string]any{"sessionId": second}), "not_found")
	f.clock.advance(4 * time.Minute)
	if len(f.rows(t, bob, "project-a")) != 0 || lifetime.Err() == nil {
		t.Fatal("named session survived its exact renewed expiry")
	}
	reopened := f.open(t, alice, map[string]any{"displayName": "first"})
	if reopened == first {
		t.Fatal("reconnect deduplicated by display name")
	}
	closedLifetime, err := f.manager.SessionContext(context.Background(), collab.Caller{Principal: alice}, reopened)
	if err != nil {
		t.Fatal(err)
	}
	result(t, f.call(t, alice, "session.close", map[string]any{"sessionId": reopened}))
	if closedLifetime.Err() == nil || len(f.rows(t, bob, "project-a")) != 0 {
		t.Fatal("close retained named presence or session lifetime")
	}
	f.open(t, alice, map[string]any{"displayName": "transient"})
	if err := f.manager.Close(); err != nil {
		t.Fatal(err)
	}
	f.manager, err = collab.New(context.Background(), f.config)
	if err != nil {
		t.Fatal(err)
	}
	f.server = f.bind(t, f.manager)
	if len(f.rows(t, bob, "project-a")) != 0 || string(before) != string(durableState(t, f)) {
		t.Fatal("new manager restored display state or display lifecycle changed durable state")
	}
}
