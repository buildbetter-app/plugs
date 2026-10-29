package events

import (
	"strings"
	"testing"

	core "github.com/buildbetter-app/plugs/internal/core/events"
	"github.com/buildbetter-app/plugs/internal/core/model"
)

func codecForTest(t *testing.T) *CursorCodec {
	t.Helper()
	codec, err := NewCursorCodec([]byte(strings.Repeat("test-key", 4)))
	if err != nil {
		t.Fatal(err)
	}
	return codec
}
func TestSignedCursorBindingsAndLosslessPositions(t *testing.T) {
	codec := codecForTest(t)
	principal := model.PrincipalRef{Kind: "user", ID: "u1"}
	filter := core.Filter{Types: []string{"bead.created"}}
	binding, err := BindingFor("event.list", filter, principal, "1", "policy-1")
	if err != nil {
		t.Fatal(err)
	}
	position := core.Position{Incarnation: "inc-1", Revision: "1e1000000000", Index: "18446744073709551616"}
	token, err := codec.Encode(position, binding)
	if err != nil {
		t.Fatal(err)
	}
	got, err := codec.Decode(token, binding)
	if err != nil || got.Revision.String() != position.Revision.String() || got.Index.String() != position.Index.String() {
		t.Fatalf("decode=%+v err=%v", got, err)
	}
	for _, tc := range []struct {
		name, method, policy string
		filter               core.Filter
		by                   model.PrincipalRef
		ontology             model.Revision
	}{
		{"method", "event.subscribe", "policy-1", filter, principal, "1"},
		{"filter", "event.list", "policy-1", core.Filter{}, principal, "1"},
		{"principal", "event.list", "policy-1", filter, model.PrincipalRef{Kind: "user", ID: "u2"}, "1"},
		{"caveats", "event.list", "policy-1", filter, model.PrincipalRef{Kind: "user", ID: "u1", Caveats: map[string]any{"project": "other"}}, "1"},
		{"ontology", "event.list", "policy-1", filter, principal, "2"},
		{"policy", "event.list", "policy-2", filter, principal, "1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			other, err := BindingFor(tc.method, tc.filter, tc.by, tc.ontology, tc.policy)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = codec.Decode(token, other); err == nil {
				t.Fatal("accepted cross-binding cursor")
			}
		})
	}
	for _, bad := range []string{token + "x", "x" + token, strings.Repeat("x", 9000), "invalid"} {
		if _, err = codec.Decode(bad, binding); err == nil {
			t.Fatal("accepted tampered cursor")
		}
	}
	if _, err = codec.Encode(core.Position{Incarnation: "i", Index: "1"}, binding); err == nil {
		t.Fatal("accepted impossible R0 index")
	}
}
