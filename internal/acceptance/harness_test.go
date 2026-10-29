package acceptance

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

type transportFunc func(context.Context, []byte) ([]byte, error)

func (f transportFunc) RoundTrip(ctx context.Context, b []byte) ([]byte, error) {
	return f(ctx, b)
}

// These tests validate the measuring tool. Only //spec:acceptance can supply
// connected application evidence; these controlled transports cannot.
func TestProjectionRetainsOrderingNullTextAndLargeIntegers(t *testing.T) {
	raw := []byte(`{"items":[{"id":"p-b"},{"id":"p-a"}],"text":"  exact\n","n":9007199254740993,"nil":null,"a/b":{"~key":false}}`)
	got, err := Project(raw, "/items", "/text", "/n", "/nil", "/a~1b/~0key")
	if err != nil {
		t.Fatal(err)
	}
	want := []byte(`{"/items":[{"id":"p-b"},{"id":"p-a"}],"/text":"  exact\n","/n":9007199254740993,"/nil":null,"/a~1b/~0key":false}`)
	if err := EqualJSON(want, got); err != nil {
		t.Fatal(err)
	}
	if err := EqualJSON([]byte(`["p-a","p-b"]`), []byte(`["p-b","p-a"]`)); err == nil {
		t.Fatal("projection must not normalize away semantic order")
	}
	if err := EqualJSON([]byte(`9007199254740993`), []byte(`9007199254740992`)); err == nil {
		t.Fatal("projection rounded a revision")
	}
}

func TestProjectionFailsForMissingAmbiguousOrMalformedInput(t *testing.T) {
	for _, raw := range []string{
		`{"x":1,"x":2}`, `{"nested":{"x":1,"x":2}}`,
		`[{"x":1,"x":2}]`, `{} null`, `{"x":`, `{"x":]}`, ``,
	} {
		t.Run(raw, func(t *testing.T) {
			if _, err := CanonicalJSON([]byte(raw)); err == nil {
				t.Fatal("accepted ambiguous or malformed JSON")
			}
		})
	}
	for _, pointer := range []string{"/missing", "/items/2", "/items/-1", "/items/01", "/items/+0", "/items/-", "/items/0/x", "items", "/~2", "/~"} {
		t.Run(pointer, func(t *testing.T) {
			if _, err := Project([]byte(`{"items":[1]}`), pointer); err == nil {
				t.Fatal("accepted missing or invalid path")
			}
		})
	}
	if _, err := Project([]byte(`null`)); err == nil {
		t.Fatal("empty projection would prove nothing")
	}
	if _, err := Project([]byte(`null`), "", ""); err == nil {
		t.Fatal("accepted duplicate path")
	}
}

func TestInputsHaveNoAmbientFallbackAndOwnTheirSeeds(t *testing.T) {
	if _, err := NewInputs(time.Time{}, "p-1"); err == nil {
		t.Fatal("accepted wall clock fallback")
	}
	for _, ids := range [][]string{{""}, {"same", "same"}} {
		if _, err := NewInputs(time.Unix(1, 0), ids...); err == nil {
			t.Fatal("accepted invalid IDs")
		}
	}
	ids := []string{"p-first", "p-second"}
	start := time.Date(2026, 10, 27, 10, 0, 0, 0, time.FixedZone("offset", 3600))
	in, err := NewInputs(start, ids...)
	if err != nil {
		t.Fatal(err)
	}
	ids[0] = "mutated"
	for _, want := range []string{"p-first", "p-second"} {
		if got, err := in.NextID(); err != nil || got != want {
			t.Fatalf("ID = %q, %v; want %q", got, err, want)
		}
	}
	if _, err := in.NextID(); err == nil {
		t.Fatal("silently generated an unseeded ID")
	}
	if err := in.Advance(-time.Second); err == nil || !in.Now().Equal(start) {
		t.Fatal("clock moved backward")
	}
	if err := in.Advance(5 * time.Minute); err != nil || !in.Now().Equal(start.Add(5*time.Minute)) {
		t.Fatal("clock did not advance exactly five minutes")
	}
}

func TestTraceRetainsTypedRefusalAndNullAsDifferentOutcomes(t *testing.T) {
	in, _ := NewInputs(time.Unix(1, 0))
	responses := []string{
		`{"jsonrpc":"2.0","id":1,"result":null}`,
		`{"jsonrpc":"2.0","id":2,"error":{"code":-32003,"message":"precondition_failed","data":{"currentRevision":3}}}`,
	}
	trace, _ := NewTrace(transportFunc(func(_ context.Context, request []byte) ([]byte, error) {
		response := responses[0]
		responses = responses[1:]
		// A transport is permitted to reuse or overwrite its input buffer.
		request[0] = '!'
		return []byte(response), nil
	}), in.Now)
	params := map[string]any{"filter": map[string]any{"query": "type=bug"}}
	for _, method := range []string{"bead.claimNext", "bead.update"} {
		if _, err := trace.Call(context.Background(), method, params); err != nil {
			t.Fatal(err)
		}
	}
	params["filter"] = nil
	frames := trace.Frames()
	if len(frames) != 2 || !strings.Contains(string(frames[0].Request), "type=bug") {
		t.Fatal("trace lost or aliased the request")
	}
	first, _ := Project(frames[0].Response, "/result")
	if err := EqualJSON([]byte(`{"/result":null}`), first); err != nil {
		t.Fatal(err)
	}
	second, _ := Project(frames[1].Response, "/error/code", "/error/data/currentRevision")
	if err := EqualJSON([]byte(`{"/error/code":-32003,"/error/data/currentRevision":3}`), second); err != nil {
		t.Fatal(err)
	}
	frames[0].Response[0] = '!'
	if !json.Valid(trace.Frames()[0].Response) {
		t.Fatal("caller mutated the retained trace")
	}
}

func TestTraceRejectsInvalidEnvelopesAndTransportFailure(t *testing.T) {
	for _, response := range []string{
		`null`, `[]`, `{"id":1,"result":null}`,
		`{"jsonrpc":"2.0","id":2,"result":null}`,
		`{"jsonrpc":"2.0","id":"1","result":null}`,
		`{"jsonrpc":"2.0","id":1}`,
		`{"jsonrpc":"2.0","id":1,"result":null,"error":null}`,
		`{"jsonrpc":"2.0","id":1,"error":null}`,
		`{"jsonrpc":"2.0","id":1,"error":{"code":"bad","message":"bad"}}`,
		`{"jsonrpc":"2.0","id":1,"error":{"code":1.5,"message":"bad"}}`,
		`{"jsonrpc":"2.0","id":1,"error":{"code":-1}}`,
		`{"jsonrpc":"2.0","id":1,"result":null,"result":{}}`,
	} {
		t.Run(response, func(t *testing.T) {
			trace, _ := NewTrace(transportFunc(func(context.Context, []byte) ([]byte, error) {
				return []byte(response), nil
			}), func() time.Time { return time.Unix(1, 0) })
			if _, err := trace.Call(context.Background(), "bead.get", map[string]any{}); err == nil {
				t.Fatal("accepted invalid envelope")
			}
			if len(trace.Frames()) != 0 {
				t.Fatal("recorded an invalid response as evidence")
			}
		})
	}
	want := errors.New("transport unavailable")
	clock := func() time.Time { return time.Unix(1, 0) }
	trace, _ := NewTrace(transportFunc(func(context.Context, []byte) ([]byte, error) { return nil, want }), clock)
	if _, err := trace.Call(context.Background(), "bead.get", nil); !errors.Is(err, want) {
		t.Fatalf("transport error was lost: %v", err)
	}
	if _, err := NewTrace(nil, clock); err == nil {
		t.Fatal("accepted missing transport")
	}
	if _, err := NewTrace(transportFunc(func(context.Context, []byte) ([]byte, error) { return nil, nil }), nil); err == nil {
		t.Fatal("accepted missing clock")
	}
}

func TestIdenticalInputsProduceIdenticalTraceBytes(t *testing.T) {
	run := func() []byte {
		in, err := NewInputs(time.Date(2026, 10, 27, 10, 0, 0, 0, time.UTC), "p-1", "p-2")
		if err != nil {
			t.Fatal(err)
		}
		trace, _ := NewTrace(transportFunc(func(_ context.Context, request []byte) ([]byte, error) {
			var call struct {
				ID json.Number `json:"id"`
			}
			if err := json.Unmarshal(request, &call); err != nil {
				return nil, err
			}
			id, err := in.NextID()
			if err != nil {
				return nil, err
			}
			return json.Marshal(map[string]any{"jsonrpc": "2.0", "id": call.ID, "result": map[string]any{"id": id, "at": in.Now()}})
		}), in.Now)
		for range 2 {
			if _, err := trace.Call(context.Background(), "measurement.selfTest", map[string]any{}); err != nil {
				t.Fatal(err)
			}
			if err := in.Advance(time.Second); err != nil {
				t.Fatal(err)
			}
		}
		data, err := json.Marshal(trace.Frames())
		if err != nil {
			t.Fatal(err)
		}
		return data
	}
	first, second := run(), run()
	if string(first) != string(second) {
		t.Fatalf("same inputs changed trace bytes\nfirst: %s\nsecond: %s", first, second)
	}
}
