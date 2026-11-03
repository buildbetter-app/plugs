package rehearsal

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/api/wire"
	"github.com/buildbetter-app/plugs/internal/client"
	"github.com/buildbetter-app/plugs/internal/core/model"
)

type countedPublicTransport struct {
	next  client.Transport
	calls int
}

type changeAfterPublicGet struct {
	next    client.Transport
	changed bool
	after   func(context.Context) error
}

func (t *changeAfterPublicGet) RoundTrip(ctx context.Context, request []byte) ([]byte, error) {
	response, err := t.next.RoundTrip(ctx, request)
	if err != nil {
		return nil, err
	}
	var call struct {
		Method string `json:"method"`
	}
	if !t.changed && json.Unmarshal(request, &call) == nil && call.Method == "bead.get" {
		t.changed = true
		if err := t.after(ctx); err != nil {
			return nil, err
		}
	}
	return response, nil
}

func (t *countedPublicTransport) RoundTrip(ctx context.Context, request []byte) ([]byte, error) {
	t.calls++
	return t.next.RoundTrip(ctx, request)
}

func TestOwnedPublicCallerTranscriptControl(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	f, err := openFixture(ctx, "memory", "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := f.close(); err != nil {
			t.Error(err)
		}
	})
	created, err := f.rpc.Call(ctx, "bead.create", map[string]any{"type": "issue", "fields": map[string]any{"title": "disposable caller control"}}, "")
	if err != nil {
		t.Fatal(err)
	}
	var commit model.Commit
	if json.Unmarshal(created.JSON, &commit) != nil || len(commit.Results) != 1 || commit.Results[0].Ref.ID == "" {
		t.Fatal("public fixture create returned no bead ID")
	}
	if _, err := f.rpc.Call(ctx, "memory.save", map[string]any{"key": "control", "content": "public synthetic context"}, ""); err != nil {
		t.Fatal(err)
	}
	before, err := readySignature(ctx, f.rpc)
	if err != nil {
		t.Fatal(err)
	}
	if count, err := compareOwnedPublicCalls(ctx, f, commit.Results[0].Ref.ID); err != nil || count != 3 {
		t.Fatalf("actual independent HTTP/MCP transcript control: count=%d err=%v", count, err)
	}
	underlying := &countedPublicTransport{next: f.transport}
	catalog, err := wire.DefaultCatalog()
	if err != nil {
		t.Fatal(err)
	}
	capture := &publicCapture{next: underlying, catalog: catalog}
	caller, err := client.New(capture, "rehearsal", "public-transcript-control")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := caller.Call(ctx, "bead.create", map[string]any{"type": "issue", "fields": map[string]any{"title": "must not dispatch"}}, ""); err == nil || !strings.Contains(err.Error(), "read-only") {
		t.Fatalf("catalog-mutating request was not refused before dispatch: %v", err)
	}
	if _, err := capture.RoundTrip(ctx, []byte(`{"jsonrpc":"2.0","id":"unexpected","method":"unknown.public","params":{}}`)); err == nil || !strings.Contains(err.Error(), "read-only") {
		t.Fatalf("unexpected public request was not refused before dispatch: %v", err)
	}
	if underlying.calls != 0 || len(capture.exchanges) != 0 {
		t.Fatal("refused request reached the owned service or entered the transcript")
	}
	after, err := readySignature(ctx, f.rpc)
	if err != nil || before != after {
		t.Fatalf("refused request changed public ready state: %v", err)
	}
}

func TestOwnedPublicCallerRefusesActualDriftAndMissingExchange(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	f, err := openFixture(ctx, "memory", "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := f.close(); err != nil {
			t.Error(err)
		}
	})
	created, err := f.rpc.Call(ctx, "bead.create", map[string]any{"type": "issue", "fields": map[string]any{"title": "capture alpha"}}, "")
	if err != nil {
		t.Fatal(err)
	}
	var commit model.Commit
	if json.Unmarshal(created.JSON, &commit) != nil || len(commit.Results) != 1 || commit.Results[0].Ref.ID == "" {
		t.Fatal("public fixture create returned no bead ID")
	}
	id := commit.Results[0].Ref.ID
	changingHTTP := &changeAfterPublicGet{next: f.transport, after: func(ctx context.Context) error {
		_, err := f.rpc.Call(ctx, "bead.update", map[string]any{"id": map[string]any{"id": id}, "fields": map[string]any{"title": "capture bravo"}}, "")
		return err
	}}
	// Each channel executes the same three reads. The real store changes only
	// after HTTP bead.get has replied, so both captures remain schema-valid and
	// call counts match while their final payloads differ.
	if count, err := compareOwnedPublicCallsWithControl(ctx, f, id, changingHTTP, 3); !changingHTTP.changed || count != 0 || err == nil || !strings.Contains(err.Error(), "response differs") {
		t.Fatalf("actual captured response drift accepted: changed=%v count=%d err=%v", changingHTTP.changed, count, err)
	}
	// Both sides again use the real, now quiescent fixture. The shadow executes
	// only the first two calls, leaving a genuinely missing captured exchange.
	if count, err := compareOwnedPublicCallsWithControl(ctx, f, id, f.transport, 2); count != 0 || err == nil || !strings.Contains(err.Error(), "equal-length") {
		t.Fatalf("incomplete actual capture accepted: count=%d err=%v", count, err)
	}
}
