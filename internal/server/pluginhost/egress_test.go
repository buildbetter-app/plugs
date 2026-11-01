package pluginhost

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/pluginapi"
	sdk "github.com/buildbetter-app/plugs/sdk/go"
	"github.com/buildbetter-app/plugs/sdk/go/conformance"
)

func TestAnonymousHostDoesNotResolveDeclaredDestination(t *testing.T) {
	var resolutions atomic.Int64
	original := net.DefaultResolver
	net.DefaultResolver = &net.Resolver{PreferGo: true, Dial: func(context.Context, string, string) (net.Conn, error) {
		resolutions.Add(1)
		return nil, errors.New("controlled DNS fixture: no network")
	}}
	defer func() { net.DefaultResolver = original }()
	for _, authenticated := range []bool{false, true} {
		resolutions.Store(0)
		native := conformance.Native(conformance.Options{After: func(_ context.Context, host pluginapi.Host, _ pluginapi.CommittedChange, _ pluginapi.SnapshotReader) error {
			response, err := host.HTTP().Get("https://i3-egress.invalid")
			if response != nil {
				response.Body.Close()
			}
			if !authenticated && (code(err) != ErrEgress || resolutions.Load() != 0) {
				t.Errorf("anonymous egress resolved a declared host: error=%v resolutions=%d", err, resolutions.Load())
			}
			if authenticated && (err == nil || resolutions.Load() == 0) {
				t.Errorf("authenticated control did not reach the controlled resolver: %v", err)
			}
			return nil
		}})
		native.Manifest.Requires.Egress = []string{"i3-egress.invalid:443"}
		host, err := New(Config{Natives: []sdk.Native{native}, Bindings: fixtureBindings(), AuthenticationConfigured: authenticated})
		if err != nil {
			t.Fatal(err)
		}
		change := proposal()
		for _, subscriber := range host.Subscribers() {
			if subscriber.Post != nil {
				if err := subscriber.Post.After(context.Background(), port.CommittedChange{RequestID: change.RequestID, Principal: change.Principal, Revision: change.BaseRevision, Changes: change.Changes}, &snapshot{}); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
}

func TestAnonymousHostRefusesDeclaredEgressBeforeDial(t *testing.T) {
	var requests atomic.Int64
	target := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer target.Close()
	for _, authenticated := range []bool{false, true} {
		name, detail := "anonymous", "anonymous mode"
		if authenticated {
			name, detail = "authenticated", "private or non-public target"
		}
		t.Run(name, func(t *testing.T) {
			var calls atomic.Int64
			native := conformance.Native(conformance.Options{After: func(_ context.Context, host pluginapi.Host, _ pluginapi.CommittedChange, _ pluginapi.SnapshotReader) error {
				calls.Add(1)
				response, err := host.HTTP().Get(target.URL)
				if response != nil {
					response.Body.Close()
				}
				if code(err) != ErrEgress || !strings.Contains(err.Error(), detail) {
					t.Errorf("declared egress: got %v, want %s (%s)", err, ErrEgress, detail)
				}
				return nil
			}})
			native.Manifest.Requires.Egress = []string{strings.TrimPrefix(target.URL, "https://")}
			host, err := New(Config{Natives: []sdk.Native{native}, Bindings: fixtureBindings(), AuthenticationConfigured: authenticated})
			if err != nil {
				t.Fatal(err)
			}
			change := proposal()
			for _, subscriber := range host.Subscribers() {
				if subscriber.Post != nil {
					if err := subscriber.Post.After(context.Background(), port.CommittedChange{RequestID: change.RequestID, Principal: change.Principal, Revision: change.BaseRevision, Changes: change.Changes}, &snapshot{}); err != nil {
						t.Fatal(err)
					}
				}
			}
			if calls.Load() != 1 || requests.Load() != 0 {
				t.Fatalf("callbacks=%d, target requests=%d", calls.Load(), requests.Load())
			}
		})
	}
}
