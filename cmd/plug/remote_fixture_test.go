package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/api/wire"
	"github.com/buildbetter-app/plugs/internal/client"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/plugins/authnapikey"
	"github.com/buildbetter-app/plugs/internal/server/authn"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/server/transport/httpjson"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
)

type remotePost struct {
	calls   atomic.Int64
	started chan struct{}
	release chan struct{}
}

func (p *remotePost) After(ctx context.Context, _ port.CommittedChange, _ port.SnapshotReader) error {
	p.calls.Add(1)
	if p.started != nil {
		select {
		case p.started <- struct{}{}:
		default:
		}
	}
	if p.release != nil {
		select {
		case <-p.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return errors.New("test post subscriber unavailable")
}

type remoteDomain struct {
	rpc.Service
	creates  atomic.Int64
	finished chan struct{}
	once     sync.Once
}

func (d *remoteDomain) CreateWithOutcome(ctx context.Context, by model.PrincipalRef, req service.CreateRequest, meta model.WriteMeta) (port.WriteOutcome, error) {
	d.creates.Add(1)
	defer d.once.Do(func() { close(d.finished) })
	return d.Service.CreateWithOutcome(ctx, by, req, meta)
}

type remoteFixture struct {
	store     *memory.Store
	lease     driver.ActiveLease
	domain    *remoteDomain
	post      *remotePost
	provider  *authnapikey.Provider
	principal model.PrincipalRef
	token     string
	keyID     string
	keyClock  atomic.Int64
	url       string
}

// This composition uses the actual admitted listener, authn provider, RPC
// registry, service, Writer and store. Only the optional post failure is injected.
func newRemoteFixture(t *testing.T, authenticated bool, post *remotePost) *remoteFixture {
	t.Helper()
	f := &remoteFixture{store: memory.New(), post: post}
	t.Cleanup(func() {
		if err := f.store.Close(); err != nil {
			t.Error(err)
		}
	})
	var err error
	f.lease, err = f.store.Lease(context.Background(), "remote-cli-test", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := f.lease.Release(context.Background()); err != nil {
			t.Error(err)
		}
	})
	ctx, cancel := context.WithCancel(context.Background())
	renewed := make(chan struct{})
	go func() {
		defer close(renewed)
		ticker := time.NewTicker(driver.LeaseRenewEvery)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := f.lease.Renew(ctx); err != nil {
					return
				}
			}
		}
	}()
	t.Cleanup(func() { cancel(); <-renewed })
	ontology := service.NewOntology(nil)
	opts := []pipeline.Option{pipeline.WithOntology(ontology)}
	if post != nil {
		opts = append(opts, pipeline.WithPostSubscribers(pipeline.PostBinding{ID: "remote-cli-warning", Subscriber: post}))
	}
	writer := pipeline.New(f.store, f.lease, opts...)
	f.domain = &remoteDomain{Service: service.New(f.store, f.lease, writer, service.WithOntology(ontology)), finished: make(chan struct{})}
	registry, err := rpc.NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	if err := rpc.BindService(registry, f.domain); err != nil {
		t.Fatal(err)
	}
	meta, err := f.store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	authConfig := authn.Config{}
	rpcConfig := rpc.Config{Incarnation: meta.Incarnation, DefaultProject: "remote", OntologyRevision: model.Natural64(1)}
	f.principal = model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}
	if authenticated {
		f.principal = model.PrincipalRef{Kind: "service", ID: "service:remote-cli"}
		f.keyClock.Store(time.Now().UnixNano())
		f.provider = authnapikey.New(authnapikey.Config{Now: func() time.Time { return time.Unix(0, f.keyClock.Load()) }})
		issued, err := f.provider.Issue(f.principal, time.Now().Add(time.Hour))
		if err != nil {
			t.Fatal(err)
		}
		f.token, f.keyID = issued.Token, issued.ID
		authConfig.Provider = f.provider
		rpcConfig.AuthMode = "authenticated"
	}
	authenticator, err := authn.New(authConfig)
	if err != nil {
		t.Fatal(err)
	}
	rpcServer, err := rpc.New(registry, rpcConfig)
	if err != nil {
		t.Fatal(err)
	}
	handler, err := httpjson.New(rpcServer, authenticator, httpjson.Config{})
	if err != nil {
		t.Fatal(err)
	}
	server, err := handler.Listen(ctx, "127.0.0.1:0", nil)
	if err != nil {
		t.Fatal(err)
	}
	f.url = "http://" + server.Addr().String()
	done := make(chan error, 1)
	go func() { done <- server.Serve() }()
	t.Cleanup(func() {
		drain, stop := context.WithTimeout(context.Background(), 3*time.Second)
		defer stop()
		if err := server.Shutdown(drain); err != nil {
			t.Error(err)
		}
		if err := <-done; err != nil && !errors.Is(err, http.ErrServerClosed) {
			t.Error(err)
		}
	})
	return f
}

func (f *remoteFixture) command(ctx context.Context, t *testing.T, args ...string) *exec.Cmd {
	t.Helper()
	cmd := exec.CommandContext(ctx, plugBinary(t), args...)
	cmd.Env = append(os.Environ(), "PLUG_SERVER="+f.url, "PLUG_TOKEN="+f.token)
	return cmd
}

func (f *remoteFixture) client(t *testing.T, project string) *client.Client {
	t.Helper()
	transport, err := client.NewHTTP(client.HTTPConfig{Server: f.url, Token: f.token})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(transport.Close)
	c, err := client.New(transport, project, "remote-cli-test")
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func (f *remoteFixture) reader(t *testing.T) driver.Reader {
	t.Helper()
	meta, err := f.store.Meta(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	r, err := f.store.Read(context.Background(), driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Close() })
	return r
}

func jsonCommands(t *testing.T, commands [][]string) string {
	t.Helper()
	var input strings.Builder
	for _, args := range commands {
		raw, err := json.Marshal(args)
		if err != nil {
			t.Fatal(err)
		}
		input.Write(raw)
		input.WriteByte('\n')
	}
	return input.String()
}

func decodeResults(t *testing.T, output string) []map[string]any {
	t.Helper()
	var results []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(output), "\n") {
		if line == "" {
			continue
		}
		decoded, err := wire.DecodeJSON([]byte(line))
		if err != nil {
			t.Fatal(err)
		}
		object, ok := decoded.(map[string]any)
		if !ok {
			t.Fatal("result is not an object")
		}
		results = append(results, object)
	}
	return results
}
