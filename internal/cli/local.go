package cli

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/buildbetter-app/plugs/internal/client"
	coreevents "github.com/buildbetter-app/plugs/internal/core/events"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/restore"
	"github.com/buildbetter-app/plugs/internal/core/service"
	serverevents "github.com/buildbetter-app/plugs/internal/server/events"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/server/transport/inprocess"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
)

type StorageConfig struct{ Driver, DSN string }

// StorageFactory is the seam for a persistent SQLite driver. It returns one
// real Store and its cleanup, without selecting a different service or Writer.
type StorageFactory func(context.Context, StorageConfig) (driver.Store, func() error, error)

type localEventProvider struct{ service serverevents.Service }

func (p localEventProvider) List(ctx context.Context, req service.EventProviderRequest) (service.EventPage, error) {
	page, err := p.service.List(ctx, serverevents.Request{
		Principal: req.Principal,
		Filter:    coreevents.Filter{Types: req.Filter.Types, Resources: req.Filter.Resources},
		After:     req.After,
		Limit:     req.Limit,
	})
	return service.EventPage{Events: page.Events, Cursor: page.Cursor}, err
}

func MemoryFactory(ctx context.Context, cfg StorageConfig) (driver.Store, func() error, error) {
	if cfg.Driver != "memory" {
		return nil, nil, fmt.Errorf("storage driver %q is not installed; use --memory for disposable storage", cfg.Driver)
	}
	s, err := (memory.Driver{}).Open(ctx, driver.Config{DSN: cfg.DSN})
	if err != nil {
		return nil, nil, err
	}
	return s, s.Close, nil
}

type LocalConfig struct {
	Project         string
	Agent           string
	Storage         StorageConfig
	Factory         StorageFactory
	Ontology        *service.Ontology
	PipelineOptions []pipeline.Option
	// PresenceVerifier is host-owned D32 admission for admin dump/restore.
	// Leaving it nil deliberately refuses those sensitive methods.
	PresenceVerifier restore.PresenceVerifier
}

type Local struct {
	Client     *client.Client
	server     *rpc.Server
	cancel     context.CancelFunc
	done       chan struct{}
	lease      driver.ActiveLease
	closeStore func() error
	once       sync.Once
	err        error
}

// OpenLocal composes one memory/SQLite-compatible store with the shared core,
// registry, byte transport and client. It never starts a listener or serves UI.
func OpenLocal(ctx context.Context, cfg LocalConfig) (*Local, error) {
	if cfg.Factory == nil {
		cfg.Factory = MemoryFactory
	}
	store, closeStore, err := cfg.Factory(ctx, cfg.Storage)
	if err != nil {
		return nil, err
	}
	if store == nil || closeStore == nil {
		if closeStore != nil {
			_ = closeStore()
		} else if store != nil {
			_ = store.Close()
		}
		return nil, fmt.Errorf("storage factory must return a store and cleanup")
	}
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		_ = closeStore()
		return nil, err
	}
	leaseCtx, leaseCancel := context.WithTimeout(ctx, 3*time.Second)
	lease, err := store.Lease(leaseCtx, "plug-cli-"+hex.EncodeToString(random[:]), driver.LeaseTTL)
	leaseCancel()
	if err != nil {
		_ = closeStore()
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, &model.Error{Data: model.ErrorData{Code: "busy", Retryable: true, Hint: "local storage lease acquisition timed out"}}
		}
		return nil, err
	}
	localCtx, cancel := context.WithCancel(ctx)
	l := &Local{cancel: cancel, done: make(chan struct{}), lease: lease, closeStore: closeStore}
	go l.renew(localCtx)
	fail := func(err error) (*Local, error) { _ = l.Close(); return nil, err }
	if cfg.Ontology == nil {
		cfg.Ontology = service.NewOntology(nil)
	}
	opts := append([]pipeline.Option(nil), cfg.PipelineOptions...)
	opts = append(opts, pipeline.WithOntology(cfg.Ontology))
	writer := pipeline.New(store, lease, opts...)
	var eventKey [32]byte
	if _, err := rand.Read(eventKey[:]); err != nil {
		return fail(err)
	}
	eventCursors, err := serverevents.NewCursorCodec(eventKey[:])
	if err != nil {
		return fail(err)
	}
	eventReader := &coreevents.Reader{
		Log:           serverevents.Log{Store: store},
		Materializer:  serverevents.HistoryMaterializer{Store: store},
		Authorization: coreevents.AllowAll{},
	}
	eventService := serverevents.Service{Reader: eventReader, Cursors: eventCursors}
	svc := service.New(store, lease, writer, service.WithOntology(cfg.Ontology), service.WithEventProvider(localEventProvider{service: eventService}))
	registry, err := rpc.NewRegistry()
	if err != nil {
		return fail(err)
	}
	if err := rpc.BindService(registry, svc); err != nil {
		return fail(err)
	}
	if err := rpc.BindTxApply(registry, svc); err != nil {
		return fail(err)
	}
	if err := rpc.BindAdmin(registry, restore.NewManager(store, lease, cfg.PresenceVerifier)); err != nil {
		return fail(err)
	}
	meta, err := store.Meta(localCtx)
	if err != nil {
		return fail(err)
	}
	server, err := rpc.New(registry, rpc.Config{
		Incarnation: meta.Incarnation, DefaultProject: cfg.Project,
		CurrentIncarnation: func(ctx context.Context) (string, error) {
			current, err := store.Meta(ctx)
			return current.Incarnation, err
		},
	})
	if err != nil {
		return fail(err)
	}
	l.server = server
	l.Client, err = client.New(inprocess.New(server), cfg.Project, cfg.Agent)
	if err != nil {
		return fail(err)
	}
	return l, nil
}

func (l *Local) renew(ctx context.Context) {
	defer close(l.done)
	ticker := time.NewTicker(driver.LeaseRenewEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-l.lease.Lost():
			return
		case <-ticker.C:
			renewCtx, cancel := context.WithTimeout(ctx, time.Second)
			err := l.lease.Renew(renewCtx)
			cancel()
			if err != nil {
				return
			}
		}
	}
}

func (l *Local) Close() error {
	l.once.Do(func() {
		l.cancel()
		<-l.done
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		l.err = errors.Join(l.lease.Release(ctx), l.closeStore())
	})
	return l.err
}
