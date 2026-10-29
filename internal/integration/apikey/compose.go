// Package apikey composes API-key admission with the real HTTP/RPC/core slice.
package apikey

import (
	"context"
	"fmt"
	"reflect"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/plugins/authnapikey"
	"github.com/buildbetter-app/plugs/internal/server/authn"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/server/transport/httpjson"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// Config is trusted host configuration. The host owns provisioning, the active
// store/lease and the returned handler's listener. This slice has no installed
// authorizer, so authn independently refuses sponsored/scoped keys.
type Config struct {
	Store          driver.Store
	Lease          driver.ActiveLease
	Keys           *authnapikey.Provider
	DefaultProject string
	HTTP           httpjson.Config
}

// New borrows resources; it never opens a store, acquires a lease, provisions
// keys or starts a listener. Close HTTP work before releasing the store/lease.
// Only the existing core-service methods and RPC discovery are bound.
func New(ctx context.Context, config Config) (*httpjson.Handler, error) {
	if config.Keys == nil || nilValue(config.Store) || nilValue(config.Lease) {
		return nil, fmt.Errorf("apikey: keys, store and active lease are required")
	}
	meta, err := config.Store.Meta(ctx)
	if err != nil {
		return nil, err
	}
	ontology := service.NewOntology(nil)
	writer := pipeline.New(config.Store, config.Lease, pipeline.WithOntology(ontology))
	domain := service.New(config.Store, config.Lease, writer, service.WithOntology(ontology))
	registry, err := rpc.NewRegistry()
	if err != nil {
		return nil, err
	}
	if err := rpc.BindService(registry, domain); err != nil {
		return nil, err
	}
	server, err := rpc.New(registry, rpc.Config{
		Incarnation: meta.Incarnation, DefaultProject: config.DefaultProject,
		AuthMode: "authenticated", OntologyRevision: model.Natural64(1),
	})
	if err != nil {
		return nil, err
	}
	admission, err := authn.New(authn.Config{Provider: config.Keys})
	if err != nil {
		return nil, err
	}
	return httpjson.New(server, admission, config.HTTP)
}

func nilValue(value any) bool {
	if value == nil {
		return true
	}
	switch v := reflect.ValueOf(value); v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}
