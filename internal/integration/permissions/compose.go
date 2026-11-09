// Package permissions composes the adopted fixed project/team authorization
// chain with API-key admission and the real loopback HTTP/RPC service.
package permissions

import (
	"context"
	"fmt"
	"reflect"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/plugins/authnapikey"
	"github.com/buildbetter-app/plugs/internal/server/authzrpc"
	"github.com/buildbetter-app/plugs/internal/server/transport/httpjson"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type Config struct {
	Store          driver.Store
	Lease          driver.ActiveLease
	Keys           *authnapikey.Provider
	Facts          authzrpc.FactsSource
	Namespace      authz.Namespace
	Caller         authzrpc.CallerSource
	DefaultProject string
	HTTP           httpjson.Config
	Audit          port.OntologyAudit
	// ClaimPolicy is an explicit host installation. Nil retains the fixed
	// chain's absence of a distinct claim-slot Transition grant.
	ClaimPolicy *authzrpc.ClaimPolicyConfig
}

// New borrows the store, lease and key ring. Keys are provisioned by the host;
// this package creates no public grant/team or credential-management method.
func New(ctx context.Context, config Config) (*authzrpc.Composition, error) {
	if nilValue(config.Store) || nilValue(config.Lease) || config.Keys == nil || config.Facts == nil {
		return nil, fmt.Errorf("permissions: keys, facts, store and active lease are required")
	}
	return authzrpc.New(ctx, authzrpc.Config{
		Store: config.Store, Lease: config.Lease, Provider: config.Keys,
		Facts: config.Facts, Namespace: config.Namespace, Caller: config.Caller,
		DefaultProject: config.DefaultProject, HTTP: config.HTTP, Audit: config.Audit,
		ClaimPolicy: config.ClaimPolicy,
	})
}

func nilValue(value any) bool {
	if value == nil {
		return true
	}
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}
