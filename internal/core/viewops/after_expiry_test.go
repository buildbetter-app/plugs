package viewops

import (
	"context"
	"errors"
	"net"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type afterExpiryStore struct {
	*syntheticStore
	queryError error
	queries    int
	closes     int
}

func (s *afterExpiryStore) Read(ctx context.Context, view driver.View) (driver.Reader, error) {
	r, err := s.syntheticStore.Read(ctx, view)
	if err != nil {
		return nil, err
	}
	return &afterExpiryReader{Reader: r, store: s}, nil
}

type afterExpiryReader struct {
	driver.Reader
	store *afterExpiryStore
}

func (r *afterExpiryReader) Query(ctx context.Context, _ driver.Query) (driver.Page[driver.Record], error) {
	r.store.queries++
	// Expire the caller's real deadline after admission, while Query is in
	// flight. Returning a partial page must not make a failed Run successful.
	<-ctx.Done()
	return driver.Page[driver.Record]{
		AsOf: *r.Snapshot().At,
		Items: []driver.Record{{
			Ref:      model.RefByID("partial", model.Natural64(0)),
			Kind:     "bead",
			Revision: model.Natural64(1),
			Body:     []byte(`{"id":"partial","type":"issue","fields":{}}`),
		}},
	}, r.store.queryError
}

func (r *afterExpiryReader) Close() error {
	r.store.closes++
	return r.Reader.Close()
}

func TestRunAfterExpiryPreservesIndependentQueryFailures(t *testing.T) {
	independent := errors.New("independent query failure")
	mixed := errors.Join(context.DeadlineExceeded, independent)
	rawTimeout := &net.OpError{Op: "read", Net: "tcp", Err: os.ErrDeadlineExceeded}
	for _, countOnly := range []bool{false, true} {
		name := "items"
		if countOnly {
			name = "count"
		}
		t.Run(name, func(t *testing.T) {
			for _, tc := range []struct {
				name string
				err  error
				busy bool
			}{
				{name: "pure_deadline", err: context.DeadlineExceeded, busy: true},
				{name: "joined_independent_failure", err: mixed},
				{name: "raw_network_timeout", err: rawTimeout},
			} {
				t.Run(tc.name, func(t *testing.T) {
					before := driver.Meta{StoreID: "store", Incarnation: "inc", Head: model.Natural64(1)}
					store := &afterExpiryStore{syntheticStore: &syntheticStore{meta: before}, queryError: tc.err}
					svc, err := New(store, &testLease{lost: make(chan struct{})}, Config{})
					if err != nil {
						t.Fatal(err)
					}
					ctx, cancel := context.WithTimeout(context.Background(), time.Second)
					defer cancel()
					result, err := svc.Run(ctx, model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}, RunRequest{
						CountOnly: countOnly, Definition: &Definition{Filter: &Filter{}},
					})
					if !errors.Is(ctx.Err(), context.DeadlineExceeded) || store.queries != 1 || store.closes != 1 {
						t.Fatalf("must reach and close one in-flight Query after expiry: ctx=%v queries=%d closes=%d", ctx.Err(), store.queries, store.closes)
					}
					if !reflect.DeepEqual(result, Result{}) || !reflect.DeepEqual(store.meta, before) {
						t.Fatalf("failed Query exposed a partial result or changed metadata: result=%+v meta=%+v", result, store.meta)
					}
					if tc.busy {
						var typed *model.Error
						if !errors.As(err, &typed) || typed.Data.Code != "busy" || !typed.Data.Retryable || typed.Data.Hint != "view read was canceled or exceeded its deadline" {
							t.Fatalf("pure expired request lost its busy mapping: %v", err)
						}
					} else if err != tc.err || !errors.Is(err, tc.err) {
						t.Fatalf("request expiry erased the Query cause: got=%v want=%v", err, tc.err)
					}
					if tc.err == mixed && (!errors.Is(err, independent) || !errors.Is(err, context.DeadlineExceeded)) {
						t.Fatalf("joined error lost a branch: %v", err)
					}
				})
			}
		})
	}
}
