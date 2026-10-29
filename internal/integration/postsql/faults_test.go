package postsql_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	coreevents "github.com/buildbetter-app/plugs/internal/core/events"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// These triggers reject actual SQL publications. They neither synthesize a
// receipt/checkpoint nor replace a driver transaction with a fixture outcome.
func rejectSQLUpdate(t *testing.T, f *fixture, boundary string) func() {
	t.Helper()
	var table string
	switch boundary {
	case "response":
		table = "replays"
	case "checkpoint":
		table = "checkpoints"
	default:
		t.Fatal("unknown SQL fault boundary")
	}
	create := "CREATE TRIGGER post_sql_reject BEFORE UPDATE ON " + table + " BEGIN SELECT RAISE(ABORT,'post_sql_rejected'); END;"
	drop := "DROP TRIGGER post_sql_reject;"
	if f.engine.name == "postgres" {
		if boundary == "response" {
			table = "receipts"
		}
		create = `CREATE FUNCTION plug_postgres.post_sql_reject() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'post_sql_rejected'; RETURN NEW; END $$;
CREATE TRIGGER post_sql_reject BEFORE UPDATE ON plug_postgres.` + table + ` FOR EACH ROW EXECUTE FUNCTION plug_postgres.post_sql_reject();`
		drop = "DROP TRIGGER post_sql_reject ON plug_postgres." + table + "; DROP FUNCTION plug_postgres.post_sql_reject();"
	}
	f.engine.sql(t, create)
	var once sync.Once
	remove := func() { once.Do(func() { f.engine.sql(t, drop) }) }
	t.Cleanup(remove)
	return remove
}

type finalization struct {
	Principal, Agent, Method, Key string
	Revision                      model.Revision
	Warnings                      json.RawMessage
}

type finalizerCapture struct {
	driver.Store
	actual  driver.ResponseFinalizer
	loseAck bool
	calls   []finalization
	errors  []error
}

func (s *finalizerCapture) FinalizeResponse(ctx context.Context, lease driver.ActiveLease, principal, agent, method, key string, revision model.Revision, warnings []model.Warning) error {
	raw, err := json.Marshal(warnings)
	if err != nil {
		return err
	}
	s.calls = append(s.calls, finalization{principal, agent, method, key, revision, raw})
	err = s.actual.FinalizeResponse(ctx, lease, principal, agent, method, key, revision, warnings)
	s.errors = append(s.errors, err)
	if err == nil && s.loseAck {
		return errors.New("injected lost acknowledgement after real SQL finalization")
	}
	return err
}

func (c finalization) finish(ctx context.Context, store driver.Store, lease driver.ActiveLease) error {
	var warnings []model.Warning
	if err := json.Unmarshal(c.Warnings, &warnings); err != nil {
		return err
	}
	return store.(driver.ResponseFinalizer).FinalizeResponse(ctx, lease, c.Principal, c.Agent, c.Method, c.Key, c.Revision, warnings)
}

type pendingObserver struct {
	driver.Store
	driver.ResponseFinalizer
	seen chan struct{}
	once sync.Once
}

func (s *pendingObserver) Read(ctx context.Context, view driver.View) (driver.Reader, error) {
	r, err := s.Store.Read(ctx, view)
	if err != nil {
		return nil, err
	}
	return &observedReader{Reader: r, observer: s}, nil
}

type observedReader struct {
	driver.Reader
	observer *pendingObserver
}

func (r *observedReader) IdempotencyByKey(ctx context.Context, principal, agent, method, key string) (*driver.IdempotencyRecord, error) {
	entry, err := r.Reader.IdempotencyByKey(ctx, principal, agent, method, key)
	if err == nil && entry != nil && entry.ResponsePending {
		r.observer.once.Do(func() { close(r.observer.seen) })
	}
	return entry, err
}

type lostCheckpointAck struct {
	coreevents.Checkpoints
	calls atomic.Int32
}

type checkpointCapture struct {
	coreevents.Checkpoints
	calls int
	err   error
}

func (c *checkpointCapture) Save(ctx context.Context, binding string, generation uint64, state coreevents.State) (uint64, error) {
	c.calls++
	next, err := c.Checkpoints.Save(ctx, binding, generation, state)
	c.err = err
	return next, err
}

func (c *lostCheckpointAck) Save(ctx context.Context, binding string, generation uint64, state coreevents.State) (uint64, error) {
	next, err := c.Checkpoints.Save(ctx, binding, generation, state)
	if c.calls.Add(1) == 1 && err == nil {
		return generation, errors.New("injected lost acknowledgement after real SQL checkpoint")
	}
	return next, err
}

var _ driver.ResponseFinalizer = (*finalizerCapture)(nil)
