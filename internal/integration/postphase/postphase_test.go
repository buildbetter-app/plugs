package postphase

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/server/pluginhost"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/pluginapi"
	sdk "github.com/buildbetter-app/plugs/sdk/go"
)

type postCallback func(context.Context, pluginapi.Host, pluginapi.CommittedChange, pluginapi.SnapshotReader) error

// nativePost is a real compiled-in SDK plugin. Keeping the manifest here makes
// the acceptance package exercise the same manifest, host, adapter and writer
// boundaries that a server composition root uses.
func nativePost(alias string, callback postCallback) sdk.Native {
	manifest := pluginapi.PluginManifest{
		ID:        "app.example." + alias,
		Alias:     alias,
		Version:   "1.0.0",
		PluginAPI: pluginapi.APIVersion,
		Runtime:   "native",
		Provides: []pluginapi.Provision{{
			Kind:          "subscriber",
			Name:          "observe",
			Phases:        []pluginapi.Phase{pluginapi.PhasePost},
			Reads:         []string{"fields.title"},
			MaxPointReads: 4,
			MaxQueries:    1,
		}},
		Requires: pluginapi.Requirements{Methods: []string{"tx.apply"}},
	}
	return sdk.Native{
		Instance: "default",
		Manifest: manifest,
		New: func(host pluginapi.Host) (sdk.Registration, error) {
			return sdk.Registration{Posts: map[string]pluginapi.PostSubscriber{
				"observe": sdk.AfterFunc(func(ctx context.Context, change pluginapi.CommittedChange, reader pluginapi.SnapshotReader) error {
					return callback(ctx, host, change, reader)
				}),
			}}, nil
		},
	}
}

type registry struct{ calls atomic.Int32 }

func (*registry) Effect(method string) (pluginhost.Effect, bool) {
	if method == "tx.apply" {
		return pluginhost.Write, true
	}
	return "", false
}

func (r *registry) Call(context.Context, pluginapi.PrincipalRef, string, json.RawMessage) (json.RawMessage, error) {
	r.calls.Add(1)
	return nil, errors.New("unexpected dispatch: post graph writes must be refused before dispatch")
}

type fixture struct {
	ctx       context.Context
	store     *memory.Store
	lease     driver.ActiveLease
	principal model.PrincipalRef
	ids       atomic.Uint64
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	store, err := memory.NewWithOptions(memory.Options{
		StoreID:     "postphase-store",
		Incarnation: "postphase-incarnation",
	})
	if err != nil {
		t.Fatal(err)
	}
	lease, err := store.Lease(ctx, "postphase-owner", time.Hour)
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	t.Cleanup(func() { _ = lease.Release(context.Background()) })
	return &fixture{ctx: ctx, store: store, lease: lease, principal: model.PrincipalRef{Kind: "user", ID: "postphase-test"}}
}

func (f *fixture) host(t *testing.T, natives ...sdk.Native) *pluginhost.Host {
	return f.hostWithTimeout(t, 0, natives...)
}

func (f *fixture) hostWithTimeout(t *testing.T, timeout time.Duration, natives ...sdk.Native) *pluginhost.Host {
	t.Helper()
	bindings := make([]pluginhost.Binding, 0, len(natives))
	for _, native := range natives {
		bindings = append(bindings, pluginhost.Binding{
			ID:         "post-" + native.Manifest.Alias,
			Alias:      native.Manifest.Alias,
			Instance:   native.Instance,
			Subscriber: "observe",
			Phase:      pluginapi.PhasePost,
			Reads:      []string{"fields.title"},
			Timeout:    timeout,
		})
	}
	dispatch := &registry{}
	t.Cleanup(func() {
		if dispatch.calls.Load() != 0 {
			t.Errorf("post graph writes reached registry: calls=%d", dispatch.calls.Load())
		}
	})
	host, err := pluginhost.New(pluginhost.Config{Natives: natives, Bindings: bindings, Dispatcher: dispatch})
	if err != nil {
		t.Fatal(err)
	}
	return host
}

func (f *fixture) writer(host *pluginhost.Host, store driver.Store, lease driver.ActiveLease) *pipeline.Writer {
	options := []pipeline.Option{
		pipeline.WithOntology(service.NewOntology(nil)),
		pipeline.WithOntologyAudit(service.NewOntologyAudit(service.StaticOntologySource(model.RefByID("ontology", "")))),
		pipeline.WithIDGenerator(func() (string, error) { return fmt.Sprintf("pl-postphase-%d", f.ids.Add(1)), nil }),
	}
	if host != nil {
		options = append(options, pipeline.WithPostSubscribers(host.Posts()...))
	}
	return pipeline.New(store, lease, options...)
}

func create(title, key string) model.ChangeSet {
	return model.ChangeSet{Ops: []model.Op{{Kind: model.OpCreate, Create: &model.CreateOp{
		Key: key, Type: "issue", Fields: map[string]any{"title": title},
	}}}}
}

func patch(ref model.Ref, title string) model.ChangeSet {
	return model.ChangeSet{Ops: []model.Op{{Kind: model.OpPatch, Patch: &model.PatchOp{
		Target: ref, Fields: map[string]any{"title": title},
	}}}}
}

func writeMeta(n int) model.WriteMeta {
	return model.WriteMeta{Method: "tx.apply", IdempotencyKey: fmt.Sprintf("%032x", n), Project: "postphase"}
}

func lookupReceipt(t *testing.T, store driver.Store, principal model.PrincipalRef, meta model.WriteMeta) *driver.IdempotencyRecord {
	t.Helper()
	principalKey, err := driver.PrincipalKey(principal)
	if err != nil {
		t.Fatal(err)
	}
	value, err := store.Meta(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	reader, err := store.Read(context.Background(), driver.View{Incarnation: value.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reader.Close() })
	entry, err := reader.IdempotencyByKey(context.Background(), principalKey, "", meta.Method, meta.IdempotencyKey)
	if err != nil {
		t.Fatal(err)
	}
	return entry
}

func head(t *testing.T, store interface {
	Meta(context.Context) (driver.Meta, error)
}) model.Revision {
	t.Helper()
	meta, err := store.Meta(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return meta.Head
}

func hostErrorCode(err error) string {
	var hostErr *pluginhost.Error
	if errors.As(err, &hostErr) {
		return hostErr.Code
	}
	return ""
}

func receipt(t *testing.T, store driver.Store, principal model.PrincipalRef, meta model.WriteMeta) *driver.IdempotencyRecord {
	t.Helper()
	entry := lookupReceipt(t, store, principal, meta)
	if entry == nil {
		t.Fatalf("missing receipt for %s", meta.IdempotencyKey)
	}
	return entry
}

type applyResult struct {
	commit model.Commit
	err    error
}

func applyAsync(ctx context.Context, writer *pipeline.Writer, by model.PrincipalRef, cs model.ChangeSet, meta model.WriteMeta) <-chan applyResult {
	result := make(chan applyResult, 1)
	go func() { commit, err := writer.Apply(ctx, by, cs, meta); result <- applyResult{commit, err} }()
	return result
}

func await[T any](t *testing.T, ctx context.Context, ch <-chan T) T {
	t.Helper()
	select {
	case result := <-ch:
		return result
	case <-ctx.Done():
		t.Fatal("timed out waiting for acceptance boundary")
		var zero T
		return zero
	}
}

func mustApply(t *testing.T, f *fixture, w *pipeline.Writer, cs model.ChangeSet, meta model.WriteMeta) model.Commit {
	t.Helper()
	result, err := w.Apply(f.ctx, f.principal, cs, meta)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func jsonBytes(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func assertJSONEqual(t *testing.T, want, got any) {
	t.Helper()
	left, right := jsonBytes(t, want), jsonBytes(t, got)
	if string(left) != string(right) {
		t.Fatalf("JSON differs: want=%s got=%s", left, right)
	}
}

func modelCode(err error) string {
	var value *model.Error
	if errors.As(err, &value) {
		return string(value.Data.Code)
	}
	return ""
}

func assertPendingError(t *testing.T, err error, meta model.WriteMeta, revision model.Revision) {
	t.Helper()
	var value *model.Error
	if !errors.As(err, &value) || value.Data.Code != "busy" || !value.Data.Retryable {
		t.Fatalf("pending response err=%v", err)
	}
	assertJSONEqual(t, map[string]any{"committedRevision": revision, "idempotencyKey": meta.IdempotencyKey, "responsePending": true}, value.Data.Details)
}

func openReader(t *testing.T, f *fixture) driver.Reader {
	t.Helper()
	meta, err := f.store.Meta(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := f.store.Read(f.ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reader.Close() })
	return reader
}

func assertTitle(raw []byte, want string) error {
	var value struct {
		Fields map[string]any `json:"fields"`
	}
	if err := json.Unmarshal(raw, &value); err != nil {
		return err
	}
	if value.Fields["title"] != want {
		return fmt.Errorf("title=%v want=%s", value.Fields["title"], want)
	}
	return nil
}

var _ pluginhost.Dispatcher = (*registry)(nil)
