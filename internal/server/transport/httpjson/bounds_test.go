package httpjson_test

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/server/transport/httpjson"
)

func TestBodyLimitIncludesUnknownLengthRequests(t *testing.T) {
	const limit = 1024
	f := newFixture(t, false, false, httpjson.Config{MaxBodyBytes: limit})
	for _, chunked := range []bool{false, true} {
		r := f.request(t, bytes.Repeat([]byte(" "), limit+1))
		if chunked {
			r.ContentLength = -1
			r.TransferEncoding = []string{"chunked"}
		}
		requireCode(t, f.send(t, r), 413, "invalid_request")
	}
	if f.domain.creates.Load() != 0 || f.store.reads.Load() != 0 || f.store.writes.Load() != 0 {
		t.Fatal("oversize body reached domain/store")
	}
	data := message(t, "bead.ready", map[string]any{}, []byte(`1`))
	data = append(data, bytes.Repeat([]byte(" "), limit-len(data))...)
	result(t, f.send(t, f.request(t, data)))
}

func rawRequest(t *testing.T, f *fixture, header, body string, halfClose bool) httpResult {
	t.Helper()
	conn, err := net.DialTimeout("tcp", f.server.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := fmt.Fprintf(conn, "POST /rpc/v1 HTTP/1.1\r\nHost: %s\r\nPlug-RPC: 1\r\nContent-Type: application/json\r\n%s\r\n%s", f.server.Addr(), header, body); err != nil {
		t.Fatal(err)
	}
	if halfClose {
		if err := conn.(*net.TCPConn).CloseWrite(); err != nil {
			t.Fatal(err)
		}
	}
	response, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: http.MethodPost})
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	return httpResult{response.StatusCode, response.Header, data}
}

func TestBodyReadFailureAndDeadlineReachNoStore(t *testing.T) {
	f := newFixture(t, false, false, httpjson.Config{RequestTimeout: 100 * time.Millisecond})
	requireCode(t, rawRequest(t, f, "Content-Length: 32\r\nConnection: close\r\n", "{", true), 400, "invalid_request")
	requireCode(t, rawRequest(t, f, "Content-Length: 32\r\nConnection: close\r\n", "{", false), 408, "busy")
	shorterRead := newFixture(t, false, false, httpjson.Config{ReadTimeout: 80 * time.Millisecond, RequestTimeout: 2 * time.Second})
	started := time.Now()
	requireCode(t, rawRequest(t, shorterRead, "Content-Length: 32\r\nConnection: close\r\n", "{", false), 408, "busy")
	if time.Since(started) > time.Second {
		t.Fatal("request timeout overrode the shorter read deadline")
	}
	if shorterRead.store.reads.Load() != 0 {
		t.Fatal("slow body reached store")
	}

	if f.domain.creates.Load() != 0 || f.store.reads.Load() != 0 || f.store.writes.Load() != 0 {
		t.Fatal("partial/slow body reached domain/store")
	}
}

func TestHeaderLimitIsAppliedByListener(t *testing.T) {
	f := newFixture(t, false, false, httpjson.Config{MaxHeaderBytes: 1024})
	r := f.request(t, []byte(`{}`))
	r.Header.Set("X-Large", strings.Repeat("v", 8192))
	response := f.send(t, r)
	if response.status != http.StatusRequestHeaderFieldsTooLarge {
		t.Fatalf("header status=%d", response.status)
	}
	if f.store.reads.Load() != 0 || f.store.writes.Load() != 0 {
		t.Fatal("oversize header reached store")
	}
}

type blockingValidator struct {
	entered  chan struct{}
	release  chan struct{}
	finished chan struct{}
}

func (v *blockingValidator) Validate(ctx context.Context, _ port.ProposedChange, _ port.SnapshotReader) (port.Verdict, error) {
	close(v.entered)
	defer close(v.finished)
	select {
	case <-v.release:
		return port.Verdict{Allow: true}, nil
	case <-ctx.Done():
		return port.Verdict{}, ctx.Err()
	}
}

func (f *fixture) createRequest(t *testing.T) *http.Request {
	t.Helper()
	return f.request(t, message(t, "bead.create", writeParams(t, map[string]any{"type": "issue", "fields": map[string]any{"title": "bounded work"}}), []byte(`1`)))
}

func roundTrip(client *http.Client, r *http.Request) (httpResult, error) {
	response, err := client.Do(r)
	if err != nil {
		return httpResult{}, err
	}
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	return httpResult{response.StatusCode, response.Header, data}, err
}

func await(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(3 * time.Second):
		t.Fatal("HTTP work did not reach expected boundary")
	}
}

func awaitReaders(t *testing.T, f *fixture) {
	t.Helper()
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for f.store.readers.Load() != 0 {
		select {
		case <-tick.C:
		case <-deadline.C:
			t.Fatal("request cancellation retained readers")
		}
	}
}

func TestConcurrencyAdmissionAndGracefulShutdown(t *testing.T) {
	v := &blockingValidator{make(chan struct{}), make(chan struct{}), make(chan struct{})}
	f := newFixture(t, false, false, httpjson.Config{MaxConcurrentRequests: 1}, pipeline.WithValidators(pipeline.ValidatorBinding{ID: "block", Validator: v}))
	request := f.createRequest(t)
	response := make(chan httpResult, 1)
	failure := make(chan error, 1)
	go func() { r, err := roundTrip(f.client, request); response <- r; failure <- err }()
	await(t, v.entered)
	requireCode(t, f.send(t, f.createRequest(t)), 429, "rate_limited")
	if f.domain.creates.Load() != 1 || f.store.writes.Load() != 0 {
		t.Fatal("saturated request reached Writer")
	}
	drained := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		drained <- f.server.Shutdown(ctx)
	}()
	close(v.release)
	if err := <-failure; err != nil {
		t.Fatal(err)
	}
	result(t, <-response)
	if err := <-drained; err != nil {
		t.Fatal(err)
	}
	awaitReaders(t, f)
	if f.store.writes.Load() != 1 {
		t.Fatal("graceful drain lost or repeated the real commit")
	}
	if _, err := f.client.Do(f.createRequest(t)); err == nil {
		t.Fatal("shutdown listener still accepted requests")
	}
	// HTTP never owns the application resources; they are still usable until
	// the fixture releases its lease and store after all network work exits.
	if _, err := f.store.Meta(context.Background()); err != nil {
		t.Fatal("HTTP shutdown closed application store")
	}
}

func TestRequestDeadlineCancelsActualWriter(t *testing.T) {
	v := &blockingValidator{make(chan struct{}), make(chan struct{}), make(chan struct{})}
	f := newFixture(t, false, false, httpjson.Config{RequestTimeout: 80 * time.Millisecond}, pipeline.WithValidators(pipeline.ValidatorBinding{ID: "deadline", Validator: v}))
	response := f.send(t, f.createRequest(t))
	await(t, v.finished)
	requireCode(t, response, 200, "subscriber_unavailable")
	awaitReaders(t, f)
	if f.store.writes.Load() != 0 {
		t.Fatal("canceled pre subscriber allowed a write")
	}
}

func TestClientCancellationAndForcedShutdownReleaseWork(t *testing.T) {
	for _, force := range []bool{false, true} {
		name := "client cancellation"
		if force {
			name = "forced shutdown"
		}
		t.Run(name, func(t *testing.T) {
			v := &blockingValidator{make(chan struct{}), make(chan struct{}), make(chan struct{})}
			f := newFixture(t, false, false, httpjson.Config{}, pipeline.WithValidators(pipeline.ValidatorBinding{ID: "cancel", Validator: v}))
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			request := f.createRequest(t).WithContext(ctx)
			finished := make(chan error, 1)
			go func() { _, err := roundTrip(f.client, request); finished <- err }()
			await(t, v.entered)
			if force {
				stop, stopCancel := context.WithCancel(context.Background())
				stopCancel()
				if err := f.server.Shutdown(stop); !errors.Is(err, context.Canceled) {
					t.Fatalf("forced shutdown err=%v", err)
				}
			} else {
				cancel()
			}
			await(t, v.finished)
			select {
			case err := <-finished:
				if err == nil {
					t.Fatal("canceled request reported success")
				}
			case <-time.After(3 * time.Second):
				t.Fatal("canceled HTTP client retained work")
			}
			awaitReaders(t, f)
			if f.store.writes.Load() != 0 {
				t.Fatal("canceled request published a write")
			}
		})
	}
}
