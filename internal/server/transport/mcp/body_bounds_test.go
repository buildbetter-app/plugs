package mcp_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/server/authn"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/server/transport/mcp"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type bodyBoundsCreate struct {
	principal model.PrincipalRef
	request   service.CreateRequest
	meta      model.WriteMeta
}

// Count entry into the real service, including calls that might fail before
// reaching the Writer or committing. The existing post subscriber counts commits.
type bodyBoundsService struct {
	rpc.Service
	creates atomic.Int64
	calls   chan bodyBoundsCreate
}

func (s *bodyBoundsService) CreateWithOutcome(ctx context.Context, by model.PrincipalRef, req service.CreateRequest, meta model.WriteMeta) (port.WriteOutcome, error) {
	s.creates.Add(1)
	s.calls <- bodyBoundsCreate{by, req, meta}
	return s.Service.CreateWithOutcome(ctx, by, req, meta)
}

type bodyBoundsReader struct {
	io.ReadCloser
	bytes int64
}

func (r *bodyBoundsReader) Read(p []byte) (int, error) {
	n, err := r.ReadCloser.Read(p)
	r.bytes += int64(n)
	return n, err
}

type bodyBoundsObservation struct {
	length   int64
	encoding []string
	read     int64
}

func TestMCPBodyBoundsBeforeProductDispatch(t *testing.T) {
	for _, size := range []struct {
		name       string
		configured int64
		limit      int
	}{
		{"default", 0, int(mcp.DefaultMaxBodyBytes)},
		{"configured", 1024, 1024},
	} {
		for _, streamed := range []bool{false, true} {
			framing := "known_length"
			if streamed {
				framing = "streamed"
			}
			t.Run(size.name+"/"+framing, func(t *testing.T) {
				f := newFixture(t)
				ontology := service.NewOntology(nil)
				writer := pipeline.New(f.store, f.lease, pipeline.WithOntology(ontology), pipeline.WithPostSubscribers(pipeline.PostBinding{ID: "mcp-warning", Subscriber: f.post}))
				domain := &bodyBoundsService{
					Service: service.New(f.store, f.lease, writer, service.WithOntology(ontology)),
					calls:   make(chan bodyBoundsCreate, 3),
				}
				registry, err := rpc.NewRegistry()
				if err != nil {
					t.Fatal(err)
				}
				if err := rpc.BindService(registry, domain); err != nil {
					t.Fatal(err)
				}
				initial := snapshotBodyBounds(t, f)
				canonical, err := rpc.New(registry, rpc.Config{Incarnation: initial.meta.Incarnation, DefaultProject: "mcp-test", AuthMode: "authenticated"})
				if err != nil {
					t.Fatal(err)
				}
				principal := model.PrincipalRef{Kind: "user", ID: "user:body-bounds"}
				const token = "public-body-bounds-fixture"
				authenticator, err := authn.New(authn.Config{Provider: notificationProvider{principals: map[string]model.PrincipalRef{token: principal}}})
				if err != nil {
					t.Fatal(err)
				}
				handler, err := mcp.New(canonical, authenticator, mcp.Config{MaxBodyBytes: size.configured, MaxConcurrentRequests: 1})
				if err != nil {
					t.Fatal(err)
				}
				observations := make(chan bodyBoundsObservation, 1)
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					body := &bodyBoundsReader{ReadCloser: r.Body}
					r.Body = body
					handler.ServeHTTP(w, r)
					if r.Header.Get("MCP-Session-Id") != "" {
						observations <- bodyBoundsObservation{r.ContentLength, r.TransferEncoding, body.bytes}
					}
				}))
				t.Cleanup(func() {
					_ = handler.Close()
					server.Close()
				})
				client := server.Client()
				client.Timeout = mcp.DefaultRequestTimeout
				endpoint := server.URL + mcp.Path
				session := initializeHTTPNotificationSession(t, client, endpoint, token)
				if domain.creates.Load() != 0 || f.post.calls.Load() != 0 || !reflect.DeepEqual(initial, snapshotBodyBounds(t, f)) {
					t.Fatal("session initialization reached product create or mutated the store")
				}

				for index, step := range []struct {
					id, key string
					length  int
				}{
					{"at-limit", "11111111-1111-4111-8111-111111111111", size.limit},
					{"refused", "22222222-2222-4222-8222-222222222222", size.limit + 1},
					{"recovery", "33333333-3333-4333-8333-333333333333", 0},
				} {
					before := snapshotBodyBounds(t, f)
					data := []byte(fmt.Sprintf(`{"jsonrpc":"2.0","id":%q,"method":"tools/call","params":{"name":"bead_create","arguments":{"type":"issue","fields":{"title":%q},"_meta":{"idempotencyKey":%q}}}}`, step.id, step.id, step.key))
					if step.length > 0 {
						// A complete legal tool call followed by JSON whitespace: the
						// extra byte changes only admission, not operation validity.
						if len(data) > step.length {
							t.Fatal("fixture tool call exceeds its legal body size")
						}
						data = append(data, bytes.Repeat([]byte(" "), step.length-len(data))...)
					}
					var reader io.Reader = bytes.NewReader(data)
					if streamed {
						// Hide Len from http.NewRequest so net/http sends actual
						// chunked framing without a Content-Length header.
						reader = struct{ io.Reader }{reader}
					}
					response := notificationRequest(t, context.Background(), client, http.MethodPost, endpoint, token, session, reader)
					body, readErr := io.ReadAll(response.Body)
					closeErr := response.Body.Close()
					if readErr != nil || closeErr != nil {
						t.Fatalf("%s response read=%v close=%v", step.id, readErr, closeErr)
					}
					observed := <-observations
					if streamed {
						if observed.length != -1 || !reflect.DeepEqual(observed.encoding, []string{"chunked"}) {
							t.Fatalf("%s did not reach MCP as an unknown-length stream: %+v", step.id, observed)
						}
					} else if observed.length != int64(len(data)) || len(observed.encoding) != 0 {
						t.Fatalf("%s did not reach MCP with its known length: %+v", step.id, observed)
					}
					if response.Header.Get("MCP-Protocol-Version") != mcp.DefaultProtocolVersion {
						t.Fatalf("%s lost MCP protocol header: %v", step.id, response.Header)
					}
					if step.id == "refused" {
						value := decode(t, body)
						if response.StatusCode != http.StatusRequestEntityTooLarge || errorCode(t, body) != "invalid_request" || value["id"] != nil || value["jsonrpc"] != "2.0" || value["result"] != nil {
							t.Fatalf("body refusal status=%d body=%s", response.StatusCode, body)
						}
						if !streamed && observed.read != 0 {
							t.Fatalf("known oversized length read %d body bytes before refusal", observed.read)
						}
						if streamed && observed.read != int64(size.limit+1) {
							t.Fatalf("stream refusal read=%d want=%d", observed.read, size.limit+1)
						}
						if domain.creates.Load() != 1 || f.post.calls.Load() != 1 || len(domain.calls) != 0 {
							t.Fatalf("refused body entered product: creates=%d callbacks=%d", domain.creates.Load(), f.post.calls.Load())
						}
						if !reflect.DeepEqual(before, snapshotBodyBounds(t, f)) {
							t.Fatal("refused body mutated metadata, records or committed changesets")
						}
						continue
					}
					value := decode(t, body)
					result, ok := value["result"].(map[string]any)
					if response.StatusCode != http.StatusOK || value["id"] != step.id || value["error"] != nil || !ok || result["isError"] == true {
						t.Fatalf("%s legal create status=%d body=%s", step.id, response.StatusCode, body)
					}
					content, ok := result["content"].([]any)
					if !ok || len(content) != 1 {
						t.Fatalf("%s missing canonical create result: %s", step.id, body)
					}
					text, ok := content[0].(map[string]any)
					if !ok || text["type"] != "text" {
						t.Fatalf("%s invalid tool result: %s", step.id, body)
					}
					var commit model.Commit
					if err := json.Unmarshal([]byte(text["text"].(string)), &commit); err != nil {
						t.Fatal(err)
					}
					if len(commit.Results) != 1 || commit.Results[0].Ref.ID == "" {
						t.Fatalf("%s did not commit exactly one create: %+v", step.id, commit)
					}
					if len(commit.Warnings) != 1 || commit.Warnings[0].Code != "post_subscriber_failed" {
						t.Fatalf("%s lost the actual post callback result: %+v", step.id, commit.Warnings)
					}
					want := int64(1)
					if index == 2 {
						want = 2
					}
					if domain.creates.Load() != want || f.post.calls.Load() != want || len(domain.calls) != 1 || observed.read != int64(len(data)) {
						t.Fatalf("%s creates=%d callbacks=%d queued=%d read=%d", step.id, domain.creates.Load(), f.post.calls.Load(), len(domain.calls), observed.read)
					}
					called := <-domain.calls
					if !reflect.DeepEqual(called.principal, principal) || !reflect.DeepEqual(called.request, service.CreateRequest{Type: "issue", Fields: map[string]any{"title": step.id}}) || called.meta.Method != "bead.create" || called.meta.Project != "mcp-test" || called.meta.IdempotencyKey != step.key || called.meta.DryRun {
						t.Fatalf("%s changed the requested create or identity: %+v", step.id, called)
					}
					after := snapshotBodyBounds(t, f)
					head, err := after.meta.Head.Uint64()
					if err != nil || head != uint64(want) || len(after.records) != int(want) || len(after.changesets) != int(want) {
						t.Fatalf("%s durable head=%v records=%d changesets=%d", step.id, after.meta.Head, len(after.records), len(after.changesets))
					}
					found := false
					for _, record := range after.records {
						if record.Ref.ID != commit.Results[0].Ref.ID {
							continue
						}
						var document pipeline.Document
						if err := json.Unmarshal(record.Body, &document); err != nil {
							t.Fatal(err)
						}
						if document.ID != record.Ref.ID || document.Type != "issue" || document.Project != "mcp-test" || document.Fields["title"] != step.id {
							t.Fatalf("%s committed a different bead: %+v", step.id, document)
						}
						found = true
					}
					if !found {
						t.Fatalf("%s returned a create absent from the store", step.id)
					}
				}
			})
		}
	}
}

type bodyBoundsSnapshot struct {
	meta       driver.Meta
	records    []driver.Record
	changesets []model.Changeset
}

func snapshotBodyBounds(t *testing.T, f *fixture) bodyBoundsSnapshot {
	t.Helper()
	ctx := context.Background()
	meta, err := f.store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := f.store.Read(ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	page, err := reader.Query(ctx, driver.Query{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if page.NextCursor != nil {
		t.Fatal("unexpected additional fixture records")
	}
	changesets, err := reader.Changesets(ctx, nil, 10)
	if err != nil {
		t.Fatal(err)
	}
	return bodyBoundsSnapshot{meta, page.Items, changesets}
}
