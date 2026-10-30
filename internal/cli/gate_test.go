package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/api/wire"
	"github.com/buildbetter-app/plugs/internal/client"
)

type gateTransport struct {
	requests []map[string]any
}

func (t *gateTransport) RoundTrip(_ context.Context, request []byte) ([]byte, error) {
	decoded, err := wire.DecodeJSON(request)
	if err != nil {
		return nil, err
	}
	value, ok := decoded.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("request is not an object")
	}
	t.requests = append(t.requests, value)
	id, _ := value["id"].(string)
	return []byte(fmt.Sprintf(`{"jsonrpc":"2.0","id":%q,"result":{"commit":{"revision":7,"results":[{"ref":{"id":"pl-gate"},"revision":7}],"warnings":[]},"state":"resolved","approvals":2}}`, id)), nil
}

func TestGateResultKeepsSessionAliasesOnNestedCommit(t *testing.T) {
	transport := &gateTransport{}
	core, err := client.New(transport, "local", "gate-test")
	if err != nil {
		t.Fatal(err)
	}
	var out, errOut strings.Builder
	s := &Session{Client: core, Out: &out, Err: &errOut, JSON: true}
	if err := s.Run(context.Background(), []string{"gate", "report", "pl-gate", "--state", "resolved"}); err != nil {
		t.Fatal(err)
	}
	if s.LastID != "pl-gate" || s.LastRevision != "7" {
		t.Fatalf("nested gate commit did not update aliases: id=%q revision=%q", s.LastID, s.LastRevision)
	}
	if err := s.Run(context.Background(), []string{"gate", "approve", "pl-gate", "--expect", "@revision"}); err != nil {
		t.Fatal(err)
	}
	if len(transport.requests) != 2 {
		t.Fatalf("requests: %d", len(transport.requests))
	}
	params, ok := transport.requests[1]["params"].(map[string]any)
	if !ok {
		t.Fatalf("second params: %#v", transport.requests[1]["params"])
	}
	expect, ok := params["expect"].([]any)
	if !ok || len(expect) != 1 || expect[0].(map[string]any)["revision"].(json.Number).String() != "7" {
		t.Fatalf("nested commit revision was not reusable: %#v", params["expect"])
	}
	if out.Len() == 0 || errOut.Len() != 0 {
		t.Fatalf("output: stdout=%q stderr=%q", out.String(), errOut.String())
	}
}

func TestGateReportUsesAdmittedHTTPRPC(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.Method != http.MethodPost || r.URL.Path != "/rpc/v1" || r.Header.Get("Plug-RPC") != "1" {
			t.Errorf("HTTP admission: method=%s path=%s plug-rpc=%q", r.Method, r.URL.Path, r.Header.Get("Plug-RPC"))
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("request body: %v", err)
		}
		decoded, err := wire.DecodeJSON(body)
		if err != nil {
			t.Errorf("request JSON: %v", err)
			return
		}
		request, ok := decoded.(map[string]any)
		if !ok || request["method"] != "gate.report" {
			t.Errorf("request method: %#v", decoded)
		}
		params, ok := request["params"].(map[string]any)
		if !ok || params["state"] != "resolved" || params["id"].(map[string]any)["id"] != "pl-gate" {
			t.Errorf("request params: %#v", params)
		}
		if _, ok := params["_meta"].(map[string]any); !ok {
			t.Errorf("request metadata missing: %#v", params)
		}
		id, _ := request["id"].(string)
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%q,"result":{"commit":{"revision":3,"results":[{"ref":{"id":"pl-gate"},"revision":3}],"warnings":[]},"state":"resolved","approvals":1}}`, id)
	}))
	defer server.Close()

	var out, errOut bytes.Buffer
	code := Execute(context.Background(), []string{
		"--server", server.URL, "--project", "gates", "--json", "gate", "report", "pl-gate",
		"--state", "resolved", "--details", `{"resolver":"timer"}`,
	}, strings.NewReader(""), &out, &errOut)
	if code != 0 || requests != 1 || errOut.Len() != 0 || !strings.Contains(out.String(), `"state":"resolved"`) {
		t.Fatalf("gate HTTP adapter: code=%d requests=%d stdout=%q stderr=%q", code, requests, out.String(), errOut.String())
	}
}

func TestParseGateApproveUsesCatalogShape(t *testing.T) {
	command, err := ParseCommand([]string{"gate", "approve", "pl-gate", "--expect", "18446744073709551616", "--idempotency-key", "approval-key"})
	if err != nil {
		t.Fatal(err)
	}
	if command.Method != "gate.approve" || command.Key != "approval-key" {
		t.Fatalf("command: %+v", command)
	}
	raw, err := json.Marshal(command.Params)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"expect":[{"ref":{"id":"pl-gate"},"revision":18446744073709551616}],"id":{"id":"pl-gate"}}`
	if string(raw) != want {
		t.Fatalf("got %s, want %s", raw, want)
	}
}

func TestParseGateReportPreservesDetailsAndState(t *testing.T) {
	command, err := ParseCommand([]string{"gate", "report", "pl-gate", "--state", "resolved", "--details", `{"provider":"timer","attempt":2}`})
	if err != nil {
		t.Fatal(err)
	}
	if command.Method != "gate.report" {
		t.Fatalf("method: %s", command.Method)
	}
	raw, err := json.Marshal(command.Params)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"details":{"attempt":2,"provider":"timer"},"id":{"id":"pl-gate"},"state":"resolved"}`
	if string(raw) != want {
		t.Fatalf("got %s, want %s", raw, want)
	}
}

func TestParseGateRejectsAmbiguousInput(t *testing.T) {
	cases := [][]string{
		{"gate"},
		{"gate", "check", "pl-gate"},
		{"gate", "approve"},
		{"gate", "approve", "pl-gate", "extra"},
		{"gate", "report", "pl-gate"},
		{"gate", "report", "pl-gate", "--state", " "},
		{"gate", "approve", "pl-gate", "--expect", ""},
		{"gate", "report", "pl-gate", "--state", "resolved", "--details", ""},
		{"gate", "report", "pl-gate", "--state", "resolved", "--details", "[]"},
		{"gate", "report", "pl-gate", "--state", "resolved", "--expect", "-1"},
	}
	for _, args := range cases {
		t.Run(strings.Join(args, "/"), func(t *testing.T) {
			if _, err := ParseCommand(args); err == nil {
				t.Fatalf("accepted %q", args)
			}
		})
	}
}
