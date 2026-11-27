package rpc_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/api/wire"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestAdminRestoreUntypedInternalErrorLogsResponseTrace(t *testing.T) {
	var output bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&output, &slog.HandlerOptions{Level: slog.LevelError})))
	t.Cleanup(func() { slog.SetDefault(previous) })

	registry, err := rpc.NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	const cause = "postgres publication conflict"
	if err := registry.Register("admin.restore.finish", func(ctx context.Context, _ rpc.Call) (any, error) {
		if method := driver.PublicationMethod(ctx); method != "admin.restore.finish" {
			t.Errorf("storage publication method = %q", method)
		}
		return nil, errors.New(cause)
	}); err != nil {
		t.Fatal(err)
	}
	server, err := rpc.New(registry, rpc.Config{Incarnation: "restore-log-test"})
	if err != nil {
		t.Fatal(err)
	}
	raw := server.Dispatch(context.Background(), []byte(`{"jsonrpc":"2.0","id":1,"method":"admin.restore.finish","params":{"restoreId":"restore-log-test","manifestSha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","humanPresence":{"mode":"presence_check","challenge":"test","proof":"test"},"_meta":{"idempotencyKey":"11111111111111111111111111111111"}}}`))

	decoded, err := wire.DecodeJSON(raw)
	if err != nil {
		t.Fatal(err)
	}
	reply := decoded.(map[string]any)
	data := reply["error"].(map[string]any)["data"].(map[string]any)
	if data["code"] != "internal_error" {
		t.Fatalf("unexpected wire error: %s", raw)
	}
	trace, ok := data["traceId"].(string)
	if !ok || trace == "" {
		t.Fatalf("missing response trace: %s", raw)
	}
	if bytes.Contains(raw, []byte(cause)) {
		t.Fatalf("untyped cause leaked over the wire: %s", raw)
	}
	logText := output.String()
	if !strings.Contains(logText, "admin restore internal error") || !strings.Contains(logText, "method=admin.restore.finish") || !strings.Contains(logText, "traceId="+trace) || !strings.Contains(logText, "error=\""+cause+"\"") {
		t.Fatalf("server log did not bind cause to response trace: %s", logText)
	}
}
