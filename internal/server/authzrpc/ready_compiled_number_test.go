package authzrpc

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func seedPersistedReadyPriority(t *testing.T, f *workflowClaimNextFixture, id, priority string) []byte {
	t.Helper()
	createdAt := f.now.UTC().Format(time.RFC3339Nano)
	body := []byte(fmt.Sprintf(`{"id":%q,"type":"issue","project":"project","createdAt":%q,"fields":{"title":"persisted priority","status":"todo","priority":%s}}`, id, createdAt, priority))
	if _, err := conformance.Seed(context.Background(), f.store, f.lease, driver.ChangesetHeader{Time: f.now.UTC()}, []conformance.FixtureChange{{
		Ref: model.RefByID(id, model.Natural64(0)), Kind: "bead", Op: model.OpCreate, Body: body,
	}}); err != nil {
		t.Fatal(err)
	}
	return body
}

func persistedReadyPriority(t *testing.T, f *workflowClaimNextFixture, id string) json.Number {
	t.Helper()
	meta, err := f.store.Meta(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	reader, err := f.store.Read(context.Background(), driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	rows, err := reader.Get(context.Background(), []model.Ref{model.RefByID(id, model.Natural64(0))})
	if err != nil || len(rows) != 1 {
		t.Fatalf("persisted priority row missing: rows=%+v err=%v", rows, err)
	}
	var document struct {
		Fields map[string]any `json:"fields"`
	}
	decoder := json.NewDecoder(bytes.NewReader(rows[0].Body))
	decoder.UseNumber()
	if err := decoder.Decode(&document); err != nil {
		t.Fatal(err)
	}
	priority, ok := document.Fields["priority"].(json.Number)
	if !ok {
		t.Fatalf("storage reload did not preserve json.Number: %#v", document.Fields["priority"])
	}
	return priority
}

func signedReadyPayload(t *testing.T, token string) (int, string) {
	t.Helper()
	raw, err := base64.RawURLEncoding.DecodeString(strings.Split(token, ".")[0])
	if err != nil {
		t.Fatal(err)
	}
	var payload struct {
		Version         int    `json:"version"`
		ReadyMembership string `json:"readyMembership"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatal(err)
	}
	return payload.Version, payload.ReadyMembership
}

func TestCompiledReadyUsesPersistedIntegralJSONPriorityNumbers(t *testing.T) {
	f := newWorkflowCursorFixture(t)
	ids := []string{
		"00000000-0000-4000-8000-000000000101",
		"00000000-0000-4000-8000-000000000102",
	}
	raw := []struct {
		id       string
		priority string
	}{
		{ids[0], "2.00"},
		{ids[1], "40e-1"},
	}
	for _, item := range raw {
		seedPersistedReadyPriority(t, f, item.id, item.priority)
		if got := string(persistedReadyPriority(t, f, item.id)); got != item.priority {
			t.Fatalf("storage changed persisted priority spelling: got=%q want=%q", got, item.priority)
		}
	}

	page, err := f.app.Service.Ready(context.Background(), f.by, service.ReadyRequest{Project: "project", Limit: 1})
	if err != nil || len(page.Items) != 1 || page.Next == nil || !strings.HasPrefix(page.ReadyMembership, "v2:") {
		t.Fatalf("guarded finite Ready rejected persisted integral numbers: page=%+v err=%v", page, err)
	}
	if page.Items[0].ID != ids[0] {
		t.Fatalf("numeric Ready order changed: got=%q want=%q", page.Items[0].ID, ids[0])
	}

	signed, err := workflowCursorRead(t, f, "http", nil, 1, workflowContinuation{}, "project")
	if err != nil || len(signed.ids) != 1 || !signed.next.more() {
		t.Fatalf("signed finite Ready rejected persisted integral numbers: page=%+v err=%v", signed, err)
	}
	version, membership := signedReadyPayload(t, signed.next.token)
	if version != 2 || !strings.HasPrefix(membership, "v2:") {
		t.Fatalf("signed Ready did not carry compiled v2 membership: version=%d membership=%q", version, membership)
	}
}

func TestCompiledReadyRefusesInvalidPersistedPriorityBoundaries(t *testing.T) {
	cases := []struct {
		name     string
		priority string
	}{
		{"fractional", "2.5"},
		{"above-p4", "5"},
		{"negative", "-1"},
		{"string", `"2"`},
		{"null", "null"},
		{"huge-exponent", "1e1000000000"},
		{"tiny-exponent", "1e-1000000000"},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newWorkflowCursorFixture(t)
			id := fmt.Sprintf("00000000-0000-4000-8000-%012d", 201+i)
			seedPersistedReadyPriority(t, f, id, tc.priority)
			page, err := f.app.Service.Ready(context.Background(), f.by, service.ReadyRequest{Project: "project", Limit: 1})
			if err == nil {
				t.Fatalf("invalid persisted priority %q was accepted", tc.priority)
			}
			if len(page.Items) != 0 {
				t.Fatalf("invalid persisted priority %q disclosed a partial page: %+v", tc.priority, page)
			}
			var typed *model.Error
			if !errors.As(err, &typed) || typed.Data.Code != "internal_error" {
				t.Fatalf("invalid persisted priority %q returned %v, want internal_error", tc.priority, err)
			}
		})
	}
}
