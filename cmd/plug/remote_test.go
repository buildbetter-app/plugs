package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os/exec"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/api/wire"
	"github.com/buildbetter-app/plugs/internal/client"
	"github.com/buildbetter-app/plugs/internal/core/model"
)

func runRemote(t *testing.T, f *remoteFixture, input string, args ...string) (string, string, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	args = append([]string{"--project", "remote", "--agent", "remote-cli-test"}, args...)
	cmd := f.command(ctx, t, args...)
	cmd.Stdin = strings.NewReader(input)
	var out, errOut bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errOut
	err := cmd.Run()
	if ctx.Err() != nil {
		t.Fatal("remote CLI timed out")
	}
	return out.String(), errOut.String(), err
}

func TestExecutableAdmittedHTTPLifecycleWarningsNoopsAndReplay(t *testing.T) {
	for _, authenticated := range []bool{false, true} {
		t.Run(map[bool]string{false: "anonymous", true: "bearer"}[authenticated], func(t *testing.T) {
			post := &remotePost{}
			f := newRemoteFixture(t, authenticated, post)
			createKey, claimKey := strings.Repeat("a", 64), strings.Repeat("c", 64)
			commands := [][]string{
				{"ready"},
				{"create", "--title", "Original", "--idempotency-key", createKey},
				{"create", "--title", "Original", "--idempotency-key", createKey},
				{"update", "@last", "--title", "Original"},
				{"claim", "@last", "--idempotency-key", claimKey},
				{"get", "@last"},
				{"claim", "@last"},
				{"get", "@last"},
				{"update", "@last", "--title", "Updated", "--expect", "2"},
				{"comment", "@last", "--body", "  Verbatim remote evidence\n"},
				{"claim", "@last", "--idempotency-key", claimKey},
				{"get", "@last"},
				{"close", "@last", "--reason", "Done", "--expect", "4"},
				{"history", "--as-of", "@revision", "--id", "@last"},
				{"get", "@last", "--as-of", "1"},
				{"ready"},
			}
			out, errOut, err := runRemote(t, f, jsonCommands(t, commands), "--json", "session")
			if err != nil {
				t.Fatalf("remote lifecycle failed: %v; %s", err, errOut)
			}
			if strings.Contains(errOut, "memory storage is disposable") || strings.Count(errOut, "warning [post_subscriber_failed]") != 7 {
				t.Fatalf("remote warning behavior changed: %s", errOut)
			}
			results := decodeResults(t, out)
			if len(results) != len(commands) || len(results[0]["items"].([]any)) != 0 || len(results[15]["items"].([]any)) != 0 {
				t.Fatal("remote readiness/lifecycle result count changed")
			}
			for _, pair := range [][2]int{{1, 1}, {2, 1}, {3, 1}, {4, 2}, {6, 2}, {8, 3}, {9, 4}, {10, 2}, {12, 5}} {
				if results[pair[0]]["revision"] != json.Number(model.Natural64(uint64(pair[1])).String()) {
					t.Fatalf("write/no-op/replay revision changed at command %d", pair[0])
				}
			}
			if post.calls.Load() != 5 || !reflect.DeepEqual(results[1], results[2]) || !reflect.DeepEqual(results[4], results[10]) {
				t.Fatal("replay changed stored result/warnings or repeated post callbacks")
			}
			lease := results[5]["lease"].(map[string]any)
			if lease["holder"] != f.principal.ID || lease["ttlMs"] != json.Number("300000") {
				t.Fatal("HTTP authn did not stamp the durable claim holder/default TTL")
			}
			for _, i := range []int{7, 11} {
				if !reflect.DeepEqual(results[i]["lease"], lease) {
					t.Fatal("same claim/replay changed durable lease")
				}
			}
			if results[14]["bead"].(map[string]any)["fields"].(map[string]any)["title"] != "Original" || results[13]["items"].([]any)[0].(map[string]any)["fields"].(map[string]any)["title"] != "Updated" {
				t.Fatal("remote historical reads substituted current state")
			}
			id := results[1]["results"].([]any)[0].(map[string]any)["ref"].(map[string]any)["id"].(string)
			reader := f.reader(t)
			changes, err := reader.Changesets(context.Background(), nil, 20)
			if err != nil || len(changes) != 5 {
				t.Fatalf("remote writes did not reach append-only storage: count=%d error=%v", len(changes), err)
			}
			for _, change := range changes {
				if !reflect.DeepEqual(change.Principal, f.principal) {
					t.Fatal("client-supplied agent label replaced admitted principal")
				}
			}
			rows, err := reader.Get(context.Background(), []model.Ref{{ID: id}})
			if err != nil || len(rows) != 1 {
				t.Fatal("remote bead missing from actual memory store", err)
			}
			var doc struct{ Comments []struct{ Body string } }
			if err := json.Unmarshal(rows[0].Body, &doc); err != nil || len(doc.Comments) != 2 || doc.Comments[0].Body != "  Verbatim remote evidence\n" || doc.Comments[1].Body != "Done" {
				t.Fatal("remote comment/close text was not persisted verbatim")
			}
			// A new process reads the same remote store; it did not create a new
			// disposable local core for the first invocation.
			fresh, _, err := runRemote(t, f, "", "--json", "get", id)
			if err != nil || decodeResults(t, fresh)[0]["bead"].(map[string]any)["status"] != "done" {
				t.Fatal("new CLI process lost remote state", err)
			}
			// Validate transient replay metadata through the actual HTTP byte
			// client as well as the executable's unchanged stored-result output.
			c := f.client(t, "remote")
			replay, err := c.Call(context.Background(), "bead.create", map[string]any{"type": "issue", "fields": map[string]any{"title": "Original"}}, createKey)
			if err != nil || !replay.Replayed || post.calls.Load() != 5 {
				t.Fatal("HTTP lost replay metadata or repeated the callback", err)
			}
		})
	}
}

func TestExecutableHTTPHumanModeShowsEveryCommittedWarning(t *testing.T) {
	f := newRemoteFixture(t, true, &remotePost{})
	out, errOut, err := runRemote(t, f, "", "--server", f.url+"/rpc/v1", "create", "--title", "Human output")
	if err != nil || !strings.Contains(out, "revision 1") || !strings.Contains(errOut, "warning [post_subscriber_failed]") || !strings.Contains(errOut, "remote-cli-warning") {
		t.Fatalf("human remote warning missing: %v %s %s", err, out, errOut)
	}
}

func TestExecutableHTTPSignedReadyAndPinnedHistoryPaging(t *testing.T) {
	f := newRemoteFixture(t, true, nil)
	commands := [][]string{
		{"create", "--title", "First", "--fields", `{"priority":0}`},
		{"create", "--title", "Second", "--fields", `{"priority":1}`},
		{"create", "--title", "Third", "--fields", `{"priority":2}`},
		{"ready", "--limit", "1"},
	}
	out, errOut, err := runRemote(t, f, jsonCommands(t, commands), "--json", "session")
	if err != nil {
		t.Fatalf("cursor setup: %v %s", err, errOut)
	}
	results := decodeResults(t, out)
	first := results[3]
	cursor := first["nextCursor"].(string)
	if len(strings.Split(cursor, ".")) != 2 || first["items"].([]any)[0].(map[string]any)["fields"].(map[string]any)["title"] != "First" {
		t.Fatal("real HTTP first page/signature missing")
	}
	secondID := results[1]["results"].([]any)[0].(map[string]any)["ref"].(map[string]any)["id"].(string)
	if _, _, err := runRemote(t, f, "", "--json", "close", secondID); err != nil {
		t.Fatal(err)
	}
	out, _, err = runRemote(t, f, "", "--json", "ready", "--limit", "1", "--cursor", cursor)
	if err != nil {
		t.Fatal(err)
	}
	next := decodeResults(t, out)[0]
	if next["nextCursor"] != nil || next["items"].([]any)[0].(map[string]any)["fields"].(map[string]any)["title"] != "Third" || next["asOf"].(map[string]any)["revision"] != json.Number("4") {
		t.Fatal("signed Ready continuation did not observe live data")
	}
	out, _, err = runRemote(t, f, "", "--json", "history", "--as-of", "3", "--limit", "1")
	if err != nil {
		t.Fatal(err)
	}
	history := decodeResults(t, out)[0]
	historyCursor := history["nextCursor"].(string)
	if _, _, err := runRemote(t, f, "", "create", "--title", "Later"); err != nil {
		t.Fatal(err)
	}
	out, _, err = runRemote(t, f, "", "--json", "history", "--as-of", "3", "--limit", "2", "--cursor", historyCursor)
	if err != nil {
		t.Fatal(err)
	}
	historyNext := decodeResults(t, out)[0]
	if historyNext["asOf"].(map[string]any)["revision"] != json.Number("3") || len(historyNext["items"].([]any)) != 2 || historyNext["nextCursor"] != nil {
		t.Fatal("history cursor moved the pinned snapshot")
	}
	for _, item := range historyNext["items"].([]any) {
		if item.(map[string]any)["status"] != "todo" {
			t.Fatal("history continuation returned a post-snapshot close")
		}
	}
	for _, args := range [][]string{
		{"ready", "--limit", "1", "--cursor", cursor + "x"},
		{"--project", "other", "ready", "--limit", "1", "--cursor", cursor},
		{"ready", "--type", "issue", "--limit", "1", "--cursor", cursor},
		{"history", "--as-of", "3", "--limit", "1", "--cursor", cursor},
		{"history", "--as-of", "4", "--limit", "1", "--cursor", historyCursor},
	} {
		out, errOut, err := runRemote(t, f, "", args...)
		if err == nil || out != "" || !strings.Contains(errOut, "cursor_invalid") {
			t.Fatalf("changed/tampered cursor was accepted: %v %s", args[:1], errOut)
		}
	}
	other, err := f.provider.Issue(model.PrincipalRef{Kind: "service", ID: "service:other-cli"}, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	transport, err := client.NewHTTP(client.HTTPConfig{Server: f.url, Token: other.Token})
	if err != nil {
		t.Fatal(err)
	}
	defer transport.Close()
	c, _ := client.New(transport, "remote", "remote-cli-test")
	_, err = c.Call(context.Background(), "bead.ready", map[string]any{"limit": 1, "cursor": cursor}, "")
	var remote *wire.Error
	if !errors.As(err, &remote) || remote.Data.Code != "cursor_invalid" {
		t.Fatal("signed cursor was not bound to admitted bearer principal")
	}
}

func TestExecutableHTTPAdmissionFailureDoesNotWriteOrExposeCredential(t *testing.T) {
	for _, mode := range []string{"missing", "unknown", "revoked", "expired"} {
		t.Run(mode, func(t *testing.T) {
			f := newRemoteFixture(t, true, &remotePost{})
			original := f.token
			code := "unauthenticated"
			switch mode {
			case "missing":
				f.token = ""
			case "unknown":
				f.token = "unknown-credential"
			case "revoked":
				if !f.provider.Revoke(f.keyID) {
					t.Fatal("test key revocation failed")
				}
			case "expired":
				f.keyClock.Add(int64(2 * time.Hour))
				code = "token_expired"
			}
			out, errOut, err := runRemote(t, f, "", "--memory", "--json", "create", "--title", "Must not commit")
			if err == nil || out != "" || !strings.Contains(errOut, code) || strings.Contains(errOut, original) || strings.Contains(errOut, "unknown-credential") || strings.Contains(errOut, "memory storage is disposable") {
				t.Fatal("admission failure lost typed identity, fell back or exposed a credential")
			}
			meta, err := f.store.Meta(context.Background())
			if err != nil || meta.Head.String() != "0" || f.domain.creates.Load() != 0 || f.post.calls.Load() != 0 {
				t.Fatal("rejected HTTP credential reached service/Writer/store")
			}
		})
	}
}

func TestExecutableHTTPCanceledCommittedWriteKeepsKeyForExplicitReplay(t *testing.T) {
	post := &remotePost{started: make(chan struct{}, 1), release: make(chan struct{})}
	f := newRemoteFixture(t, true, post)
	var release sync.Once
	t.Cleanup(func() { release.Do(func() { close(post.release) }) })
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	cmd := f.command(ctx, t, "--project", "remote", "--agent", "remote-cli-test", "--json", "create", "--title", "Committed before cancellation")
	var out, errOut bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errOut
	cmd.Stdin = strings.NewReader("")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case <-post.started: // The real Writer committed; its response is still held.
	case <-time.After(2 * time.Second):
		_ = cmd.Process.Kill()
		<-done
		t.Fatal("real remote write never reached the committed post phase")
	}
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		_ = cmd.Process.Kill()
		<-done
		t.Fatal(err)
	}
	select {
	case err := <-done:
		var exit *exec.ExitError
		if !errors.As(err, &exit) || exit.ExitCode() != 1 || out.Len() != 0 || !strings.Contains(errOut.String(), "context canceled") {
			t.Fatal("remote in-flight cancellation did not stop the CLI with no result")
		}
	case <-time.After(2 * time.Second):
		_ = cmd.Process.Kill()
		<-done
		t.Fatal("HTTP call prevented CLI signal cancellation")
	}
	match := regexp.MustCompile(`write idempotency key: ([0-9a-f]{64})`).FindStringSubmatch(errOut.String())
	if len(match) != 2 {
		t.Fatal("lost HTTP response did not retain its generated idempotency key")
	}
	key := match[1]
	if raw, err := hex.DecodeString(key); err != nil || len(raw) != 32 {
		t.Fatal("lost-response key did not retain random 256-bit material")
	}
	release.Do(func() { close(post.release) })
	select {
	case <-f.domain.finished:
	case <-time.After(2 * time.Second):
		t.Fatal("committed HTTP result was not durably finalized after disconnect")
	}
	meta, err := f.store.Meta(context.Background())
	if err != nil || meta.Head.String() != "1" || f.domain.creates.Load() != 1 || post.calls.Load() != 1 {
		t.Fatal("HTTP cancellation retried or lost the committed write")
	}
	// Replay through a new real executable using precisely the printed key.
	replayed, replayErr, err := runRemote(t, f, "", "--json", "create", "--title", "Committed before cancellation", "--idempotency-key", key)
	if err != nil || !strings.Contains(replayErr, "warning [post_subscriber_failed]") {
		t.Fatalf("explicit lost-response replay failed: %v %s", err, replayErr)
	}
	result := decodeResults(t, replayed)[0]
	if result["revision"] != json.Number("1") || post.calls.Load() != 1 {
		t.Fatal("explicit retry duplicated the write or its post callback")
	}
	c := f.client(t, "remote")
	r, err := c.Call(context.Background(), "bead.create", map[string]any{"type": "issue", "fields": map[string]any{"title": "Committed before cancellation"}}, key)
	if err != nil || !r.Replayed || string(r.JSON) != strings.TrimSpace(replayed) {
		t.Fatal("lost-response replay changed stored JSON/warnings or lost transient metadata")
	}
	outString, errorString, err := runRemote(t, f, "", "--json", "create", "--title", "Changed operation", "--idempotency-key", key)
	if err == nil || outString != "" || !strings.Contains(errorString, "idempotency_key_reused") {
		t.Fatal("a changed operation reused the committed key")
	}
	changes, err := f.reader(t).Changesets(context.Background(), nil, 10)
	if err != nil || len(changes) != 1 || changes[0].IdempotencyKey != key || !reflect.DeepEqual(changes[0].Principal, f.principal) {
		t.Fatal("lost/replayed response did not preserve one admitted durable changeset")
	}
}
