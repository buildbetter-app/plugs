package b04claimidentity_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/server/pluginbuild"
	"github.com/buildbetter-app/plugs/internal/server/pluginhost"
	"github.com/buildbetter-app/plugs/internal/server/pluginmanifest"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/pluginapi"
)

const a4Binding = "b04-a4-wasm-derive"
const a4Observer = "b04-a4-observer"

func (f *fixture) assignedWork(title, project string, assignment *model.ClaimAssignment) model.Ref {
	f.t.Helper()
	payload := map[string]any{"type": "issue", "fields": map[string]any{"title": title, "description": "typed durable preassignment", "priority": 0}, "assignment": assignment}
	commit := f.request(f.bootstrap, credential{}, "bead.create", project, f.key("assigned-create-"+title), "setup", payload).commit(f.t)
	if len(commit.Results) != 1 {
		f.t.Fatal("assigned create omitted its actual result")
	}
	id := commit.Results[0].Ref
	doc, err := pipeline.DecodeDocument(commit.Results[0].Record)
	if err != nil || !reflect.DeepEqual(doc.Assignment, assignment) || doc.Lease != nil {
		f.t.Fatalf("public create did not retain exact preassignment without a lease: %v", err)
	}
	return id
}

func TestB04TypedPreassignmentPreservesIdentityAndLiteralPoolEligibility(t *testing.T) {
	eachStore(t, false, func(t *testing.T, e engine) {
		f := newFixture(t, e)
		f.claimPools = []string{poolA}
		f.install()
		f.bootstrap = f.anonymousHandler()
		owner := f.issue("agent:worker.primary", "user:pool-a", nil)
		foreign := f.issue("agent:worker__primary", "user:pool-a", nil)
		assigned := f.assignedWork("exact-owner", poolA, &model.ClaimAssignment{Kind: "agent", ID: owner.principal.ID})
		// A similar authenticated ID and an impersonating display label cannot
		// consume another identity's durable assignment, even without a lease.
		f.refuse(foreign, "bead.claim", poolA, f.key("preassigned-foreign"), "worker.primary", target(assigned), "busy")
		key := f.key("preassigned-owner")
		f.remember(owner, "bead.claim", key, "")
		before := f.state()
		first := f.call(owner, "bead.claim", poolA, key, "worker__primary", target(assigned))
		claimed := f.winner(before, owner, "bead.claim", key, "", assigned, first, true)
		stored, err := pipeline.DecodeDocument(f.state().record(t, assigned).Body)
		if err != nil || stored.Assignment != nil {
			t.Fatal("winning claim did not consume preassignment into its exact durable lease owner")
		}
		read := f.request(f.bootstrap, credential{}, "bead.get", poolA, f.key("preassigned-read"), "setup", target(assigned))
		var got struct {
			Bead struct {
				Assignee   string
				Assignment *model.ClaimAssignment
			}
		}
		if read.code != "" || json.Unmarshal(read.envelope["result"], &got) != nil || got.Bead.Assignee != owner.principal.ID || !reflect.DeepEqual(got.Bead.Assignment, &model.ClaimAssignment{Kind: "agent", ID: owner.principal.ID}) {
			t.Fatalf("public hydration lost exact assignee: %s", read.raw)
		}
		noopKey := f.key("preassigned-noop")
		scope := f.remember(owner, "bead.claim", noopKey, "worker--primary")
		before = f.state()
		noop := f.call(owner, "bead.claim", poolA, noopKey, "worker--primary", target(assigned)).commit(t)
		sameClaimState(t, claimed, noop)
		onlyNoopReceipt(t, before, f.state(), scope, noop)

		pool := f.assignedWork("literal-pool", poolA, &model.ClaimAssignment{Kind: "pool", ID: poolA})
		confusable := f.assignedWork("confusable-pool", poolA, &model.ClaimAssignment{Kind: "pool", ID: poolB})
		f.refuse(foreign, "bead.claim", poolA, f.key("preassigned-pool-confusable"), poolA, target(confusable), "busy")
		selector := map[string]any{"filter": map[string]any{"resources": []string{pool.ID, confusable.ID}}}
		key = f.key("preassigned-pool-next")
		f.remember(foreign, "bead.claimNext", key, "")
		before = f.state()
		win := f.call(foreign, "bead.claimNext", poolA, key, poolB, selector)
		f.winner(before, foreign, "bead.claimNext", key, "", pool, win, true)
		before = f.state()
		f.call(foreign, "bead.claimNext", poolA, key, "changed-display", selector).replay(t, win)
		sameState(t, before, f.state())
		// The terminal probe must apply the same exact pool eligibility as the
		// live walker. An unconfigured confusable alias is an ineligible row.
		emptyKey := f.key("preassigned-pool-empty")
		scope = f.remember(foreign, "bead.claimNext", emptyKey, "")
		before = f.state()
		empty := f.call(foreign, "bead.claimNext", poolA, emptyKey, poolA, selector)
		if empty.code != "" || !bytes.Equal(empty.envelope["result"], []byte("null")) {
			t.Fatalf("ineligible pool front is not a committed empty result: %s", empty.raw)
		}
		after := f.state()
		head, _ := before.Meta.Head.Uint64()
		receipt := after.Receipts[scope]
		if after.Meta.Head != model.Natural64(head+1) || len(after.Changesets) != len(before.Changesets)+1 || receipt == nil || receipt.Commit.Revision != after.Meta.Head || len(receipt.Commit.Results) != 0 || receipt.ResponsePending || len(receipt.Changeset.Changes) != 0 || len(receipt.Changeset.Events) != 0 || !reflect.DeepEqual(before.Records, after.Records) || !reflect.DeepEqual(before.Versions, after.Versions) || !reflect.DeepEqual(before.Derived, after.Derived) || !reflect.DeepEqual(before.LeaseOverlays, after.LeaseOverlays) {
			t.Fatal("empty typed-pool result published a bead/history/event or lost its sole final receipt")
		}
		f.remember(foreign, "bead.claimNext", emptyKey, "different-label")
		before = f.state()
		replay := f.call(foreign, "bead.claimNext", poolA, emptyKey, "different-label", selector)
		var replayMeta struct{ Replayed bool }
		_ = json.Unmarshal(replay.envelope["_meta"], &replayMeta)
		if replay.code != "" || !bytes.Equal(replay.envelope["result"], []byte("null")) || !replayMeta.Replayed {
			t.Fatal("typed-pool terminal result did not replay")
		}
		sameState(t, before, f.state())
		crossProject := f.assignedWork("pool-without-project-authority", poolB, &model.ClaimAssignment{Kind: "pool", ID: poolA})
		f.refuse(foreign, "bead.claim", poolB, f.key("preassigned-pool-no-authority"), poolA, target(crossProject), "forbidden")
		for i, bad := range []any{map[string]any{"kind": "future", "id": poolA}, map[string]any{"kind": "pool", "id": ""}, map[string]any{"kind": "pool", "id": poolA, "verified": true}, nil} {
			badKey := f.key(fmt.Sprintf("bad-assignment-%d", i))
			f.remember(credential{}, "bead.create", badKey, "setup")
			before := f.state()
			r := f.request(f.bootstrap, credential{}, "bead.create", poolA, badKey, "setup", map[string]any{"type": "issue", "fields": map[string]any{"title": "invalid-assignment"}, "assignment": bad})
			if r.code != "invalid_params" {
				t.Fatalf("invalid typed assignment was admitted: %s", r.raw)
			}
			sameState(t, before, f.state())
		}
		t.Logf("%s: durable exact actor assignment, foreign-ID refusal, literal configured pool only, pool does not grant project authority, hydrated owner, one winning history and replay", e.name)
	})
}

func TestB04TypedAnonymousPreassignmentRetainsExactUnverifiedLabel(t *testing.T) {
	eachStore(t, false, func(t *testing.T, e engine) {
		f := newFixture(t, e)
		id := f.assignedWork("anonymous-preassignment", poolA, &model.ClaimAssignment{Kind: "anonymous", ID: "worker.primary"})
		for _, label := range []string{"worker__primary", "worker-primary", "worker/primary"} {
			key := f.key("anonymous-preassignment-foreign-" + label)
			f.remember(credential{}, "bead.claim", key, label)
			before := f.state()
			r := f.request(f.bootstrap, credential{}, "bead.claim", poolA, key, label, target(id))
			if r.code != "busy" {
				t.Fatalf("anonymous assignment normalized its unverified label: %s", r.raw)
			}
			sameState(t, before, f.state())
		}
		key := f.key("anonymous-preassignment-win")
		f.remember(credential{}, "bead.claim", key, "worker.primary")
		before := f.state()
		r := f.request(f.bootstrap, credential{}, "bead.claim", poolA, key, "worker.primary", target(id))
		f.winner(before, credential{}, "bead.claim", key, "worker.primary", id, r, false)
		before = f.state()
		f.request(f.bootstrap, credential{}, "bead.claim", poolA, key, "worker.primary", target(id)).replay(t, r)
		sameState(t, before, f.state())
		t.Logf("%s: typed anonymous preassignment is an exact unverified P60 label, never an authenticated actor alias", e.name)
	})
}

// The source is committed and digest-pinned in a disposable checkout on the
// remote runner, then compiled and admitted by the real contained WASI route.
// A callback must read the observer through plug.call and derive a visible
// title from the verified principal, read revision, and proposal snapshot.
const a4GuestSource = `//go:build wasip1
package main
import ("encoding/json"; "fmt"; "unsafe")
var arena [2 << 20]byte
var next uint32 = 1024
var base = uint32(uintptr(unsafe.Pointer(&arena[0])))
//go:wasmimport env plug.call
func snapshotCall(ptr, size uint32) uint64
//go:wasmexport plug_alloc
func allocate(size uint32) uint32 {
 if size == 0 || size > uint32(len(arena))-next { return 0 }
 ptr := base+next; next += size; return ptr
}
func encode(value any) uint64 {
 data,_ := json.Marshal(value); ptr := allocate(uint32(len(data)))
 if ptr==0 { return 0 }; copy(arena[ptr-base:ptr-base+uint32(len(data))],data)
 return uint64(ptr)<<32|uint64(len(data))
}
//go:wasmexport plug_handle
func handle(ptr,size uint32) uint64 {
 var req struct { Op string; Payload struct {
  BaseRevision json.Number
  Principal struct { Kind,ID string; Sponsor struct { ID string } }
  Changes []struct { Resource json.RawMessage; After struct { Fields map[string]any } }
 } }
 if ptr<base || uint64(ptr-base)+uint64(size)>uint64(len(arena)) || json.Unmarshal(arena[ptr-base:ptr-base+size],&req)!=nil || req.Op!="mutate" || len(req.Payload.Changes)!=1 { return encode(map[string]any{"error":"expected one actual mutation"}) }
 p:=req.Payload.Principal
 if p.Kind!="agent" || p.Sponsor.ID!="user:pool-a" { return encode(map[string]any{"error":"verified actor changed"}) }
 read,_:=json.Marshal(map[string]any{"op":"read","method":"get","params":map[string]any{"refs":[]any{map[string]string{"id":"b04-a4-observer"}}}})
 rp:=allocate(uint32(len(read))); if rp==0 {return 0}; copy(arena[rp-base:rp-base+uint32(len(read))],read)
 packed:=snapshotCall(rp,uint32(len(read))); out,n:=uint32(packed>>32),uint32(packed)
 if out<base || uint64(out-base)+uint64(n)>uint64(len(arena)) {return encode(map[string]any{"error":"invalid read response"})}
 var reply struct {Result json.RawMessage; Error string}
 if json.Unmarshal(arena[out-base:out-base+n],&reply)!=nil || reply.Error!="" {return encode(map[string]any{"error":"observer read refused"})}
 var rows []struct {Revision json.Number; Body struct {Fields map[string]any}}
 if json.Unmarshal(reply.Result,&rows)!=nil || len(rows)!=1 || len(rows[0].Body.Fields)!=1 || req.Payload.BaseRevision=="" {return encode(map[string]any{"error":"observer not masked and pinned"})}
 title,ok:=req.Payload.Changes[0].After.Fields["title"].(string); observed,valid:=rows[0].Body.Fields["title"].(string)
 if !ok || !valid {return encode(map[string]any{"error":"title absent"})}
 derived:=fmt.Sprintf("%s|actor=%s|observer=%s|read=%s|base=%s",title,p.ID,observed,rows[0].Revision,req.Payload.BaseRevision)
 return encode(map[string]any{"patches":[]any{map[string]any{"resource":req.Payload.Changes[0].Resource,"paths":map[string]string{"fields.title":derived}}}})
}
func main() {}
`

func a4Artifact(t *testing.T) *pluginbuild.Artifact {
	t.Helper()
	module := os.Getenv("PLUG_SOURCE_MODULE_ROOT")
	if !filepath.IsAbs(module) {
		t.Fatal("remote isolated PLUG_SOURCE_MODULE_ROOT is required")
	}
	root := t.TempDir()
	files := map[string]string{
		"go.mod":           "module fixture.example/b04a4\n\ngo 1.26.5\n\ntoolchain go1.26.10\n",
		"plugin.go":        a4GuestSource,
		"plug-plugin.json": `{"manifest_version":1,"plugins":[{"id":"app.example.b04a4","alias":"b04a4","version":"1.0.0","plugin_api":"1.0","runtime":"wasip1","provides":[{"kind":"subscriber","name":"derive","phases":["mutate"],"reads":["fields.title"],"owns":["fields.title"],"max_point_reads":1}]}]}`,
	}
	for name, data := range files {
		if err := os.WriteFile(filepath.Join(root, name), []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
	}
	git := func(args ...string) string {
		cmd := exec.Command("git", append([]string{"-c", "user.name=B04 source fixture", "-c", "user.email=b04-fixture@invalid.local", "-c", "commit.gpgsign=false"}, args...)...)
		cmd.Dir = root
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("local fixture git %s: %v: %s", args[0], err, out)
		}
		return strings.TrimSpace(string(out))
	}
	git("init", "-q", "--template=", "--initial-branch=main")
	git("add", "--all")
	git("commit", "-q", "-m", "test: pin B04 actual WASI guest")
	checkout := pluginmanifest.Checkout{Root: root, Commit: git("rev-parse", "HEAD")}
	bundle, err := pluginmanifest.Load(context.Background(), checkout)
	if err != nil {
		t.Fatal(err)
	}
	checkout.ExpectedDigest = bundle.Digest()
	compiler, err := exec.LookPath("go")
	if err != nil {
		t.Fatal(err)
	}
	artifact, err := pluginbuild.Build(context.Background(), pluginbuild.Request{Runtime: pluginapi.RuntimeWASI, Source: checkout, ModuleRoot: module, GoBinary: compiler})
	if err != nil {
		t.Fatalf("contained source-built WASI: %v", err)
	}
	t.Cleanup(func() {
		if err := artifact.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := artifact.Verify(context.Background()); err != nil {
		t.Fatal(err)
	}
	wasm, err := os.ReadFile(artifact.Path())
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(wasm)
	if artifact.ArtifactDigest() != hex.EncodeToString(digest[:]) || artifact.SourceDigest() != checkout.ExpectedDigest || artifact.Commit() != checkout.Commit {
		t.Fatal("WASI artifact/source identity drift")
	}
	t.Logf("actual WASI guest commit=%s source=%s manifest=%s artifact=%s", checkout.Commit, artifact.SourceDigest(), artifact.ManifestDigest(), artifact.ArtifactDigest())
	if out := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR"); out != "" {
		if err := os.WriteFile(filepath.Join(out, strings.ReplaceAll(t.Name(), "/", "_")+".wasm"), wasm, 0600); err != nil {
			t.Fatal(err)
		}
	}
	return artifact
}

func (f *fixture) a4Mutators(artifact *pluginbuild.Artifact) []pipeline.MutatorBinding {
	f.t.Helper()
	host, err := artifact.Host(f.ctx, pluginhost.Config{Bindings: []pluginhost.Binding{{ID: a4Binding, Alias: "b04a4", Instance: "default", Subscriber: "derive", Phase: pluginapi.PhaseMutate, Reads: []string{"fields.title"}, Owns: []string{"fields.title"}, MaxPointReads: 1, Timeout: 250 * time.Millisecond}}})
	if err != nil {
		f.t.Fatal(err)
	}
	bindings := host.Mutators()
	if len(bindings) != 1 || bindings[0].ID != a4Binding || bindings[0].FailOpen {
		f.t.Fatal("actual fail-closed WASI binding missing")
	}
	if _, ok := bindings[0].Mutator.(port.TimedMutator); !ok {
		f.t.Fatal("actual host lost execution-pool timing")
	}
	return bindings
}

func (f *fixture) a4Queue(count int) map[string]bool {
	f.t.Helper()
	// Ordinary setup creates the workflow's initial todo state. Retire those
	// two setup beads through the real close slot before adding the A4 queue.
	for _, name := range []string{"a1", "a2"} {
		f.request(f.bootstrap, credential{}, "bead.close", poolA, f.key("a4-close-"+name), "setup", target(f.work[name])).commit(f.t)
	}
	created := time.Now().UTC()
	changes := make([]conformance.FixtureChange, 0, count+1)
	ids := map[string]bool{}
	for i := 0; i <= count; i++ {
		id, status, title := fmt.Sprintf("b04-a4-work-%03d", i), "todo", fmt.Sprintf("work-%03d", i)
		if i == count {
			id, status, title = a4Observer, "done", "observed"
		} else {
			ids[id] = true
		}
		body, err := json.Marshal(pipeline.Document{ID: id, Type: "issue", Project: poolA, CreatedAt: &created, Fields: map[string]any{"title": title, "description": "A4 source fixture", "status": status, "priority": 0}})
		if err != nil {
			f.t.Fatal(err)
		}
		changes = append(changes, conformance.FixtureChange{Ref: ref(id), Kind: "bead", Op: model.OpCreate, Body: body, ChangedPaths: []string{"fields"}, Graph: &conformance.FixtureGraphSource{}})
	}
	if _, err := conformance.Seed(f.ctx, f.store, f.lease, driver.ChangesetHeader{Time: created, Principal: model.PrincipalRef{Kind: "system", ID: "b04-a4-fixture"}}, changes); err != nil {
		f.t.Fatal(err)
	}
	return ids
}

// This is ordinary storage lease maintenance inside the remote test, not a
// Paseo worker/heartbeat. The fixture's original 90-second deadline is retained.
func (f *fixture) a4Renewal() {
	f.t.Helper()
	ctx, stop := context.WithCancel(f.ctx)
	done := make(chan error, 1)
	go func() {
		ticker := time.NewTicker(driver.LeaseRenewEvery)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				done <- nil
				return
			case <-ticker.C:
				if err := f.lease.Renew(ctx); err != nil {
					if ctx.Err() != nil {
						err = nil
					}
					done <- err
					return
				}
			}
		}
	}()
	f.t.Cleanup(func() {
		stop()
		if err := <-done; err != nil {
			f.t.Errorf("A4 host lease renewal: %v", err)
		}
	})
}

// One public network operation with a fixed five-second request deadline.
// The benchmark has no semaphore. Its separately counted bounded retry loop
// retains the Lane E 64-attempt budget and backoffs; each retry uses this same
// deadline, principal, selector and idempotency key.
func a4HTTP(ctx context.Context, client *http.Client, url string, by credential, key string, payload map[string]any) (reply, error) {
	params := map[string]any{"_meta": map[string]any{"project": poolA, "idempotencyKey": key}}
	for k, v := range payload {
		params[k] = v
	}
	body, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": key, "method": "bead.claimNext", "params": params})
	if err != nil {
		return reply{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return reply{}, err
	}
	req.Header.Set("Plug-RPC", "1")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+by.key.Token)
	started := time.Now().UTC()
	resp, err := client.Do(req)
	if err != nil {
		return reply{started: started, finished: time.Now().UTC()}, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	return reply{raw: raw, status: resp.StatusCode, started: started, finished: time.Now().UTC(), retryAfter: resp.Header.Get("Retry-After")}, err
}

// Honor both canonical retryAfterMs and HTTP Retry-After without shortening
// the bounded backoff. A hint that cannot be represented is an error, never
// permission to retry early or enlarge the fixture/request deadlines.
func a4RetryDelay(r reply, milliseconds *model.Natural, backoff time.Duration) (time.Duration, error) {
	if milliseconds != nil {
		n, err := milliseconds.Uint64()
		if err != nil || n > uint64((1<<63-1)/time.Millisecond) {
			return 0, fmt.Errorf("invalid A4 retryAfterMs")
		}
		if wait := time.Duration(n) * time.Millisecond; wait > backoff {
			backoff = wait
		}
	}
	if hint := strings.TrimSpace(r.retryAfter); hint != "" {
		var wait time.Duration
		if seconds, err := strconv.ParseUint(hint, 10, 64); err == nil {
			if seconds > uint64((1<<63-1)/time.Second) {
				return 0, fmt.Errorf("invalid A4 Retry-After seconds")
			}
			wait = time.Duration(seconds) * time.Second
		} else {
			when, err := http.ParseTime(hint)
			if err != nil {
				return 0, fmt.Errorf("invalid A4 Retry-After date")
			}
			wait = time.Until(when)
		}
		if wait > backoff {
			backoff = wait
		}
	}
	return backoff, nil
}

func (f *fixture) a4Winner(before, after durableState, by credential, key string, r reply, observer driver.Record) string {
	f.t.Helper()
	commit := r.commit(f.t)
	if len(commit.Results) != 1 {
		f.t.Fatalf("A4 returned null or non-singular winner: %s", r.raw)
	}
	id := commit.Results[0].Ref.ID
	row := after.record(f.t, ref(id))
	old := before.record(f.t, ref(id))
	doc, err := pipeline.DecodeDocument(row.Body)
	if err != nil {
		f.t.Fatal(err)
	}
	oldDoc, err := pipeline.DecodeDocument(old.Body)
	if err != nil {
		f.t.Fatal(err)
	}
	observed, err := pipeline.DecodeDocument(observer.Body)
	if err != nil {
		f.t.Fatal(err)
	}
	title, _ := doc.Fields["title"].(string)
	prefix := fmt.Sprintf("%s|actor=%s|observer=%s|read=%s|base=", oldDoc.Fields["title"], by.principal.ID, observed.Fields["title"], observer.Revision)
	var base uint64
	if !strings.HasPrefix(title, prefix) {
		f.t.Fatalf("winner lacks actual masked WASI read/actor proof: %q", title)
	}
	if _, err := fmt.Sscan(strings.TrimPrefix(title, prefix), &base); err != nil {
		f.t.Fatal(err)
	}
	head, _ := before.Meta.Head.Uint64()
	revision, _ := commit.Revision.Uint64()
	observedRevision, _ := observer.Revision.Uint64()
	if base < head || base < observedRevision || base >= revision || row.Revision != commit.Revision || commit.Results[0].Revision != row.Revision || doc.Lease == nil || !reflect.DeepEqual(doc.Lease.Principal, by.principal) || doc.Lease.Agent != "" || doc.Lease.TtlMs != model.Natural64(300000) || doc.Lease.Epoch != model.Natural64(f.lease.Epoch()) || doc.Fields["status"] != "in_progress" || doc.StartedAt == nil {
		f.t.Fatalf("A4 winner lost snapshot/claim invariants: %+v", doc)
	}
	var hydrated bytes.Buffer
	if json.Compact(&hydrated, commit.Results[0].Record) != nil || !bytes.Equal(hydrated.Bytes(), row.Body) { // Compare semantic JSON below: encoders may choose whitespace.
		var returned, stored any
		if json.Unmarshal(commit.Results[0].Record, &returned) != nil || json.Unmarshal(row.Body, &stored) != nil || !reflect.DeepEqual(returned, stored) {
			f.t.Fatal("A4 omitted the hydrated durable postimage")
		}
	}
	versions := after.Versions[id]
	if len(versions) != len(before.Versions[id])+1 || !reflect.DeepEqual(versions[:len(versions)-1], before.Versions[id]) || versions[len(versions)-1].Revision != commit.Revision {
		f.t.Fatal("A4 winner lacks one preserved history addition")
	}
	var change *model.Changeset
	for i := range after.Changesets {
		if after.Changesets[i].Revision == commit.Revision {
			change = &after.Changesets[i]
			break
		}
	}
	if change == nil || len(change.Changes) != 1 || change.Changes[0].Ref.ID != id || change.IdempotencyKey != key || !reflect.DeepEqual(change.Principal, by.principal) || !reflect.DeepEqual(change.Chain, []string{"authz/fixed"}) || !reflect.DeepEqual(change.Mutators, []string{a4Binding}) || len(change.Events) != 1 || change.Events[0].Type != model.EventBeadClaimed || change.Events[0].Resource.Ref.ID != id || change.Events[0].Provenance.Method != "bead.claimNext" || change.Events[0].Provenance.Cause.Root != key {
		f.t.Fatalf("A4 winner lost sole event/actual WASI/authz provenance: %+v", change)
	}
	start := doc.Lease.ExpiresAt.Add(-5 * time.Minute)
	if start.Before(r.started) || start.After(r.finished) || change.Time.Before(r.started) || change.Time.After(r.finished) || !versions[len(versions)-1].CreatedAt.Equal(change.Time) {
		f.t.Fatal("A4 history/lease lies outside the actual HTTP request")
	}
	scope := f.remember(by, "bead.claimNext", key, "")
	receipt := after.Receipts[scope]
	if receipt == nil || receipt.ResponsePending || !reflect.DeepEqual(receipt.Commit, commit) || !reflect.DeepEqual(receipt.Changeset, *change) {
		f.t.Fatal("A4 winner receipt differs from the public result/history")
	}
	return id
}

func TestB04A4TwoHundredSimultaneousPublicAgentsWithWASMMutator(t *testing.T) {
	artifact := a4Artifact(t)
	eachStore(t, false, func(t *testing.T, e engine) {
		f := newFixture(t, e)
		f.a4Renewal()
		ids := f.a4Queue(200)
		// A4 explicitly exercises a 200-request host. Use the existing host
		// capacity option; all execution and request deadlines stay unchanged.
		f.httpConfig.MaxConcurrentRequests = 200
		f.installMutators(f.a4Mutators(artifact)...)
		const agents = 200
		callers := make([]credential, agents)
		keys := make([]string, agents)
		principalSet, tokenSet := map[string]bool{}, map[string]bool{}
		for i := range callers {
			callers[i] = f.issue(fmt.Sprintf("agent:b04-a4-%03d", i), "user:pool-a", nil)
			keys[i] = f.key(fmt.Sprintf("a4-worker-%03d", i))
			f.remember(callers[i], "bead.claimNext", keys[i], "")
			principal, _ := driver.PrincipalKey(callers[i].principal)
			if principalSet[principal] || tokenSet[callers[i].key.Token] {
				t.Fatal("A4 identity denominator is not distinct")
			}
			principalSet[principal] = true
			tokenSet[callers[i].key.Token] = true
		}
		before := f.state()
		var arrived atomic.Int64
		var firstArrivals sync.Map
		allArrived := make(chan struct{})
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// All 200 network requests reach this barrier before any claim is
			// dispatched. There is no concurrency cap around the application.
			_, repeated := firstArrivals.LoadOrStore(r.Header.Get("Authorization"), true)
			if !repeated && arrived.Add(1) == agents {
				close(allArrived)
			}
			select {
			case <-allArrived:
				f.app.Handler.ServeHTTP(w, r)
			case <-r.Context().Done():
				return
			}
		}))
		defer server.Close()
		transport := &http.Transport{MaxIdleConns: agents, MaxIdleConnsPerHost: agents}
		defer transport.CloseIdleConnections()
		client := &http.Client{Transport: transport}
		type result struct {
			index     int
			r         reply
			err       error
			attempts  int
			responses []reply
			waits     []time.Duration
		}
		const retryBudget = 64
		results := make(chan result, agents)
		start := make(chan struct{})
		began := time.Now()
		for i := range callers {
			go func(i int) {
				<-start
				out := result{index: i}
				for attempt := 1; attempt <= retryBudget; attempt++ {
					out.attempts++
					out.r, out.err = a4HTTP(f.ctx, client, server.URL+"/rpc/v1", callers[i], keys[i], nil)
					if out.err != nil {
						break
					}
					out.responses = append(out.responses, out.r)
					// This only decides whether to start the next bounded attempt.
					// The main goroutine validates every retained response against
					// the closed error catalog and transport trace-ID contract.
					var envelope struct {
						Error *struct {
							Data struct {
								Code         string
								Retryable    bool
								RetryAfterMS *model.Natural `json:"retryAfterMs"`
							}
						}
					}
					if json.Unmarshal(out.r.raw, &envelope) != nil || envelope.Error == nil || !envelope.Error.Data.Retryable || attempt == retryBudget {
						break
					}
					code := envelope.Error.Data.Code
					switch code {
					case "write_conflict", "busy", "rate_limited", "authn_unavailable", "authz_unavailable":
					default:
						results <- out
						return
					}
					backoff := time.Duration(1+i%4) * time.Millisecond
					if code == "rate_limited" {
						backoff = time.Duration(3+i%7) * time.Millisecond
					}
					backoff, out.err = a4RetryDelay(out.r, envelope.Error.Data.RetryAfterMS, backoff)
					if out.err != nil {
						break
					}
					out.waits = append(out.waits, backoff)
					timer := time.NewTimer(backoff)
					select {
					case <-timer.C:
					case <-f.ctx.Done():
						timer.Stop()
						out.err = f.ctx.Err()
						results <- out
						return
					}
				}
				results <- out
			}(i)
		}
		close(start)
		outcomes := make([]result, agents)
		conflicts, failures, attempts, initialFailures := 0, 0, 0, 0
		initialCodes, allCodes := map[string]int{}, map[string]int{}
		for range agents {
			out := <-results
			if out.attempts < 1 || out.attempts > retryBudget {
				t.Fatal("A4 worker changed the bounded retry budget")
			}
			attempts += out.attempts
			for attempt, r := range out.responses {
				r = f.checkedReply(r)
				if attempt > 0 && (len(out.waits) < attempt || r.started.Sub(out.responses[attempt-1].finished) < out.waits[attempt-1]) {
					t.Fatal("A4 retried before the bounded backoff or advertised retry-after hint")
				}
				out.r = r
				if r.code == "write_conflict" {
					conflicts++
				}
				if r.code != "" {
					allCodes[r.code]++
					if attempt == 0 {
						initialFailures++
						initialCodes[r.code]++
					}
				}
			}
			outcomes[out.index] = out
			if out.err != nil {
				failures++
				t.Logf("A4 worker%d transport failure: %v", out.index, out.err)
				continue
			}
			if len(out.responses) != out.attempts {
				t.Fatal("A4 attempt ledger lost a public response")
			}
			if out.r.code != "" {
				failures++
				t.Logf("A4 worker%d code=%s", out.index, out.r.code)
			}
		}
		t.Logf("A4 engine=%s simultaneous_arrivals=%d initial_attempts=%d initial_failures=%d initial_codes=%v attempts=%d retry_starts=%d all_codes=%v write_conflict=%d final_failures=%d elapsed=%s actual_wasm=%s authz=authz/fixed no_http_semaphore=true retry_budget=%d request_deadline=5s callback_budget=250ms", e.name, arrived.Load(), agents, initialFailures, initialCodes, attempts, attempts-agents, allCodes, conflicts, failures, time.Since(began), artifact.ArtifactDigest(), retryBudget)
		if arrived.Load() != agents || conflicts*100 >= agents || failures != 0 {
			t.Fatalf("A4 200-agent bound failed: arrivals=%d conflicts=%d failures=%d", arrived.Load(), conflicts, failures)
		}
		retryResponses := 0
		for _, n := range allCodes {
			retryResponses += n
		}
		if attempts != agents+retryResponses {
			t.Fatal("A4 attempts do not conserve all typed retry responses and final successes")
		}
		after := f.state()
		head, _ := before.Meta.Head.Uint64()
		if after.Meta.Head != model.Natural64(head+agents) || len(after.Changesets) != len(before.Changesets)+agents || len(after.Records) != len(before.Records) || !reflect.DeepEqual(after.Changesets[:len(before.Changesets)], before.Changesets) {
			t.Fatal("A4 publication census differs from exactly200 wins")
		}
		observer := before.record(t, ref(a4Observer))
		winners := map[string]bool{}
		for i, out := range outcomes {
			id := f.a4Winner(before, after, callers[i], keys[i], out.r, observer)
			if !ids[id] || winners[id] {
				t.Fatalf("A4 duplicated or invented winner%s", id)
			}
			winners[id] = true
		}
		if len(winners) != agents {
			t.Fatal("A4 winner denominator changed")
		}
		for _, row := range before.Records {
			if !ids[row.Ref.ID] && (!reflect.DeepEqual(row, after.record(t, row.Ref)) || !reflect.DeepEqual(before.Versions[row.Ref.ID], after.Versions[row.Ref.ID])) {
				t.Fatalf("A4 changed unrelated row%s", row.Ref.ID)
			}
		}
		if !reflect.DeepEqual(before.Ontology, after.Ontology) || !reflect.DeepEqual(before.Versions[ontologySourceID], after.Versions[ontologySourceID]) {
			t.Fatal("A4 altered ontology authority")
		}
		replayed := f.call(callers[0], "bead.claimNext", poolA, keys[0], "", nil)
		replayed.replay(t, outcomes[0].r)
		sameState(t, after, f.state())
		t.Logf("A4 engine=%s eventual_winners=%d distinct_principals=%d distinct_keys=%d sole_history_event_receipt_per_winner=true head_delta=200 unrelated_rows_conserved=true replay_conserved=true", e.name, len(winners), len(principalSet), len(keys))
	})
}

// Pause only after the real guest has returned its actual masked observer
// read and derived patch. Forward the timed interface and retain the original
// 250ms callback bound, including this controlled pause; no clock is changed.
type a4LaggingMutator struct {
	inner   port.Mutator
	timed   port.TimedMutator
	paused  chan model.Revision
	release chan struct{}
	calls   atomic.Int64
}

func (m *a4LaggingMutator) Mutate(ctx context.Context, p port.ProposedChange, r port.SnapshotReader) ([]port.Patch, error) {
	return m.MutateWithTimeout(ctx, 250*time.Millisecond, p, r)
}

func (m *a4LaggingMutator) MutateWithTimeout(ctx context.Context, timeout time.Duration, p port.ProposedChange, r port.SnapshotReader) ([]port.Patch, error) {
	started := time.Now()
	patches, err := m.timed.MutateWithTimeout(ctx, timeout, p, r)
	if err != nil {
		return nil, err
	}
	if m.calls.Add(1) == 1 {
		remaining := timeout - time.Since(started)
		if remaining <= 0 {
			return nil, context.DeadlineExceeded
		}
		timer := time.NewTimer(remaining)
		defer timer.Stop()
		m.paused <- p.BaseRevision
		select {
		case <-m.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
			return nil, context.DeadlineExceeded
		}
	}
	return patches, nil
}

func TestB04A4LaggingPublicClaimRestartsActualWASMSnapshot(t *testing.T) {
	artifact := a4Artifact(t)
	eachStore(t, false, func(t *testing.T, e engine) {
		f := newFixture(t, e)
		f.a4Renewal()
		ids := f.a4Queue(1)
		bindings := f.a4Mutators(artifact)
		lag := &a4LaggingMutator{inner: bindings[0].Mutator, timed: bindings[0].Mutator.(port.TimedMutator), paused: make(chan model.Revision, 1), release: make(chan struct{})}
		bindings[0].Mutator = lag
		f.installMutators(bindings...)
		by := f.issue("agent:b04-a4-lagging", "user:pool-a", nil)
		key := f.key("a4-lagging")
		f.remember(by, "bead.claimNext", key, "")
		before := f.state()
		server := httptest.NewServer(f.app.Handler)
		defer server.Close()
		type result struct {
			r   reply
			err error
		}
		resultCh := make(chan result, 1)
		go func() {
			r, err := a4HTTP(f.ctx, server.Client(), server.URL+"/rpc/v1", by, key, nil)
			resultCh <- result{r, err}
		}()
		var paused model.Revision
		select {
		case paused = <-lag.paused:
		case out := <-resultCh:
			t.Fatalf("claim completed before actual WASI pause: err=%v raw=%s", out.err, out.r.raw)
		case <-f.ctx.Done():
			t.Fatal(f.ctx.Err())
		}
		if paused != before.Meta.Head {
			close(lag.release)
			t.Fatalf("lagging guest read wrong base: %s want%s", paused, before.Meta.Head)
		}
		// This separate real public write changes exactly the row the guest
		// already read. The claim remains paused with no transaction lock.
		observerWrite := f.request(f.bootstrap, credential{}, "bead.update", poolA, f.key("a4-observer-change"), "setup", map[string]any{"id": ref(a4Observer), "fields": map[string]any{"title": "fresh-observer"}}).commit(t)
		atPause := f.meta().Head
		close(lag.release)
		if atPause != observerWrite.Revision {
			t.Fatalf("paused claim published before restart: head%s observer%s", atPause, observerWrite.Revision)
		}
		out := <-resultCh
		if out.err != nil {
			t.Fatal(out.err)
		}
		r := f.checkedReply(out.r)
		after := f.state()
		head, _ := before.Meta.Head.Uint64()
		if after.Meta.Head != model.Natural64(head+2) || len(after.Changesets) != len(before.Changesets)+2 || lag.calls.Load() < 2 {
			t.Fatalf("lagging snapshot was not restarted exactly once at publication: head%s calls%d", after.Meta.Head, lag.calls.Load())
		}
		observer := after.record(t, ref(a4Observer))
		if observer.Revision != observerWrite.Revision || len(after.Versions[a4Observer]) != len(before.Versions[a4Observer])+1 {
			t.Fatal("competing public observer write/history changed")
		}
		id := f.a4Winner(before, after, by, key, r, observer)
		if !ids[id] {
			t.Fatal("lagging claim invented a winner")
		}
		for _, row := range before.Records {
			if row.Ref.ID != id && row.Ref.ID != a4Observer && (!reflect.DeepEqual(row, after.record(t, row.Ref)) || !reflect.DeepEqual(before.Versions[row.Ref.ID], after.Versions[row.Ref.ID])) {
				t.Fatal("lagging claim changed unrelated state")
			}
		}
		f.call(by, "bead.claimNext", poolA, key, "", nil).replay(t, r)
		sameState(t, after, f.state())
		t.Logf("A4 engine=%s actual_wasm=%s paused_base=%s observer_commit=%s winning_commit=%s actual_guest_calls=%d callback_budget=250ms request_deadline=5s one_winner_history_event_receipt=true", e.name, artifact.ArtifactDigest(), paused, observerWrite.Revision, r.commit(t).Revision, lag.calls.Load())
	})
}

// Every prefix row matches the requested project/type and is genuinely ready.
// Only the installed caller's ref caveat makes it ineligible to win. Unlike a
// selector-excluded prefix, these rows consume the first actual queue page.
func TestB04PublicClaimNextWalksMatchingReadyFrontBeyondOnePage(t *testing.T) {
	eachStore(t, false, func(t *testing.T, e engine) {
		f := newFixture(t, e)
		const prefix = 1000
		created := time.Now().UTC()
		changes := make([]conformance.FixtureChange, 0, prefix+1)
		winner := ref("b04-front-target")
		for i := 0; i <= prefix; i++ {
			id, priority := fmt.Sprintf("b04-front-%04d", i), 0
			if i == prefix {
				id, priority = winner.ID, 1
			}
			body, err := json.Marshal(pipeline.Document{
				ID: id, Type: "issue", Project: poolA, CreatedAt: &created,
				Fields: map[string]any{"title": id, "description": "matching ready front", "status": "todo", "priority": priority},
			})
			if err != nil {
				t.Fatal(err)
			}
			changes = append(changes, conformance.FixtureChange{Ref: ref(id), Kind: "bead", Op: model.OpCreate, Body: body, ChangedPaths: []string{"fields"}, Graph: &conformance.FixtureGraphSource{}})
		}
		if _, err := conformance.Seed(f.ctx, f.store, f.lease,
			driver.ChangesetHeader{Time: created, Principal: model.PrincipalRef{Kind: "system", ID: "b04-ready-front-fixture"}}, changes); err != nil {
			t.Fatal(err)
		}
		by := f.issue("agent:front-tail", "user:pool-a", map[string]any{
			"actions":  []string{string(authz.Modify), "transition"},
			"projects": []model.Ref{ref(poolA)}, "refs": []model.Ref{winner},
		})
		selector := map[string]any{"filter": map[string]any{"types": []string{"issue"}}}
		// Observe the real unguarded queue source through its public read route.
		// The tail has lower priority, so the first page must be full without it.
		ready := f.request(f.bootstrap, credential{}, "bead.ready", poolA, f.key("front-observer"), "front-observer", map[string]any{"limit": 1000, "filter": selector["filter"]})
		var front struct{ Items []struct{ ID string } }
		if ready.code != "" || json.Unmarshal(ready.envelope["result"], &front) != nil || len(front.Items) != 1000 {
			t.Fatalf("matching ready front is not one full source page: %s", ready.raw)
		}
		for _, row := range front.Items {
			if row.ID == winner.ID {
				t.Fatal("tail unexpectedly entered the first matching ready page")
			}
		}
		key := f.key("matching-front-win")
		f.remember(by, "bead.claimNext", key, "front-tail")
		before := f.state()
		if len(before.Records) != prefix+4 {
			t.Fatalf("complete fixture census=%d want=%d", len(before.Records), prefix+4)
		}
		first := f.call(by, "bead.claimNext", poolA, key, "front-tail", selector)
		f.winner(before, by, "bead.claimNext", key, "front-tail", winner, first, true)
		before = f.state()
		f.call(by, "bead.claimNext", poolA, key, "changed-label", selector).replay(t, first)
		sameState(t, before, f.state())
		t.Logf("%s: %d matching ready rows precede the permitted hydrated winner; one version/event/receipt, denied rows conserved", e.name, prefix)
	})
}

// A separate real transaction retains the selected record lock throughout the
// public call. PostgreSQL must use its SKIP LOCKED path; memory enforces the same
// logical contract. SQLite serializes actual writers with BEGIN IMMEDIATE and
// is covered by the three-store matching-front and publication witnesses.
func TestB04PublicClaimNextSkipsHeldCandidateWithoutPublication(t *testing.T) {
	eachStoreNamed(t, []string{"memory", "postgres"}, func(t *testing.T, e engine) {
		f := newFixture(t, e)
		by := f.issue("agent:held-front-worker", "user:pool-a", nil)
		selected, next := f.work["a1"], f.work["a2"]
		// Give the selected row a unique earlier priority, independent of IDs
		// or the request's deterministic equal-key rotation.
		f.request(f.bootstrap, credential{}, "bead.update", poolA, f.key("selected-priority"), "setup", map[string]any{"id": selected, "fields": map[string]any{"priority": 0}}).commit(t)
		ready := f.request(f.bootstrap, credential{}, "bead.ready", poolA, f.key("held-front-observer"), "setup", nil)
		var front struct{ Items []struct{ ID string } }
		if ready.code != "" || json.Unmarshal(ready.envelope["result"], &front) != nil || len(front.Items) != 2 || front.Items[0].ID != selected.ID {
			t.Fatalf("held candidate is not the first actual ready row: %s", ready.raw)
		}
		held, err := f.store.Write(f.ctx, f.lease)
		if err != nil {
			t.Fatal(err)
		}
		defer held.Close()
		states, err := held.Lock(f.ctx, driver.LockSet{{Key: driver.LockKey{Kind: driver.LockRecord, Ref: &selected}, Mode: driver.LockUpdate}})
		if err != nil || len(states) != 1 || !states[0].Present {
			t.Fatalf("actual selected record lock: states=%+v err=%v", states, err)
		}
		key := f.key("held-front-next")
		f.remember(by, "bead.claimNext", key, "held-front")
		before := f.state()
		first := f.call(by, "bead.claimNext", poolA, key, "held-front", nil)
		f.winner(before, by, "bead.claimNext", key, "held-front", next, first, true)
		// The holder still owns its real lock and sees its unchanged preimage.
		rows, err := held.Get(f.ctx, []model.Ref{selected})
		if err != nil || len(rows) != 1 || !bytes.Equal(rows[0].Body, before.record(t, selected).Body) {
			t.Fatalf("skipped held candidate changed: %v", err)
		}
		after := f.state()
		if err := held.Rollback(f.ctx); err != nil {
			t.Fatal(err)
		}
		sameState(t, after, f.state())
		// Releasing the real lock makes that untouched candidate claimable.
		secondKey := f.key("released-front-next")
		f.remember(by, "bead.claimNext", secondKey, "held-front")
		before = f.state()
		second := f.call(by, "bead.claimNext", poolA, secondKey, "held-front", nil)
		f.winner(before, by, "bead.claimNext", secondKey, "held-front", selected, second, true)
		t.Logf("%s: public claimNext passed a real held first candidate, conserved its row/history, and claimed it once after release", e.name)
	})
}

func TestB04PublicClaimNextLockedFrontNeverPublishesNull(t *testing.T) {
	eachStoreNamed(t, []string{"memory", "postgres"}, func(t *testing.T, e engine) {
		f := newFixture(t, e)
		by := f.issue("agent:locked-front-worker", "user:pool-a", nil)
		selected := f.work["a1"]
		held, err := f.store.Write(f.ctx, f.lease)
		if err != nil {
			t.Fatal(err)
		}
		defer held.Close()
		states, err := held.Lock(f.ctx, driver.LockSet{{Key: driver.LockKey{Kind: driver.LockRecord, Ref: &selected}, Mode: driver.LockUpdate}})
		if err != nil || len(states) != 1 || !states[0].Present {
			t.Fatalf("actual selected record lock: states=%+v err=%v", states, err)
		}
		selector := map[string]any{"filter": map[string]any{"resources": []string{selected.ID}}}
		key := f.key("locked-front-refusal")
		f.refuse(by, "bead.claimNext", poolA, key, "locked-front", selector, "write_conflict")
		before := f.state()
		if err := held.Rollback(f.ctx); err != nil {
			t.Fatal(err)
		}
		sameState(t, before, f.state())
		first := f.call(by, "bead.claimNext", poolA, key, "locked-front", selector)
		f.winner(before, by, "bead.claimNext", key, "locked-front", selected, first, true)
		t.Logf("%s: locked nonempty front returns catalog write_conflict without null/receipt/history; released same key wins once", e.name)
	})
}

// The HTTP transport supplies a fresh trace ID after catalog mapping. The
// expected retryability comes from the canonical error catalog, never from a
// caller-provided domain flag. Exercise both public claim endpoints and the
// installed authentication/authorization chain on every real engine.
func TestB04PublicClaimErrorsCarryCatalogRetryabilityAndTraceID(t *testing.T) {
	eachStore(t, false, func(t *testing.T, e engine) {
		f := newFixture(t, e)
		owner := f.issue("agent:trace-owner", "user:pool-a", nil)
		foreign := f.issue("agent:trace-foreign", "user:pool-a", nil)

		// The core service returns a typed domain error before the HTTP
		// transport maps it through the catalog. Its TraceID must stay empty;
		// only the public envelope owns trace generation. Exercise both claim
		// entry points directly so this boundary cannot be inferred from the
		// transport response alone.
		_, rawClaimErr := f.app.Service.ClaimWithOutcome(f.ctx, owner.principal, service.ClaimRequest{}, model.WriteMeta{Method: "bead.claim", Project: poolA})
		assertRawCoreRefusal(t, rawClaimErr, "invalid_params")
		rawNext, rawNextErr := f.app.Service.ClaimNextWithOutcome(f.ctx, owner.principal, service.ClaimNextRequest{}, model.WriteMeta{Method: "bead.claimNext"})
		if rawNext != nil {
			t.Fatalf("raw claimNext refusal returned an outcome: %+v", rawNext)
		}
		assertRawCoreRefusal(t, rawNextErr, "project_required")

		key := f.key("trace-owner-win")
		before := f.state()
		first := f.call(owner, "bead.claim", poolA, key, "trace-owner", target(f.work["a1"]))
		f.winner(before, owner, "bead.claim", key, "trace-owner", f.work["a1"], first, true)

		f.refuse(foreign, "bead.claim", poolA, f.key("trace-busy"), "trace-foreign", target(f.work["a1"]), "busy")
		f.refuse(credential{}, "bead.claim", poolA, f.key("trace-unauthenticated"), "trace-anonymous", target(f.work["a2"]), "unauthenticated")
		f.refuse(owner, "bead.claimNext", poolA, f.key("trace-invalid"), "trace-owner", map[string]any{"limit": 1}, "invalid_params")
		f.refuse(foreign, "bead.claimNext", poolB, f.key("trace-forbidden"), "trace-foreign", nil, "forbidden")
		t.Logf("%s: claim and claimNext HTTP refusals carry catalog retryability and transport trace IDs", e.name)
	})
}

func assertRawCoreRefusal(t *testing.T, err error, code model.ErrorCode) {
	t.Helper()
	if err == nil {
		t.Fatalf("raw core call unexpectedly succeeded; want %s", code)
	}
	var refusal *model.Error
	if !errors.As(err, &refusal) {
		t.Fatalf("raw core refusal is not a model.Error: %T %v", err, err)
	}
	if refusal.Data.Code != code {
		t.Fatalf("raw core refusal code=%q want=%q", refusal.Data.Code, code)
	}
	if refusal.Data.TraceID != "" {
		t.Fatalf("raw core refusal leaked transport trace ID %q", refusal.Data.TraceID)
	}
}

// The identity comes from a real issued bearer. Display spellings do not
// authenticate an alias, and are never normalized into a PrincipalRef ID.
func TestB04AuthenticatedOwnerSpellingsAndForeignIDs(t *testing.T) {
	eachStore(t, false, func(t *testing.T, e engine) {
		f := newFixture(t, e)
		owner := f.issue("agent:worker.primary", "user:pool-a", nil)
		secondKey := f.issue("agent:worker.primary", "user:pool-a", nil)
		id := f.work["a1"]
		key := f.key("owner-win")
		f.remember(owner, "bead.claim", key, "worker.primary")
		before := f.state()
		first := f.call(owner, "bead.claim", poolA, key, "worker.primary", target(id))
		claimed := f.winner(before, owner, "bead.claim", key, "worker.primary", id, first, true)

		for _, spelling := range []string{"worker__primary", "worker-primary", "worker/primary", "worker--primary", ""} {
			key := f.key("noop-" + spelling)
			scope := f.remember(secondKey, "bead.claim", key, spelling)
			before := f.state()
			got := f.call(secondKey, "bead.claim", poolA, key, spelling, target(id)).commit(t)
			sameClaimState(t, claimed, got)
			onlyNoopReceipt(t, before, f.state(), scope, got)
		}
		// The same canonical principal/key replays across display labels rather
		// than creating another receipt namespace or another claim mutation.
		f.remember(secondKey, "bead.claim", key, "worker__primary")
		before = f.state()
		f.call(secondKey, "bead.claim", poolA, key, "worker__primary", target(id)).replay(t, first)
		sameState(t, before, f.state())

		// These are different AUTHENTICATED IDs, including the superficially
		// similar spelling. No separator rewrite can grant another identity
		// the existing lease, even if its client label impersonates the owner.
		for _, foreignID := range []string{"agent:worker__primary", "agent:worker.secondary-3", "agent:worker--primary"} {
			foreign := f.issue(foreignID, "user:pool-a", nil)
			f.refuse(foreign, "bead.claim", poolA, f.key("foreign-"+foreignID), "worker.primary", target(id), "busy")
		}

		// Both the real authentication boundary and the current policy boundary
		// run before a receipt can convey a successful result.
		f.refuse(credential{}, "bead.claim", poolA, f.key("missing-bearer"), "worker.primary", target(id), "unauthenticated")
		otherSponsor := f.issue("agent:worker.primary", "user:pool-b", nil)
		f.refuse(otherSponsor, "bead.claim", poolA, key, "worker.primary", target(id), "forbidden")
		if !f.keys.Revoke(secondKey.key.ID) {
			t.Fatal("test bearer was not revoked")
		}
		f.refuse(secondKey, "bead.claim", poolA, key, "worker.primary", target(id), "unauthenticated")
		if e.name != "memory" {
			f.reopen()
			f.remember(owner, "bead.claim", key, "after-reopen")
			before = f.state()
			f.call(owner, "bead.claim", poolA, key, "after-reopen", target(id)).replay(t, first)
			sameState(t, before, f.state())
			noopKey := f.key("noop-after-reopen")
			scope := f.remember(owner, "bead.claim", noopKey, "worker__primary")
			before = f.state()
			got := f.call(owner, "bead.claim", poolA, noopKey, "worker__primary", target(id)).commit(t)
			sameClaimState(t, claimed, got)
			onlyNoopReceipt(t, before, f.state(), scope, got)
			foreign := f.issue("agent:worker__primary", "user:pool-a", nil)
			f.refuse(foreign, "bead.claim", poolA, f.key("foreign-after-reopen"), "worker.primary", target(id), "busy")
		}
		f.revokePoolA()
		f.refuse(owner, "bead.claim", poolA, key, "worker.primary", target(id), "forbidden")
		f.refuse(owner, "bead.claim", poolA, f.key("new-after-revocation"), "worker__primary", target(id), "forbidden")
		t.Logf("%s: authenticated spelling labels, quiet same-owner receipts, distinct foreign IDs, missing/revoked bearer and current-policy replay refusal", e.name)
	})
}

// This exercises installed team/project/caveat policy for pool work. It is
// intentionally not a replacement for Beads' exact claim.pools registry.
func TestB04InstalledPoolPolicyConfinement(t *testing.T) {
	eachStore(t, false, func(t *testing.T, e engine) {
		f := newFixture(t, e)
		workerA := f.issue("agent:pool-worker", "user:pool-a", nil)
		workerB := f.issue("agent:pool-worker-b", "user:pool-b", nil)
		bounded := f.issue("agent:pool-bounded", "user:pool-a", map[string]any{
			"actions":  []string{string(authz.Modify)},
			"projects": []model.Ref{ref(poolA)},
			"refs":     []model.Ref{f.work["a2"]},
		})
		for _, invalid := range []struct {
			name   string
			params map[string]any
		}{
			{"limit", map[string]any{"limit": 1}},
			{"offset", map[string]any{"offset": 1}},
			{"brief", map[string]any{"brief": true}},
			{"actor", map[string]any{"actor": "agent:pool-worker-b"}},
		} {
			f.refuse(workerA, "bead.claimNext", poolA, f.key("invalid-"+invalid.name), poolA, invalid.params, "invalid_params")
		}
		// The pool names differ only by separators. They are still exact,
		// distinct project bindings with distinct installed team grants.
		f.refuse(workerA, "bead.claim", poolB, f.key("foreign-pool-direct"), poolB, target(f.work["b1"]), "forbidden")
		f.refuse(workerB, "bead.claim", poolA, f.key("reverse-pool-direct"), poolA, target(f.work["a1"]), "forbidden")
		f.refuse(workerA, "bead.claimNext", poolB, f.key("foreign-pool-next"), poolB, nil, "forbidden")
		f.refuse(bounded, "bead.claim", poolA, f.key("outside-ref-caveat"), poolA, target(f.work["a1"]), "forbidden")

		key := f.key("bounded-next")
		f.remember(bounded, "bead.claimNext", key, poolB)
		before := f.state()
		selector := map[string]any{"filter": map[string]any{"resources": []string{f.work["a1"].ID, f.work["a2"].ID}}}
		first := f.call(bounded, "bead.claimNext", poolA, key, poolB, selector)
		f.winner(before, bounded, "bead.claimNext", key, poolB, f.work["a2"], first, true)
		if e.name != "memory" {
			f.reopen()
		}
		f.remember(bounded, "bead.claimNext", key, poolA)
		before = f.state()
		f.call(bounded, "bead.claimNext", poolA, key, poolA, selector).replay(t, first)
		sameState(t, before, f.state())
		f.refuse(bounded, "bead.claimNext", poolA, key, poolA, map[string]any{"filter": map[string]any{"resources": []string{f.work["a1"].ID}}}, "idempotency_key_reused")
		f.refuse(workerA, "bead.claim", poolB, f.key("foreign-after-reopen"), "pool__alpha", target(f.work["b1"]), "forbidden")
		// The other pool has real eligible work: a refusal was a policy result,
		// not an absent/closed row or an already-held lease.
		otherKey := f.key("other-pool-win")
		f.remember(workerB, "bead.claim", otherKey, poolA)
		before = f.state()
		other := f.call(workerB, "bead.claim", poolB, otherKey, poolA, target(f.work["b1"]))
		f.winner(before, workerB, "bead.claim", otherKey, poolA, f.work["b1"], other, true)
		f.revokePoolA()
		f.refuse(bounded, "bead.claimNext", poolA, key, poolA, selector, "forbidden")
		t.Logf("%s: installed exact team/project pool policy and project/ref caveats constrain direct/next winners, label changes and receipt replay", e.name)
	})
}

func TestB04AnonymousLabelsRemainUnverifiedExactScopes(t *testing.T) {
	eachStore(t, false, func(t *testing.T, e engine) {
		f := newFixture(t, e)
		id := f.work["a1"]
		by := credential{}
		key := f.key("anonymous-win")
		f.remember(by, "bead.claim", key, "worker.primary")
		before := f.state()
		first := f.request(f.bootstrap, by, "bead.claim", poolA, key, "worker.primary", target(id))
		claimed := f.winner(before, by, "bead.claim", key, "worker.primary", id, first, false)
		for _, label := range []string{"worker__primary", "worker--primary", "worker.secondary-3"} {
			foreignKey := f.key("anonymous-foreign-" + label)
			scope := f.remember(by, "bead.claim", foreignKey, label)
			before := f.state()
			r := f.request(f.bootstrap, by, "bead.claim", poolA, foreignKey, label, target(id))
			if r.code != "busy" || len(r.envelope["result"]) != 0 {
				t.Fatalf("anonymous label was normalized or treated as a verified alias: %s", r.raw)
			}
			after := f.state()
			sameState(t, before, after)
			if after.Receipts[scope] != nil {
				t.Fatal("foreign anonymous label persisted a successful receipt")
			}
		}
		if e.name != "memory" {
			f.reopen()
		}
		// Any local process may assert the same label. This succeeds under the
		// one anonymous principal and demonstrates P60's absence of isolation.
		noopKey := f.key("anonymous-same-label")
		scope := f.remember(by, "bead.claim", noopKey, "worker.primary")
		before = f.state()
		got := f.request(f.bootstrap, by, "bead.claim", poolA, noopKey, "worker.primary", target(id)).commit(t)
		sameClaimState(t, claimed, got)
		onlyNoopReceipt(t, before, f.state(), scope, got)
		before = f.state()
		f.request(f.bootstrap, by, "bead.claim", poolA, key, "worker.primary", target(id)).replay(t, first)
		sameState(t, before, f.state())
		doc, err := pipeline.DecodeDocument(f.state().record(t, id).Body)
		if err != nil || doc.Lease.Principal.Kind != "anonymous" || doc.Lease.Principal.ID != "anonymous" {
			t.Fatal("anonymous label manufactured an authenticated principal")
		}
		t.Logf("%s: P60 labels remain exact unverified lease/receipt scopes under one anonymous principal", e.name)
	})
}

func TestB04SQLClaimPublicationFailureConservesState(t *testing.T) {
	eachStore(t, true, func(t *testing.T, e engine) {
		f := newFixture(t, e)
		owner := f.issue("agent:sql-claim-worker", "user:pool-a", nil)
		id := f.work["a1"]
		key := f.key("sql-failed-claim")
		scope := f.remember(owner, "bead.claim", key, "sql-worker")
		before := f.state()
		e.publicationFault(t, true)
		failed := f.call(owner, "bead.claim", poolA, key, "sql-worker", target(id))
		e.publicationFault(t, false)
		if failed.code != "internal_error" || failed.status != http.StatusOK || len(failed.envelope["result"]) != 0 {
			t.Fatalf("real SQL publication failure must refuse the write: %s", failed.raw)
		}
		after := f.state()
		sameState(t, before, after)
		if after.Receipts[scope] != nil || bytes.Contains(after.record(t, id).Body, []byte(`"lease"`)) {
			t.Fatal("failed SQL claim left a lease or successful receipt")
		}
		first := f.call(owner, "bead.claim", poolA, key, "sql-worker", target(id))
		f.winner(before, owner, "bead.claim", key, "sql-worker", id, first, true)
		f.reopen()
		before = f.state()
		f.call(owner, "bead.claim", poolA, key, "sql-worker", target(id)).replay(t, first)
		sameState(t, before, f.state())
		t.Logf("%s: SQL publication trigger rolls back every observed claim component, same-key retry wins once, and reopen preserves replay", e.name)
	})
}
