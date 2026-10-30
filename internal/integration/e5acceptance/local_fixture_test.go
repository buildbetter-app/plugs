package e5acceptance_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/plugins/authnapikey"
	"github.com/buildbetter-app/plugs/internal/plugins/authzfixed"
	"github.com/buildbetter-app/plugs/internal/server/authzrpc"
	"github.com/buildbetter-app/plugs/internal/server/pluginbuild"
	"github.com/buildbetter-app/plugs/internal/server/pluginhost"
	"github.com/buildbetter-app/plugs/internal/server/pluginmanifest"
	"github.com/buildbetter-app/plugs/internal/server/transport/httpjson"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/pluginapi"
)

//go:embed fixture-local-wasi/go.mod.template fixture-local-wasi/plug-plugin.json fixture-local-wasi/plugin.go
var localGuest embed.FS

const targetID = "00000000-0000-4000-8000-000000000001"
const watchedID = "00000000-0000-4000-8000-000000000002"
const bindingID = "e5-local-derive"

type localFixture struct {
	store       *memory.Store
	lease       driver.ActiveLease
	facts       *authzrpc.MutableFacts
	base        authzfixed.Facts
	composition *authzrpc.Composition
	host        *pluginhost.Host
	artifact    *pluginbuild.Artifact
	provider    *authnapikey.Provider
	tokens      map[string]string
	sourceRoot  string
	evidence    map[string]any
	requests    int
}

func demand(t *testing.T, ok bool, message string) {
	t.Helper()
	if !ok {
		t.Fatal(message)
	}
}

func localGit(t *testing.T, root string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = root
	cmd.Env = []string{
		"PATH=" + os.Getenv("PATH"), "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=" + os.DevNull,
		"GIT_AUTHOR_NAME=PlugFixture", "GIT_AUTHOR_EMAIL=fixture@example.test",
		"GIT_COMMITTER_NAME=PlugFixture", "GIT_COMMITTER_EMAIL=fixture@example.test",
		"GIT_AUTHOR_DATE=2001-01-01T00:00:00Z", "GIT_COMMITTER_DATE=2001-01-01T00:00:00Z",
	}
	out, err := cmd.Output()
	demand(t, err == nil, "local fixture Git operation failed")
	return strings.TrimSpace(string(out))
}

func writeEvidence(t *testing.T, name string, data []byte) {
	t.Helper()
	output := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR")
	if output == "" {
		return
	}
	demand(t, os.MkdirAll(output, 0o700) == nil, "evidence directory failed")
	demand(t, os.WriteFile(filepath.Join(output, name), data, 0o600) == nil, "evidence write failed")
}

func newLocalFixture(t *testing.T) *localFixture {
	t.Helper()
	f := &localFixture{tokens: map[string]string{}, evidence: map[string]any{}}
	f.sourceRoot = os.Getenv("PLUG_SOURCE_MODULE_ROOT")
	demand(t, filepath.IsAbs(f.sourceRoot), "explicit isolated source root is required")
	f.evidence["serverCommit"] = localGit(t, f.sourceRoot, "rev-parse", "HEAD")
	f.evidence["serverTree"] = localGit(t, f.sourceRoot, "rev-parse", "HEAD^{tree}")
	source := t.TempDir()
	for _, name := range []string{"go.mod.template", "plug-plugin.json", "plugin.go"} {
		data, err := localGuest.ReadFile("fixture-local-wasi/" + name)
		demand(t, err == nil, "embedded guest source missing")
		dest := name
		if name == "go.mod.template" {
			dest = "go.mod"
		}
		demand(t, os.WriteFile(filepath.Join(source, dest), data, 0o600) == nil, "guest checkout write failed")
	}
	localGit(t, source, "init", "-q", "--template=", "--initial-branch=main")
	localGit(t, source, "add", "--all")
	localGit(t, source, "commit", "-q", "-m", "test: pin local WASI fixture")
	checkout := pluginmanifest.Checkout{Root: source, Commit: localGit(t, source, "rev-parse", "HEAD")}
	bundle, err := pluginmanifest.Load(context.Background(), checkout)
	demand(t, err == nil, "complete guest checkout was not admitted")
	checkout.ExpectedDigest = bundle.Digest()
	demand(t, len(checkout.ExpectedDigest) == 64, "guest source digest is missing")
	// A wrong source pin must refuse before any compiler or guest executes.
	wrong := checkout
	wrong.ExpectedDigest = strings.Repeat("0", 64)
	_, err = pluginmanifest.Load(context.Background(), wrong)
	demand(t, err != nil, "wrong guest source digest was accepted")
	goBinary := os.Getenv("PLUG_E5_GO_BINARY")
	if goBinary == "" {
		goBinary, err = exec.LookPath("go")
		demand(t, err == nil, "source compiler unavailable")
	}
	demand(t, goBinary != "", "source compiler unavailable")
	started := time.Now()
	f.artifact, err = pluginbuild.Build(context.Background(), pluginbuild.Request{
		Runtime: pluginapi.RuntimeWASI, Source: checkout, ModuleRoot: f.sourceRoot, GoBinary: goBinary,
	})
	if err != nil {
		t.Fatalf("real contained WASI build failed (%T)", err)
	}
	artifactPath := f.artifact.Path()
	t.Cleanup(func() {
		demand(t, f.artifact.Close() == nil, "artifact cleanup failed")
		_, err := os.Stat(artifactPath)
		demand(t, os.IsNotExist(err), "owned artifact survived cleanup")
		writeEvidence(t, "cleanup.json", []byte(`{"artifactRemoved":true,"databaseStarted":false,"secondWriter":false}`))
	})
	demand(t, f.artifact.Verify(context.Background()) == nil, "artifact verification failed")
	demand(t, f.artifact.SourceDigest() == checkout.ExpectedDigest && f.artifact.Commit() == checkout.Commit, "artifact lost source identity")
	wasm, err := os.ReadFile(artifactPath)
	demand(t, err == nil, "built WASI bytes missing")
	digest := sha256.Sum256(wasm)
	demand(t, f.artifact.ArtifactDigest() == hex.EncodeToString(digest[:]), "compiled artifact digest differs")
	writeEvidence(t, "guest.wasm", wasm)
	f.evidence["guestCommit"] = checkout.Commit
	f.evidence["guestSourceDigest"] = f.artifact.SourceDigest()
	f.evidence["guestManifestDigest"] = f.artifact.ManifestDigest()
	f.evidence["guestArtifactDigest"] = f.artifact.ArtifactDigest()
	f.evidence["guestFiles"] = f.artifact.Files()
	f.evidence["guestBuildElapsedSeconds"] = time.Since(started).Seconds()
	binding := pluginhost.Binding{ID: bindingID, Alias: "e5local", Instance: "default", Subscriber: "derive", Phase: pluginapi.PhaseMutate,
		Reads: []string{"fields.title"}, Owns: []string{"fields.title"}, MaxPointReads: 1, Timeout: time.Second, FailOpen: false}
	f.host, err = f.artifact.Host(context.Background(), pluginhost.Config{Bindings: []pluginhost.Binding{binding}})
	demand(t, err == nil, "verified WASI host binding failed")
	bindings := f.host.Mutators()
	demand(t, len(bindings) == 1 && bindings[0].ID == bindingID && !bindings[0].FailOpen, "actual host mutator missing")
	f.evidence["binding"] = binding
	f.store = memory.New()
	t.Cleanup(func() { demand(t, f.store.Close() == nil, "store cleanup failed") })
	f.lease, err = f.store.Lease(context.Background(), "e5-local-wasi", driver.LeaseTTL)
	demand(t, err == nil, "owned memory lease unavailable")
	t.Cleanup(func() { demand(t, f.lease.Release(context.Background()) == nil, "lease cleanup failed") })
	var seed []conformance.FixtureChange
	for _, row := range []struct{ id, title string }{{targetID, "seed"}, {watchedID, "observed"}} {
		body, _ := json.Marshal(map[string]any{"id": row.id, "type": "issue", "project": "project", "fields": map[string]any{
			"title": row.title, "description": "hidden", "status": "todo", "priority": 0,
		}})
		seed = append(seed, conformance.FixtureChange{Ref: model.RefByID(row.id, model.Natural64(0)), Kind: "bead", Op: model.OpCreate, Body: body})
	}
	_, err = conformance.Seed(context.Background(), f.store, f.lease, driver.ChangesetHeader{}, seed)
	demand(t, err == nil, "canonical memory storage seed failed")
	meta, err := f.store.Meta(context.Background())
	demand(t, err == nil, "owned store metadata failed")
	ns := authz.Namespace{StoreID: string(meta.StoreID), Incarnation: meta.Incarnation, InstallationID: "fixed", Authority: model.RefByID("authority", model.Natural64(0))}
	project := model.RefByID("project", model.Natural64(0))
	f.base = authzfixed.Facts{Namespace: ns, Projects: []authzfixed.Project{{Ref: project, Name: "project", InstallationID: "fixed", Active: true}},
		Grants: []authz.Grant{{Subject: authz.SubjectRef{Kind: "user", ID: "user:sponsor"}, Scope: authz.Scope{Kind: authz.ProjectScope, Ref: project}, Role: "involved"}}}
	f.facts, err = authzrpc.NewMutableFacts(f.base)
	demand(t, err == nil, "adopted facts invalid")
	f.provider = authnapikey.New(authnapikey.Config{AuthorizationInstalled: func() bool { return true }})
	f.composition, err = authzrpc.New(context.Background(), authzrpc.Config{
		Store: f.store, Lease: f.lease, Provider: f.provider, Facts: f.facts, Namespace: ns, DefaultProject: "project", Mutators: bindings,
	})
	demand(t, err == nil, "same-Writer authorizing construction failed")
	for _, sponsor := range []string{"user:sponsor", "user:outsider"} {
		issued, err := f.provider.Issue(localAgent(sponsor), time.Now().Add(time.Minute))
		demand(t, err == nil, "disposable authenticated agent issue failed")
		f.tokens[sponsor] = issued.Token // In-process only; never log or retain credentials.
	}
	return f
}

func localAgent(sponsor string) model.PrincipalRef {
	return model.PrincipalRef{Kind: "agent", ID: "agent:local", Sponsor: &model.PrincipalRef{Kind: "user", ID: sponsor}, Caveats: map[string]any{
		"actions": []string{"modify"}, "projects": []model.Ref{model.RefByID("project", model.Natural64(0))},
	}}
}

type localReply struct {
	Result json.RawMessage `json:"result"`
	Error  *struct {
		Data model.ErrorData `json:"data"`
	} `json:"error"`
	Meta struct {
		Replayed bool `json:"replayed"`
	} `json:"_meta"`
}

func (f *localFixture) call(t *testing.T, sponsor, title string, key int) localReply {
	t.Helper()
	f.requests++
	params := map[string]any{"id": model.RefByID(targetID, model.Natural64(0)), "fields": map[string]string{"title": title},
		"_meta": map[string]string{"idempotencyKey": fmt.Sprintf("%032x", key)}}
	body, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": key, "method": "bead.update", "params": params})
	demand(t, err == nil, "request encode failed")
	request := httptest.NewRequest(http.MethodPost, "http://127.0.0.1"+httpjson.Path, bytes.NewReader(body))
	request.RemoteAddr = "127.0.0.1:12345"
	request.Header.Set("Plug-RPC", "1")
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+f.tokens[sponsor])
	response := httptest.NewRecorder()
	f.composition.Handler.ServeHTTP(response, request)
	demand(t, response.Code == http.StatusOK, "authenticated handler HTTP refusal")
	var reply localReply
	demand(t, json.Unmarshal(response.Body.Bytes(), &reply) == nil, "RPC response malformed")
	return reply
}

func (f *localFixture) reader(t *testing.T) driver.Reader {
	t.Helper()
	meta, err := f.store.Meta(context.Background())
	demand(t, err == nil, "metadata unavailable")
	r, err := f.store.Read(context.Background(), driver.View{Incarnation: meta.Incarnation})
	demand(t, err == nil, "memory reader unavailable")
	t.Cleanup(func() { _ = r.Close() })
	return r
}

func (f *localFixture) record(t *testing.T) driver.Record {
	t.Helper()
	rows, err := f.reader(t).Get(context.Background(), []model.Ref{model.RefByID(targetID, model.Natural64(0))})
	demand(t, err == nil && len(rows) == 1, "stored target missing")
	return rows[0]
}

func (f *localFixture) receipt(t *testing.T, sponsor string, key int) *driver.IdempotencyRecord {
	t.Helper()
	principal, err := driver.PrincipalKey(localAgent(sponsor))
	demand(t, err == nil, "principal key failed")
	receipt, err := f.reader(t).IdempotencyByKey(context.Background(), principal, "", "bead.update", fmt.Sprintf("%032x", key))
	demand(t, err == nil, "durable receipt lookup failed")
	return receipt
}

func (f *localFixture) head(t *testing.T) string {
	t.Helper()
	meta, err := f.store.Meta(context.Background())
	demand(t, err == nil, "head read failed")
	return meta.Head.String()
}

func sameJSON(t *testing.T, left, right any) bool {
	t.Helper()
	decode := func(value any) any {
		data, err := json.Marshal(value)
		demand(t, err == nil, "value encode failed")
		var out any
		dec := json.NewDecoder(bytes.NewReader(data))
		dec.UseNumber()
		demand(t, dec.Decode(&out) == nil, "value decode failed")
		return out
	}
	return reflect.DeepEqual(decode(left), decode(right))
}

func demandTitle(t *testing.T, row driver.Record, title string) {
	t.Helper()
	doc, err := pipeline.DecodeDocument(row.Body)
	demand(t, err == nil && doc.Fields["title"] == title && doc.Fields["description"] == "hidden", "actual guest patch or undeclared field boundary differs")
}
