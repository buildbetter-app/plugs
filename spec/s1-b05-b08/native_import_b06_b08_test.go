package s1b05b08

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/integration/converterroundtrip"
	"github.com/buildbetter-app/plugs/internal/plugins/authnoidc"
	"github.com/buildbetter-app/plugs/internal/server/authn"
	"github.com/buildbetter-app/plugs/internal/server/composition"
	"github.com/buildbetter-app/plugs/internal/server/transport/httpjson"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// This consumer uses the existing receiver and import transaction unchanged.
// Interactive admission authenticates the importer; it supplies no native
// holder, transition grant, or policy record. Builtin native source currently
// has no such authority. The ordinary anonymous runtime below is an explicit
// no-auth comparison, separate from the installed-authority production tests.
type b068ImportedRuntime struct {
	f        *b08CloseFixture
	runtime  *composition.Runtime
	http     *httptest.Server
	project  string
	output   string
	calls    int
	lifetime int
}

func b068ImportArchive(t *testing.T, engine string, capture b068Capture, member, sample string) *b068ImportedRuntime {
	t.Helper()
	output := filepath.Join(capture.Directory, "plug-"+engine)
	if err := os.Mkdir(output, 0o700); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(capture.Directory, member))
	if err != nil {
		t.Fatal(err)
	}
	f := newB08CloseFixture(t, engine, ontology.DefaultDocument())
	before := f.conservation(t)
	inspection, err := converterroundtrip.InspectImport(f.ctx, bytes.NewReader(data))
	if err != nil {
		after := f.conservation(t)
		nativeJSON(t, filepath.Join(output, "inspection-refusal.json"), map[string]any{"archiveSHA256": b068SHA(data), "error": err.Error(), "before": before, "after": after})
		b08SameConservation(t, before, after)
		t.Fatalf("entire actual %s native archive refused by existing receiver: %v", capture.Card, err)
	}
	idp := newB068IDP(t)
	provider, err := authnoidc.New(idp.config())
	if err != nil {
		t.Fatal(err)
	}
	session := b068Login(t, idp, provider)
	authenticator, err := authn.New(authn.Config{CoreSessions: provider})
	if err != nil {
		t.Fatal(err)
	}
	identity, err := authenticator.Admit(f.ctx, session.Token)
	if err != nil || identity.RequireInteractiveUser() != nil {
		t.Fatal("actual signed OIDC import admission failed")
	}
	admission, err := json.Marshal(map[string]any{"method": "offline.restore.import", "actor": identity.Principal(), "sessionID": session.ID,
		"source": inspection.SourceStoreID, "archive": inspection.ArchiveSHA256, "derived": inspection.DerivedSHA256})
	if err != nil {
		t.Fatal(err)
	}
	profile := driver.RestoreImportProfile{
		Admission:     driver.RestoreImportAdmission{Mode: "authenticated", Actor: identity.Principal(), Authority: "restore-admin", AdmissionDigest: b068SHA(admission)},
		SourceStoreID: inspection.SourceStoreID, Method: "offline.restore.import", IdempotencyKey: key(capture.Card + "-native-import/" + engine),
		ArchiveDigest: inspection.ArchiveSHA256, DerivedDigest: inspection.DerivedSHA256, HistoryMode: driver.RestoreHistoryModeSnapshot,
	}
	if err := authn.RequireInteractiveUser(authn.WithIdentity(f.ctx, identity), identity.Principal()); err != nil {
		t.Fatal(err)
	}
	before = f.conservation(t)
	report, err := converterroundtrip.RestoreImportWithLease(f.ctx, bytes.NewReader(data), f.store, f.lease, profile)
	if err != nil {
		after := f.conservation(t)
		nativeJSON(t, filepath.Join(output, "import-refusal.json"), map[string]any{"inspection": inspection, "profile": profile, "error": err.Error(), "before": before, "after": after})
		b08SameConservation(t, before, after)
		t.Fatalf("actual complete native import transaction refused: %v", err)
	}
	nativeJSON(t, filepath.Join(output, "import-admission.json"), map[string]any{"inspection": inspection, "profile": profile, "targetBefore": report.Target, "targetAfter": report.Restored,
		"liveInteractiveUser": true, "inputBytesUnchanged": report.Archive != nil && bytes.Equal(data, report.Archive.TarBytes()), "historyScope": "explicit snapshot; source history is not qualified"})
	if report.Archive == nil || !bytes.Equal(data, report.Archive.TarBytes()) {
		t.Fatal("existing receiver did not retain the complete native archive bytes")
	}
	if err := f.lease.Release(f.ctx); err != nil {
		t.Fatal(err)
	}
	f.lease = nil
	parent := f.document(t, model.RefByID(sample, model.Natural64(0)))
	p := &b068ImportedRuntime{f: f, project: parent.Project, output: output}
	p.open(t)
	t.Cleanup(func() {
		p.close(t)
		f.store, f.lease = nil, nil
	})
	nativeJSON(t, filepath.Join(output, "scope.json"), map[string]any{"source": "unchanged actual native " + member, "roles": "all records and roles imported without selection", "authorization": "true no-auth counterproof only; no native policy, holder or installed transition grant invented", "installedAuthorityAcceptance": false, "NativeAdmission": "None"})
	return p
}

func (p *b068ImportedRuntime) open(t *testing.T) {
	t.Helper()
	runtime, err := composition.Open(p.f.ctx, composition.Config{
		Driver: p.f.engine, DSN: p.f.dsn, Project: p.project, AuthMode: "anonymous",
		StoreFactory: func(context.Context, string, driver.Config) (driver.Store, error) { return p.f.store, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	p.runtime = runtime
	p.f.store, p.f.lease, p.f.service, p.f.server = runtime.Store, runtime.Lease, runtime.Service, runtime.RPC
	p.http = httptest.NewServer(runtime.HTTPHandler)
	p.lifetime++
	nativeJSON(t, filepath.Join(p.output, fmt.Sprintf("http-lifetime-%02d-open.json", p.lifetime)), map[string]any{"url": p.http.URL, "driver": p.f.engine, "dsn": p.f.dsn, "at": time.Now().UTC(), "boundary": "actual owned loopback HTTP listener and production composition runtime"})
}

func (p *b068ImportedRuntime) close(t *testing.T) {
	t.Helper()
	if p.runtime == nil {
		return
	}
	url := p.http.URL
	p.http.Close()
	p.http.CloseClientConnections()
	p.http = nil
	err := p.runtime.Close(context.Background())
	p.runtime = nil
	nativeJSON(t, filepath.Join(p.output, fmt.Sprintf("http-lifetime-%02d-close.json", p.lifetime)), map[string]any{"url": url, "listenerClosed": true, "runtimeCloseError": fmt.Sprint(err), "at": time.Now().UTC()})
	if err != nil {
		t.Error(err)
	}
}

func (p *b068ImportedRuntime) reopen(t *testing.T) {
	t.Helper()
	if p.f.storage == nil {
		return
	}
	p.close(t)
	store, err := p.f.storage.Open(p.f.ctx, driver.Config{DSN: p.f.dsn})
	if err != nil {
		t.Fatal(err)
	}
	p.f.store, p.f.lease = store, nil
	p.open(t)
}

func (p *b068ImportedRuntime) call(t *testing.T, method, label string, params map[string]any) b08Reply {
	t.Helper()
	scope := p.f.scope(method, label, actor)
	copyParams := make(map[string]any, len(params)+1)
	for key, value := range params {
		copyParams[key] = value
	}
	copyParams["_meta"] = map[string]any{"project": p.project, "agent": actor, "idempotencyKey": scope.key}
	raw, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 8, "method": method, "params": copyParams})
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequestWithContext(p.f.ctx, http.MethodPost, p.http.URL+httpjson.Path, bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Plug-RPC", "1")
	request.Header.Set("Content-Type", "application/json")
	start := time.Now().UTC()
	response, err := p.http.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	body, readErr := io.ReadAll(response.Body)
	closeErr := response.Body.Close()
	end := time.Now().UTC()
	nativeJSON(t, filepath.Join(p.output, fmt.Sprintf("call-%03d.json", p.calls)), map[string]any{"method": method, "url": request.URL.String(), "request": json.RawMessage(raw), "httpStatus": response.StatusCode, "response": string(body), "readError": fmt.Sprint(readErr), "closeError": fmt.Sprint(closeErr), "start": start, "end": end})
	p.calls++
	if readErr != nil || closeErr != nil || response.StatusCode != http.StatusOK {
		t.Fatalf("actual imported-runtime HTTP %s status=%d read=%v close=%v", method, response.StatusCode, readErr, closeErr)
	}
	var reply b08Reply
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	if err := decoder.Decode(&reply); err != nil {
		t.Fatal(err)
	}
	return reply
}
