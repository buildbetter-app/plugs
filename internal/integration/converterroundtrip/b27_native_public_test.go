package converterroundtrip_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontologymigration"
	"github.com/buildbetter-app/plugs/internal/core/restore"
	"github.com/buildbetter-app/plugs/internal/core/reverseconverter"
	"github.com/buildbetter-app/plugs/internal/integration/converterroundtrip"
	"github.com/buildbetter-app/plugs/internal/server/composition"
	"github.com/buildbetter-app/plugs/internal/server/transport/httpjson"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type b27NativePublicHost struct {
	t       *testing.T
	runtime *composition.Runtime
	config  composition.Config
	project string
	keys    int
	trace   []map[string]any
}

func b27PublicNativeDecode(t *testing.T, raw []byte, value any) {
	t.Helper()
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	if err := d.Decode(value); err != nil {
		t.Fatal(err)
	}
	if err := d.Decode(new(any)); err != io.EOF {
		t.Fatal("trailing public native data", err)
	}
}

// b27PublicNativeErrorData keeps refusal evidence limited to the typed public
// error envelope. It deliberately excludes the outer message, request
// parameters, response bodies, and headers.
func b27PublicNativeErrorData(raw []byte) (string, map[string]any) {
	var envelope struct {
		Error *struct {
			Data map[string]json.RawMessage `json:"data"`
		} `json:"error"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil || envelope.Error == nil {
		return "", nil
	}
	data := make(map[string]any, len(envelope.Error.Data))
	for _, key := range []string{"code", "retryable", "retryAfterMs", "hint", "traceId", "details"} {
		encoded, ok := envelope.Error.Data[key]
		if !ok {
			continue
		}
		var value any
		decoder := json.NewDecoder(bytes.NewReader(encoded))
		decoder.UseNumber()
		if decoder.Decode(&value) == nil {
			data[key] = value
		}
	}
	code, _ := data["code"].(string)
	return code, data
}

func openB27NativePublicHost(t *testing.T, engine, label string, reference *converterroundtrip.B27NativeReferenceForTest) *b27NativePublicHost {
	t.Helper()
	return openB27NativePublicArchiveHost(t, engine, label, "review-project", reference.Data, func(t *testing.T, archive *dumpformat.Archive, store driver.Store, lease driver.ActiveLease) driver.SourcePlanePlanV1 {
		return reference.Plan(t, store, lease)
	})
}

func openB27NativePublicArchiveHost(t *testing.T, engine, label, project string, data []byte, plan func(*testing.T, *dumpformat.Archive, driver.Store, driver.ActiveLease) driver.SourcePlanePlanV1) *b27NativePublicHost {
	t.Helper()
	h := &b27NativePublicHost{t: t, project: project}
	h.config = composition.Config{Driver: engine, Project: "review-project", AuthMode: "anonymous", NativeImportOntology: true,
		RestorePresence: restore.PresenceVerifierFunc(func(ctx context.Context, method, digest string, p restore.Presence) error {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if method == "" || len(digest) != 64 || p["proof"] != "b27-native-public" {
				return fmt.Errorf("missing bound disposable public presence")
			}
			return nil
		}),
	}
	if engine == "sqlite" {
		h.config.DSN = filepath.Join(t.TempDir(), "b27-native-public.db")
	} else if engine == "postgres" {
		h.config.DSN = converterroundtrip.JointPostgresDSNForTest(t)
	}
	h.config.Project = project
	h.config.RestoreSourcePlanner = func(ctx context.Context, reader io.Reader, target driver.SourcePlaneTargetV1) (driver.SourcePlanePlanV1, error) {
		raw, err := io.ReadAll(reader)
		if err != nil || !bytes.Equal(raw, data) {
			return driver.SourcePlanePlanV1{}, fmt.Errorf("source planner requires the exact original merged native archive")
		}
		archive, err := dumpformat.Read(bytes.NewReader(raw))
		if err != nil {
			return driver.SourcePlanePlanV1{}, err
		}
		defer archive.ReleaseNativeDecoded()
		planned := plan(t, archive, h.runtime.Store, h.runtime.Lease)
		if planned.Target != target {
			return driver.SourcePlanePlanV1{}, driver.ErrConflict
		}
		return planned, ctx.Err()
	}
	h.open()
	t.Cleanup(func() {
		if h.runtime != nil {
			if err := h.runtime.Close(context.Background()); err != nil {
				t.Error(err)
			}
		}
		root := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR")
		if root == "" {
			t.Error("canonical public evidence directory required")
			return
		}
		raw, err := json.MarshalIndent(h.trace, "", "  ")
		if err == nil {
			name := strings.ReplaceAll(t.Name(), "/", "_") + "-" + label + "-http.json"
			err = os.WriteFile(filepath.Join(root, name), append(raw, '\n'), 0600)
		}
		if err != nil {
			t.Error(err)
		}
	})
	return h
}

func (h *b27NativePublicHost) open() {
	h.t.Helper()
	var err error
	h.runtime, err = composition.Open(context.Background(), h.config)
	if err != nil {
		h.t.Fatal("open actual native public host", err)
	}
	capability, err := driver.RequireWorkflowCheckpointStore(h.runtime.Store, h.runtime.Lease)
	if err != nil {
		h.t.Fatal(err)
	}
	wantLifetime, wantPersistent := driver.CheckpointLifetimeProcess, false
	if h.config.Driver != "memory" {
		wantLifetime, wantPersistent = driver.CheckpointLifetimeRestart, true
	}
	persistence, ok := capability.(driver.PersistentCheckpointStore)
	if capability.WorkflowCheckpointLifetime() != wantLifetime {
		h.t.Fatal("public host checkpoint lifetime/persistence differs")
	}
	if h.config.Driver == "memory" {
		// Memory checkpoints are process-lifetime and may not expose persistence.
		return
	}
	if !ok || persistence.Persistent() != wantPersistent {
		h.t.Fatal("public host checkpoint lifetime/persistence differs")
	}
}

func (h *b27NativePublicHost) reopen() {
	h.t.Helper()
	if h.config.Driver == "memory" {
		return // The same process-local memory host remains alive.
	}
	if err := h.runtime.Close(context.Background()); err != nil {
		h.t.Fatal(err)
	}
	h.runtime = nil
	h.open()
}

func (h *b27NativePublicHost) call(project, method string, params map[string]any, wantCode string) map[string]any {
	h.t.Helper()
	h.keys++
	params["_meta"] = map[string]any{"project": project, "idempotencyKey": fmt.Sprintf("%032d", h.keys)}
	raw, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": h.keys, "method": method, "params": params})
	if err != nil {
		h.t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "http://127.0.0.1"+httpjson.Path, bytes.NewReader(raw))
	req.RemoteAddr = "127.0.0.1:12345"
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Plug-RPC", "1")
	out := httptest.NewRecorder()
	h.runtime.HTTPHandler.ServeHTTP(out, req)
	var reply struct {
		Result map[string]any  `json:"result"`
		Error  json.RawMessage `json:"error"`
	}
	b27PublicNativeDecode(h.t, out.Body.Bytes(), &reply)
	code, errorData := b27PublicNativeErrorData(out.Body.Bytes())
	// Large dumps and protected opaque source families are recorded only by
	// their exact wire commitments, never exposed as diagnostic field values.
	entry := map[string]any{"method": method, "project": project, "http_status": out.Code,
		"request_sha256": fmt.Sprintf("%x", sha256.Sum256(raw)), "request_bytes": len(raw),
		"response_sha256": fmt.Sprintf("%x", sha256.Sum256(out.Body.Bytes())), "response_bytes": out.Body.Len(), "code": code}
	if len(errorData) != 0 {
		entry["error_data"] = errorData
	}
	h.trace = append(h.trace, entry)
	if wantCode != "*" && (code != wantCode || (wantCode == "" && (reply.Result == nil || reply.Error != nil)) || (wantCode != "" && reply.Error == nil)) {
		h.t.Fatalf("%s returned code %q, expected %q; HTTP %d; error_data=%v", method, code, wantCode, out.Code, errorData)
	}
	if wantCode == "*" && reply.Error == nil {
		h.t.Fatalf("%s returned code %q, expected %q; HTTP %d; error_data=%v", method, code, wantCode, out.Code, errorData)
	}
	return reply.Result
}

func b27NativePublicPresence() map[string]any {
	return map[string]any{"mode": "presence_check", "challenge": "b27-native-public", "proof": "b27-native-public"}
}

func (h *b27NativePublicHost) restore(data []byte, archive *dumpformat.Archive, history, historyDigest, historyTime, beginCode string) {
	h.restoreWithFinish(data, archive, history, historyDigest, historyTime, beginCode, "")
}

func (h *b27NativePublicHost) restoreWithFinish(data []byte, archive *dumpformat.Archive, history, historyDigest, historyTime, beginCode, finishCode string) {
	h.t.Helper()
	var manifest map[string]any
	raw, err := json.Marshal(archive.Manifest())
	if err != nil {
		h.t.Fatal(err)
	}
	b27PublicNativeDecode(h.t, raw, &manifest)
	params := map[string]any{"manifest": manifest, "history": history, "humanPresence": b27NativePublicPresence()}
	if historyDigest != "" {
		params["historyDigest"] = historyDigest
	}
	if historyTime != "" {
		params["historyTime"] = historyTime
	}
	begin := h.call(h.project, "admin.restore.begin", params, beginCode)
	if beginCode != "" {
		return
	}
	id, ok := begin["restoreId"].(string)
	if !ok || id == "" {
		h.t.Fatal("public native import omitted session")
	}
	for page, offset := 0, 0; offset < len(data); page++ {
		end := min(offset+256*1024, len(data))
		part := data[offset:end]
		sum := sha256.Sum256(part)
		h.call(h.project, "admin.restore.chunk", map[string]any{"restoreId": id, "page": page, "data": base64.StdEncoding.EncodeToString(part), "sha256": hex.EncodeToString(sum[:]), "humanPresence": b27NativePublicPresence()}, "")
		offset = end
	}
	sum := sha256.Sum256(raw)
	finished := h.call(h.project, "admin.restore.finish", map[string]any{"restoreId": id, "manifestSha256": hex.EncodeToString(sum[:]), "humanPresence": b27NativePublicPresence()}, map[bool]string{true: "*", false: ""}[finishCode != ""])
	if finishCode != "" {
		if len(h.trace) == 0 || h.trace[len(h.trace)-1]["code"] == "" {
			h.t.Fatal("public restore finish did not retain a typed refusal")
		}
		finishTrace := h.trace[len(h.trace)-1]
		if _, ok := finishTrace["error_data"].(map[string]any); !ok {
			h.t.Fatal("public restore finish did not retain structured refusal data")
		}
		h.t.Logf("b27_public_restore_finish_refusal=%v", finishTrace["error_data"])
		return
	}
	if finished["state"] != "complete" {
		h.t.Fatal("public native import did not complete")
	}
}

func TestB27RichNativePublicRestoreHistoryBindingRefusals(t *testing.T) {
	reference := converterroundtrip.B27RichReferenceForTest(t)

	t.Run("wrong-digest", func(t *testing.T) {
		host := openB27NativePublicHost(t, "memory", "wrong-digest", reference)
		_, historyTime := reference.HistoryEvidence(t, host.runtime.Store, host.runtime.Lease)
		host.restoreWithFinish(reference.Data, reference.Archive, "full", strings.Repeat("0", 64), historyTime, "", "wrong-digest")
		meta, err := host.runtime.Store.Meta(context.Background())
		if err != nil || !meta.Head.IsZero() {
			t.Fatalf("wrong history digest changed target: meta=%+v err=%v", meta, err)
		}
	})

	t.Run("wrong-time", func(t *testing.T) {
		host := openB27NativePublicHost(t, "memory", "wrong-time", reference)
		historyDigest, historyTime := reference.HistoryEvidence(t, host.runtime.Store, host.runtime.Lease)
		when, err := time.Parse(time.RFC3339Nano, historyTime)
		if err != nil {
			t.Fatal(err)
		}
		host.restoreWithFinish(reference.Data, reference.Archive, "full", historyDigest, when.Add(time.Second).Format(time.RFC3339Nano), "", "wrong-time")
		meta, err := host.runtime.Store.Meta(context.Background())
		if err != nil || !meta.Head.IsZero() {
			t.Fatalf("wrong history time changed target: meta=%+v err=%v", meta, err)
		}
	})

	t.Run("partial", func(t *testing.T) {
		host := openB27NativePublicHost(t, "memory", "partial", reference)
		host.restore(reference.Data, reference.Archive, "partial", "", "", "restore_violation")
		meta, err := host.runtime.Store.Meta(context.Background())
		if err != nil || !meta.Head.IsZero() {
			t.Fatalf("partial refusal changed target: meta=%+v err=%v", meta, err)
		}
	})

	t.Run("native-scope", func(t *testing.T) {
		source := openB27NativePublicHost(t, "memory", "native-source", reference)
		historyDigest, historyTime := reference.HistoryEvidence(t, source.runtime.Store, source.runtime.Lease)
		source.restore(reference.Data, reference.Archive, "full", historyDigest, historyTime, "")
		nativeData, nativeArchive := source.dump()
		target := openB27NativePublicHost(t, "memory", "native-target", reference)
		target.restore(nativeData, nativeArchive, "full", historyDigest, historyTime, "restore_violation")
		meta, err := target.runtime.Store.Meta(context.Background())
		if err != nil || !meta.Head.IsZero() {
			t.Fatalf("native-scope refusal changed target: meta=%+v err=%v", meta, err)
		}
	})
}

func (h *b27NativePublicHost) dump() ([]byte, *dumpformat.Archive) {
	h.t.Helper()
	var data bytes.Buffer
	cursor, id := "", ""
	for page := 0; ; page++ {
		if page > 2048 {
			h.t.Fatal("public native dump failed to terminate within decoded ceiling")
		}
		params := map[string]any{"history": "head", "pageSize": 1024 * 1024, "humanPresence": b27NativePublicPresence()}
		if cursor != "" {
			params["cursor"] = cursor
		}
		result := h.call(h.project, "admin.dump", params, "")
		current, ok := result["dumpId"].(string)
		if !ok || current == "" || (id != "" && id != current) {
			h.t.Fatal("public dump changed pinned session")
		}
		id = current
		chunk, err := base64.StdEncoding.DecodeString(result["data"].(string))
		if err != nil {
			h.t.Fatal(err)
		}
		data.Write(chunk)
		if result["done"] == true {
			break
		}
		cursor = fmt.Sprintf("%s:%s", id, result["nextPage"].(json.Number))
	}
	archive, err := dumpformat.Read(bytes.NewReader(data.Bytes()))
	if err != nil || !archive.Native() || archive.Manifest().History != "head" {
		h.t.Fatal("actual public native head archive invalid", err)
	}
	h.t.Cleanup(archive.ReleaseNativeDecoded)
	h.t.Logf("b27_public_native_head_dump bytes=%d sha256=%x", data.Len(), sha256.Sum256(data.Bytes()))
	return data.Bytes(), archive
}

func (h *b27NativePublicHost) assertPublic(reference *converterroundtrip.B27NativeReferenceForTest) map[string]json.RawMessage {
	h.t.Helper()
	meta, err := h.runtime.Store.Meta(context.Background())
	if err != nil {
		h.t.Fatal(err)
	}
	want := reference.IDs()
	bodies := map[string]json.RawMessage{}
	for _, project := range []string{"review-project", "delivery-project"} {
		cursor := ""
		for page := 0; ; page++ {
			if page > len(want) {
				h.t.Fatal("public native census failed to terminate")
			}
			params := map[string]any{"asOf": map[string]any{"revision": meta.Head}, "limit": 3}
			if cursor != "" {
				params["cursor"] = cursor
			}
			result := h.call(project, "history.query", params, "")
			at := result["asOf"].(map[string]any)
			if fmt.Sprint(at["revision"]) != meta.Head.String() || at["incarnation"] != meta.Incarnation {
				h.t.Fatal("public native census changed pinned snapshot")
			}
			items := result["items"].([]any)
			if len(items) > 3 {
				h.t.Fatal("public native census exceeded page limit")
			}
			for _, value := range items {
				item := value.(map[string]any)
				id := item["id"].(string)
				if want[id] != project || bodies[id] != nil {
					h.t.Fatal("public native census contains extra/repeated identity", project, id)
				}
				ref := model.RefByID(id, model.Natural64(0))
				image := h.call(project, "history.get", map[string]any{"ref": ref, "asOf": map[string]any{"revision": meta.Head}}, "")
				raw, err := json.Marshal(image["body"])
				if err != nil {
					h.t.Fatal(err)
				}
				bodies[id] = raw
			}
			cursor, _ = result["nextCursor"].(string)
			if cursor == "" {
				break
			}
		}
	}
	if len(bodies) != len(want) {
		h.t.Fatal("public native census omitted original identities")
	}
	reference.AssertPublic(h.t, bodies)
	h.t.Logf("b27_public_native_census original_projects=2 exact_ids=%d terminal=true", len(bodies))
	return bodies
}

func (h *b27NativePublicHost) assertCatalog(reference *converterroundtrip.B27NativeReferenceForTest) {
	h.t.Helper()
	for _, project := range []string{"review-project", "delivery-project"} {
		want, origins := reference.Catalog(h.t, project)
		result := h.call(project, "ontology.get", map[string]any{"project": project}, "")
		doc := result["document"].(map[string]any)
		got := map[string]string{}
		for name, value := range doc["statuses"].(map[string]any) {
			got[name] = value.(map[string]any)["category"].(string)
		}
		if !reflect.DeepEqual(got, want) {
			h.t.Fatal("installed catalog differs from independent stored+YAML union", project, got, want)
		}
		keys := make([]string, 0, len(origins))
		for key := range origins {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, origin := range keys {
			h.t.Logf("b27_native_catalog project=%s origin=%s exact_names=%q", project, origin, origins[origin])
		}
	}
}

func TestB27RichNativePublicImportCensusCatalogAndHeadRoundtrip(t *testing.T) {
	reference := converterroundtrip.B27RichReferenceForTest(t)
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			source := openB27NativePublicHost(t, engine, "source", reference)
			historyDigest, historyTime := reference.HistoryEvidence(t, source.runtime.Store, source.runtime.Lease)
			source.restore(reference.Data, reference.Archive, "full", historyDigest, historyTime, "")
			reference.AssertImported(t, source.runtime.Store, historyDigest, historyTime)
			original := source.assertPublic(reference)
			source.assertCatalog(reference)
			source.reopen()
			reference.AssertImported(t, source.runtime.Store, historyDigest, historyTime)
			if !reflect.DeepEqual(original, source.assertPublic(reference)) {
				t.Fatal("SQL reopen changed public native bodies")
			}
			source.assertCatalog(reference)
			data, archive := source.dump()
			before, err := source.runtime.Store.Meta(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			// P44 forbids nonempty restore. Its typed refusal must preserve
			// exact state durably, including after a second SQL reopen.
			source.restore(data, archive, "snapshot", "", "", "restore_requires_empty")
			source.reopen()
			after, err := source.runtime.Store.Meta(context.Background())
			retained, _ := source.dump()
			if err != nil || !reflect.DeepEqual(before, after) || !bytes.Equal(data, retained) || !reflect.DeepEqual(original, source.assertPublic(reference)) {
				t.Fatal("nonempty refusal failed durable exact conservation", err)
			}
			source.assertCatalog(reference)
			target := openB27NativePublicHost(t, engine, "roundtrip", reference)
			target.restore(data, archive, "snapshot", "", "", "")
			if !reflect.DeepEqual(original, target.assertPublic(reference)) {
				t.Fatal("public head roundtrip changed native document bodies")
			}
			// Native restore carries the source-plane marker as historical
			// provenance. It must not be adopted as a live converter session,
			// because the native target has a new store incarnation and no
			// matching converter receipt.
			marker := func() []byte {
				t.Helper()
				meta, err := target.runtime.Store.Meta(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				r, err := target.runtime.Store.Read(context.Background(), driver.View{Incarnation: meta.Incarnation})
				if err != nil {
					t.Fatal(err)
				}
				defer r.Close()
				rows, err := r.Get(context.Background(), []model.Ref{model.RefByID(driver.SourcePlaneCommitRecordID, model.Natural64(0))})
				if err != nil || len(rows) != 1 || rows[0].Kind != "instance_config" {
					t.Fatalf("native source provenance marker missing: %v", err)
				}
				return append([]byte(nil), rows[0].Body...)
			}
			beforeReopen := marker()
			if _, _, found, err := converterroundtrip.RecoverBoundSourceImport(context.Background(), target.runtime.Store); err != nil || found {
				t.Fatalf("native historical marker was adopted as an active import: found=%t err=%v", found, err)
			}
			assertNativeHistoricalReverseSucceeds(t, target.runtime.Store, reference.Data, reference.Archive)
			target.reopen()
			if !reflect.DeepEqual(original, target.assertPublic(reference)) || !bytes.Equal(beforeReopen, marker()) {
				t.Fatal("first target reopen changed native provenance/readback")
			}
			assertNativeHistoricalReverseSucceeds(t, target.runtime.Store, reference.Data, reference.Archive)
			if _, _, found, err := converterroundtrip.RecoverBoundSourceImport(context.Background(), target.runtime.Store); err != nil || found {
				t.Fatalf("first target reopen adopted historical marker: found=%t err=%v", found, err)
			}
			target.reopen()
			if !reflect.DeepEqual(original, target.assertPublic(reference)) || !bytes.Equal(beforeReopen, marker()) {
				t.Fatal("second target reopen changed native provenance/readback")
			}
			assertNativeHistoricalReverseSucceeds(t, target.runtime.Store, reference.Data, reference.Archive)
			if _, _, found, err := converterroundtrip.RecoverBoundSourceImport(context.Background(), target.runtime.Store); err != nil || found {
				t.Fatalf("second target reopen adopted historical marker: found=%t err=%v", found, err)
			}
		})
	}
}

func assertNativeHistoricalReverseSucceeds(t *testing.T, store driver.Store, source []byte, archive *dumpformat.Archive) {
	t.Helper()
	directives := make([]ontologymigration.Directive, 0, len(archive.Manifest().Ontology))
	for _, directive := range archive.Manifest().Ontology {
		directives = append(directives, ontologymigration.Directive{
			ID: directive.ID, Kind: ontologymigration.Kind(directive.Kind),
			Target: ontologymigration.Target{Project: directive.Target.Project, Type: directive.Target.Type, ID: directive.Target.ID},
			From:   directive.From, To: directive.To,
		})
	}
	options := reverseconverter.Options{All: true, Directives: directives}
	actual, err := converterroundtrip.ReverseRestoredWithOptions(context.Background(), store, options)
	if err != nil {
		t.Fatalf("native historical reverse failed: %v", err)
	}
	want, err := reverseconverter.ConvertArchiveWithOptions(source, options)
	if err != nil || !bytes.Equal(actual.Result.JSONL, want.JSONL) || !reflect.DeepEqual(actual.Result.Projection, want.Projection) {
		t.Fatalf("native historical reverse differs from source: err=%v", err)
	}
	assertNativeHistoricalBeadsReadback(t, actual.Result)
}

func assertNativeHistoricalBeadsReadback(t *testing.T, result reverseconverter.Result) {
	t.Helper()
	const binary = "/tmp/plug-swarm/work/restore-roundtrip-r2-20261031/bd"
	const wantHash = "9370e9a9234aed2697403a59a7d0c65b6d261c99720d386f5ad446535fc0bdd5"
	data, err := os.ReadFile(binary)
	digest := sha256.Sum256(data)
	if err != nil || hex.EncodeToString(digest[:]) != wantHash {
		t.Fatalf("pinned Beads artifact changed: err=%v", err)
	}
	root := t.TempDir()
	workspace := filepath.Join(root, "beads")
	home := filepath.Join(root, "home")
	if err := os.MkdirAll(home, 0o700); err != nil {
		t.Fatal(err)
	}
	env := []string{"PATH=/usr/bin:/bin", "HOME=" + home, "XDG_CONFIG_HOME=" + filepath.Join(home, ".config"), "BEADS_DIR=" + filepath.Join(workspace, ".beads"), "DO_NOT_TRACK=1", "GIT_CONFIG_NOSYSTEM=1", "GIT_AUTHOR_NAME=Public roundtrip", "GIT_AUTHOR_EMAIL=public@example.invalid", "GIT_COMMITTER_NAME=Public roundtrip", "GIT_COMMITTER_EMAIL=public@example.invalid"}
	run := func(args ...string) []byte {
		cmd := exec.CommandContext(context.Background(), binary, args...)
		cmd.Dir, cmd.Env = workspace, env
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("bd %s failed: %v output=%s", args[0], err, out)
		}
		return out
	}
	if err := os.Mkdir(workspace, 0o700); err != nil {
		t.Fatal(err)
	}
	run("init", "--prefix", "review-project", "--skip-hooks", "--skip-agents", "--non-interactive", "--quiet")
	input := filepath.Join(workspace, "reverse.jsonl")
	if err := os.WriteFile(input, result.JSONL, 0o600); err != nil {
		t.Fatal(err)
	}
	run("import", input, "--json")
	var listed []map[string]any
	if err := json.Unmarshal(run("list", "--all", "--include-ephemeral", "--include-gates", "--json"), &listed); err != nil {
		t.Fatal("bd list JSON", err)
	}
	wantIDs := map[string]bool{}
	for _, issue := range result.Projection.Issues {
		wantIDs[issue.ID] = true
	}
	gotIDs := map[string]bool{}
	for _, issue := range listed {
		if id, ok := issue["id"].(string); ok {
			gotIDs[id] = true
		}
	}
	if !reflect.DeepEqual(gotIDs, wantIDs) {
		t.Fatalf("bd list readback IDs differ: got=%d want=%d", len(gotIDs), len(wantIDs))
	}
	for _, issue := range result.Projection.Issues {
		var shown []map[string]any
		if err := json.Unmarshal(run("show", issue.ID, "--json"), &shown); err != nil || len(shown) == 0 {
			t.Fatalf("bd show readback for %s failed: %v", issue.ID, err)
		}
	}
	ids := make([]string, 0, len(wantIDs))
	for id := range wantIDs {
		ids = append(ids, id)
	}
	if len(result.Projection.Edges) != 0 {
		var dependencies []map[string]any
		args := append([]string{"dep", "list"}, ids...)
		args = append(args, "--json")
		if err := json.Unmarshal(run(args...), &dependencies); err != nil || len(dependencies) != len(result.Projection.Edges) {
			t.Fatalf("bd dependency readback differs: got=%d want=%d err=%v", len(dependencies), len(result.Projection.Edges), err)
		}
	}
}
