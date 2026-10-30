package composition

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/client"
	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/restore"
	"github.com/buildbetter-app/plugs/internal/integration/converterroundtrip"
	"github.com/buildbetter-app/plugs/internal/server/authn"
	"github.com/buildbetter-app/plugs/internal/server/transport/inprocess"
)

func compositionRestorePresence() restore.Presence {
	return restore.Presence{"mode": "presence_check", "challenge": "composition-restore", "proof": "composition-restore"}
}

func compositionRestoreVerifier() restore.PresenceVerifier {
	return restore.PresenceVerifierFunc(func(ctx context.Context, method, digest string, presence restore.Presence) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if method == "" || digest == "" || presence["proof"] != "composition-restore" {
			return errors.New("presence is not bound to the restore operation")
		}
		return nil
	})
}

func compositionSyntheticArchive(t *testing.T) ([]byte, map[string]any, string) {
	t.Helper()
	path := filepath.Join("..", "..", "integration", "converterroundtrip", "testdata", "synthetic-plug-dump-1.tar")
	if root := os.Getenv("TEST_SRCDIR"); root != "" {
		path = filepath.Join(root, os.Getenv("TEST_WORKSPACE"), "internal/integration/converterroundtrip/testdata/synthetic-plug-dump-1.tar")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	archive, err := dumpformat.Read(strings.NewReader(string(data)))
	if err != nil {
		t.Fatal(err)
	}
	manifestBytes, ok := archive.Member("manifest.json")
	if !ok {
		t.Fatal("converter fixture has no manifest")
	}
	var manifest map[string]any
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		t.Fatal(err)
	}
	canonical, err := json.Marshal(archive.Manifest())
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(canonical)
	return data, manifest, hex.EncodeToString(sum[:])
}

func compositionRestoreStatus(t *testing.T, result client.Result) map[string]any {
	t.Helper()
	status := resultObject(t, result)
	if status["state"] == nil || status["restoreId"] == nil {
		t.Fatalf("restore status is incomplete: %v", status)
	}
	return status
}

func TestComposedConverterRestoreUsesProductionBeginChunkFinishAndRecovery(t *testing.T) {
	data, manifest, manifestHash := compositionSyntheticArchive(t)
	presence := compositionRestorePresence()
	f := startRuntime(t, Config{
		Driver: "memory", Project: "restore-composition", Address: DefaultAddress,
		RestorePresence: compositionRestoreVerifier(),
	})
	beginKey := strings.Repeat("b", 32)
	finishKey := strings.Repeat("f", 32)
	pageHash := sha256.Sum256(data)
	pageDigest := hex.EncodeToString(pageHash[:])
	begin, err := f.client.Call(context.Background(), "admin.restore.begin", map[string]any{
		"manifest": manifest, "history": "head", "humanPresence": presence,
	}, beginKey)
	if err != nil {
		t.Fatalf("production converter begin: %v", err)
	}
	status := compositionRestoreStatus(t, begin)
	restoreID := status["restoreId"].(string)
	if _, err := f.client.Call(context.Background(), "admin.restore.chunk", map[string]any{
		"restoreId": restoreID, "page": 0, "data": base64.StdEncoding.EncodeToString(data), "sha256": pageDigest, "humanPresence": presence,
	}, strings.Repeat("c", 32)); err != nil {
		t.Fatalf("production converter chunk: %v", err)
	}
	finished, err := f.client.Call(context.Background(), "admin.restore.finish", map[string]any{
		"restoreId": restoreID, "manifestSha256": manifestHash, "humanPresence": presence,
	}, finishKey)
	if err != nil {
		t.Fatalf("production converter finish: %v", err)
	}
	if got := compositionRestoreStatus(t, finished)["state"]; got != "complete" {
		t.Fatalf("production converter finish state=%v", got)
	}
	before, err := f.r.Store.Meta(context.Background())
	if err != nil || before.Head.IsZero() {
		t.Fatalf("converter import did not publish a head: %+v err=%v", before, err)
	}

	// A second paged session with the same admitted begin key must recover the
	// durable receipt without replaying records into the now-nonempty target.
	retryBegin, err := f.client.Call(context.Background(), "admin.restore.begin", map[string]any{
		"manifest": manifest, "history": "head", "humanPresence": presence,
	}, beginKey)
	if err != nil {
		t.Fatalf("same-bound retry begin: %v", err)
	}
	retryID := compositionRestoreStatus(t, retryBegin)["restoreId"].(string)
	if _, err := f.client.Call(context.Background(), "admin.restore.chunk", map[string]any{
		"restoreId": retryID, "page": 0, "data": base64.StdEncoding.EncodeToString(data), "sha256": pageDigest, "humanPresence": presence,
	}, strings.Repeat("d", 32)); err != nil {
		t.Fatalf("same-bound retry chunk: %v", err)
	}
	recovered, err := f.client.Call(context.Background(), "admin.restore.finish", map[string]any{
		"restoreId": retryID, "manifestSha256": manifestHash, "humanPresence": presence,
	}, strings.Repeat("e", 32))
	if err != nil || compositionRestoreStatus(t, recovered)["state"] != "complete" {
		t.Fatalf("same-bound recovery result=%s err=%v", recovered.JSON, err)
	}
	after, err := f.r.Store.Meta(context.Background())
	if err != nil || after.Head != before.Head || after.Incarnation != before.Incarnation {
		t.Fatalf("same-bound recovery replayed or changed target: before=%+v after=%+v err=%v", before, after, err)
	}

	// A changed G4 agent scope cannot inherit the receipt, even though the
	// archive and begin key are otherwise identical.
	foreignTransport := inprocess.New(f.r.RPC)
	foreign, err := client.New(foreignTransport, "restore-composition", "foreign-agent")
	if err != nil {
		t.Fatal(err)
	}
	foreignBegin, err := foreign.Call(context.Background(), "admin.restore.begin", map[string]any{
		"manifest": manifest, "history": "head", "humanPresence": presence,
	}, beginKey)
	if err != nil {
		t.Fatalf("foreign retry begin: %v", err)
	}
	foreignID := compositionRestoreStatus(t, foreignBegin)["restoreId"].(string)
	if _, err := foreign.Call(context.Background(), "admin.restore.chunk", map[string]any{
		"restoreId": foreignID, "page": 0, "data": base64.StdEncoding.EncodeToString(data), "sha256": pageDigest, "humanPresence": presence,
	}, strings.Repeat("g", 32)); err != nil {
		t.Fatalf("foreign retry chunk: %v", err)
	}
	if _, err := foreign.Call(context.Background(), "admin.restore.finish", map[string]any{
		"restoreId": foreignID, "manifestSha256": manifestHash, "humanPresence": presence,
	}, strings.Repeat("h", 32)); err == nil {
		t.Fatal("foreign G4 binding inherited the completed import")
	}
}

func TestRestoreImportAuthenticatedBindingRequiresLiveInteractiveUser(t *testing.T) {
	a := &restoreImportAdapter{authMode: "authenticated"}
	principal := model.PrincipalRef{Kind: "user", ID: "user:restore"}
	if _, _, _, _, err := a.binding(context.Background(), principal, ""); !errors.Is(err, authn.ErrHumanPresenceRequired) {
		t.Fatalf("provider-shaped user without live session was admitted: %v", err)
	}
	for _, other := range []model.PrincipalRef{
		{Kind: "service", ID: "service:restore"},
		{Kind: "agent", ID: "agent:restore", Sponsor: &model.PrincipalRef{Kind: "user", ID: "user:restore"}},
		{Kind: "anonymous", ID: "anonymous"},
	} {
		if _, _, _, _, err := a.binding(context.Background(), other, ""); !errors.Is(err, authn.ErrUnauthenticated) && !errors.Is(err, authn.ErrHumanPresenceRequired) {
			t.Fatalf("non-user principal was not refused: %+v err=%v", other, err)
		}
	}
}

func TestRestoreImportAnonymousBindingKeepsHonestScope(t *testing.T) {
	a := &restoreImportAdapter{authMode: "anonymous"}
	actor := model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}
	mode, got, scope, authority, err := a.binding(context.Background(), actor, "local-agent")
	if err != nil || mode != "anonymous-local" || !reflect.DeepEqual(got, actor) || scope != "local-agent" || authority != "trusted-human-presence" {
		t.Fatalf("anonymous binding=%q %+v %q %q err=%v", mode, got, scope, authority, err)
	}
	if _, _, _, _, err := a.binding(context.Background(), model.PrincipalRef{Kind: "user", ID: "user:operator"}, "local-agent"); !errors.Is(err, authn.ErrHumanPresenceRequired) {
		t.Fatalf("user was promoted through anonymous mode: %v", err)
	}
	if _, _, _, _, err := a.binding(context.Background(), actor, " bad "); err == nil {
		t.Fatal("noncanonical anonymous scope was accepted")
	}
}

func TestRestoreImportAdmissionDigestIsBoundAndOpaque(t *testing.T) {
	actor := model.PrincipalRef{Kind: "user", ID: "user:restore"}
	one, err := importAdmissionDigest("authenticated", actor, "", "restore-admin", "pl", strings.Repeat("a", 64), strings.Repeat("b", 64), strings.Repeat("c", 32))
	if err != nil || len(one) != 64 {
		t.Fatalf("digest=%q err=%v", one, err)
	}
	two, err := importAdmissionDigest("authenticated", actor, "", "restore-admin", "pl", strings.Repeat("a", 64), strings.Repeat("b", 64), strings.Repeat("d", 32))
	if err != nil || one == two || one == strings.Repeat("a", 64) {
		t.Fatalf("binding digest was not complete/opaque: one=%q two=%q", one, two)
	}
	three, err := importAdmissionDigest("authenticated", actor, "", "restore-admin", "pl", strings.Repeat("e", 64), strings.Repeat("b", 64), strings.Repeat("c", 32))
	if err != nil || one == three {
		t.Fatalf("changed archive with the same derived digest reused admission binding: one=%q three=%q", one, three)
	}
}

func TestRestoreImportAdapterFailsClosedWithoutTrustedHostPieces(t *testing.T) {
	a := &restoreImportAdapter{}
	_, err := a.importArchive(context.Background(), model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}, strings.NewReader("not-an-archive"), RestoreImportRequest{})
	if !errors.Is(err, converterroundtrip.ErrRestoreUnavailable) {
		t.Fatalf("adapter did not fail closed: %v", err)
	}
}
