package b13memory_test

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/restore"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

const readbackBody = "  After restore\nExact Ω 東京\t  "

var readbackPresence = map[string]any{"mode": "presence_check", "challenge": "b13-readback-disposable", "proof": "b13-test-verifier"}

type readbackAdmin struct {
	f      *fixture
	server *rpc.Server
}

// The injected verifier is only a disposable test admission authority. It does
// not establish authenticated production or interactive human admission.
func bindReadbackAdmin(f *fixture) readbackAdmin {
	f.t.Helper()
	verifier := restore.PresenceVerifierFunc(func(ctx context.Context, method, digest string, got restore.Presence) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		sum, err := hex.DecodeString(digest)
		if !strings.HasPrefix(method, "admin.") || err != nil || len(sum) != sha256.Size || !reflect.DeepEqual(map[string]any(got), readbackPresence) {
			return fmt.Errorf("B13 test presence method/digest/proof refused")
		}
		return nil
	})
	registry, err := rpc.NewRegistry()
	if err != nil {
		f.t.Fatal(err)
	}
	if err := rpc.BindAdmin(registry, restore.NewManager(f.store, f.lease, verifier)); err != nil {
		f.t.Fatal(err)
	}
	meta, err := f.store.Meta(f.ctx)
	if err != nil {
		f.t.Fatal(err)
	}
	server, err := rpc.New(registry, rpc.Config{DefaultProject: project, Incarnation: meta.Incarnation, CurrentIncarnation: func(ctx context.Context) (string, error) {
		current, err := f.store.Meta(ctx)
		return current.Incarnation, err
	}})
	if err != nil {
		f.t.Fatal(err)
	}
	return readbackAdmin{f, server}
}

func (a readbackAdmin) call(method string, params map[string]any, receiptKey string) reply {
	previous := a.f.server
	a.f.server = a.server
	defer func() { a.f.server = previous }()
	return a.f.call(method, params, receiptKey)
}

type readbackArchive struct {
	data     []byte
	manifest dumpformat.Manifest
	members  map[string][]byte
}

func readbackDigest(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// Only actual admin.dump responses supply the archive. The oracle reads tar
// members directly, without calling the production archive reader/projector.
func (a readbackAdmin) dump(history string) readbackArchive {
	f := a.f
	f.t.Helper()
	before := f.readbackState()
	var data bytes.Buffer
	id, cursor := "", ""
	for page := 0; ; page++ {
		if page >= 4096 {
			f.t.Fatal("public B13 dump exceeded bounded page inventory")
		}
		params := map[string]any{"history": history, "pageSize": 1024, "humanPresence": readbackPresence}
		if cursor != "" {
			params["cursor"] = cursor
		}
		value := decode[struct {
			DumpID   string         `json:"dumpId"`
			Page     model.Natural  `json:"page"`
			Data     string         `json:"data"`
			SHA256   string         `json:"sha256"`
			Done     bool           `json:"done"`
			NextPage *model.Natural `json:"nextPage"`
		}](f.t, result(f.t, a.call("admin.dump", params, "")))
		chunk, err := base64.StdEncoding.DecodeString(value.Data)
		if err != nil || value.DumpID == "" || id != "" && id != value.DumpID || value.Page.String() != fmt.Sprint(page) || len(chunk) == 0 || len(chunk) > 1024 || value.SHA256 != readbackDigest(chunk) {
			f.t.Fatalf("public dump page lost identity/order/bytes/digest: %+v err=%v", value, err)
		}
		id = value.DumpID
		data.Write(chunk)
		if value.Done {
			if value.NextPage != nil || page == 0 {
				f.t.Fatal("dump was not a complete multi-page public transfer")
			}
			break
		}
		if value.NextPage == nil || value.NextPage.String() != fmt.Sprint(page+1) {
			f.t.Fatal("dump omitted the exact next page")
		}
		cursor = id + ":" + value.NextPage.String()
	}
	requireSame(f.t, before, f.readbackState())
	archive := readbackArchive{data: data.Bytes(), members: map[string][]byte{}}
	reader := tar.NewReader(bytes.NewReader(archive.data))
	for {
		header, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			f.t.Fatal(err)
		}
		if _, duplicate := archive.members[header.Name]; duplicate || header.Typeflag != tar.TypeReg || header.Size < 0 || header.Size > 4<<20 {
			f.t.Fatal("public dump emitted a duplicate, nonregular or oversized member")
		}
		payload, err := io.ReadAll(reader)
		if err != nil {
			f.t.Fatal(err)
		}
		archive.members[header.Name] = payload
	}
	archive.manifest = decode[dumpformat.Manifest](f.t, archive.members["manifest.json"])
	m := archive.manifest
	if len(archive.members) != 6 || len(m.Files) != 5 || m.Format != "plug-dump/1" || m.Schema != 1 || m.Scope != "plug-native-store" || m.History != history || m.NativeStoreID != string(before.Meta.StoreID) || m.StoreID != string(before.Meta.StoreID) || m.NativeIncarnation != before.Meta.Incarnation || m.NativeHead != before.Meta.Head.String() || m.RecordCount != len(before.Records) {
		f.t.Fatalf("public dump changed its source identity/history/census: %+v", m)
	}
	archiveHash := sha256.New()
	for _, name := range []string{"allocator/snapshot.json", "changesets.ndjson", "config/sources.json", "records/native.ndjson", "versions.ndjson"} {
		payload, exists := archive.members[name]
		file, registered := m.Files[name]
		if !exists || !registered || file.SHA256 != readbackDigest(payload) || file.Size != int64(len(payload)) {
			f.t.Fatalf("public dump member %s differs from its digest/size registration", name)
		}
		_, _ = io.WriteString(archiveHash, name)
		_, _ = archiveHash.Write(payload)
	}
	if m.NativeDigest != hex.EncodeToString(archiveHash.Sum(nil)) {
		f.t.Fatal("public dump native digest does not bind every non-manifest member")
	}
	// This fresh fixture installs no allocator and allocates no child. Assert
	// its complete canonical payload directly, without the production codec.
	wantAllocator := struct {
		Version       string            `json:"version"`
		StoreID       string            `json:"store_id"`
		Incarnation   string            `json:"incarnation"`
		At            model.Revision    `json:"at"`
		CoverageFloor model.Revision    `json:"coverage_floor"`
		Projects      []json.RawMessage `json:"projects"`
		Parents       []json.RawMessage `json:"parents"`
	}{
		Version: "plug-native-allocator/1", StoreID: string(before.Meta.StoreID),
		Incarnation: before.Meta.Incarnation, At: before.Meta.Head,
		CoverageFloor: model.Natural64(0), Projects: []json.RawMessage{}, Parents: []json.RawMessage{},
	}
	allocator := archive.members["allocator/snapshot.json"]
	if !before.Meta.HistoryFloor.IsZero() || m.NativeAllocatorProfile != "plug-native-allocator/1" || m.NativeAllocatorDigest != readbackDigest(allocator) || !bytes.Equal(allocator, encode(f.t, wantAllocator)) {
		f.t.Fatalf("public dump allocator payload/profile/digest/source binding differs: manifest=%+v payload=%s", m, allocator)
	}
	images := readbackLines[struct {
		Ref      model.Ref       `json:"ref"`
		Kind     string          `json:"kind"`
		Revision model.Revision  `json:"revision"`
		Body     json.RawMessage `json:"body"`
	}](f, archive.members["records/native.ndjson"])
	if len(images) != len(before.Records) {
		f.t.Fatal("public dump omitted a persisted row")
	}
	seen := map[string]bool{}
	for _, image := range images {
		row := stored(f.t, before, image.Ref.ID)
		if seen[image.Ref.ID] || !image.Ref.Branch.IsZero() || row.Kind != image.Kind || row.Revision.String() != image.Revision.String() || !bytes.Equal(row.Body, image.Body) {
			f.t.Fatalf("public dump changed persisted body/envelope: %+v", image)
		}
		seen[image.Ref.ID] = true
	}
	versions := readbackLines[driver.Version](f, archive.members["versions.ndjson"])
	wantVersions := map[string][]byte{}
	for _, chain := range before.Versions {
		if history == "head" && len(chain) > 0 {
			chain = chain[len(chain)-1:]
		}
		for _, v := range chain {
			wantVersions[v.Ref.ID+":"+v.Revision.String()] = encode(f.t, v)
		}
	}
	if len(versions) != len(wantVersions) || m.VersionCount != len(versions) {
		f.t.Fatal("public dump changed the retained version/tombstone census")
	}
	for _, v := range versions {
		id := v.Ref.ID + ":" + v.Revision.String()
		if !bytes.Equal(encode(f.t, v), wantVersions[id]) {
			f.t.Fatalf("public dump changed or duplicated durable version %s", id)
		}
		delete(wantVersions, id)
	}
	sets := readbackLines[model.Changeset](f, archive.members["changesets.ndjson"])
	if history == "head" {
		if len(sets) != 0 || m.ChangesetCount != 0 {
			f.t.Fatal("head snapshot claimed source changeset history")
		}
	} else if m.ChangesetCount != len(before.Changesets) || !bytes.Equal(encode(f.t, sets), encode(f.t, before.Changesets)) {
		f.t.Fatal("full producer lost the source changesets before consumer admission")
	}
	f.t.Logf("B13 public dump engine=%s history=%s records=%d versions=%d bytes=%d sha256=%s", f.engine, history, len(images), len(versions), len(archive.data), readbackDigest(archive.data))
	return archive
}

func readbackLines[T any](f *fixture, data []byte) []T {
	f.t.Helper()
	values := make([]T, 0)
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	for {
		var value T
		if err := decoder.Decode(&value); err == io.EOF {
			return values
		} else if err != nil {
			f.t.Fatal(err)
		}
		values = append(values, value)
	}
}

// Upload real producer bytes unchanged. Begin/chunk may create private manager
// state, but no durable target state or successful receipt until finish.
func (a readbackAdmin) upload(archive readbackArchive, label string) (map[string]any, string, durableState) {
	f := a.f
	f.t.Helper()
	pages := (len(archive.data) + 1023) / 1024
	beginKey, finishKey := key(label+"-begin"), key(label+"-finish")
	f.scopes[scope{"admin.restore.begin", beginKey}] = true
	f.scopes[scope{"admin.restore.finish", finishKey}] = true
	for page := 0; page < pages; page++ {
		f.scopes[scope{"admin.restore.chunk", key(fmt.Sprintf("%s-chunk-%d", label, page))}] = true
	}
	before := f.readbackState()
	manifest := decode[map[string]any](f.t, encode(f.t, archive.manifest))
	begin := decode[struct {
		ID    string `json:"restoreId"`
		State string `json:"state"`
	}](f.t, result(f.t, a.call("admin.restore.begin", map[string]any{"manifest": manifest, "history": archive.manifest.History, "humanPresence": readbackPresence}, beginKey)))
	if begin.ID == "" || begin.State != "receiving" {
		f.t.Fatalf("restore begin did not admit a receiving session: %+v", begin)
	}
	for page, offset := 0, 0; offset < len(archive.data); page++ {
		end := min(offset+1024, len(archive.data))
		chunk := archive.data[offset:end]
		status := decode[struct {
			ID       string        `json:"restoreId"`
			State    string        `json:"state"`
			Received model.Natural `json:"receivedPages"`
		}](f.t, result(f.t, a.call("admin.restore.chunk", map[string]any{"restoreId": begin.ID, "page": page, "data": base64.StdEncoding.EncodeToString(chunk), "sha256": readbackDigest(chunk), "humanPresence": readbackPresence}, key(fmt.Sprintf("%s-chunk-%d", label, page)))))
		if status.ID != begin.ID || status.State != "receiving" || status.Received.String() != fmt.Sprint(page+1) {
			f.t.Fatalf("restore upload changed identity/order/state: %+v", status)
		}
		offset = end
	}
	requireSame(f.t, before, f.readbackState())
	return map[string]any{"restoreId": begin.ID, "manifestSha256": readbackDigest(encode(f.t, archive.manifest)), "humanPresence": readbackPresence}, finishKey, before
}

func TestB13PublicArchiveMemoryBriefReadback(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			source := newFixture(t, engine)
			source.retainReadbackTrace("source")
			emptyRevision := source.seedExistingRows() // Existing empty/config rows only.
			bead, _ := source.accept("bead.create", map[string]any{"type": "issue", "fields": map[string]any{"title": explicitKey}}, key("readback-source-bead"), model.OpCreate)
			beadID := bead.Results[0].Ref.ID
			metadata := map[string]any{"source": "public producer", "literal.dot/Ω": " kept "}
			first, _ := source.accept("memory.save", map[string]any{"key": explicitKey, "content": originalBody, "metadata": metadata}, key("readback-source-first"), model.OpCreate)
			id := first.Results[0].Ref.ID
			replaced, _ := source.accept("memory.save", map[string]any{"key": explicitKey, "content": replacedBody, "metadata": metadata}, key("readback-source-replace"), model.OpPatch)
			if replaced.Results[0].Ref.ID != id {
				t.Fatal("source replacement changed identity")
			}
			derived, _ := source.accept("memory.save", map[string]any{"content": "Keep these exact\nwords."}, key("readback-source-derived"), model.OpCreate)
			neighbor, _ := source.accept("memory.save", map[string]any{"key": explicitKey + "suffix", "content": "日本語 !!!"}, key("readback-source-neighbor"), model.OpCreate)
			forgotten, _ := source.accept("memory.save", map[string]any{"key": "forgotten Ω", "content": "must remain absent after readback"}, key("readback-source-forgotten"), model.OpCreate)
			forgottenID := forgotten.Results[0].Ref.ID
			source.accept("memory.delete", map[string]any{"id": forgottenID}, key("readback-source-forget"), model.OpDelete)
			want := map[string]memoryValue{
				"b13-empty":                {ID: "b13-empty", Key: "empty legacy ✓", Content: "", Revision: emptyRevision},
				id:                         {ID: id, Key: explicitKey, Content: replacedBody, Metadata: metadata, Revision: replaced.Revision},
				derived.Results[0].Ref.ID:  {ID: derived.Results[0].Ref.ID, Key: "keep-these-exact-words", Content: "Keep these exact\nwords.", Revision: derived.Revision},
				neighbor.Results[0].Ref.ID: {ID: neighbor.Results[0].Ref.ID, Key: explicitKey + "suffix", Content: "日本語 !!!", Revision: neighbor.Revision},
			}
			source.verifyConnected(want, beadID)
			assertReadbackHistory(source, id, first.Revision, originalBody)
			source.reopenReadback("source-before-dump")
			sourceState := source.readbackState()
			producer := bindReadbackAdmin(source)
			full, head := producer.dump("full"), producer.dump("head")
			source.close() // Consumer cannot read through a live producer handle.

			target := newFixture(t, engine)
			target.retainReadbackTrace("target")
			for s := range source.scopes {
				target.scopes[s] = true // Source receipts must not become target receipts.
			}
			consumer := bindReadbackAdmin(target)
			fullBeginKey := key("readback-full-begin")
			target.scopes[scope{"admin.restore.begin", fullBeginKey}] = true
			fullBefore, fullFrames := target.readbackState(), len(target.frames)
			fullManifest := decode[map[string]any](t, encode(t, full.manifest))
			refused := consumer.call("admin.restore.begin", map[string]any{"manifest": fullManifest, "history": "full", "humanPresence": readbackPresence}, fullBeginKey)
			target.readbackRefusal(refused, "restore_violation", fullBefore)
			if refused.Error.Code != -32050 || refused.Error.Message != "restore contains an unrecorded violation" || refused.Error.Data.Retryable || refused.Error.Data.RetryAfterMS != nil || refused.Error.Data.Hint != "full-history restore is unavailable; source versions and changesets cannot be replayed by this driver" || !reflect.DeepEqual(refused.Error.Data.Details, map[string]any{"reason": "not-yet-supported", "mode": "full", "history": "full"}) {
				t.Fatalf("full-history refusal hid the actual capability dependency: %+v", refused.Error)
			}
			if len(target.frames) != fullFrames+1 {
				t.Fatal("full-history refusal must stop at Begin without chunk or finish calls")
			}
			target.reopenReadback("full-history-refused")
			consumer = bindReadbackAdmin(target)
			finishParams, finishKey, emptyTarget := consumer.upload(head, "readback-head")
			finished := decode[struct {
				ID          string `json:"restoreId"`
				State       string `json:"state"`
				Incarnation string `json:"incarnation"`
			}](t, result(t, consumer.call("admin.restore.finish", finishParams, finishKey)))
			after := target.readbackState()
			if finished.ID != finishParams["restoreId"] || finished.State != "complete" || finished.Incarnation == "" || after.Meta.Incarnation != finished.Incarnation || after.Meta.Incarnation == sourceState.Meta.Incarnation || after.Meta.Incarnation == emptyTarget.Meta.Incarnation || after.Meta.StoreID != sourceState.Meta.StoreID || after.Meta.Head.String() != "1" || after.Meta.HistoryFloor.String() != "1" || len(after.Records) != len(sourceState.Records) || len(after.Changesets) != 0 {
				t.Fatalf("head readback identity/floor/state differs: status=%+v meta=%+v", finished, after.Meta)
			}
			receipt := after.Receipts[scope{"admin.restore.finish", finishKey}]
			if receipt == nil || receipt.ResponsePending || receipt.RequestHash != readbackDigest(head.data) || receipt.Commit.Revision.String() != "1" || len(receipt.Commit.Results) != len(sourceState.Records) || receipt.Changeset.Kind != model.ChangesetRestore || receipt.Changeset.Revision.String() != "1" || receipt.Changeset.Incarnation != after.Meta.Incarnation || receipt.Changeset.IdempotencyKey != finishKey || !reflect.DeepEqual(receipt.Changeset.Principal, anonymous) || len(receipt.Changeset.Changes) != len(sourceState.Records) {
				t.Fatalf("head readback omitted its independent final restore receipt: %+v", receipt)
			}
			resultIDs, changeIDs := map[string]bool{}, map[string]bool{}
			for _, result := range receipt.Commit.Results {
				stored(t, after, result.Ref.ID)
				if resultIDs[result.Ref.ID] || !result.Ref.Branch.IsZero() || result.Revision.String() != "1" {
					t.Fatal("restore receipt has duplicate or misbound result identities")
				}
				resultIDs[result.Ref.ID] = true
			}
			for _, change := range receipt.Changeset.Changes {
				if changeIDs[change.Ref.ID] || !resultIDs[change.Ref.ID] || !change.Ref.Branch.IsZero() || change.Op != model.OpCreate || !change.FromRevision.IsZero() {
					t.Fatal("restore receipt changed an admitted identity or fabricated a prior revision")
				}
				changeIDs[change.Ref.ID] = true
			}
			for s, prior := range emptyTarget.Receipts {
				if s != (scope{"admin.restore.finish", finishKey}) && !reflect.DeepEqual(prior, after.Receipts[s]) {
					t.Fatalf("head readback changed an unrelated receipt scope: %+v", s)
				}
			}
			for s := range source.scopes {
				if after.Receipts[s] != nil {
					t.Fatalf("head readback fabricated a source write receipt: %+v", s)
				}
			}
			for _, row := range sourceState.Records {
				got := stored(t, after, row.Ref.ID)
				versions := after.Versions[row.Ref.ID]
				if got.Kind != row.Kind || !bytes.Equal(got.Body, row.Body) || got.Revision.String() != "1" || len(versions) != 1 || !versions[0].Base || versions[0].Deleted || versions[0].Revision.String() != "1" || !bytes.Equal(versions[0].Body, row.Body) {
					t.Fatalf("head readback lost exact record bytes or invented source history: id=%s row=%+v versions=%+v", row.Ref.ID, got, versions)
				}
			}
			if len(after.Versions[forgottenID]) != 0 {
				t.Fatal("head restore resurrected source-deleted history")
			}
			target.nextID = source.nextID
			target.bind() // Fresh public memory/brief server binds the NEW incarnation.
			for id, value := range want {
				value.Revision = model.Natural64(1)
				want[id] = value
			}
			target.reopenReadback("head-restored")
			target.readbackValues(want, beadID)
			target.readbackRefuse("memory.get", map[string]any{"id": forgottenID}, "", "not_found")
			target.readbackRefuse("history.get", map[string]any{"ref": ref(id), "asOf": map[string]any{"revision": first.Revision, "incarnation": sourceState.Meta.Incarnation}}, "", "incarnation_changed")
			target.readbackRefuse("memory.save", map[string]any{"key": "empty legacy ✓", "content": ""}, key("readback-empty-refused"), "invalid_params")
			target.readbackRefuse("memory.delete", map[string]any{"id": "b13-setting"}, key("readback-setting-refused"), "not_found")
			target.readbackRefuse("bead.get", map[string]any{"id": ref(id)}, "", "not_found")
			target.search("mIXeD kEY/ω CAFÉ", id, neighbor.Results[0].Ref.ID)
			target.search("current TEXT", id)
			target.search("config-only-decoy")
			planes := target.readbackState()
			params := map[string]any{"key": explicitKey, "content": readbackBody, "metadata": metadata}
			updated, updateReply := target.readbackAccept("memory.save", params, key("readback-target-replace"), model.OpPatch)
			if updated.Results[0].Ref.ID != id {
				t.Fatal("post-readback same-key replacement changed identity")
			}
			want[id] = memoryValue{ID: id, Key: explicitKey, Content: readbackBody, Metadata: metadata, Revision: updated.Revision}
			assertReadbackHistory(target, id, model.Natural64(1), replacedBody)
			target.readbackValues(want, beadID)
			target.readbackRefuse("memory.save", map[string]any{"key": explicitKey, "content": originalBody}, key("readback-target-replace"), "idempotency_key_reused")
			target.reopenReadback("target-replaced")
			target.replayReadback("memory.save", params, key("readback-target-replace"), updateReply)
			deleteParams := map[string]any{"id": id}
			_, deleteReply := target.readbackAccept("memory.delete", deleteParams, key("readback-target-delete"), model.OpDelete)
			target.readbackAccept("memory.delete", map[string]any{"id": "b13-empty"}, key("readback-target-delete-empty"), model.OpDelete)
			delete(want, id)
			delete(want, "b13-empty")
			target.reopenReadback("target-deleted")
			target.replayReadback("memory.save", params, key("readback-target-replace"), updateReply)
			target.replayReadback("memory.delete", deleteParams, key("readback-target-delete"), deleteReply)
			target.readbackRefuse("memory.get", map[string]any{"id": id}, "", "not_found")
			target.readbackRefuse("memory.get", map[string]any{"id": "b13-empty"}, "", "not_found")
			target.readbackValues(want, beadID)
			target.readbackOtherPlanes(planes, beadID)
			t.Logf("B13 public producer/consumer readback complete engine=%s; full-history capability remains refused; reference/full-card qualification excluded", engine)
		})
	}
}

func assertReadbackHistory(f *fixture, id string, revision model.Revision, content string) {
	f.t.Helper()
	before := f.readbackState()
	value := decode[struct {
		Body json.RawMessage `json:"body"`
	}](f.t, result(f.t, f.call("history.get", map[string]any{"ref": ref(id), "asOf": map[string]any{"revision": revision}}, "")))
	got := memoryRow(f.t, value.Body)
	if got.ID != id || got.Key != explicitKey || got.Content != content {
		f.t.Fatalf("readback history lost exact memory content: %+v", got)
	}
	requireSame(f.t, before, f.readbackState())
}
