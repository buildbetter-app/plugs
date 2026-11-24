package restore_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/buildbetter-app/plugs/internal/cli"
	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/restore"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

type generatedBlob struct {
	size  int64
	value byte
}

type generatedBlobReader struct {
	remaining int64
	value     byte
	peak      *int
}

func (r *generatedBlobReader) Read(dst []byte) (int, error) {
	*r.peak = max(*r.peak, len(dst))
	if r.remaining == 0 {
		return 0, io.EOF
	}
	n := min(len(dst), int(r.remaining))
	for i := range dst[:n] {
		dst[i] = r.value
	}
	r.remaining -= int64(n)
	return n, nil
}

type reverseStreamStore struct {
	driver.Store
	blobs map[driver.Hash]generatedBlob
	peak  int
}

func (s *reverseStreamStore) Read(ctx context.Context, view driver.View) (driver.Reader, error) {
	r, err := s.Store.Read(ctx, view)
	if err != nil {
		return nil, err
	}
	return &reverseStreamSnapshot{Reader: r, AllocatorSnapshotReader: r.(driver.AllocatorSnapshotReader), source: s}, nil
}

type reverseStreamSnapshot struct {
	driver.Reader
	driver.AllocatorSnapshotReader
	source *reverseStreamStore
}

func (s *reverseStreamSnapshot) Query(ctx context.Context, q driver.Query) (driver.Page[driver.Record], error) {
	page, err := s.Reader.Query(ctx, q)
	if err != nil || page.NextCursor != nil {
		return page, err
	}
	members := map[string]dumpformat.File{}
	for hash, blob := range s.source.blobs {
		members["records/"+string(hash)+".ndjson"] = dumpformat.File{SHA256: string(hash), Size: blob.size}
	}
	body, err := json.Marshal(map[string]any{"members": members, "source_store": "public-test"})
	if err != nil {
		return page, err
	}
	page.Items = append(page.Items, driver.Record{
		Ref: model.RefByID("restore/archive", model.Natural64(0)), Kind: "restore-archive", Revision: page.AsOf,
		Body: body,
	})
	return page, nil
}

func (s *reverseStreamSnapshot) OpenBlob(_ context.Context, hash driver.Hash) (io.ReadCloser, int64, error) {
	blob, ok := s.source.blobs[hash]
	if !ok {
		return nil, 0, driver.ErrNotFound
	}
	return io.NopCloser(&generatedBlobReader{remaining: blob.size, value: blob.value, peak: &s.source.peak}), blob.size, nil
}

type measuredArchiveReader struct {
	io.ReaderAt
	peak int
}

func (r *measuredArchiveReader) ReadAt(dst []byte, off int64) (int, error) {
	r.peak = max(r.peak, len(dst))
	return r.ReaderAt.ReadAt(dst, off)
}

func TestReverseHeadStreamOverLegacyCap(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	source, err := cli.OpenLocal(ctx, cli.LocalConfig{
		Project: "fixture", Agent: "source", Storage: cli.StorageConfig{Driver: "provided"},
		PresenceVerifier: trustedPresenceVerifier(),
		Factory: func(context.Context, cli.StorageConfig) (driver.Store, func() error, error) {
			return store, store.Close, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	if _, err := source.Client.Call(ctx, "bead.create", map[string]any{
		"type": "issue", "fields": map[string]any{"title": "streamed public snapshot"},
	}, ""); err != nil {
		t.Fatal(err)
	}
	input := &reverseStreamStore{Store: store, blobs: map[driver.Hash]generatedBlob{}}
	for _, value := range []byte{1, 17, 255} {
		const size = 45 << 20
		h := sha256.New()
		part := bytes.Repeat([]byte{value}, 1<<20)
		for range size / len(part) {
			_, _ = h.Write(part)
		}
		input.blobs[driver.Hash(hex.EncodeToString(h.Sum(nil)))] = generatedBlob{size, value}
	}
	path := filepath.Join(t.TempDir(), "reverse-head.tar")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	h := sha256.New()
	manifest, err := restore.WriteReverseHead(ctx, input, io.MultiWriter(f, h))
	if err != nil {
		t.Fatal(err)
	}
	info, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() <= dumpformat.MaxArchiveBytes || input.peak == 0 || input.peak > 1<<20 {
		t.Fatalf("archive bytes=%d peak export copy=%d", info.Size(), input.peak)
	}
	target, err := (sqlite.Driver{}).Open(ctx, driver.Config{DSN: filepath.Join(t.TempDir(), "target.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	lease, err := target.Lease(ctx, "reverse-stream-test", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release(ctx)
	manager := restore.NewManager(target, lease, trustedPresenceVerifier())
	principal := model.PrincipalRef{Kind: "user", ID: "restore-owner"}
	raw, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	var overflow *restore.ArchiveTooLargeError
	_, err = manager.RestoreReverseHead(ctx, nil, restore.MaxReverseArchiveBytes+512, sha(raw), "", principal, model.WriteMeta{}, restore.Presence(presence))
	if !errors.As(err, &overflow) || overflow.Code() != "archive_too_large" || overflow.ArchiveBytes != restore.MaxReverseArchiveBytes+512 || overflow.LimitBytes != restore.MaxReverseArchiveBytes {
		t.Fatalf("typed restore limit differs: %v", err)
	}
	t.Logf("restore refusal code=%s archive bytes=%d limit bytes=%d", overflow.Code(), overflow.ArchiveBytes, overflow.LimitBytes)
	archive := &measuredArchiveReader{ReaderAt: f}
	_, err = manager.RestoreReverseHead(ctx, archive, info.Size(), sha(raw), hex.EncodeToString(h.Sum(nil)),
		principal, model.WriteMeta{}, restore.Presence(presence))
	if err != nil {
		t.Fatal(err)
	}
	if archive.peak == 0 || archive.peak > 4<<20 {
		t.Fatalf("peak native archive read request=%d", archive.peak)
	}
	meta, err := target.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	r, err := target.Read(ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	for hash, original := range input.blobs {
		reader, size, err := r.(driver.BlobStreamReader).OpenBlob(ctx, hash)
		if err != nil {
			t.Fatal(err)
		}
		got := sha256.New()
		n, err := io.Copy(got, reader)
		if closeErr := reader.Close(); size != original.size || n != size || errors.Join(err, closeErr) != nil || hex.EncodeToString(got.Sum(nil)) != string(hash) {
			t.Fatalf("restored blob bytes=%d expected=%d errors=%v %v", n, original.size, err, closeErr)
		}
	}
	t.Logf("archive bytes=%d export copy bound=%d restore read bound=%d", info.Size(), input.peak, archive.peak)

	input.blobs = map[driver.Hash]generatedBlob{}
	for i := range 17 {
		input.blobs[driver.Hash(sha([]byte{byte(i)}))] = generatedBlob{size: dumpformat.MaxArchiveBytes}
	}
	input.peak = 0
	var output bytes.Buffer
	_, err = restore.WriteReverseHead(ctx, input, &output)
	if !errors.As(err, &overflow) || overflow.Code() != "archive_too_large" || overflow.ArchiveBytes <= overflow.LimitBytes ||
		overflow.LimitBytes != restore.MaxReverseArchiveBytes || input.peak != 0 || output.Len() != 0 {
		t.Fatalf("typed export limit differs: error=%v reads=%d output=%d", err, input.peak, output.Len())
	}
	t.Logf("export refusal code=%s archive bytes=%d limit bytes=%d", overflow.Code(), overflow.ArchiveBytes, overflow.LimitBytes)
}
