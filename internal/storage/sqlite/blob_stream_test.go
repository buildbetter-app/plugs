package sqlite

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"io"
	"path/filepath"
	"testing"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestNativeReverseBlobStreamsPublishAtomically(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "public-blob-stream.sqlite")
	rawStore, err := (Driver{}).Open(ctx, driver.Config{DSN: path})
	if err != nil {
		t.Fatal(err)
	}
	store := rawStore.(*Store)
	defer store.Close()
	hash := func(raw []byte) driver.Hash {
		sum := sha256.Sum256(raw)
		return driver.Hash(hex.EncodeToString(sum[:]))
	}
	legacy := bytes.Repeat([]byte{0, 0xff, 17}, blobChunkBytes)
	legacyHash := hash(legacy)
	if _, err := store.db.ExecContext(ctx, `INSERT INTO blobs(hash,rev,data) VALUES(?,?,?)`, legacyHash, revKey(0), legacy); err != nil {
		t.Fatal(err)
	}
	chunked := bytes.Repeat([]byte{0xf0, 0, 3, 9}, blobChunkBytes/2+1)
	chunkedHash := hash(chunked)
	publish := func(expected driver.Hash) error {
		return store.immediate(ctx, nil, func(c *sql.Conn) error {
			return publishNativeRestoreBlobs(ctx, c, revKey(0), func(put func(driver.Hash, int64, io.Reader) error) error {
				return put(expected, int64(len(chunked)), bytes.NewReader(chunked))
			})
		})
	}
	wrong := hash([]byte("different bytes"))
	if err := publish(wrong); err == nil {
		t.Fatal("expected failed digest to roll back the blob and its chunks")
	}
	var occupied bool
	if err := store.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM blobs WHERE hash=?)
		OR EXISTS(SELECT 1 FROM sqlite_master WHERE name='blob_chunks')`, wrong).Scan(&occupied); err != nil || occupied {
		t.Fatalf("failed publication escaped its transaction: occupied=%t, err=%v", occupied, err)
	}
	if err := publish(chunkedHash); err != nil {
		t.Fatal(err)
	}
	var peak, count, sum int
	if err := store.db.QueryRowContext(ctx, `SELECT MAX(length(data)), COUNT(*), SUM(length(data))
		FROM blob_chunks WHERE hash=?`, chunkedHash).Scan(&peak, &count, &sum); err != nil {
		t.Fatal(err)
	}
	if peak != blobChunkBytes || count != 3 || sum != len(chunked) {
		t.Fatalf("storage chunk inventory differs: peak=%d count=%d bytes=%d", peak, count, sum)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := (Driver{}).Open(ctx, driver.Config{DSN: path})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	meta, err := reopened.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := reopened.Read(ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	buffer := make([]byte, blobChunkBytes+17)
	for _, fixture := range [][]byte{legacy, chunked} {
		want := hash(fixture)
		input, size, err := snapshot.(driver.BlobStreamReader).OpenBlob(ctx, want)
		if err != nil || size != int64(len(fixture)) {
			t.Fatalf("open bytes=%d err=%v", size, err)
		}
		h := sha256.New()
		for {
			n, readErr := input.Read(buffer)
			if n > blobChunkBytes {
				t.Fatalf("unbounded storage read of %d bytes", n)
			}
			_, _ = h.Write(buffer[:n])
			if readErr == io.EOF {
				break
			}
			if readErr != nil {
				t.Fatal(readErr)
			}
		}
		if err := input.Close(); err != nil || hex.EncodeToString(h.Sum(nil)) != string(want) {
			t.Fatalf("streamed readback differs: %v", err)
		}
		actual, err := snapshot.Blob(ctx, want)
		if err != nil || !bytes.Equal(actual, fixture) {
			t.Fatalf("ordinary blob readback differs: %v", err)
		}
	}
	legacyInput, _, err := snapshot.(driver.BlobStreamReader).OpenBlob(ctx, legacyHash)
	if err != nil {
		t.Fatal(err)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := legacyInput.Read(buffer); !errors.Is(err, driver.ErrTxClosed) {
		t.Fatalf("pinned reader did not close the legacy blob: %v", err)
	}
}
