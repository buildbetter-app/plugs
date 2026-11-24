package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"runtime"
	"unsafe"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"modernc.org/libc"
	sqlite3 "modernc.org/sqlite/lib"
)

const blobChunkBytes = 1 << 20

type blobInfo struct {
	row, size int64
	revision  string
	chunked   bool
}

// length(data) reads the column's size from the record header without
// fetching the overflow pages. Select only metadata in the pinned snapshot.
func (r *reader) blobInfo(ctx context.Context, hash driver.Hash) (blobInfo, error) {
	var info blobInfo
	var size sql.NullInt64
	query := `SELECT rowid, rev, length(data), NULL FROM blobs WHERE hash=? AND rev<=?`
	if r.meta.SchemaVersion == streamSchemaVersion {
		query = `SELECT rowid, rev, length(data), stream_size FROM blobs WHERE hash=? AND rev<=?`
	}
	err := r.tx.QueryRowContext(ctx, query,
		string(hash), revKey(r.at)).Scan(&info.row, &info.revision, &info.size, &size)
	if errors.Is(err, sql.ErrNoRows) {
		return blobInfo{}, driver.ErrNotFound
	}
	if size.Valid {
		info.chunked, info.size = true, size.Int64
	}
	return info, err
}

func (r *reader) Blob(ctx context.Context, hash driver.Hash) ([]byte, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.check(ctx); err != nil {
		return nil, err
	}
	if r.owner != nil {
		if data, ok := r.owner.blobs[hash]; ok {
			return bytes.Clone(data), nil
		}
	}
	info, err := r.blobInfo(ctx, hash)
	if err != nil {
		return nil, err
	}
	if !info.chunked {
		var data []byte
		err := r.tx.QueryRowContext(ctx, `SELECT data FROM blobs WHERE rowid=?`, info.row).Scan(&data)
		return data, err
	}
	data := make([]byte, info.size)
	for off := 0; off < len(data); off += blobChunkBytes {
		chunk, err := r.readBlobChunk(ctx, hash, int64(off/blobChunkBytes), info.size-int64(off))
		if err != nil {
			return nil, err
		}
		copy(data[off:], chunk)
	}
	return data, nil
}

func (r *reader) OpenBlob(ctx context.Context, hash driver.Hash) (io.ReadCloser, int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.check(ctx); err != nil {
		return nil, 0, err
	}
	if r.owner != nil {
		if data, ok := r.owner.blobs[hash]; ok {
			return io.NopCloser(bytes.NewReader(data)), int64(len(data)), nil
		}
	}
	info, err := r.blobInfo(ctx, hash)
	if err != nil {
		return nil, 0, err
	}
	if info.size == 0 {
		return io.NopCloser(bytes.NewReader(nil)), 0, nil
	}
	if info.chunked {
		return &chunkedBlob{reader: r, ctx: ctx, hash: hash, size: info.size}, info.size, nil
	}
	b := &legacyBlob{reader: r, ctx: ctx, size: info.size, tls: libc.NewTLS()}
	checkRow := fmt.Sprintf(`SELECT hash, rev FROM blobs WHERE rowid=%d`, info.row)
	if err := b.open("blobs", "data", info.row, checkRow, string(hash), info.revision); err != nil {
		return nil, 0, errors.Join(err, b.close())
	}
	if r.legacyBlobs == nil {
		r.legacyBlobs = make(map[*legacyBlob]struct{})
	}
	r.legacyBlobs[b] = struct{}{}
	return b, info.size, nil
}

func (r *reader) readBlobChunk(ctx context.Context, hash driver.Hash, ordinal, remaining int64) ([]byte, error) {
	// The length filter also bounds driver allocations on a damaged store.
	size := min(remaining, blobChunkBytes)
	var data []byte
	err := r.tx.QueryRowContext(ctx, `SELECT data FROM blob_chunks WHERE hash=? AND ordinal=? AND length(data)=?`,
		string(hash), ordinal, size).Scan(&data)
	if errors.Is(err, sql.ErrNoRows) {
		err = io.ErrUnexpectedEOF
	}
	return data, err
}

type chunkedBlob struct {
	reader    *reader
	ctx       context.Context
	hash      driver.Hash
	size      int64
	offset    int64
	remainder []byte
	closed    bool
}

func (b *chunkedBlob) Read(dst []byte) (int, error) {
	b.reader.mu.Lock()
	defer b.reader.mu.Unlock()
	if b.closed {
		return 0, driver.ErrTxClosed
	}
	if err := b.reader.check(b.ctx); err != nil {
		return 0, err
	}
	if len(dst) == 0 {
		return 0, nil
	}
	if b.offset == b.size {
		return 0, io.EOF
	}
	if len(b.remainder) == 0 {
		data, err := b.reader.readBlobChunk(b.ctx, b.hash, b.offset/blobChunkBytes, b.size-b.offset)
		if err != nil {
			return 0, err
		}
		b.remainder = data
	}
	n := copy(dst, b.remainder)
	b.remainder = b.remainder[n:]
	b.offset += int64(n)
	return n, nil
}

func (b *chunkedBlob) Close() error {
	b.reader.mu.Lock()
	defer b.reader.mu.Unlock()
	b.closed, b.remainder = true, nil
	return nil
}

// The SQL driver has no incremental BLOB interface. Legacy values are
// immutable and are never deleted. After the pinned SQL lookup admits one,
// use a second read-only SQLite transaction to bind the same row, hash and
// publishing revision. Incremental reads then use that handle's WAL snapshot.
type legacyBlob struct {
	reader       *reader
	ctx          context.Context
	size, offset int64
	tls          *libc.TLS
	db, handle   uintptr
	// C out parameters may not point at a Go stack slot whose address could
	// change while the translated SQLite code is executing.
	output uintptr
}

func (b *legacyBlob) open(table, column string, row int64, checkRow string, identity ...string) error {
	b.output = libc.Xcalloc(b.tls, 1, 8)
	if b.output == 0 {
		return fmt.Errorf("sqlite: allocate incremental blob handle")
	}
	path, err := libc.CString(b.reader.store.path)
	if err != nil {
		return err
	}
	defer libc.Xfree(b.tls, path)
	rc := sqlite3.Xsqlite3_open_v2(b.tls, path, b.output, sqlite3.SQLITE_OPEN_READONLY, 0)
	b.db = *(*uintptr)(unsafe.Pointer(b.output))
	if rc != sqlite3.SQLITE_OK {
		return b.err("open", rc)
	}
	begin, err := libc.CString("BEGIN")
	if err != nil {
		return err
	}
	defer libc.Xfree(b.tls, begin)
	if rc := sqlite3.Xsqlite3_exec(b.tls, b.db, begin, 0, 0, 0); rc != sqlite3.SQLITE_OK {
		return b.err("begin", rc)
	}
	if err := b.verifyRow(checkRow, identity); err != nil {
		return err
	}
	var names []uintptr
	for _, name := range []string{"main", table, column} {
		p, err := libc.CString(name)
		if err != nil {
			return err
		}
		defer libc.Xfree(b.tls, p)
		names = append(names, p)
	}
	rc = sqlite3.Xsqlite3_blob_open(b.tls, b.db, names[0], names[1], names[2], row, 0, b.output)
	if rc != sqlite3.SQLITE_OK {
		return b.err("bind", rc)
	}
	b.handle = *(*uintptr)(unsafe.Pointer(b.output))
	if int64(sqlite3.Xsqlite3_blob_bytes(b.tls, b.handle)) != b.size {
		return fmt.Errorf("sqlite: incremental blob size changed")
	}
	return nil
}

func (b *legacyBlob) verifyRow(checkRow string, identity []string) error {
	query, err := libc.CString(checkRow)
	if err != nil {
		return err
	}
	defer libc.Xfree(b.tls, query)
	rc := sqlite3.Xsqlite3_prepare_v2(b.tls, b.db, query, -1, b.output, 0)
	if rc != sqlite3.SQLITE_OK {
		return b.err("prepare", rc)
	}
	stmt := *(*uintptr)(unsafe.Pointer(b.output))
	defer sqlite3.Xsqlite3_finalize(b.tls, stmt)
	rc = sqlite3.Xsqlite3_step(b.tls, stmt)
	if rc != sqlite3.SQLITE_ROW {
		return b.err("read identity", rc)
	}
	for col, value := range identity {
		if libc.GoString(sqlite3.Xsqlite3_column_text(b.tls, stmt, int32(col))) != value {
			return fmt.Errorf("sqlite: incremental blob identity changed")
		}
	}
	return nil
}

func (b *legacyBlob) err(operation string, rc int32) error {
	return fmt.Errorf("sqlite: incremental blob %s: %s (%d)", operation, libc.GoString(sqlite3.Xsqlite3_errstr(b.tls, rc)), rc)
}

func (b *legacyBlob) Read(dst []byte) (int, error) {
	b.reader.mu.Lock()
	defer b.reader.mu.Unlock()
	return b.readLocked(dst)
}

func (b *legacyBlob) readLocked(dst []byte) (int, error) {
	if b.handle == 0 {
		return 0, driver.ErrTxClosed
	}
	if err := b.reader.check(b.ctx); err != nil {
		return 0, err
	}
	if len(dst) == 0 {
		return 0, nil
	}
	if b.offset == b.size {
		return 0, io.EOF
	}
	n := min(int64(len(dst)), b.size-b.offset, blobChunkBytes)
	var pin runtime.Pinner
	pin.Pin(&dst[0])
	rc := sqlite3.Xsqlite3_blob_read(b.tls, b.handle, uintptr(unsafe.Pointer(&dst[0])), int32(n), int32(b.offset))
	pin.Unpin()
	if rc != sqlite3.SQLITE_OK {
		return 0, b.err("read", rc)
	}
	b.offset += n
	return int(n), nil
}

func (b *legacyBlob) Close() error {
	b.reader.mu.Lock()
	defer b.reader.mu.Unlock()
	return b.close()
}

func (b *legacyBlob) close() error {
	var err error
	if b.handle != 0 {
		if rc := sqlite3.Xsqlite3_blob_close(b.tls, b.handle); rc != sqlite3.SQLITE_OK {
			err = b.err("close blob", rc)
		}
		b.handle = 0
	}
	if b.db != 0 {
		if rc := sqlite3.Xsqlite3_close_v2(b.tls, b.db); rc != sqlite3.SQLITE_OK {
			err = errors.Join(err, b.err("close database", rc))
		}
		b.db = 0
	}
	if b.tls != nil {
		libc.Xfree(b.tls, b.output)
		b.tls.Close()
		b.tls = nil
	}
	delete(b.reader.legacyBlobs, b)
	return err
}

func (r *reader) closeLegacyBlobs() error {
	var err error
	for blob := range r.legacyBlobs {
		err = errors.Join(err, blob.close())
	}
	return err
}

var _ driver.BlobStreamReader = (*reader)(nil)
