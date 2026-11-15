// Package historystream implements engine-private, disk-backed replay staging.
// It never builds OfflineHistory, a history plan, or an aggregate mapping slice.
package historystream

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"math"
	"os"
	"path/filepath"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/historyverify"
	_ "modernc.org/sqlite"
)

const PageBytes = 32 * 1024

// Image is a private spool extent, never caller-supplied authority.
type Image struct {
	Offset, Bytes int64
	SHA256        string
}

type Row struct {
	Ref      model.Ref
	Kind     string
	Revision model.Revision
	Deleted  bool
	Count    uint64
	Image    Image
}

// Stage caches physical-preimage admission from New. Rechecking the
// checkpoint's request hash for every physical frame would rescan the entire
// canonical source-plane plan, which can be tens of megabytes.
type Stage struct {
	dir           string
	db            *sql.DB
	file          *os.File
	binding       driver.OfflineHistoryStreamBinding
	proof         historyverify.Checkpoint
	physical      bool
	digest        hash.Hash
	commit        *driver.OfflineHistoryStreamCommit
	revision      model.Revision
	table         *driver.OfflineHistoryStreamTable
	images        [2]Image
	hashes        [2]hash.Hash
	counts        [2]uint64
	closed, final bool
	closeErr      error
	failed        error
}

func invalid(reason string) error {
	return fmt.Errorf("%w: %s", driver.ErrOfflineHistoryStream, reason)
}

func New(ctx context.Context, binding driver.OfflineHistoryStreamBinding, proof historyverify.Checkpoint) (_ *Stage, err error) {
	physical := binding.Header.Source == driver.NativeTableHistorySource || binding.Profile.SourcePlanes != nil
	if _, _, err = proof.Check(binding, physical); err != nil {
		return nil, err
	}
	s := &Stage{binding: binding, proof: proof, physical: physical, digest: sha256.New()}
	s.dir, err = os.MkdirTemp("", "plug-history-stage-")
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			_ = s.Close()
		}
	}()
	s.file, err = os.OpenFile(filepath.Join(s.dir, "images"), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	s.db, err = sql.Open("sqlite", filepath.Join(s.dir, "index"))
	if err != nil {
		return nil, err
	}
	s.db.SetMaxOpenConns(4)
	// The scratch index is disposable. WAL permits the indexed replay cursor
	// and bounded entry inserts to overlap without competing rollback locks.
	// Its journal is not publication durability: only the real engine commits.
	_, err = s.db.ExecContext(ctx, `PRAGMA journal_mode=WAL; PRAGMA synchronous=OFF; PRAGMA cache_size=-1024; PRAGMA temp_store=FILE;
CREATE TABLE objects(key TEXT PRIMARY KEY, ref BLOB NOT NULL, kind TEXT NOT NULL, rev TEXT NOT NULL, count TEXT NOT NULL, present INTEGER NOT NULL, off INTEGER NOT NULL, size INTEGER NOT NULL, hash TEXT NOT NULL);
CREATE TABLE rows(n INTEGER PRIMARY KEY, key TEXT NOT NULL, ref BLOB NOT NULL, kind TEXT NOT NULL, rev TEXT NOT NULL, deleted INTEGER NOT NULL, count TEXT NOT NULL, off INTEGER NOT NULL, size INTEGER NOT NULL, hash TEXT NOT NULL, op TEXT NOT NULL, previous TEXT NOT NULL, UNIQUE(key,rev));
CREATE INDEX row_revision ON rows(rev,n);
CREATE TABLE commits(rev TEXT PRIMARY KEY, data BLOB NOT NULL);
CREATE TABLE frames(n INTEGER PRIMARY KEY, rev TEXT NOT NULL, kind TEXT NOT NULL, data BLOB NOT NULL);
CREATE TABLE ids(n INTEGER PRIMARY KEY, rev TEXT NOT NULL, scope TEXT NOT NULL, kind TEXT NOT NULL, value TEXT NOT NULL, UNIQUE(scope,kind,value));
CREATE TABLE current(key TEXT PRIMARY KEY, ref BLOB NOT NULL, kind TEXT NOT NULL, size INTEGER NOT NULL, hash TEXT NOT NULL);
CREATE TABLE entries(rev TEXT PRIMARY KEY, off INTEGER NOT NULL, size INTEGER NOT NULL);`)
	if err != nil {
		return nil, err
	}
	_, _ = s.digest.Write([]byte(driver.OfflineHistoryStreamVersion + "\n"))
	raw, err := json.Marshal(binding.Header)
	if err != nil {
		return nil, err
	}
	_, _ = s.digest.Write(raw)
	_, _ = s.digest.Write([]byte{'\n'})
	return s, nil
}

// Close is independent of request cancellation, idempotent, and only removes
// private resources. It has no engine/receipt handle and cannot undo publication.
func (s *Stage) Close() error {
	if s == nil {
		return nil
	}
	if s.closed {
		return s.closeErr
	}
	s.closed = true
	if s.db != nil {
		s.closeErr = errors.Join(s.closeErr, s.db.Close())
	}
	if s.file != nil {
		s.closeErr = errors.Join(s.closeErr, s.file.Close())
	}
	if s.dir != "" {
		s.closeErr = errors.Join(s.closeErr, os.RemoveAll(s.dir))
	}
	return s.closeErr
}

func revisionKey(r model.Revision) (string, error) {
	v, err := r.Uint64()
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%020d", v), nil
}

func (s *Stage) Stage(ctx context.Context, revision model.Revision, f driver.OfflineHistoryStreamFrame) (err error) {
	if s.closed || s.final {
		return invalid("staging is closed or finalized")
	}
	if s.failed != nil {
		return s.failed
	}
	defer func() {
		if err != nil {
			s.failed = err
		}
	}()
	if err = ctx.Err(); err != nil {
		return err
	}
	raw, err := json.Marshal(f)
	if err != nil {
		return err
	}
	_, _ = s.digest.Write(raw)
	_, _ = s.digest.Write([]byte{'\n'})
	key, err := revisionKey(revision)
	if err != nil {
		return err
	}
	if f.Kind != "chunk" {
		if _, err = s.db.ExecContext(ctx, `INSERT INTO frames(rev,kind,data) VALUES(?,?,?)`, key, f.Kind, raw); err != nil {
			return err
		}
	}
	switch f.Kind {
	case "gap", "boundary":
		if s.commit != nil || s.table != nil {
			return invalid("boundary inside commit")
		}
	case "commit":
		if s.commit != nil || f.Commit == nil {
			return invalid("nested or missing commit")
		}
		if f.Commit.Physical != nil {
			if !s.physical {
				return historyverify.ErrCheckpoint{Reason: "physical preimages were not reconciled"}
			}
		}
		value := *f.Commit
		s.commit, s.revision = &value, revision
		data, err := json.Marshal(value)
		if err != nil {
			return err
		}
		_, err = s.db.ExecContext(ctx, `INSERT INTO commits(rev,data) VALUES(?,?)`, key, data)
		return err
	case "id":
		if s.commit == nil || f.ID == nil || s.table != nil {
			return invalid("ID without commit")
		}
		// Commit/event identities are unique within their original source scope;
		// ordered parents may be reused by later commits but not within one list.
		scope := s.commit.SourceStoreID + "\x00" + s.commit.Project
		if f.ID.Kind == "parent" {
			scope += "\x00" + key
		}
		_, err = s.db.ExecContext(ctx, `INSERT INTO ids(rev,scope,kind,value) VALUES(?,?,?,?)`, key, scope, f.ID.Kind, f.ID.Value)
		if err != nil {
			return invalid("duplicate source provenance")
		}
	case "table":
		if s.commit == nil || s.table != nil || f.Table == nil {
			return invalid("table outside commit")
		}
		if f.Table.PhysicalBefore != nil || f.Table.PhysicalAfter != nil {
			if !s.physical {
				return historyverify.ErrCheckpoint{Reason: "physical preimages were not reconciled"}
			}
		}
		t := *f.Table
		s.table = &t
		s.images = [2]Image{{Offset: -1}, {Offset: -1}}
		s.counts = [2]uint64{}
		s.hashes = [2]hash.Hash{sha256.New(), sha256.New()}
	case "chunk":
		if s.table == nil || f.Chunk == nil {
			return invalid("chunk outside image")
		}
		i := 0
		if f.Chunk.Image == "after" {
			i = 1
		} else if f.Chunk.Image != "before" {
			return invalid("unknown image")
		}
		decl := s.table.Before
		if i == 1 {
			decl = s.table.After
		}
		if decl == nil || f.Chunk.Offset != s.counts[i] || uint64(len(f.Chunk.Data)) > decl.Bytes-s.counts[i] {
			return invalid("image offset/size differs")
		}
		off, err := s.file.Seek(0, io.SeekEnd)
		if err != nil {
			return err
		}
		if s.images[i].Offset < 0 {
			s.images[i].Offset = off
		}
		if _, err = s.file.Write(f.Chunk.Data); err != nil {
			return err
		}
		_, _ = s.hashes[i].Write(f.Chunk.Data)
		s.counts[i] += uint64(len(f.Chunk.Data))
	case "table-end":
		return s.endTable(ctx)
	case "commit-end":
		if s.commit == nil || s.table != nil {
			return invalid("incomplete commit")
		}
		s.commit = nil
	default:
		return invalid("unsupported stage frame")
	}
	return nil
}

func (s *Stage) endTable(ctx context.Context) error {
	t := s.table
	if t == nil {
		return invalid("table end without table")
	}
	for i, decl := range []*driver.OfflineHistoryStreamImage{t.Before, t.After} {
		if decl == nil {
			continue
		}
		if s.counts[i] != decl.Bytes || decl.Bytes > math.MaxInt64 || hex.EncodeToString(s.hashes[i].Sum(nil)) != decl.SHA256 {
			return invalid("image bytes/hash differ")
		}
		s.images[i].Bytes = int64(decl.Bytes)
		s.images[i].SHA256 = decl.SHA256
		if err := validateJSON(ctx, s.Reader(s.images[i])); err != nil {
			return err
		}
	}
	id, err := driver.EncodeLockKey(driver.LockKey{Kind: driver.LockRecord, Ref: &t.Ref})
	if err != nil {
		return err
	}
	var kind, rev, count, oldHash string
	var present bool
	var old Image
	err = s.db.QueryRowContext(ctx, `SELECT kind,rev,count,present,off,size,hash FROM objects WHERE key=?`, id).Scan(&kind, &rev, &count, &present, &old.Offset, &old.Bytes, &oldHash)
	known := err == nil
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if known && kind != t.Kind {
		return invalid("history changes record kind")
	}
	preimage := false
	if t.Before != nil && present && old.Bytes == s.images[0].Bytes && oldHash == s.images[0].SHA256 {
		preimage, err = s.same(ctx, old, s.images[0])
		if err != nil {
			return err
		}
	}
	if t.Before == nil && present || t.Before != nil && !preimage {
		return invalid("unproven replay preimage")
	}
	if t.Before != nil && t.After != nil {
		same, e := s.same(ctx, s.images[0], s.images[1])
		if e != nil {
			return e
		}
		if same {
			return invalid("source delta has no durable change")
		}
	}
	if t.After != nil && t.Kind == "bead" {
		if err = s.validateHistoricalRecord(ctx, s.images[1]); err != nil {
			return err
		}
	}
	previous := model.Natural64(0)
	var n uint64
	if known {
		if err = json.Unmarshal([]byte(count), &n); err != nil {
			return err
		}
		var v uint64
		if _, err = fmt.Sscanf(rev, "%d", &v); err != nil {
			return err
		}
		previous = model.Natural64(v)
	}
	if n == math.MaxUint64 {
		return invalid("version count exhausted")
	}
	n++
	image := s.images[1]
	op := model.OpPatch
	if t.Before == nil {
		op = model.OpCreate
	}
	if t.After == nil {
		image = Image{Offset: -1}
		op = model.OpDelete
	}
	if err = s.addRow(ctx, Row{Ref: t.Ref, Kind: t.Kind, Revision: s.revision, Deleted: t.After == nil, Count: n, Image: image}, op, previous); err != nil {
		return err
	}
	s.table = nil
	return nil
}

func (s *Stage) addRow(ctx context.Context, r Row, op model.OpKind, previous model.Revision) error {
	id, err := driver.EncodeLockKey(driver.LockKey{Kind: driver.LockRecord, Ref: &r.Ref})
	if err != nil {
		return err
	}
	ref, err := json.Marshal(r.Ref)
	if err != nil {
		return err
	}
	rev, err := revisionKey(r.Revision)
	if err != nil {
		return err
	}
	count := fmt.Sprint(r.Count)
	if _, err = s.db.ExecContext(ctx, `INSERT INTO rows(key,ref,kind,rev,deleted,count,off,size,hash,op,previous) VALUES(?,?,?,?,?,?,?,?,?,?,?)`, id, ref, r.Kind, rev, r.Deleted, count, r.Image.Offset, r.Image.Bytes, r.Image.SHA256, string(op), previous.String()); err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO objects VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(key) DO UPDATE SET rev=excluded.rev,count=excluded.count,present=excluded.present,off=excluded.off,size=excluded.size,hash=excluded.hash`, id, ref, r.Kind, rev, count, !r.Deleted, r.Image.Offset, r.Image.Bytes, r.Image.SHA256)
	return err
}

func (s *Stage) Reader(image Image) io.Reader {
	return io.NewSectionReader(s.file, image.Offset, image.Bytes)
}

func (s *Stage) same(ctx context.Context, a, b Image) (bool, error) {
	if a.Bytes != b.Bytes || a.Offset < 0 || b.Offset < 0 {
		return false, nil
	}
	aR, bR := ContextReader(ctx, s.Reader(a)), ContextReader(ctx, s.Reader(b))
	left, right := make([]byte, PageBytes), make([]byte, PageBytes)
	for {
		n, e := aR.Read(left)
		m, f := bR.Read(right)
		if e != nil && e != io.EOF {
			return false, e
		}
		if f != nil && f != io.EOF {
			return false, f
		}
		if n != m || !bytes.Equal(left[:n], right[:m]) {
			return false, nil
		}
		if e != nil || f != nil {
			return e == io.EOF && f == io.EOF, nil
		}
	}
}

// Copy hands bounded borrowed pages to an engine encoder/writer. SQL adapters
// never materialize an image, changeset or final mapping document in Go memory.
func (s *Stage) Copy(ctx context.Context, image Image, write func([]byte) error) error {
	r := s.Reader(image)
	buf := make([]byte, PageBytes)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		n, err := r.Read(buf)
		if n > 0 {
			if e := write(buf[:n]); e != nil {
				return e
			}
		}
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

// WithImage gives one engine write a bounded image buffer. Native publication
// carries the existing decoded budget in ctx; reserve the complete SQL value
// for the duration of the write so the one-write path cannot outrun that
// ceiling. The hard ceiling also bounds callers that do not carry accounting.
func (s *Stage) WithImage(ctx context.Context, image Image, write func([]byte) error) error {
	if image.Bytes < 0 || image.Offset < 0 {
		return invalid("invalid staged image")
	}
	const maxPublicationImageBytes = uint64(2 << 30)
	bytes := uint64(image.Bytes)
	if bytes > maxPublicationImageBytes || bytes > uint64(^uint(0)>>1) {
		return invalid("staged image exceeds publication budget")
	}
	budget := dumpformat.NativeDeltaBudgetFromContext(ctx)
	if budget != nil {
		if err := budget.ChargeBytes("offline_publication_image", bytes); err != nil {
			return err
		}
		defer budget.ReleaseBytes("offline_publication_image", bytes)
	}
	data := make([]byte, int(bytes))
	if _, err := io.ReadFull(ContextReader(ctx, s.Reader(image)), data); err != nil {
		return err
	}
	return write(data)
}
