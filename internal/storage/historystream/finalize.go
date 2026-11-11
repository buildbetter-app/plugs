package historystream

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/historyverify"
)

func (s *Stage) checkpoint(ctx context.Context, current bool) (historyverify.Root, error) {
	query := `SELECT ref,kind,size,hash FROM objects WHERE present=1 ORDER BY key`
	if current {
		query = `SELECT ref,kind,size,hash FROM current ORDER BY key`
	}
	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return historyverify.Root{}, err
	}
	defer rows.Close()
	h := historyverify.NewRootHasher()
	for rows.Next() {
		var raw []byte
		var ref model.Ref
		var kind, digest string
		var size uint64
		if err = rows.Scan(&raw, &kind, &size, &digest); err != nil {
			return historyverify.Root{}, err
		}
		if err = json.Unmarshal(raw, &ref); err != nil {
			return historyverify.Root{}, err
		}
		if err = h.Add(ref, kind, size, digest); err != nil {
			return historyverify.Root{}, err
		}
	}
	return h.Root(), rows.Err()
}

// Finalize consumes the independently rebuilt current image already staged by
// the trusted host through WriteTx.Put. Only this one current image is retained
// by the ordinary transaction; all history and mapping work is disk backed.
func (s *Stage) Finalize(ctx context.Context, h driver.ChangesetHeader, current map[string]*driver.Record, locks driver.LockSet, bumps []driver.LockKey) (err error) {
	if s.closed || s.final || s.failed != nil || s.commit != nil || s.table != nil {
		return invalid("incomplete, failed or consumed private stage")
	}
	s.final = true
	defer func() {
		if err != nil {
			s.failed = err
		}
	}()
	if err = ctx.Err(); err != nil {
		return err
	}
	if hex.EncodeToString(s.digest.Sum(nil)) != s.binding.Profile.HistoryDigest {
		return invalid("private stage transcript differs from the completed source")
	}
	head, tail, err := s.proof.Check(s.binding, s.binding.Profile.SourcePlanes != nil || s.binding.Header.Source == driver.NativeTableHistorySource)
	if err != nil {
		return err
	}
	actual, err := s.checkpoint(ctx, false)
	if err != nil {
		return err
	}
	if actual != head {
		return historyverify.ErrCheckpoint{Reason: "replayed HEAD differs from verified checkpoint"}
	}
	if err = s.binding.Profile.ValidateHeader(h); err != nil {
		return err
	}
	if h.Incarnation != s.binding.Meta.Incarnation || !h.Time.Equal(s.binding.When) || h.Response.Revision != s.binding.Meta.Head || !h.Branch.IsZero() {
		return invalid("final receipt differs from prepared target/time/head")
	}
	if err = driver.ValidateHeaderLocks(h.LockKeys, locks); err != nil {
		return err
	}
	marker := model.RefByID(driver.OfflineHistoryEvidenceID, model.Natural64(0))
	markerKey, _ := driver.EncodeLockKey(driver.LockKey{Kind: driver.LockRecord, Ref: &marker})
	for key, r := range current {
		if r == nil || key == markerKey || r.Ref.ID == driver.OfflineHistoryEvidenceID {
			return invalid("invalid independently rebuilt current record")
		}
		encoded, e := driver.EncodeLockKey(driver.LockKey{Kind: driver.LockRecord, Ref: &r.Ref})
		if e != nil || encoded != key || !r.Ref.Branch.IsZero() {
			return invalid("current record identity differs")
		}
		if e = validateJSON(ctx, bytesReader(r.Body)); e != nil {
			return e
		}
		if e = s.binding.Profile.ValidateRecord(*r, s.binding.When); e != nil {
			return e
		}
		raw, e := json.Marshal(r.Ref)
		if e != nil {
			return e
		}
		digest := sha256.Sum256(r.Body)
		// The joint commit record contains the newly allocated incarnation. It
		// is engine-time correspondence evidence, not an archived checkpoint
		// image; SourcePlaneImportPublication independently validates it.
		if s.binding.Profile.SourcePlanes == nil || r.Ref.ID != driver.SourcePlaneCommitRecordID {
			if _, e = s.db.ExecContext(ctx, `INSERT INTO current VALUES(?,?,?,?,?)`, key, raw, r.Kind, len(r.Body), hex.EncodeToString(digest[:])); e != nil {
				return e
			}
		}
		var kind, hash string
		var present bool
		var size int64
		e = s.db.QueryRowContext(ctx, `SELECT kind,present,size,hash FROM objects WHERE key=?`, key).Scan(&kind, &present, &size, &hash)
		if e == nil {
			if !present || kind != r.Kind || size != int64(len(r.Body)) || hash != hex.EncodeToString(digest[:]) {
				return invalid("rebuilt current image differs from replay")
			}
		} else if errors.Is(e, sql.ErrNoRows) {
			if s.binding.Header.Source != driver.NativeTableHistorySource && (r.Kind == "bead" || r.Kind == "memory") {
				return invalid("current business record has no source history")
			}
			image, e := s.appendBytes(r.Body)
			if e != nil {
				return e
			}
			if e = s.addRow(ctx, Row{Ref: r.Ref, Kind: r.Kind, Revision: s.binding.Meta.Head, Count: 1, Image: image}, model.OpCreate, model.Natural64(0)); e != nil {
				return e
			}
		} else {
			return e
		}
	}
	var missing bool
	if err = s.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM objects WHERE present=1 AND rev<>? AND NOT EXISTS(SELECT 1 FROM current WHERE current.key=objects.key))`, mustRevision(s.binding.Meta.Head)).Scan(&missing); err != nil {
		return err
	}
	if missing {
		return invalid("rebuilt current image omits a historical head record")
	}
	actual, err = s.checkpoint(ctx, true)
	if err != nil {
		return err
	}
	if actual != tail {
		return historyverify.ErrCheckpoint{Reason: "rebuilt current image differs from verified archive/derived checkpoint"}
	}
	seen := map[string]bool{}
	for _, c := range h.Changes {
		key, e := driver.EncodeLockKey(driver.LockKey{Kind: driver.LockRecord, Ref: &c.Ref})
		if e != nil {
			return e
		}
		if seen[key] || c.Op != model.OpCreate || !c.FromRevision.IsZero() || key != markerKey && current[key] == nil {
			return invalid("final declared creates differ from current image and marker")
		}
		seen[key] = true
	}
	if len(seen) != len(current)+1 || !seen[markerKey] {
		return invalid("final receipt omits current image or history marker")
	}
	image, err := s.evidence(ctx)
	if err != nil {
		return err
	}
	if err = s.addRow(ctx, Row{Ref: marker, Kind: "beads-archive", Revision: s.binding.Meta.Head, Count: 1, Image: image}, model.OpCreate, model.Natural64(0)); err != nil {
		return err
	}
	if err = s.checkLocks(ctx, locks, bumps); err != nil {
		return err
	}
	if err = s.entries(ctx); err != nil {
		return err
	}
	return nil
}

func mustRevision(r model.Revision) string { key, _ := revisionKey(r); return key }

func (s *Stage) CheckAuxiliary(a historyverify.Auxiliary) error {
	root, err := historyverify.AuxiliaryRoot(a)
	if err != nil {
		return err
	}
	return s.proof.CheckAuxiliary(root)
}

// byteSliceReader avoids a copy of the host's one current-image body.
type byteSliceReader struct{ data []byte }

func (r *byteSliceReader) Read(b []byte) (int, error) {
	if len(r.data) == 0 {
		return 0, io.EOF
	}
	n := copy(b, r.data)
	r.data = r.data[n:]
	return n, nil
}
func bytesReader(b []byte) io.Reader { return &byteSliceReader{b} }

func (s *Stage) appendBytes(data []byte) (Image, error) {
	off, err := s.file.Seek(0, io.SeekEnd)
	if err != nil {
		return Image{}, err
	}
	if _, err = s.file.Write(data); err != nil {
		return Image{}, err
	}
	h := sha256.Sum256(data)
	return Image{Offset: off, Bytes: int64(len(data)), SHA256: hex.EncodeToString(h[:])}, nil
}

func (s *Stage) checkLocks(ctx context.Context, locks driver.LockSet, bumps []driver.LockKey) error {
	held, bumped := map[string]bool{}, map[string]bool{}
	for _, r := range locks {
		if r.Mode != driver.LockShare {
			key, _ := driver.EncodeLockKey(r.Key)
			held[key] = true
		}
	}
	for _, k := range bumps {
		key, _ := driver.EncodeLockKey(k)
		bumped[key] = true
	}
	rows, err := s.db.QueryContext(ctx, `SELECT key,ref,kind FROM objects ORDER BY key`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var key, kind string
		var raw []byte
		var ref model.Ref
		if err = rows.Scan(&key, &raw, &kind); err != nil {
			return err
		}
		if !held[key] {
			return invalid("history record lacks update lock")
		}
		if kind == "bead" {
			if err = json.Unmarshal(raw, &ref); err != nil {
				return err
			}
			if !ref.IsID() {
				return invalid("history bead requires resolved ID")
			}
			sentinel, e := driver.NewBeadSentinel(ref.ID)
			if e != nil {
				return e
			}
			k, _ := driver.EncodeLockKey(driver.LockKey{Kind: driver.LockSentinel, Sentinel: sentinel})
			if !held[k] || !bumped[k] {
				return invalid("history bead lacks exact sentinel update lock/bump")
			}
		}
	}
	return rows.Err()
}

func (s *Stage) Rows(ctx context.Context, visit func(Row) error) error {
	if !s.final || s.failed != nil {
		return invalid("private stage is not publishable")
	}
	rows, err := s.db.QueryContext(ctx, `SELECT ref,kind,rev,deleted,count,off,size,hash FROM rows ORDER BY n`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var r Row
		var raw []byte
		var revision, count string
		var v uint64
		if err = rows.Scan(&raw, &r.Kind, &revision, &r.Deleted, &count, &r.Image.Offset, &r.Image.Bytes, &r.Image.SHA256); err != nil {
			return err
		}
		if err = json.Unmarshal(raw, &r.Ref); err != nil {
			return err
		}
		if _, err = fmt.Sscanf(revision, "%d", &v); err != nil {
			return err
		}
		r.Revision = model.Natural64(v)
		if _, err = fmt.Sscanf(count, "%d", &r.Count); err != nil {
			return err
		}
		if err = visit(r); err != nil {
			return err
		}
	}
	return rows.Err()
}

func (s *Stage) Entries(ctx context.Context, visit func(model.Revision, Image) error) error {
	if !s.final || s.failed != nil {
		return invalid("private entries are not publishable")
	}
	rows, err := s.db.QueryContext(ctx, `SELECT rev,off,size FROM entries ORDER BY rev`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var key string
		var image Image
		var v uint64
		if err = rows.Scan(&key, &image.Offset, &image.Bytes); err != nil {
			return err
		}
		if _, err = fmt.Sscanf(key, "%d", &v); err != nil {
			return err
		}
		if err = visit(model.Natural64(v), image); err != nil {
			return err
		}
	}
	return rows.Err()
}
