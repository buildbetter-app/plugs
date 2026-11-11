package historystream

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func writeJSON(w io.Writer, v any) error {
	raw, err := json.Marshal(v)
	if err != nil {
		return err
	}
	_, err = w.Write(raw)
	return err
}
func literal(w io.Writer, text string) error { _, err := io.WriteString(w, text); return err }

func (s *Stage) encoded(write func(io.Writer) error) (Image, error) {
	off, err := s.file.Seek(0, io.SeekEnd)
	if err != nil {
		return Image{}, err
	}
	h := sha256.New()
	w := io.MultiWriter(s.file, h)
	if err = write(w); err != nil {
		return Image{}, err
	}
	end, err := s.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return Image{}, err
	}
	return Image{Offset: off, Bytes: end - off, SHA256: hex.EncodeToString(h.Sum(nil))}, nil
}

func (s *Stage) encodeBody(ctx context.Context, value any, body Image) (Image, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return Image{}, err
	}
	if body.Offset < 0 {
		return s.appendBytes(raw)
	}
	field := []byte(`"Body":null`)
	at := bytes.Index(raw, field)
	if at < 0 {
		return Image{}, invalid("missing body encoding slot")
	}
	return s.encoded(func(w io.Writer) error {
		if _, err := w.Write(raw[:at]); err != nil {
			return err
		}
		if err := literal(w, `"Body":"`); err != nil {
			return err
		}
		encoder := base64.NewEncoder(base64.StdEncoding, w)
		err := s.Copy(ctx, body, func(part []byte) error { _, e := encoder.Write(part); return e })
		if err != nil {
			return err
		}
		if err = encoder.Close(); err != nil {
			return err
		}
		if err = literal(w, `"`); err != nil {
			return err
		}
		_, err = w.Write(raw[at+len(field):])
		return err
	})
}

func (s *Stage) RecordJSON(ctx context.Context, row Row) (Image, error) {
	return s.encodeBody(ctx, driver.Record{Ref: row.Ref, Kind: row.Kind, Revision: row.Revision}, row.Image)
}
func (s *Stage) VersionJSON(ctx context.Context, row Row) (Image, error) {
	return s.encodeBody(ctx, driver.Version{Ref: row.Ref, Revision: row.Revision, Base: true, Deleted: row.Deleted, CreatedAt: s.binding.When, Principal: s.binding.Profile.Admission.Actor}, row.Image)
}

func (s *Stage) evidence(ctx context.Context) (Image, error) {
	return s.encoded(func(w io.Writer) error {
		base := struct {
			Version  string                    `json:"streamVersion"`
			Source   string                    `json:"source"`
			Mode     driver.RestoreHistoryMode `json:"mode"`
			Complete bool                      `json:"complete"`
			First    uint64                    `json:"first"`
			Head     uint64                    `json:"head"`
			Floor    model.Revision            `json:"floor"`
			Archive  string                    `json:"archiveSha256"`
			Derived  string                    `json:"derivedSha256"`
			History  string                    `json:"historySha256"`
		}{driver.OfflineHistoryStreamVersion, s.binding.Header.Source, s.binding.Profile.HistoryMode, s.binding.Header.Complete, s.binding.Header.First, s.binding.Header.Head, s.binding.Meta.HistoryFloor, s.binding.Profile.ArchiveDigest, s.binding.Profile.DerivedDigest, s.binding.Profile.HistoryDigest}
		raw, err := json.Marshal(base)
		if err != nil {
			return err
		}
		if _, err = w.Write(raw[:len(raw)-1]); err != nil {
			return err
		}
		if err = literal(w, `,"gaps":[`); err != nil {
			return err
		}
		if err = s.framePayloads(ctx, w, "gap"); err != nil {
			return err
		}
		if err = literal(w, `],"nativeBoundaries":[`); err != nil {
			return err
		}
		if err = s.framePayloads(ctx, w, "boundary"); err != nil {
			return err
		}
		if err = literal(w, `],"mapping":[`); err != nil {
			return err
		}
		rows, err := s.db.QueryContext(ctx, `SELECT rev,data FROM commits ORDER BY rev`)
		if err != nil {
			return err
		}
		first := true
		for rows.Next() {
			var key string
			var data []byte
			var c driver.OfflineHistoryStreamCommit
			var rev uint64
			if err = rows.Scan(&key, &data); err != nil {
				break
			}
			if err = json.Unmarshal(data, &c); err != nil {
				break
			}
			if c.Baseline {
				continue
			}
			if _, err = fmt.Sscanf(key, "%d", &rev); err != nil {
				break
			}
			if !first {
				if err = literal(w, ","); err != nil {
					break
				}
			}
			first = false
			mapping := driver.SourceImportRevision{SourceStoreID: c.SourceStoreID, SourceSequence: c.SourceSequence, Project: c.Project, Sequence: c.Sequence, Revision: model.Natural64(rev)}
			part, e := json.Marshal(mapping)
			if e != nil {
				err = e
				break
			}
			// Replace the two nil ID slices with bounded ordered queries; ordered
			// parents are additional retained provenance, never revision authority.
			part = bytes.TrimSuffix(part, []byte("}"))
			at := bytes.Index(part, []byte(`,"eventIds":`))
			if at < 0 {
				err = invalid("mapping encoding changed")
				break
			}
			part = part[:at]
			if _, err = w.Write(part); err != nil {
				break
			}
			for _, kind := range []string{"event", "commit", "parent"} {
				name := kind + "Ids"
				if err = literal(w, `,"`+name+`":[`); err != nil {
					break
				}
				if err = s.idValues(ctx, w, key, kind); err != nil {
					break
				}
				if err = literal(w, "]"); err != nil {
					break
				}
			}
			if err != nil {
				break
			}
			if err = literal(w, "}"); err != nil {
				break
			}
		}
		if err == nil {
			err = rows.Err()
		}
		_ = rows.Close()
		if err != nil {
			return err
		}
		if err = literal(w, `],"sourceFrames":[`); err != nil {
			return err
		}
		frames, err := s.db.QueryContext(ctx, `SELECT rev,data FROM frames ORDER BY n`)
		if err != nil {
			return err
		}
		defer frames.Close()
		first = true
		for frames.Next() {
			var key string
			var frame []byte
			var rev uint64
			if err = frames.Scan(&key, &frame); err != nil {
				return err
			}
			if _, err = fmt.Sscanf(key, "%d", &rev); err != nil {
				return err
			}
			if !first {
				if err = literal(w, ","); err != nil {
					return err
				}
			}
			first = false
			if err = literal(w, `{"revision":`); err != nil {
				return err
			}
			if err = writeJSON(w, model.Natural64(rev)); err != nil {
				return err
			}
			if err = literal(w, `,"frame":`); err != nil {
				return err
			}
			if _, err = w.Write(frame); err != nil {
				return err
			}
			if err = literal(w, "}"); err != nil {
				return err
			}
		}
		if err = frames.Err(); err != nil {
			return err
		}
		return literal(w, "]}")
	})
}

func (s *Stage) framePayloads(ctx context.Context, w io.Writer, kind string) error {
	rows, err := s.db.QueryContext(ctx, `SELECT data FROM frames WHERE kind=? ORDER BY n`, kind)
	if err != nil {
		return err
	}
	defer rows.Close()
	first := true
	for rows.Next() {
		var data []byte
		var f driver.OfflineHistoryStreamFrame
		if err = rows.Scan(&data); err != nil {
			return err
		}
		if err = json.Unmarshal(data, &f); err != nil {
			return err
		}
		if !first {
			if err = literal(w, ","); err != nil {
				return err
			}
		}
		first = false
		if kind == "gap" {
			err = writeJSON(w, f.Gap)
		} else {
			err = writeJSON(w, f.Boundary)
		}
		if err != nil {
			return err
		}
	}
	return rows.Err()
}

func (s *Stage) idValues(ctx context.Context, w io.Writer, revision, kind string) error {
	rows, err := s.db.QueryContext(ctx, `SELECT value FROM ids WHERE rev=? AND kind=? ORDER BY n`, revision, kind)
	if err != nil {
		return err
	}
	defer rows.Close()
	first := true
	for rows.Next() {
		var value string
		if err = rows.Scan(&value); err != nil {
			return err
		}
		if !first {
			if err = literal(w, ","); err != nil {
				return err
			}
		}
		first = false
		if err = writeJSON(w, value); err != nil {
			return err
		}
	}
	return rows.Err()
}

func (s *Stage) entries(ctx context.Context) error {
	rows, err := s.db.QueryContext(ctx, `SELECT rev FROM commits UNION SELECT ? ORDER BY rev`, mustRevision(s.binding.Meta.Head))
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var key string
		var rev uint64
		if err = rows.Scan(&key); err != nil {
			return err
		}
		if _, err = fmt.Sscanf(key, "%d", &rev); err != nil {
			return err
		}
		image, err := s.entry(ctx, model.Natural64(rev))
		if err != nil {
			return err
		}
		if _, err = s.db.ExecContext(ctx, `INSERT INTO entries VALUES(?,?,?)`, key, image.Offset, image.Bytes); err != nil {
			return err
		}
	}
	return rows.Err()
}

func (s *Stage) entry(ctx context.Context, revision model.Revision) (Image, error) {
	return s.encoded(func(w io.Writer) error {
		entry := model.Changeset{Revision: revision, Branch: model.Natural64(0), Incarnation: s.binding.Meta.Incarnation, Kind: model.ChangesetRestore, Time: s.binding.When, Principal: s.binding.Profile.Admission.Actor, Chain: []string{}}
		if revision == s.binding.Meta.Head {
			entry.IdempotencyKey = s.binding.Profile.IdempotencyKey
		}
		raw, err := json.Marshal(entry)
		if err != nil {
			return err
		}
		field := []byte(`"changes":null`)
		at := bytes.Index(raw, field)
		if at < 0 {
			return invalid("changeset encoding changed")
		}
		if _, err = w.Write(raw[:at]); err != nil {
			return err
		}
		if err = literal(w, `"changes":[`); err != nil {
			return err
		}
		rows, err := s.db.QueryContext(ctx, `SELECT ref,op,previous FROM rows WHERE rev=? ORDER BY n`, mustRevision(revision))
		if err != nil {
			return err
		}
		defer rows.Close()
		first := true
		for rows.Next() {
			var refData []byte
			var previous string
			var change model.ChangeRecord
			if err = rows.Scan(&refData, &change.Op, &previous); err != nil {
				return err
			}
			if err = json.Unmarshal(refData, &change.Ref); err != nil {
				return err
			}
			change.FromRevision = model.Natural(previous)
			change.ChangedPaths = []string{""}
			if !first {
				if err = literal(w, ","); err != nil {
					return err
				}
			}
			first = false
			if err = writeJSON(w, change); err != nil {
				return err
			}
		}
		if err = rows.Err(); err != nil {
			return err
		}
		if err = literal(w, "]"); err != nil {
			return err
		}
		_, err = w.Write(raw[at+len(field):])
		return err
	})
}
