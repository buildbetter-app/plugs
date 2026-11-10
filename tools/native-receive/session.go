package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/jackc/pgx/v5"
)

// The authorized rehearsal owns only a fresh destination. SQLite remains in a
// sibling staging directory through publication, reopen and readback, and is
// linked into the requested absent path only after the session finishes.
// PostgreSQL requires an absent Plug schema in the caller's disposable DB.
type disposableSession struct {
	target, stageDir, stageFile string
	pg                          *pgx.Conn
	finished                    bool
}

func newDisposableSession(ctx context.Context, engine, target string) (*disposableSession, string, error) {
	s := &disposableSession{target: target}
	switch engine {
	case "memory":
		if target != "" {
			return nil, "", driver.ErrUnsupported
		}
		return s, target, nil
	case "sqlite":
		if !filepath.IsAbs(target) {
			return nil, "", driver.ErrUnsupported
		}
		if _, err := os.Lstat(target); !errors.Is(err, os.ErrNotExist) {
			return nil, "", fmt.Errorf("%w: rehearsal destination must be absent", driver.ErrRestoreTargetNotEmpty)
		}
		dir, err := os.MkdirTemp(filepath.Dir(target), ".native-receive-")
		if err != nil {
			return nil, "", err
		}
		s.stageDir, s.stageFile = dir, filepath.Join(dir, "store.db")
		return s, s.stageFile, nil
	case "postgres":
		conn, err := pgx.Connect(ctx, target)
		if err != nil {
			return nil, "", err
		}
		var absent bool
		err = conn.QueryRow(ctx, "SELECT to_regnamespace('plug_postgres') IS NULL").Scan(&absent)
		if err != nil || !absent {
			_ = conn.Close(context.Background())
			return nil, "", fmt.Errorf("%w: rehearsal requires absent Plug schema", driver.ErrRestoreTargetNotEmpty)
		}
		s.pg = conn
		return s, target, nil
	default:
		return nil, "", driver.ErrUnsupported
	}
}

func (s *disposableSession) finish(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.stageFile != "" {
		// Link is atomic and refuses an occupied path; the completed SQLite
		// connection has closed and checkpointed before this point.
		if err := os.Link(s.stageFile, s.target); err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			_ = os.Remove(s.target)
			return err
		}
	}
	s.finished = true
	return nil
}

func (s *disposableSession) close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var result error
	if s.pg != nil {
		if !s.finished {
			_, result = s.pg.Exec(ctx, "DROP SCHEMA IF EXISTS plug_postgres CASCADE")
		}
		if err := s.pg.Close(ctx); result == nil {
			result = err
		}
	}
	if s.stageDir != "" {
		if err := os.RemoveAll(s.stageDir); result == nil {
			result = err
		}
	}
	return result
}
