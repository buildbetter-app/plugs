package sqlite

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// The journal is one fixed-size record, not a second publication transaction.
// Its epoch and token digest bind it to the durable SQLite owner; an old record
// can never extend a released or successor lease. All writers own the existing
// publication flock and the exact registered lease's mutex. Atomic replacement
// lets independent handles/processes read a complete record without that flock.
const heartbeatFile = "lease-heartbeat"
const heartbeatSize = 8 + sha256.Size + 8

func (s *Store) readLease(ctx context.Context, q queryer) (leaseRow, error) {
	r, err := readLease(ctx, q)
	if err != nil || r.token == "" {
		return r, err
	}
	data, err := os.ReadFile(filepath.Join(s.lockDir, heartbeatFile))
	if errors.Is(err, os.ErrNotExist) {
		return r, nil // Stores predating the journal use the SQLite expiry.
	}
	if err != nil {
		return r, err
	}
	if len(data) != heartbeatSize {
		return r, fmt.Errorf("sqlite: invalid lease heartbeat journal")
	}
	digest := sha256.Sum256([]byte(r.token))
	if binary.BigEndian.Uint64(data[:8]) == r.epoch && string(data[8:8+sha256.Size]) == string(digest[:]) {
		r.expires = max(r.expires, int64(binary.BigEndian.Uint64(data[8+sha256.Size:])))
	}
	return r, nil
}

// persistHeartbeat runs under l.mu while the publication flock excludes a
// different owner. No SQLite statement, COMMIT, or fsync of the database holds
// this mutex. Renewal's own journal I/O must finish within the previous TTL:
// the caller rechecks its OLD deadline after this function, before acceptance.
func (l *activeLease) persistHeartbeat(expires int64) error {
	var data [heartbeatSize]byte
	binary.BigEndian.PutUint64(data[:8], l.epoch)
	digest := sha256.Sum256([]byte(l.token))
	copy(data[8:], digest[:])
	binary.BigEndian.PutUint64(data[8+sha256.Size:], uint64(expires))
	f, err := os.CreateTemp(l.store.lockDir, ".lease-heartbeat-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	defer f.Close()
	if _, err = f.Write(data[:]); err != nil {
		return err
	}
	if err = f.Sync(); err != nil {
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = os.Rename(f.Name(), filepath.Join(l.store.lockDir, heartbeatFile)); err != nil {
		return err
	}
	dir, err := os.Open(l.store.lockDir)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
