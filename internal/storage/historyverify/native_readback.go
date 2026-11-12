package historyverify

import (
	"context"
	"os"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// OpenReceivingSource lends one independent pull cursor to the driver. That
// cursor obeys the driver's terminal Close contract; its Close does not destroy
// the verifier owner's original spool or proof needed by clean recovery. The
// enclosing operation must keep this owner live until publication is resolved.
func (s *NativeArchiveStream) OpenReceivingSource(ctx context.Context) (driver.OfflineHistoryStreamSource, error) {
	if ctx == nil {
		return nil, ErrCheckpoint{"missing receiving context"}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s == nil || s.closed || !s.sealed || s.receivingIssued {
		return nil, ErrCheckpoint{"receiving cursor is unavailable or consumed"}
	}
	file, err := os.Open(s.path)
	if err != nil {
		return nil, err
	}
	s.receivingIssued = true
	cursor := &nativeReceivingCursor{owner: s, file: file}
	s.receivingCursor = cursor
	return cursor, nil
}

type nativeReceivingCursor struct {
	owner   *NativeArchiveStream
	file    *os.File
	release func()
	closed  bool
	err     error
}

func (c *nativeReceivingCursor) Next(ctx context.Context, limits driver.OfflineHistoryStreamLimits) (driver.OfflineHistoryStreamFrame, error) {
	if c == nil || c.closed || c.owner == nil || c.owner.closed {
		return driver.OfflineHistoryStreamFrame{}, ErrCheckpoint{"receiving cursor is closed"}
	}
	if c.release != nil {
		c.release()
		c.release = nil
	}
	frame, release, err := c.owner.readFrame(ctx, c.file, limits)
	if err != nil {
		return driver.OfflineHistoryStreamFrame{}, err
	}
	c.release = release
	return frame, nil
}

func (c *nativeReceivingCursor) Close() error {
	if c == nil || c.closed {
		if c == nil {
			return nil
		}
		return c.err
	}
	c.closed = true
	if c.release != nil {
		c.release()
		c.release = nil
	}
	if c.file != nil {
		c.err = c.file.Close()
		c.file = nil
	}
	if c.owner != nil {
		c.owner.receivingCursor = nil
		c.owner = nil
	}
	return c.err
}

// VisitVerifiedFrames exposes borrowed verifier-authored values for read-only
// correspondence checks. It confers no Seal/checkpoint authority and never
// traverses original capture files. Callbacks may not retain frame payloads.
func (s *NativeArchiveStream) VisitVerifiedFrames(ctx context.Context, visit func(driver.OfflineHistoryStreamFrame) error) error {
	if visit == nil {
		return ErrCheckpoint{"missing verified-frame visitor"}
	}
	return s.walk(ctx, visit)
}

// ValidateNativeCurrent compares the complete selected native namespace from
// an independently exhausted target snapshot to authenticated HEAD/current
// candidates. It includes both sources and refuses omissions, duplicates and
// extras. The caller owns record payloads; only a transient key index is added.
func (s *NativeArchiveStream) ValidateNativeCurrent(ctx context.Context, records []driver.Record) error {
	if ctx == nil || s == nil || s.closed || s.digest == "" || len(records) != len(s.nativeCurrent) {
		return ErrCheckpoint{"native current readback census differs"}
	}
	work, release := s.archive.NativeDeltaAccountingScope()
	defer release()
	if err := work.ChargeNativeDeltaBytes("collection_readback_current_keys", uint64(len(records))*256); err != nil {
		return err
	}
	seen := map[string]bool{}
	for _, record := range records {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := work.ChargeNativeDeltaBytes("collection_readback_current_key_bytes", uint64(len(record.Ref.ID)+len(record.Ref.Key))*2+128); err != nil {
			return err
		}
		key, err := checkpointKey(record.Ref)
		want, present := s.nativeCurrent[key]
		if err != nil || seen[key] || !present || want != convertedRecord(record) {
			return ErrCheckpoint{"native current readback record differs"}
		}
		seen[key] = true
	}
	return nil
}

var _ driver.OfflineHistoryStreamSource = (*nativeReceivingCursor)(nil)
