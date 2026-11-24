package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"modernc.org/libc"
)

// ReadNativeReverseReceipt keeps the request and changeset checks of ordinary
// replay, while visiting the persisted result array one entry at a time.
func (r *reader) ReadNativeReverseReceipt(ctx context.Context, principal, agent, method, key string, keep func(driver.Ref) bool) (*driver.IdempotencyRecord, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.check(ctx); err != nil {
		return nil, err
	}
	scope := scopeKey(principal, agent, method, key)
	var info blobInfo
	var entry []byte
	var entrySize sql.NullInt64
	var created int64
	var finalized, pending int
	record := &driver.IdempotencyRecord{}
	err := r.tx.QueryRowContext(ctx, `SELECT rowid, created, rev, request_hash, length(response),
		length(changeset), CASE WHEN length(changeset)<=? THEN changeset END, finalized, pending
		FROM replays WHERE scope=? AND rev<=?`, driver.NativeHeadRecordBytes, scope, revKey(r.at)).
		Scan(&info.row, &created, &info.revision, &record.RequestHash, &info.size, &entrySize, &entry, &finalized, &pending)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if !r.store.now().Before(time.Unix(0, created).Add(driver.IdempotencyWindow)) {
		return nil, nil
	}
	if entrySize.Valid && entrySize.Int64 != int64(len(entry)) {
		return nil, fmt.Errorf("sqlite: reverse receipt changeset exceeds buffer bound")
	}
	if len(entry) != 0 {
		if err := json.Unmarshal(entry, &record.Changeset); err != nil {
			return nil, err
		}
	}
	record.ResponsePending = pending != 0
	source := &legacyBlob{reader: r, ctx: ctx, size: info.size, tls: libc.NewTLS()}
	query := fmt.Sprintf(`SELECT scope, created, rev, request_hash, finalized, pending FROM replays WHERE rowid=%d`, info.row)
	if err := source.open("replays", "response", info.row, query,
		scope, strconv.FormatInt(created, 10), info.revision, record.RequestHash, strconv.Itoa(finalized), strconv.Itoa(pending)); err != nil {
		return nil, errors.Join(err, source.close())
	}
	err = readReverseReceipt(source, record, keep)
	if err := errors.Join(err, source.close()); err != nil {
		return nil, err
	}
	revision, err := parseRev(info.revision)
	if err != nil || revision != record.Commit.Revision {
		return nil, fmt.Errorf("sqlite: reverse receipt revision differs")
	}
	return record, nil
}

// The decoder may read ahead, so both the bytes it can fetch during one
// operation and the length of each decoded value are checked independently.
type reverseReceiptInput struct {
	source *legacyBlob
	unread int
}

func (r *reverseReceiptInput) Read(dst []byte) (int, error) {
	if r.unread == 0 {
		return 0, fmt.Errorf("sqlite: reverse receipt value exceeds buffer bound")
	}
	n, err := r.source.readLocked(dst[:min(len(dst), r.unread)])
	r.unread -= n
	return n, err
}

func readReverseReceipt(source *legacyBlob, result *driver.IdempotencyRecord, keep func(driver.Ref) bool) error {
	const metadataBytes = 1 << 20
	input := &reverseReceiptInput{source: source, unread: metadataBytes}
	decoder := json.NewDecoder(input)
	if token, err := decoder.Token(); err != nil || token != json.Delim('{') {
		return fmt.Errorf("sqlite: invalid reverse receipt envelope")
	}
	fields := make(map[string]json.RawMessage)
	selected := make([]model.OpResult, 0)
	for decoder.More() {
		input.unread = metadataBytes
		token, err := decoder.Token()
		key, ok := token.(string)
		if err != nil || !ok || fields[key] != nil {
			return fmt.Errorf("sqlite: invalid or duplicate reverse receipt field")
		}
		switch key {
		case "results":
			records, err := readReverseReceiptResults(input, decoder, keep)
			if err != nil {
				return err
			}
			selected = records
			fields[key] = json.RawMessage("[]")
		case "revision", "warnings", "wouldCommit", "violations", "leaseExpiresAt", "leaseTtlMs", "attemptID":
			input.unread = metadataBytes
			var value json.RawMessage
			if err := decoder.Decode(&value); err != nil || len(value) > metadataBytes {
				return fmt.Errorf("sqlite: invalid or oversized reverse receipt metadata")
			}
			fields[key] = value
		default:
			return fmt.Errorf("sqlite: unknown reverse receipt field")
		}
	}
	input.unread = metadataBytes
	if token, err := decoder.Token(); err != nil || token != json.Delim('}') {
		return fmt.Errorf("sqlite: incomplete reverse receipt")
	}
	if _, err := decoder.Token(); err != io.EOF {
		return fmt.Errorf("sqlite: trailing reverse receipt data")
	}
	if attempt := fields["attemptID"]; attempt != nil {
		if err := json.Unmarshal(attempt, &result.AttemptID); err != nil {
			return err
		}
		delete(fields, "attemptID")
	}
	metadata, err := json.Marshal(fields)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(metadata, &result.Commit); err != nil {
		return err
	}
	result.Commit.Results = selected
	return nil
}

func readReverseReceiptResults(input *reverseReceiptInput, decoder *json.Decoder, keep func(driver.Ref) bool) ([]model.OpResult, error) {
	if token, err := decoder.Token(); err != nil || token != json.Delim('[') {
		return nil, fmt.Errorf("sqlite: invalid reverse receipt results")
	}
	selected := make([]model.OpResult, 0)
	var retained int
	for decoder.More() {
		input.unread = driver.NativeHeadRecordBytes + (32 << 10)
		var raw json.RawMessage
		if err := decoder.Decode(&raw); err != nil || len(raw) > driver.NativeHeadRecordBytes {
			return nil, fmt.Errorf("sqlite: invalid or oversized reverse receipt result")
		}
		var identity struct {
			Ref      model.Ref       `json:"ref"`
			Revision model.Revision  `json:"revision"`
			Record   json.RawMessage `json:"record"`
			SetBy    json.RawMessage `json:"setBy"`
			Replaced *bool           `json:"replaced"`
		}
		envelope := json.NewDecoder(bytes.NewReader(raw))
		envelope.DisallowUnknownFields()
		if err := envelope.Decode(&identity); err != nil {
			return nil, err
		}
		if err := driver.CheckReverseJSONShape(identity.SetBy); err != nil {
			return nil, err
		}
		if len(identity.Record) != 0 {
			if err := driver.CheckReverseRecordJSON(identity.Ref, "beads-archive", identity.Record, 64); err != nil {
				return nil, err
			}
		}
		var row model.OpResult
		if err := json.Unmarshal(raw, &row); err != nil {
			return nil, err
		}
		if keep != nil && keep(row.Ref) {
			retained += len(raw)
			if retained > driver.NativeHeadMaterialBytes/2 {
				return nil, fmt.Errorf("sqlite: selected reverse receipt results exceed buffer bound")
			}
			selected = append(selected, row)
		}
	}
	input.unread = 1 << 20
	if token, err := decoder.Token(); err != nil || token != json.Delim(']') {
		return nil, fmt.Errorf("sqlite: incomplete reverse receipt results")
	}
	return selected, nil
}

var _ driver.NativeReverseReceiptReader = (*reader)(nil)
