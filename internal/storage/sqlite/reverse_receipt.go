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
	// Startup streams the published changeset. Standalone native reverse
	// continues to use the transport's existing whole-record allowance.
	entryLimit := driver.NativeHeadRecordBytes
	if driver.SourceRecoveryRead(ctx) {
		entryLimit = 0
	}
	err := r.tx.QueryRowContext(ctx, `SELECT rowid, created, rev, request_hash, length(response),
		length(changeset), CASE WHEN length(changeset)<=? THEN changeset END, finalized, pending
		FROM replays WHERE scope=? AND rev<=?`, entryLimit, scope, revKey(r.at)).
		Scan(&info.row, &created, &info.revision, &record.RequestHash, &info.size, &entrySize, &entry, &finalized, &pending)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	// Cold source recovery uses the original import receipt as provenance.
	// Its age only limits ordinary idempotency replay. Keep that TTL for
	// ordinary readers and native reverse, and retain the startup checks on
	// identity, revision, changeset, and the admitted archive.
	retainedImport := driver.RetainSourceImportReceipt(ctx, method)
	if !retainedImport && !r.store.now().Before(time.Unix(0, created).Add(driver.IdempotencyWindow)) {
		return nil, nil
	}
	record.ResponsePending = pending != 0
	query := fmt.Sprintf(`SELECT scope, created, rev, request_hash, finalized, pending FROM replays WHERE rowid=%d`, info.row)
	identity := []string{scope, strconv.FormatInt(created, 10), info.revision, record.RequestHash, strconv.Itoa(finalized), strconv.Itoa(pending)}
	if entrySize.Int64 > 0 {
		if entryLimit == 0 {
			if err := r.readReverseReplayBlob(ctx, "changeset", info.row, entrySize.Int64, query, identity, func(source io.Reader) error {
				return readSourceRecoveryChangeset(source, &record.Changeset)
			}); err != nil {
				return nil, err
			}
		} else if entrySize.Int64 != int64(len(entry)) {
			return nil, reverseMetadataLimit("value_bytes", uint64(entrySize.Int64), uint64(entryLimit), "receipt/changeset.json", "$", 0)
		} else if err := json.Unmarshal(entry, &record.Changeset); err != nil {
			return nil, err
		}
	}
	if err := r.readReverseReplayBlob(ctx, "response", info.row, info.size, query, identity, func(source io.Reader) error {
		return readReverseReceipt(ctx, source, record, keep)
	}); err != nil {
		return nil, err
	}
	revision, err := parseRev(info.revision)
	if err != nil || revision != record.Commit.Revision {
		return nil, fmt.Errorf("sqlite: reverse receipt revision differs")
	}
	return record, nil
}

func (r *reader) readReverseReplayBlob(ctx context.Context, column string, row, size int64, query string, identity []string, read func(io.Reader) error) error {
	source := &legacyBlob{reader: r, ctx: ctx, size: size, tls: libc.NewTLS()}
	err := source.open("replays", column, row, query, identity...)
	if err == nil {
		err = read(readerFunc(source.readLocked))
	}
	return errors.Join(err, source.close())
}

type readerFunc func([]byte) (int, error)

func (f readerFunc) Read(dst []byte) (int, error) { return f(dst) }

// The decoder may read ahead, so both the bytes it can fetch during one
// operation and the length of each decoded value are checked independently.
type reverseReceiptInput struct {
	source io.Reader
	unread int
	err    error
}

var errReverseReceiptReadLimit = errors.New("reverse receipt read limit")

func (r *reverseReceiptInput) Read(dst []byte) (int, error) {
	if r.err != nil {
		return 0, r.err
	}
	if r.unread == 0 {
		return 0, errReverseReceiptReadLimit
	}
	n, err := r.source.Read(dst[:min(len(dst), r.unread)])
	r.unread -= n
	r.err = err
	return n, err
}

const reverseReceiptMetadataBytes = 1 << 20

type reverseReceiptJSON struct {
	*json.Decoder
	input  *reverseReceiptInput
	member string
}

func newReverseReceiptJSON(source io.Reader, member string) *reverseReceiptJSON {
	input := &reverseReceiptInput{source: source, unread: reverseReceiptMetadataBytes}
	return &reverseReceiptJSON{Decoder: json.NewDecoder(input), input: input, member: member}
}

func (d *reverseReceiptJSON) locatedError(err error, limit int, path string, index uint64) error {
	if errors.Is(err, errReverseReceiptReadLimit) {
		return reverseMetadataLimit("value_bytes", uint64(limit)+1, uint64(limit), d.member, path, index)
	}
	return err
}

func (d *reverseReceiptJSON) token(path string, index uint64) (json.Token, error) {
	d.input.unread = reverseReceiptMetadataBytes
	token, err := d.Token()
	return token, d.locatedError(err, reverseReceiptMetadataBytes, path, index)
}

func (d *reverseReceiptJSON) delimiter(expected json.Delim, path string, index uint64) error {
	token, err := d.token(path, index)
	if err != nil {
		return err
	}
	if token != expected {
		return fmt.Errorf("sqlite: invalid reverse receipt framing")
	}
	return nil
}

func (d *reverseReceiptJSON) finish() error {
	if _, err := d.token("$", 0); !errors.Is(err, io.EOF) {
		if err != nil {
			return err
		}
		return fmt.Errorf("sqlite: trailing reverse receipt data")
	}
	return nil
}

func (d *reverseReceiptJSON) value(raw *json.RawMessage, limit, enclosingDepth int, path string, index uint64) error {
	d.input.unread = limit + (32 << 10)
	if err := d.Decode(raw); err != nil {
		var syntax *json.SyntaxError
		if errors.As(err, &syntax) {
			// A failed value scan leaves its prefix in Buffered. Inspect just
			// that value incrementally so Go's scanner ceiling cannot hide
			// the tighter, located Plug depth refusal.
			prefix := json.NewDecoder(d.Buffered())
			prefix.UseNumber()
			if depth := receiptPrefixDepth(prefix, enclosingDepth); depth > driver.NativeHeadJSONDepth {
				return reverseMetadataLimit("nesting_depth", uint64(depth), driver.NativeHeadJSONDepth, d.member, path, index)
			}
		}
		return d.locatedError(err, limit, path, index)
	}
	if len(*raw) > limit {
		return reverseMetadataLimit("value_bytes", uint64(len(*raw)), uint64(limit), d.member, path, index)
	}
	return nil
}

func receiptPrefixDepth(prefix *json.Decoder, enclosingDepth int) int {
	depth := enclosingDepth
	for {
		token, err := prefix.Token()
		if err != nil {
			return depth
		}
		switch token {
		case json.Delim('{'), json.Delim('['):
			depth++
		case json.Delim('}'), json.Delim(']'):
			depth--
		}
		if depth <= enclosingDepth || depth > driver.NativeHeadJSONDepth {
			return depth
		}
	}
}

// A restore can publish a change per projected record. Recover the complete
// ordered census for header validation without loading its encoded container.
func readSourceRecoveryChangeset(source io.Reader, changeset *model.Changeset) error {
	decoder := newReverseReceiptJSON(source, "receipt/changeset.json")
	if err := decoder.delimiter('{', "$", 0); err != nil {
		return err
	}
	fields := make(map[string]json.RawMessage)
	var arrays model.Changeset
	var retained uint64
	for decoder.More() {
		token, err := decoder.token("$.<id>", 0)
		if err != nil {
			return err
		}
		key, ok := token.(string)
		if !ok || fields[key] != nil {
			return fmt.Errorf("sqlite: invalid or duplicate reverse changeset field")
		}
		var field json.RawMessage
		switch key {
		case "changes":
			field, err = readRecoveryReceiptArray(decoder, "$.changes", &retained, &arrays.Changes)
		case "events":
			field, err = readRecoveryReceiptArray(decoder, "$.events", &retained, &arrays.Events)
		case "chain":
			field, err = readRecoveryReceiptArray(decoder, "$.chain", &retained, &arrays.Chain)
		case "mutators":
			field, err = readRecoveryReceiptArray(decoder, "$.mutators", &retained, &arrays.Mutators)
		case "revision", "branch", "incarnation", "kind", "time", "principal", "idempotencyKey":
			path := "$." + key
			err = decoder.value(&field, reverseReceiptMetadataBytes, 1, path, 0)
			if err == nil {
				_, err = driver.ReverseJSONFootprint(field, 1)
				err = driver.LocateReverseJSONLimit(err, decoder.member, path, 0)
			}
		default:
			return fmt.Errorf("sqlite: unknown reverse changeset field")
		}
		if err != nil {
			return err
		}
		fields[key] = field
	}
	if err := decoder.delimiter('}', "$", 0); err != nil {
		return err
	}
	if err := decoder.finish(); err != nil {
		return err
	}
	// The model validates the event plan against the assembled changes.
	// Preserve an explicit null for the strict header decoder to reject.
	if arrays.Events != nil {
		delete(fields, "events")
	}
	metadata, err := json.Marshal(fields)
	if err != nil {
		return err
	}
	var decoded model.Changeset
	if err := json.Unmarshal(metadata, &decoded); err != nil {
		return err
	}
	decoded.Changes, decoded.Events, decoded.Chain, decoded.Mutators = arrays.Changes, arrays.Events, arrays.Chain, arrays.Mutators
	if err := decoded.ValidateEvents(); err != nil {
		return err
	}
	*changeset = decoded
	return nil
}

func readRecoveryReceiptArray[T string | model.ChangeRecord | model.EventDescriptor](decoder *reverseReceiptJSON, path string, retained *uint64, values *[]T) (json.RawMessage, error) {
	token, err := decoder.token(path, 0)
	if err != nil {
		return nil, err
	}
	if token == nil {
		return json.RawMessage("null"), nil
	}
	if token != json.Delim('[') {
		return nil, fmt.Errorf("sqlite: invalid reverse changeset array")
	}
	const retainedLimit = 1 << 30
	*values = make([]T, 0)
	path += "[]"
	var raw json.RawMessage
	var index uint64
	for ; decoder.More(); index++ {
		if err := decoder.value(&raw, driver.ReverseJSONElementBytes, 2, path, index); err != nil {
			return nil, err
		}
		footprint, err := driver.ReverseJSONElementFootprint(raw, 2)
		if err != nil {
			return nil, driver.LocateReverseJSONLimit(err, decoder.member, path, index)
		}
		*retained += footprint + 96
		if *retained > retainedLimit {
			return nil, reverseMetadataLimit("retained_metadata_bytes", *retained, retainedLimit, decoder.member, path, index)
		}
		var value T
		if err := model.DecodeChangesetArrayElement(raw, &value); err != nil {
			return nil, err
		}
		*values = append(*values, value)
	}
	return json.RawMessage("[]"), decoder.delimiter(']', path, index)
}

func readReverseReceipt(ctx context.Context, source io.Reader, result *driver.IdempotencyRecord, keep func(driver.Ref) bool) error {
	decoder := newReverseReceiptJSON(source, "receipt/commit.json")
	if err := decoder.delimiter('{', "$", 0); err != nil {
		return err
	}
	fields := make(map[string]json.RawMessage)
	selected := make([]model.OpResult, 0)
	for decoder.More() {
		token, err := decoder.token("$.<id>", 0)
		if err != nil {
			return err
		}
		key, ok := token.(string)
		if !ok || fields[key] != nil {
			return fmt.Errorf("sqlite: invalid or duplicate reverse receipt field")
		}
		switch key {
		case "results":
			records, err := readReverseReceiptResults(ctx, decoder, keep)
			if err != nil {
				return err
			}
			selected = records
			fields[key] = json.RawMessage("[]")
		case "revision", "warnings", "wouldCommit", "violations", "leaseExpiresAt", "leaseTtlMs", "attemptID":
			var value json.RawMessage
			path := "$." + key
			if err := decoder.value(&value, reverseReceiptMetadataBytes, 1, path, 0); err != nil {
				return err
			}
			if _, err := driver.ReverseJSONFootprint(value, 1); err != nil {
				return driver.LocateReverseJSONLimit(err, decoder.member, path, 0)
			}
			fields[key] = value
		default:
			return fmt.Errorf("sqlite: unknown reverse receipt field")
		}
	}
	if err := decoder.delimiter('}', "$", 0); err != nil {
		return err
	}
	if err := decoder.finish(); err != nil {
		return err
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

func readReverseReceiptResults(ctx context.Context, decoder *reverseReceiptJSON, keep func(driver.Ref) bool) ([]model.OpResult, error) {
	if err := decoder.delimiter('[', "$.results", 0); err != nil {
		return nil, err
	}
	selected := make([]model.OpResult, 0)
	var retained int
	limit, totalLimit := reverseMetadataBounds(ctx)
	var raw json.RawMessage
	var index uint64
	for ; decoder.More(); index++ {
		if err := decoder.value(&raw, limit, 2, "$.results[]", index); err != nil {
			return nil, err
		}
		if err := driver.CheckSourceRecoveryMetadata(raw, 2); err != nil {
			return nil, driver.LocateReverseJSONLimit(err, decoder.member, "$.results[]", index)
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
			return nil, driver.LocateReverseJSONLimit(err, "receipt/commit.json", "$.results[].setBy", index)
		}
		if len(identity.Record) != 0 {
			if err := checkReverseMetadata(ctx, identity.Ref, "beads-archive", identity.Record, 1); err != nil {
				return nil, driver.LocateReverseJSONLimit(err, "receipt/commit.json", "$.results[].record", index)
			}
		}
		var row model.OpResult
		if err := json.Unmarshal(raw, &row); err != nil {
			return nil, err
		}
		if keep != nil && keep(row.Ref) {
			retained += len(raw)
			if retained > totalLimit {
				return nil, reverseMetadataLimit("retained_metadata_bytes", uint64(retained), uint64(totalLimit), "receipt/commit.json", "$.results[]", index)
			}
			selected = append(selected, row)
		}
	}
	if err := decoder.delimiter(']', "$.results[]", index); err != nil {
		return nil, err
	}
	return selected, nil
}

func reverseMetadataBounds(ctx context.Context) (value, retained int) {
	if driver.SourceRecoveryRead(ctx) {
		return driver.SourceRecoveryMetadataBytes, 1 << 30
	}
	return driver.NativeHeadRecordBytes, driver.NativeHeadMaterialBytes / 2
}

func checkReverseMetadata(ctx context.Context, ref driver.Ref, kind string, raw []byte, depth int) error {
	if driver.SourceRecoveryRead(ctx) {
		return driver.CheckSourceRecoveryMetadata(raw, depth)
	}
	return driver.CheckReverseRecordJSON(ref, kind, raw, depth)
}

func reverseMetadataLimit(resource string, measured, limit uint64, member, path string, index uint64) error {
	return driver.LocateReverseJSONLimit(&driver.ReverseJSONLimitError{
		Resource: resource, Measured: measured, Limit: limit,
	}, member, path, index)
}

var _ driver.NativeReverseReceiptReader = (*reader)(nil)
