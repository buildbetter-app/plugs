package postgres

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func branchZero(b model.Branch) error {
	if err := b.Validate(); err != nil {
		return err
	}
	if !b.IsZero() {
		return fmt.Errorf("%w: postgres supports branch zero", driver.ErrUnsupported)
	}
	return nil
}
func keyID(key driver.LockKey) (string, error) {
	if key.Kind == driver.LockRecord && key.Ref != nil {
		if err := branchZero(key.Ref.Branch); err != nil {
			return "", err
		}
	}
	value, err := driver.EncodeLockKey(key)
	if err != nil {
		return "", err
	}
	// Bytea keeps identities independent of database collation. Bound indexed
	// keys explicitly rather than relying on PostgreSQL's btree page limit.
	if len(value) > 2048 {
		return "", fmt.Errorf("%w: postgres indexed lock key exceeds 2048 bytes", driver.ErrUnsupported)
	}
	return value, nil
}
func recordKey(ref driver.Ref) (string, error) {
	return keyID(driver.LockKey{Kind: driver.LockRecord, Ref: &ref})
}
func canonical(s string) bool {
	return s != "" && strings.TrimSpace(s) == s && !strings.ContainsRune(s, '\x00')
}

func encode(value any) ([]byte, error) { return json.Marshal(value) }
func decode(data []byte, value any) error {
	d := json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()
	return d.Decode(value)
}
func copied[T any](value T) (T, error) {
	var result T
	data, err := encode(value)
	if err == nil {
		err = decode(data, &result)
	}
	return result, err
}

// Lock identities are opaque bytes. JSON's replacement of invalid UTF-8 can
// collapse distinct admitted keys, so own their pointers without serializing.
func copyLockKey(key driver.LockKey) driver.LockKey {
	if key.Ref != nil {
		ref := *key.Ref
		key.Ref = &ref
	}
	if key.Derived != nil {
		derived := *key.Derived
		key.Derived = &derived
	}
	return key
}

func digest(value any) (string, error) {
	data, err := encode(value)
	if err != nil {
		return "", err
	}
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:]), nil
}

func scopeKey(principal, agent, method, key string) ([]byte, error) {
	if !strings.HasPrefix(principal, "anonymous:") {
		agent = ""
	}
	if !canonical(principal) || !canonical(method) || !canonical(key) || (agent != "" && !canonical(agent)) {
		return nil, fmt.Errorf("postgres: invalid idempotency scope")
	}
	data, err := encode([]string{principal, agent, method, key})
	if len(data) > 2048 {
		return nil, fmt.Errorf("%w: postgres idempotency scope exceeds 2048 bytes", driver.ErrUnsupported)
	}
	return data, err
}

func limitValue(n int) (int, error) {
	if n < 0 || n > 10000 {
		return 0, fmt.Errorf("postgres: limit must be between 0 and 10000")
	}
	if n == 0 {
		return 100, nil
	}
	return n, nil
}

func responseAt(input model.Commit, revision model.Revision) (model.Commit, error) {
	input.Revision = revision
	if input.Results == nil {
		input.Results = []model.OpResult{}
	}
	if input.Warnings == nil {
		input.Warnings = []model.Warning{}
	}
	return copied(input)
}

// A no-op has no changeset. The public zero Changeset is deliberately not a
// valid wire ChangesetKind, so its durable representation uses absence.
type receiptValue struct {
	AttemptID       string `json:",omitempty"`
	RequestHash     string
	Commit          model.Commit
	Changeset       *model.Changeset `json:",omitempty"`
	ResponsePending bool
}

func encodeReceipt(value driver.IdempotencyRecord) ([]byte, error) {
	disk := receiptValue{AttemptID: value.AttemptID, RequestHash: value.RequestHash, Commit: value.Commit, ResponsePending: value.ResponsePending}
	if value.Changeset.Kind != "" {
		disk.Changeset = &value.Changeset
	}
	return encode(disk)
}
func decodeReceipt(raw []byte) (driver.IdempotencyRecord, error) {
	var disk receiptValue
	err := decode(raw, &disk)
	value := driver.IdempotencyRecord{AttemptID: disk.AttemptID, RequestHash: disk.RequestHash, Commit: disk.Commit, ResponsePending: disk.ResponsePending}
	if disk.Changeset != nil {
		value.Changeset = *disk.Changeset
	}
	return value, err
}
