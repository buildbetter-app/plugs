// Package historyverify owns the opaque result passed from archive verification
// to streaming storage. Receiving-side issuance authenticates the archive and actual planner result.
package historyverify

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// ErrCheckpoint is a determinate refusal, before any publication. An opaque
// digest supplied by a caller is not a verified checkpoint.
type ErrCheckpoint struct{ Reason string }

func (e ErrCheckpoint) Error() string { return "unverified streaming checkpoint: " + e.Reason }

// Root commits ordered converted identities, kinds, lengths and exact bytes.
// It is distinct from the archive, derived-image and physical table digests.
type Root struct {
	Records uint64
	SHA256  string
}

type result struct {
	current    driver.Meta
	header     driver.OfflineHistoryStreamHeader
	lifetime   driver.CheckpointLifetime
	when       time.Time
	request    string
	head, tail Root
	auxiliary  Root
	physical   bool
}

// Checkpoint is immutable and unconstructible outside this verifier package.
// Copies refer to the same immutable result. The zero value never authorizes a
// stream. NativeArchiveStream seals only after complete real archive replay,
// independent HEAD/current verification, physical reconciliation and binding
// the final admitted request to the actual receiving planner result.
type Checkpoint struct{ verified *result }

func mint(request driver.OfflineHistoryStreamRequest, head, current, auxiliary Root, physical bool) (Checkpoint, error) {
	return mintWithAccounting(request, head, current, auxiliary, physical, driver.RestoreImportProfileAccounting{})
}

func mintWithAccounting(request driver.OfflineHistoryStreamRequest, head, current, auxiliary Root, physical bool, accounting driver.RestoreImportProfileAccounting) (Checkpoint, error) {
	hash, err := request.Profile.RequestHashWithAccounting(accounting)
	if err != nil {
		return Checkpoint{}, err
	}
	return Checkpoint{verified: &result{current: request.Current, header: request.Header, lifetime: request.Lifetime, when: request.When, request: hash, head: head, tail: current, auxiliary: auxiliary, physical: physical}}, nil
}

// Check exposes only the two verified converted roots after matching the whole
// admitted request. New target incarnation randomness is checked by the core's
// separate publication proof. This result cannot mint or retarget another seal.
func (p Checkpoint) Check(binding driver.OfflineHistoryStreamBinding, physical bool) (Root, Root, error) {
	v := p.verified
	if v == nil {
		return Root{}, Root{}, ErrCheckpoint{"missing verifier-issued value"}
	}
	hash, err := binding.Profile.RequestHash()
	if err != nil {
		return Root{}, Root{}, err
	}
	if v.current != binding.Current || v.header != binding.Header || v.lifetime != binding.Lifetime || !v.when.Equal(binding.When) || v.request != hash {
		return Root{}, Root{}, ErrCheckpoint{"value belongs to another archive, checkpoint or admitted request"}
	}
	if physical && !v.physical {
		return Root{}, Root{}, ErrCheckpoint{"physical preimages were not reconciled"}
	}
	return v.head, v.tail, nil
}

// RootHasher uses constant state; callers walk their indexed checkpoint in
// encoded record-lock-key order. It never stores record bodies or a key list.
type RootHasher struct {
	hash hash.Hash
	last string
	n    uint64
}

func NewRootHasher() *RootHasher {
	h := sha256.New()
	_, _ = h.Write([]byte("plug-history-checkpoint/1\n"))
	return &RootHasher{hash: h}
}

func (h *RootHasher) Add(ref model.Ref, kind string, length uint64, bodySHA256 string) error {
	key, err := driver.EncodeLockKey(driver.LockKey{Kind: driver.LockRecord, Ref: &ref})
	if err != nil || kind == "" || key <= h.last || len(bodySHA256) != 64 || h.n == ^uint64(0) {
		return ErrCheckpoint{"invalid or unordered converted checkpoint identity"}
	}
	if _, err = hex.DecodeString(bodySHA256); err != nil {
		return err
	}
	raw, err := json.Marshal(struct {
		Ref    model.Ref `json:"ref"`
		Kind   string    `json:"kind"`
		Bytes  uint64    `json:"bytes"`
		SHA256 string    `json:"sha256"`
	}{ref, kind, length, bodySHA256})
	if err != nil {
		return fmt.Errorf("checkpoint identity: %w", err)
	}
	_, _ = h.hash.Write(raw)
	_, _ = h.hash.Write([]byte{'\n'})
	h.last, h.n = key, h.n+1
	return nil
}

func (h *RootHasher) Root() Root {
	return Root{Records: h.n, SHA256: hex.EncodeToString(h.hash.Sum(nil))}
}
