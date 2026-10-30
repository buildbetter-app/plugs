package driver

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

var ErrRestoreTargetNotEmpty = errors.New("restore target is not empty")

// OfflineRestoreTx is an optional, trusted offline capability, never an RPC
// authorization bypass. Call only after complete source validation, Lock and
// Head, and before staging anything. Head is the global writer fence. Prepare
// checks all durable data, not just live records, and stages a new incarnation
// and a head-only history floor of 1. Commit repeats emptiness under the atomic
// publication fence, and publishes metadata with the records/log/receipt. A
// rollback changes nothing. Ordinary transactions never enter this mode.
type OfflineRestoreTx interface {
	PrepareOfflineRestore(context.Context, StoreID) (Meta, error)
}

func NewOfflineRestoreMeta(current Meta, source StoreID) (Meta, error) {
	if source == "" || strings.TrimSpace(string(source)) != string(source) || strings.ContainsRune(string(source), '\x00') {
		return Meta{}, fmt.Errorf("invalid restore source store identity")
	}
	var identity [32]byte
	if _, err := rand.Read(identity[:]); err != nil {
		return Meta{}, err
	}
	current.StoreID = source
	current.Incarnation = hex.EncodeToString(identity[:])
	current.Head = model.Natural64(1)
	current.HistoryFloor = model.Natural64(1)
	return current, nil
}

func ValidateOfflineRestoreHeader(h ChangesetHeader) error {
	if h.Kind != model.ChangesetRestore || h.Method != "offline.restore" || h.Principal.Kind != "system" || h.Principal.ID != "plug-converter" || h.Principal.Sponsor != nil || len(h.Principal.Caveats) != 0 || h.ResponsePending || h.IdempotencyKey == "" || len(h.RequestHash) != 64 {
		return fmt.Errorf("offline restore requires a final keyed restore receipt")
	}
	_, err := hex.DecodeString(h.RequestHash)
	if err != nil {
		return err
	}
	for _, change := range h.Changes {
		if change.Op != model.OpCreate || !change.FromRevision.IsZero() {
			return fmt.Errorf("offline restore requires only new record bases")
		}
	}
	return nil
}
