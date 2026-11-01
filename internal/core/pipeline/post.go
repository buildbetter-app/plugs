package pipeline

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// PostDeadLetter identifies a failed synchronous whole-changeset invocation.
// Only metadata is retained; images remain in canonical graph history. This
// is distinct from an event dispatcher's per-event delivery checkpoint. A
// failure never advances that checkpoint or claims native crash redelivery.
type PostDeadLetter struct {
	Incarnation string         `json:"incarnation"`
	Revision    model.Revision `json:"revision"`
	Binding     string         `json:"binding"`
	FailedAt    time.Time      `json:"failedAt"`
	Reason      string         `json:"reason"`
}

// PostDeadLetterKey is deterministic and bounds the storage key even when a
// configured binding ID is long. Each committed invocation has its own entry,
// so concurrent failures and later successes cannot overwrite earlier ones.
func PostDeadLetterKey(incarnation string, revision model.Revision, binding string) string {
	data, _ := json.Marshal([]string{incarnation, revision.String(), binding})
	digest := sha256.Sum256(data)
	return "post-dead-letter:" + hex.EncodeToString(digest[:])
}

func (w *Writer) deadLetterPost(ctx context.Context, incarnation string, revision model.Revision, binding string) error {
	entry := PostDeadLetter{Incarnation: incarnation, Revision: revision, Binding: binding, FailedAt: w.cfg.now().UTC(), Reason: "post_subscriber_failed"}
	payload, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	key := PostDeadLetterKey(incarnation, revision, binding)
	// Callback cancellation and caller disconnection must not discard an
	// observed failure. Storage still fences the write with the original lease.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
	defer cancel()
	for attempt := 0; attempt < 4; attempt++ {
		_, saveErr := w.cfg.postFailures.SaveCheckpoint(ctx, w.lease, key, 0, payload)
		// A successful CAS can lose its acknowledgement. Read the real metadata
		// before retrying; never repeat the subscriber or fabricate its outcome.
		stored, loadErr := w.cfg.postFailures.LoadCheckpoint(ctx, key)
		if loadErr == nil && stored.Generation != 0 {
			var actual PostDeadLetter
			if err := decode(stored.Payload, &actual); err != nil {
				return err
			}
			if actual.Incarnation != incarnation || actual.Revision.String() != revision.String() || actual.Binding != binding || actual.Reason != entry.Reason || actual.FailedAt.IsZero() {
				return fmt.Errorf("post dead-letter identity mismatch")
			}
			return nil
		}
		code, _, mapped := driver.ErrorMapping(saveErr)
		if ctx.Err() != nil || mapped && code == "lease_not_held" || attempt == 3 {
			break
		}
		if err := responsePause(ctx); err != nil {
			break
		}
	}
	return fmt.Errorf("post dead-letter metadata unavailable")
}
