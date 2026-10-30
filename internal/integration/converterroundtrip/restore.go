// Package converterroundtrip admits the actual offline converter format and
// reports why it cannot yet be restored through the ordinary Writer API.
package converterroundtrip

import (
	"context"
	"errors"
	"io"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

var (
	ErrRestoreUnavailable = errors.New("trusted offline restore seam unavailable: source IDs, status, attribution, allocator and archive retention require an atomic empty-store restore")
	ErrTargetNotEmpty     = errors.New("restore target is not empty")
)

// Report retains all parsed source evidence, including config and unverified
// history. No record is applied or silently treated as an ordinary import.
type Report struct {
	Archive *dumpformat.Archive
	Target  driver.Meta
}

// Restore validates the complete input and inspects target metadata. This
// checkpoint always refuses: the current Writer API cannot preserve source
// identity. The metadata read is diagnostic, not atomic empty-target admission.
// There is no permissive/partial option and no production RPC/admin binding.
func Restore(ctx context.Context, input io.Reader, target driver.Store) (Report, error) {
	if err := ctx.Err(); err != nil {
		return Report{}, err
	}
	archive, err := dumpformat.Read(input)
	if err != nil {
		return Report{}, err
	}
	if err := ctx.Err(); err != nil {
		return Report{}, err
	}
	meta, err := target.Meta(ctx)
	report := Report{Archive: archive, Target: meta}
	if err != nil {
		return report, err
	}
	if !meta.Head.IsZero() {
		return report, ErrTargetNotEmpty
	}
	return report, ErrRestoreUnavailable
}
