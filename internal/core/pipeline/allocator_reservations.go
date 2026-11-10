package pipeline

import "context"

type allocatorReservationsKey struct{}

// WithAllocatorReservations reserves admitted source identities and their
// targets across the store. The trusted host supplies this immutable census;
// it grants no read/write authority and does not affect adaptive root counts.
// Keep it separate from source-operation dispatch, which a joined delete may
// replace while preserving the same admitted identities.
func WithAllocatorReservations(ctx context.Context, ids []string) context.Context {
	reserved := make(map[string]bool, len(ids))
	for _, id := range ids {
		if id != "" {
			reserved[id] = true
		}
	}
	return context.WithValue(ctx, allocatorReservationsKey{}, reserved)
}

func allocatorReserved(ctx context.Context, id string) bool {
	reserved, _ := ctx.Value(allocatorReservationsKey{}).(map[string]bool)
	return reserved[id]
}
