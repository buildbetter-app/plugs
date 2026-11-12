package driver

import "context"

// ReadWorkKind identifies executed work, rather than estimating it from the
// number of output rows. SQL executor visits are deliberately not represented:
// materializing a row in Go does not reveal the database's execution plan.
type ReadWorkKind string

const (
	ReadQueryCalls     ReadWorkKind = "query_calls"
	ReadQueryReturned  ReadWorkKind = "query_rows_returned"
	ReadGetCalls       ReadWorkKind = "get_calls"
	ReadPayloadRecords ReadWorkKind = "payload_records_materialized"
	ReadPayloadBytes   ReadWorkKind = "payload_body_bytes_materialized"
	// Numeric child identity candidates include rejected/staged identities and
	// memory tree navigation nodes; SQL executor visits still need plan proof.
	ReadChildIDCandidates ReadWorkKind = "numeric_child_id_candidates"
	ReadSelectorKeys      ReadWorkKind = "selector_keys_scanned"
	ReadPrefixStates      ReadWorkKind = "prefix_metadata_states_decoded"
	ReadPrefixBytes       ReadWorkKind = "prefix_metadata_state_bytes_decoded"
	ReadAggregateInputs   ReadWorkKind = "aggregate_input_visits"
	ReadAggregateGroups   ReadWorkKind = "aggregate_groups_constructed"
	ReadItemProjections   ReadWorkKind = "item_projections_constructed"
)

// ReadWorkObserver is optional request-local instrumentation. Implementations
// must support concurrent calls and must not change the operation or its data.
type ReadWorkObserver interface {
	ObserveReadWork(ReadWorkKind, int64)
}

type readWorkKey struct{}

func WithReadWorkObserver(ctx context.Context, observer ReadWorkObserver) context.Context {
	if observer == nil {
		return ctx
	}
	return context.WithValue(ctx, readWorkKey{}, observer)
}

// ReportReadWork is called at the owning materialization/iteration boundary,
// including work inside a WriteTx. Without an observer it has no side effects.
func ReportReadWork(ctx context.Context, kind ReadWorkKind, amount int64) {
	if observer, ok := ctx.Value(readWorkKey{}).(ReadWorkObserver); ok {
		observer.ObserveReadWork(kind, amount)
	}
}

// ReportReadPayload counts a reconstructed live record, even when a later
// predicate, authorization check or page boundary excludes it from output.
func ReportReadPayload(ctx context.Context, body []byte) {
	ReportReadWork(ctx, ReadPayloadRecords, 1)
	ReportReadWork(ctx, ReadPayloadBytes, int64(len(body)))
}
