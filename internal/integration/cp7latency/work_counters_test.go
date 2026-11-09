package cp7latency_test

import (
	"sync/atomic"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// These observations come from the executed SQL-driver/core boundaries, not
// output cardinality. Readers and WriteTx values remain the concrete driver.
type counters struct {
	queries, returned, gets                       atomic.Int64
	payloads, payloadBytes, selectorKeys          atomic.Int64
	prefixStates, prefixBytes                     atomic.Int64
	aggregateInputs, aggregateGroups, projections atomic.Int64
}

type readCounts struct {
	Queries  int64 `json:"query_calls"`
	Returned int64 `json:"query_rows_returned"`
	Gets     int64 `json:"get_calls"`
	// Observe/Lock internals are outside this instrumentation's scope.
	Observed *int64 `json:"observed_keys"`
}

type executedReadWork struct {
	Payloads        int64 `json:"payload_records_materialized"`
	PayloadBytes    int64 `json:"payload_body_bytes_materialized"`
	SelectorKeys    int64 `json:"selector_keys_scanned"`
	PrefixStates    int64 `json:"prefix_metadata_states_decoded"`
	PrefixBytes     int64 `json:"prefix_metadata_state_bytes_decoded"`
	AggregateInputs int64 `json:"aggregate_input_visits"`
	AggregateGroups int64 `json:"aggregate_groups_constructed"`
	Projections     int64 `json:"item_projections_constructed"`
}

func (c *counters) ObserveReadWork(kind driver.ReadWorkKind, amount int64) {
	var counter *atomic.Int64
	switch kind {
	case driver.ReadQueryCalls:
		counter = &c.queries
	case driver.ReadQueryReturned:
		counter = &c.returned
	case driver.ReadGetCalls:
		counter = &c.gets
	case driver.ReadPayloadRecords:
		counter = &c.payloads
	case driver.ReadPayloadBytes:
		counter = &c.payloadBytes
	case driver.ReadSelectorKeys:
		counter = &c.selectorKeys
	case driver.ReadPrefixStates:
		counter = &c.prefixStates
	case driver.ReadPrefixBytes:
		counter = &c.prefixBytes
	case driver.ReadAggregateInputs:
		counter = &c.aggregateInputs
	case driver.ReadAggregateGroups:
		counter = &c.aggregateGroups
	case driver.ReadItemProjections:
		counter = &c.projections
	default:
		panic("unrecognized read-work observation")
	}
	counter.Add(amount)
}

func (c *counters) reset() {
	for _, counter := range []*atomic.Int64{&c.queries, &c.returned, &c.gets,
		&c.payloads, &c.payloadBytes, &c.selectorKeys, &c.prefixStates, &c.prefixBytes,
		&c.aggregateInputs, &c.aggregateGroups, &c.projections} {
		counter.Store(0)
	}
}

func (c *counters) value() readCounts {
	return readCounts{Queries: c.queries.Load(), Returned: c.returned.Load(), Gets: c.gets.Load()}
}

func (c *counters) work() executedReadWork {
	return executedReadWork{Payloads: c.payloads.Load(), PayloadBytes: c.payloadBytes.Load(),
		SelectorKeys: c.selectorKeys.Load(), PrefixStates: c.prefixStates.Load(), PrefixBytes: c.prefixBytes.Load(),
		AggregateInputs: c.aggregateInputs.Load(), AggregateGroups: c.aggregateGroups.Load(), Projections: c.projections.Load()}
}
