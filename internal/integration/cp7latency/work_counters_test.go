package cp7latency_test

import (
	"sync/atomic"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// These observations come from the executed SQL-driver/core boundaries, not
// output cardinality. Readers and WriteTx values remain the concrete driver.
type counters struct {
	readWorkCounters
	validation readWorkCounters
}

type readWorkCounters struct {
	queries, returned, gets                       atomic.Int64
	payloads, payloadBytes, selectorKeys          atomic.Int64
	prefixStates, prefixBytes                     atomic.Int64
	aggregateInputs, aggregateGroups, projections atomic.Int64
	sourceHeaders, sourceImages, sourceQueries    atomic.Int64
}

type readCounts struct {
	Queries  int64 `json:"query_calls"`
	Returned int64 `json:"query_rows_returned"`
	Gets     int64 `json:"get_calls"`
	// Key/state metadata and SQL executor work remain unmeasured. Actual
	// live Observe payload bodies are counted at the existing driver sites.
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
	SourceHeaders   int64 `json:"source_headers_returned"`
	SourceImages    int64 `json:"source_images_reconstructed"`
	SourceQueries   int64 `json:"source_index_fetch_queries"`
}

func (c *counters) ObserveReadWork(kind driver.ReadWorkKind, amount int64) {
	c.readWorkCounters.observe(kind, amount)
}

func (c *counters) ObserveScopedReadWork(scope driver.ReadWorkScope, kind driver.ReadWorkKind, amount int64) {
	if scope != driver.ReadWorkOntologyValidation {
		panic("unrecognized read-work scope")
	}
	// The driver already delivered this event to the global callback.
	// Only the scoped counters change here: totals must not count it twice.
	c.validation.observe(kind, amount)
}

func (c *readWorkCounters) observe(kind driver.ReadWorkKind, amount int64) {
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
	case driver.ReadSourceHeaders:
		counter = &c.sourceHeaders
	case driver.ReadSourceImages:
		counter = &c.sourceImages
	case driver.ReadSourceQueries:
		counter = &c.sourceQueries
	default:
		panic("unrecognized read-work observation")
	}
	counter.Add(amount)
}

func (c *counters) reset() {
	c.readWorkCounters.reset()
	c.validation.reset()
}

func (c *readWorkCounters) reset() {
	for _, counter := range []*atomic.Int64{&c.queries, &c.returned, &c.gets,
		&c.payloads, &c.payloadBytes, &c.selectorKeys, &c.prefixStates, &c.prefixBytes,
		&c.aggregateInputs, &c.aggregateGroups, &c.projections,
		&c.sourceHeaders, &c.sourceImages, &c.sourceQueries} {
		counter.Store(0)
	}
}

func (c *readWorkCounters) value() readCounts {
	return readCounts{Queries: c.queries.Load(), Returned: c.returned.Load(), Gets: c.gets.Load()}
}

// Payloads count actual live full-body materializations, including a stored
// source SQL Scan and its later bytes.Clone separately, and fresh Observe/Get
// bodies. SourceImages count reconstructed index images once. Neither metric
// is a distinct-ID count or SQL executor/CPU/allocation-total measurement.
// Core snapshot output clones, key/state metadata, coverage probes and commit
// internals remain outside these instrumented driver/sourceindex boundaries.
func (c *readWorkCounters) work() executedReadWork {
	return executedReadWork{Payloads: c.payloads.Load(), PayloadBytes: c.payloadBytes.Load(),
		SelectorKeys: c.selectorKeys.Load(), PrefixStates: c.prefixStates.Load(), PrefixBytes: c.prefixBytes.Load(),
		AggregateInputs: c.aggregateInputs.Load(), AggregateGroups: c.aggregateGroups.Load(), Projections: c.projections.Load(),
		SourceHeaders: c.sourceHeaders.Load(), SourceImages: c.sourceImages.Load(), SourceQueries: c.sourceQueries.Load()}
}
