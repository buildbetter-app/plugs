package dumpformat

import (
	"fmt"
	"math"
)

const NativeDeltaDecodedAccounting = "native-delta-decoded-accounting/1"
const NativeDeltaDefaultDecodedLimit uint64 = 2 << 30

// NativeDeltaDecodedLimitError reports a measured candidate size before the
// candidate is retained. MeasuredBytes saturates only on integer overflow,
// explicitly reported by Overflow. It is a logical payload count, not RSS.
type NativeDeltaDecodedLimitError struct {
	Accounting          string                  `json:"accounting"`
	Phase               string                  `json:"phase"`
	MeasuredBytes       uint64                  `json:"measured_bytes"`
	LimitBytes          uint64                  `json:"limit_bytes"`
	Overflow            bool                    `json:"overflow"`
	AdmittedBytes       uint64                  `json:"admitted_bytes"`
	RejectedChargeBytes uint64                  `json:"rejected_charge_bytes"`
	Usage               NativeDeltaDecodedUsage `json:"admitted_usage"`
}

func (e *NativeDeltaDecodedLimitError) Error() string {
	return fmt.Sprintf("native delta decoded-size limit: accounting=%s phase=%s measured_bytes=%d limit_bytes=%d overflow=%t", e.Accounting, e.Phase, e.MeasuredBytes, e.LimitBytes, e.Overflow)
}

type NativeDeltaDecodedUsage struct {
	Accounting                string            `json:"accounting"`
	LimitBytes                uint64            `json:"limit_bytes"`
	LimitOrigin               string            `json:"limit_origin"`
	PeakAccountedDecodedBytes uint64            `json:"peak_accounted_decoded_bytes"`
	Categories                map[string]uint64 `json:"categories"`
	Charges                   map[string]uint64 `json:"charges"`
}

// NativeDeltaBudget conservatively counts logical decoded payloads. Charges
// are cumulative and never released, even for shared allocations or discarded
// checkpoints. JSON charges use encoding/json's canonical map order and a
// terminal newline. Callers charge before retaining each bounded object.
// Wire parts are not charged: the native transport has no aggregate cap.
type NativeDeltaBudget struct {
	limit, used uint64
	categories  map[string]uint64
	charges     map[string]uint64
	origin      string
}

func NewNativeDeltaBudget(limit uint64) *NativeDeltaBudget {
	origin := "explicit library option"
	if limit == 0 {
		limit = NativeDeltaDefaultDecodedLimit
		origin = "default"
	}
	return &NativeDeltaBudget{limit: limit, categories: map[string]uint64{}, charges: map[string]uint64{}, origin: origin}
}

func (b *NativeDeltaBudget) ChargeBytes(phase string, size uint64) error {
	overflow := size > math.MaxUint64-b.used
	measured := b.used + size
	if overflow {
		measured = math.MaxUint64
	}
	if overflow || measured > b.limit {
		return &NativeDeltaDecodedLimitError{Accounting: NativeDeltaDecodedAccounting, Phase: phase, MeasuredBytes: measured, LimitBytes: b.limit, Overflow: overflow, AdmittedBytes: b.used, RejectedChargeBytes: size, Usage: b.Usage()}
	}
	b.used = measured
	b.categories[phase] += size
	b.charges[phase]++
	return nil
}

func (b *NativeDeltaBudget) ChargeJSON(phase string, value any) error {
	count, err := NativeDeltaJSONSize(value)
	if err != nil {
		return fmt.Errorf("count native delta %s: %w", phase, err)
	}
	return b.ChargeBytes(phase, count)
}

func (b *NativeDeltaBudget) Usage() NativeDeltaDecodedUsage {
	categories := make(map[string]uint64, len(b.categories))
	for key, count := range b.categories {
		categories[key] = count
	}
	charges := make(map[string]uint64, len(b.charges))
	for key, count := range b.charges {
		charges[key] = count
	}
	return NativeDeltaDecodedUsage{Accounting: NativeDeltaDecodedAccounting, LimitBytes: b.limit, LimitOrigin: b.origin, PeakAccountedDecodedBytes: b.used, Categories: categories, Charges: charges}
}
