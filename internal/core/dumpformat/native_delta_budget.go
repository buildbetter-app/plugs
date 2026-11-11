package dumpformat

import (
	"fmt"
	"math"
)

const NativeDeltaDecodedAccounting = "native-delta-decoded-accounting/3"
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

// Categories and Charges describe the simultaneous admitted high-water mark.
// Current* describe live reservations. Neither map counts discarded work again.
type NativeDeltaDecodedUsage struct {
	Accounting                   string            `json:"accounting"`
	LimitBytes                   uint64            `json:"limit_bytes"`
	LimitOrigin                  string            `json:"limit_origin"`
	PeakAccountedDecodedBytes    uint64            `json:"peak_accounted_decoded_bytes"`
	Categories                   map[string]uint64 `json:"categories"`
	Charges                      map[string]uint64 `json:"charges"`
	CurrentAccountedDecodedBytes uint64            `json:"current_accounted_decoded_bytes"`
	CurrentCategories            map[string]uint64 `json:"current_categories"`
	CurrentCharges               map[string]uint64 `json:"current_charges"`
	PeakRetainedBatchBytes       uint64            `json:"peak_retained_batch_bytes"`
	CumulativeCategories         map[string]uint64 `json:"cumulative_categories"`
	CumulativeCharges            map[string]uint64 `json:"cumulative_charges"`
	CumulativeOverflow           bool              `json:"cumulative_overflow"`
}

// NativeDeltaBudget counts simultaneously retained logical decoded payloads,
// including live transient candidates. JSON uses canonical encoding/json plus
// a terminal newline, not allocator or RSS bytes. Scope reservations are
// released when their objects cease to be retained. Wire parts are outside this
// decoded domain. A budget is owned by one sequential receiving operation.
type NativeDeltaBudget struct {
	limit, used                             uint64
	categories                              map[string]uint64
	charges                                 map[string]uint64
	origin                                  string
	parent                                  *NativeDeltaBudget
	closed                                  bool
	peak, peakBatches                       uint64
	peakCategories, peakCharges             map[string]uint64
	cumulativeCategories, cumulativeCharges map[string]uint64
	cumulativeOverflow                      bool
	observer                                func(NativeDeltaAllocationEvent)
	id, nextID                              uint64
	children                                map[*NativeDeltaBudget]struct{}
}

// NativeDeltaAllocationEvent is an optional scalar ownership trace. It is
// separate from payload/cumulative/RSS accounting and never retains a body.
type NativeDeltaAllocationEvent struct {
	Action string `json:"action"`
	Owner  uint64 `json:"owner"`
	Parent uint64 `json:"parent,omitempty"`
	Phase  string `json:"phase,omitempty"`
	Bytes  uint64 `json:"bytes"`
	Held   uint64 `json:"held"`
	Peak   uint64 `json:"peak"`
}

func (b *NativeDeltaBudget) Observe(observer func(NativeDeltaAllocationEvent)) {
	b.root().observer = observer
}

func (b *NativeDeltaBudget) trace(action, phase string, size uint64) {
	root := b.root()
	if root.observer == nil {
		return
	}
	var parent uint64
	if b.parent != nil {
		parent = b.parent.id
	}
	root.observer(NativeDeltaAllocationEvent{Action: action, Owner: b.id, Parent: parent, Phase: phase, Bytes: size, Held: root.used, Peak: root.peak})
}

// ReservedBytes reports this owner's live named domain, including its open
// children. It permits a consumer to admit a known prepared-image clone before
// calling an existing errorless byte-detaching accessor.
func (b *NativeDeltaBudget) ReservedBytes(phase string) uint64 {
	return b.categories[phase]
}

func NewNativeDeltaBudget(limit uint64) *NativeDeltaBudget {
	origin := "explicit library option"
	if limit == 0 {
		limit = NativeDeltaDefaultDecodedLimit
		origin = "default"
	}
	return &NativeDeltaBudget{limit: limit, origin: origin, id: 1, nextID: 1, categories: map[string]uint64{}, charges: map[string]uint64{}, peakCategories: map[string]uint64{}, peakCharges: map[string]uint64{}, cumulativeCategories: map[string]uint64{}, cumulativeCharges: map[string]uint64{}}
}

func (b *NativeDeltaBudget) root() *NativeDeltaBudget {
	for b.parent != nil {
		b = b.parent
	}
	return b
}

// Scope shares the ceiling and high-water mark, while owning its reservations.
// Releasing an owner also unwinds any unfinished child candidates. Transfers
// require all children to have ended; no child may escape a transferred owner.
func (b *NativeDeltaBudget) Scope() *NativeDeltaBudget {
	if b.closed {
		panic("native delta child of closed owner")
	}
	root := b.root()
	root.nextID++
	child := &NativeDeltaBudget{limit: b.limit, origin: b.origin, parent: b, id: root.nextID, categories: map[string]uint64{}, charges: map[string]uint64{}}
	if b.children == nil {
		b.children = map[*NativeDeltaBudget]struct{}{}
	}
	b.children[child] = struct{}{}
	return child
}

func nativeDeltaCounts(in map[string]uint64) map[string]uint64 {
	out := make(map[string]uint64, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func (b *NativeDeltaBudget) ChargeBytes(phase string, size uint64) error {
	root := b.root()
	for scope := b; scope != nil; scope = scope.parent {
		if scope.closed {
			return fmt.Errorf("native delta accounting scope closed")
		}
	}
	overflow := size > math.MaxUint64-root.used
	measured := root.used + size
	if overflow {
		measured = math.MaxUint64
	}
	if overflow || measured > root.limit {
		b.trace("refuse", phase, size)
		return &NativeDeltaDecodedLimitError{Accounting: NativeDeltaDecodedAccounting, Phase: phase, MeasuredBytes: measured, LimitBytes: root.limit, Overflow: overflow, AdmittedBytes: root.used, RejectedChargeBytes: size, Usage: root.Usage()}
	}
	for scope := b; scope != nil; scope = scope.parent {
		scope.used += size
		scope.categories[phase] += size
		scope.charges[phase]++
	}

	addDiagnostic := func(counts map[string]uint64, n uint64) {
		if n > math.MaxUint64-counts[phase] {
			counts[phase] = math.MaxUint64
			root.cumulativeOverflow = true
		} else {
			counts[phase] += n
		}
	}
	addDiagnostic(root.cumulativeCategories, size)
	addDiagnostic(root.cumulativeCharges, 1)
	if root.used > root.peak {
		root.peak = root.used
		root.peakCategories = nativeDeltaCounts(root.categories)
		root.peakCharges = nativeDeltaCounts(root.charges)
	}
	if root.categories["offline_batches"] > root.peakBatches {
		root.peakBatches = root.categories["offline_batches"]
	}
	b.trace("reserve", phase, size)
	return nil
}

// ReleaseBytes releases one explicitly owned reservation, for replacing a
// rolling table. Invalid releases are programming errors, never silent resets.
func (b *NativeDeltaBudget) ReleaseBytes(phase string, size uint64) {
	if b.closed || b.categories[phase] < size || b.charges[phase] == 0 {
		panic("invalid native delta accounting release")
	}
	for scope := b; scope != nil; scope = scope.parent {
		scope.used -= size
		scope.categories[phase] -= size
		scope.charges[phase]--
		if scope.charges[phase] == 0 {
			delete(scope.categories, phase)
			delete(scope.charges, phase)
		}
	}
	b.trace("release", phase, size)
}

func (b *NativeDeltaBudget) Release() {
	if b.closed || b.parent == nil {
		return
	}
	for child := range b.children {
		child.Release()
	}
	for phase, size := range b.categories {
		for scope := b.parent; scope != nil; scope = scope.parent {
			scope.used -= size
			scope.categories[phase] -= size
			scope.charges[phase] -= b.charges[phase]
			if scope.charges[phase] == 0 {
				delete(scope.categories, phase)
				delete(scope.charges, phase)
			}
		}
		b.trace("release", phase, size)
	}
	b.used = 0
	b.categories = map[string]uint64{}
	b.charges = map[string]uint64{}
	b.closed = true
	delete(b.parent.children, b)
}

// Retain transfers this scope's admitted reservations to its parent without
// copying payloads or changing live/peak totals. All child scopes must already
// be released. It is used only when a complete private candidate is returned.
func (b *NativeDeltaBudget) Retain() {
	if b.closed || b.parent == nil {
		return
	}
	b.RetainTo(b.parent)
}

// RetainTo transfers a complete candidate to an open ancestor. Intermediate
// page owners stop owning it, while the ancestor/root total remains unchanged.
// No byte copy or second allocation charge occurs during this handoff.
func (b *NativeDeltaBudget) RetainTo(owner *NativeDeltaBudget) {
	if b.closed {
		return
	}
	if len(b.children) != 0 {
		panic("native delta transfer has unfinished children")
	}
	found := false
	for scope := b.parent; scope != nil; scope = scope.parent {
		if scope == owner && !scope.closed {
			found = true
			break
		}
	}
	if !found {
		panic("native delta transfer owner is not an open ancestor")
	}
	for scope := b.parent; scope != owner; scope = scope.parent {
		scope.used -= b.used
		for phase, size := range b.categories {
			scope.categories[phase] -= size
			scope.charges[phase] -= b.charges[phase]
			if scope.charges[phase] == 0 {
				delete(scope.categories, phase)
				delete(scope.charges, phase)
			}
		}
	}
	if root := b.root(); root.observer != nil {
		root.observer(NativeDeltaAllocationEvent{Action: "transfer", Owner: b.id, Parent: owner.id, Bytes: b.used, Held: root.used, Peak: root.peak})
	}
	b.used = 0
	b.categories = nil
	b.charges = nil
	b.closed = true
	delete(b.parent.children, b)
}

func (b *NativeDeltaBudget) ChargeJSON(phase string, value any) error {
	count, err := NativeDeltaJSONSize(value)
	if err != nil {
		return fmt.Errorf("count native delta %s: %w", phase, err)
	}
	return b.ChargeBytes(phase, count)
}

func (b *NativeDeltaBudget) Usage() NativeDeltaDecodedUsage {
	root := b.root()
	return NativeDeltaDecodedUsage{Accounting: NativeDeltaDecodedAccounting, LimitBytes: root.limit, LimitOrigin: root.origin, PeakAccountedDecodedBytes: root.peak, Categories: nativeDeltaCounts(root.peakCategories), Charges: nativeDeltaCounts(root.peakCharges), CurrentAccountedDecodedBytes: root.used, CurrentCategories: nativeDeltaCounts(root.categories), CurrentCharges: nativeDeltaCounts(root.charges), PeakRetainedBatchBytes: root.peakBatches, CumulativeCategories: nativeDeltaCounts(root.cumulativeCategories), CumulativeCharges: nativeDeltaCounts(root.cumulativeCharges), CumulativeOverflow: root.cumulativeOverflow}
}
