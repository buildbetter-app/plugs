package dumpformat

import (
	"context"
	"fmt"
	"sort"
)

func (a *Archive) decodedBudget() *NativeDeltaBudget {
	if a == nil {
		return nil
	}
	if a.nativeBudget != nil {
		return a.nativeBudget
	}
	if a.delta != nil {
		return a.delta.budget
	}
	return nil
}

// NativeDecodedUsage reports the same versioned live/cumulative domains for
// v1 and v2. The ceiling counts logical payload representations, not RSS.
func (a *Archive) NativeDecodedUsage() (NativeDeltaDecodedUsage, bool) {
	if a.decodedBudget() == nil {
		return NativeDeltaDecodedUsage{}, false
	}
	return a.decodedBudget().Usage(), true
}

// VisitNativeSourceHistories borrows the authenticated original views in
// source/project order. No historical row copy is made. A callback must not
// mutate or retain the borrowed tables, maps or slices. Scalar identities may
// be copied. V2 exposes only HEAD here; its other images require stream replay.
func (a *Archive) VisitNativeSourceHistories(ctx context.Context, visit func(NativeHistory) error) error {
	if a == nil || visit == nil {
		return fmt.Errorf("native source visitor unavailable")
	}
	var work *NativeDeltaBudget
	if budget := a.decodedBudget(); budget != nil {
		work = budget.Scope()
		defer work.Release()
		if err := work.ChargeBytes("source_visit_positions", uint64(len(a.nativeHistories))*8); err != nil {
			return err
		}
	}
	indices := make([]int, len(a.nativeHistories))
	for i := range indices {
		indices[i] = i
	}
	sort.Slice(indices, func(i, j int) bool {
		left, right := a.nativeHistories[indices[i]], a.nativeHistories[indices[j]]
		if left.StoreID != right.StoreID {
			return left.StoreID < right.StoreID
		}
		for k := 0; k < len(left.Projects) && k < len(right.Projects); k++ {
			if left.Projects[k] != right.Projects[k] {
				return left.Projects[k] < right.Projects[k]
			}
		}
		return len(left.Projects) < len(right.Projects)
	})
	for _, i := range indices {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := visit(a.nativeHistories[i]); err != nil {
			return err
		}
	}
	return ctx.Err()
}

// RecordsChecked reserves detached portable source records before cloning.
// The caller owns them until its conversion accounting scope is released.
func (a *Archive) RecordsChecked(kind string) ([]Record, error) {
	if err := a.ChargeNativeDeltaJSON("detached_portable_records", a.byKind[kind]); err != nil {
		return nil, err
	}
	return a.Records(kind), nil
}

func (a *Archive) ManifestChecked() (Manifest, error) {
	if err := a.ChargeNativeDeltaJSON("detached_manifest", a.manifest); err != nil {
		return Manifest{}, err
	}
	return a.Manifest(), nil
}

func (a *Archive) MemberChecked(path string) ([]byte, bool, error) {
	if err := a.ChargeNativeDeltaBytes("detached_member", uint64(len(a.members[path]))); err != nil {
		return nil, false, err
	}
	data, ok := a.Member(path)
	return data, ok, nil
}

func (a *Archive) TarBytesChecked() ([]byte, error) {
	if err := a.ChargeNativeDeltaBytes("detached_source_tar", uint64(len(a.tarBytes))); err != nil {
		return nil, err
	}
	return a.TarBytes(), nil
}
