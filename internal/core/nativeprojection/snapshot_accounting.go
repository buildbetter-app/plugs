package nativeprojection

import "reflect"

func sliceCopyBytes[T any](values []T) uint64 {
	return uint64(len(values)) * uint64(reflect.TypeFor[T]().Size())
}
func pointerCopyBytes[T any](value *T) uint64 {
	if value == nil {
		return 0
	}
	return uint64(reflect.TypeFor[T]().Size())
}

// Count only allocations made by detachSnapshot; immutable strings/ref values
// are shared. Map entries count their key/value slots; Go map bucket and object
// headers remain allocator overhead, covered by the measured heap margin.
func snapshotCopyBytes(image *restorePlan, view Snapshot) uint64 {
	var n uint64
	for _, r := range image.records {
		n += uint64(len(r.body))
	}
	n += sliceCopyBytes(view.Derived)
	for _, d := range view.Derived {
		n += uint64(len(d.Value))
	}
	for key, body := range view.Blobs {
		_ = key
		n += uint64(reflect.TypeFor[string]().Size()+reflect.TypeFor[[]byte]().Size()) + uint64(len(body))
	}
	n += sliceCopyBytes(view.Allocators)
	for _, a := range view.Allocators {
		n += uint64(len(a.Children)) * uint64(reflect.TypeFor[string]().Size()+reflect.TypeFor[uint64]().Size())
	}
	n += sliceCopyBytes(view.Sentinels) + sliceCopyBytes(view.NativeFamilies) + sliceCopyBytes(view.NativeSchemas) + sliceCopyBytes(view.NativeHistories)
	for _, s := range view.NativeSchemas {
		n += sliceCopyBytes(s.Profile.Absences) + sliceCopyBytes(s.PlugDefaults) + sliceCopyBytes(s.TargetOmissions)
		for _, d := range s.PlugDefaults {
			n += uint64(len(d.Value))
		}
	}
	for _, h := range view.NativeHistories {
		n += pointerCopyBytes(h.Coverage)
		if h.Coverage == nil {
			continue
		}
		n += sliceCopyBytes(h.Coverage.Boundaries)
		for _, b := range h.Coverage.Boundaries {
			n += sliceCopyBytes(b.ChildCommitIDs) + pointerCopyBytes(b.CommitSchemaVersion) + sliceCopyBytes(b.SQLDoltCodes)
		}
	}
	return n
}
