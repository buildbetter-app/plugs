package converterroundtrip

import (
	"reflect"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/nativeprojection"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func importedAllocator(archive *dumpformat.Archive, project dumpformat.Project) (driver.OfflineAllocator, error) {
	return nativeprojection.ImportedAllocator(archive, project)
}

// Immutable import evidence retains the initial counter image. Later ordinary
// writes may only advance counters; they cannot change the admitted policy.
func allocatorContinues(seed, actual driver.OfflineAllocator) bool {
	if actual.LastID < seed.LastID {
		return false
	}
	for parent, n := range seed.Children {
		if actual.Children[parent] < n {
			return false
		}
	}
	seed.LastID, actual.LastID = 0, 0
	seed.Children, actual.Children = nil, nil
	return reflect.DeepEqual(seed, actual)
}
