package converterroundtrip

import (
	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/nativeprojection"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type NativePlugDefault = nativeprojection.NativePlugDefault
type NativeTargetOmission = nativeprojection.NativeTargetOmission
type NativeSchemaReport = nativeprojection.NativeSchemaReport

func nativeSchemaAccounting(archive *dumpformat.Archive, profile driver.RestoreImportProfile, docs map[string]pipeline.Document, allocators []driver.OfflineAllocator) ([]NativeSchemaReport, error) {
	return nativeprojection.NativeSchemaAccounting(archive, profile, docs, allocators)
}
