package converterroundtrip

import (
	"context"
	"time"
	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/nativeprojection"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func targetStatus(source string) (string, bool) {
	return nativeprojection.TargetStatus(source)
}

func validSourceID(id string, project dumpformat.Project, imported bool) bool {
	return nativeprojection.ValidSourceID(id, project, imported)
}

func stringValue(value any) string {
	return nativeprojection.StringValue(value)
}

func cloneRow(input map[string]any) map[string]any {
	return nativeprojection.CloneRow(input)
}

func cloneValue(value any) any {
	return nativeprojection.CloneValue(value)
}

func sourceCreationTime(source dumpformat.Record) (*time.Time, error) {
	return nativeprojection.SourceCreationTime(source)
}

func importedSourceLink(body map[string]any, kind string, targetKind model.LinkTargetKind) (pipeline.Link, error) {
	return nativeprojection.ImportedSourceLink(body, kind, targetKind)
}

func importedVersionedSourceLink(source dumpformat.Record, kind string, targetKind model.LinkTargetKind, edgeVersion int) (pipeline.Link, error) {
	return nativeprojection.ImportedVersionedSourceLink(source, kind, targetKind, edgeVersion)
}

func nativeHistoryAccounting(archive *dumpformat.Archive) ([]NativeHistoryReport, error) {
	return nativeprojection.NativeHistoryAccounting(archive)
}

func nativeTableIdentity(scope dumpformat.NativeHistory) (dumpformat.SourceIdentity, error) {
	return nativeprojection.NativeTableIdentity(scope)
}

func nativePolicyRefusal(mode driver.RestoreHistoryMode, archive *dumpformat.Archive, scope dumpformat.NativeHistory) error {
	return nativeprojection.NativePolicyRefusal(mode, archive, scope)
}

func nativeImageRecords(identity dumpformat.SourceIdentity, tables map[string]dumpformat.NativeTable, owners ...*dumpformat.Archive) ([]driver.Record, error) {
	return nativeprojection.NativeImageRecords(identity, tables, owners...)
}

func nativeLeaseUnexpired(fields dumpformat.Row, when time.Time) (bool, error) {
	return nativeprojection.NativeLeaseUnexpired(fields, when)
}

func nativeCurrentClasses(scope dumpformat.NativeHistory) (map[string]string, map[string]int, error) {
	return nativeprojection.NativeCurrentClasses(scope)
}

func nativeCurrentRecords(archive *dumpformat.Archive, when time.Time, contexts ...context.Context) ([]driver.Record, []NativeFamilyReport, error) {
	return nativeprojection.NativeCurrentRecords(archive, when, contexts...)
}

func nativeDeltaNormalizedRow(row dumpformat.NativeDeltaRow) (dumpformat.Row, error) {
	return nativeprojection.NativeDeltaNormalizedRow(row)
}

func nativeDeltaCurrentRecords(ctx context.Context, archive *dumpformat.Archive, when time.Time) ([]driver.Record, []NativeFamilyReport, error) {
	return nativeprojection.NativeDeltaCurrentRecords(ctx, archive, when)
}

func nativeOperationContext(contexts []context.Context) context.Context {
	return nativeprojection.NativeOperationContext(contexts)
}

func marshalNativePayload(archive *dumpformat.Archive, phase string, value any) ([]byte, error) {
	return nativeprojection.MarshalNativePayload(archive, phase, value)
}

func nativeSourcePlan(archive *dumpformat.Archive, binding driver.SourcePlaneImportBindingV1) (driver.SourcePlanePlanV1, error) {
	return nativeprojection.NativeSourcePlan(archive, binding)
}

