package converterroundtrip

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestNativeDeltaBatchAccountingMatchesActualLoweredBytes(t *testing.T) {
	identity := dumpformat.SourceIdentity{StoreID: "public-store", Project: "public"}
	table := func(value string) dumpformat.NativeTable {
		return dumpformat.NativeTable{Columns: []string{"value"}, Rows: []dumpformat.Row{{"value": value}}}
	}
	states := []map[string]dumpformat.NativeTable{
		{"one": table("<first>&"), "dropped": table("old")},
		{"one": table("changed"), "added": table("new")},
		{"one": table("changed"), "added": table("new")},
		{},
	}
	before := []driver.Record{}
	var previous map[string]dumpformat.NativeTable
	for i, tables := range states {
		image := dumpformat.NativeHistoryImage{Sequence: uint64(i + 1), CommitID: "public-commit", Tables: tables}
		got, err := nativeDeltaBatchSize(uint64(i+1), identity, image, before, previous)
		if err != nil {
			t.Fatal(err)
		}
		after, err := nativeImageRecords(identity, tables)
		if err != nil {
			t.Fatal(err)
		}
		batch := driver.SourceHistoryBatch{SourceStoreID: identity.StoreID, Project: identity.Project, SourceSequence: image.Sequence, Sequence: uint64(i + 1), CommitIDs: []string{image.CommitID}, EventIDs: []string{}, Changes: historyDelta(before, after)}
		raw, err := json.Marshal(batch)
		if err != nil {
			t.Fatal(err)
		}
		if got != uint64(len(raw)+1) {
			t.Fatalf("transition %d measured=%d actual=%d", i, got, len(raw)+1)
		}
		before = after
		previous = tables
	}
}

type nativeDeltaBudgetReader struct {
	driver.Reader
	blobs map[driver.Hash][]byte
}

func (r nativeDeltaBudgetReader) Blob(_ context.Context, hash driver.Hash) ([]byte, error) {
	data, ok := r.blobs[hash]
	if !ok {
		return nil, driver.ErrConflict
	}
	return data, nil
}

func TestNativeDeltaRecoveryRetainsMeasuredLimitCauseAndConflict(t *testing.T) {
	// The head charge must refuse before parsing/retaining its contents. This
	// does not represent a valid source fixture or a successful receiving run.
	head := []byte("bounded public head charge")
	root := dumpformat.NativeCaptureSet{Format: dumpformat.NativeCaptureSetFormat, Contract: dumpformat.NativeDeltaFormat, StoreID: "public-store", Projects: []string{"public"}, Head: strings.Repeat("a", 32), HeadArchive: dumpformat.File{SHA256: hashBytes(head), Size: int64(len(head))}, PartCount: 1, StreamBytes: 1, FinalPartSHA256: strings.Repeat("b", 64), StreamSHA256: strings.Repeat("c", 64)}
	raw := encode(t, root)
	digest := hashBytes(raw)
	reader := nativeDeltaBudgetReader{blobs: map[driver.Hash][]byte{driver.Hash(digest): raw, driver.Hash(hashBytes(head)): head}}
	evidence := archiveEvidence{ArchiveSHA256: digest, NativeDecodedLimitBytes: 1, NativeCaptureFiles: map[string]dumpformat.File{"head.tar": root.HeadArchive}}
	profile := driver.RestoreImportProfile{ArchiveDigest: digest, HistoryTime: "2026-11-10T00:00:00Z", HistoryMode: driver.RestoreHistoryModeFull, SourcePlanes: &driver.SourcePlaneImportBindingV1{}}
	for _, invoke := range []func() error{
		func() error { return validateHistoryRecovery(context.Background(), reader, profile, evidence, nil) },
		func() error {
			_, _, _, err := recoverNativeFamilies(context.Background(), reader, profile, evidence, nil)
			return err
		},
	} {
		err := invoke()
		var limit *dumpformat.NativeDeltaDecodedLimitError
		if !errors.Is(err, driver.ErrConflict) || !errors.As(err, &limit) {
			t.Fatalf("recovery lost conflict or typed measured cause: %v", err)
		}
		if limit.Phase != "head_tar" || limit.MeasuredBytes != uint64(len(head)) || limit.LimitBytes != 1 || limit.AdmittedBytes != 0 || limit.RejectedChargeBytes != uint64(len(head)) || limit.Overflow || len(limit.Usage.Categories) != 0 {
			t.Fatalf("recovery changed pre-retain measured refusal: %+v", limit)
		}
		before, _ := json.Marshal(limit)
		wrapped := nativeDeltaRecoveryError(fmt.Errorf("recovery lowering: %w", err))
		var again *dumpformat.NativeDeltaDecodedLimitError
		if !errors.As(wrapped, &again) || !errors.Is(wrapped, driver.ErrConflict) {
			t.Fatal("lowering wrapper lost typed cause")
		}
		after, _ := json.Marshal(again)
		if !reflect.DeepEqual(before, after) {
			t.Fatal("measured refusal fields changed in wrapping")
		}
	}
	if nativeDeltaRecoveryError(errors.New("ordinary recovery failure")) != driver.ErrConflict {
		t.Fatal("legacy conflict behavior changed")
	}
}
