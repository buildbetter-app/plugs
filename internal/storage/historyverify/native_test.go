package historyverify

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
)

// These producer tests use the actual frozen public capture. They do not mint a
// checkpoint; real issuance is exercised by the receiving production tests.
func nativeVerifierArchive(t *testing.T) *dumpformat.Archive {
	t.Helper()
	raw, err := os.ReadFile("../../integration/converterroundtrip/testdata/public-native-delta-small-0e4d5da4.tar.gz")
	if err != nil || nativeSHA(raw) != "66e2a1410cb6de0532e8bd453f06353d3fde8159ed9d6ffe4b0ed17f20bf6ef8" {
		t.Fatal("frozen public capture", err)
	}
	z, err := gzip.NewReader(bytes.NewReader(raw))
	if err != nil { t.Fatal(err) }
	defer z.Close()
	files := map[string][]byte{}
	input := tar.NewReader(z)
	for {
		h, err := input.Next()
		if err == io.EOF { break }
		if err != nil { t.Fatal(err) }
		if h.Typeflag != tar.TypeReg || !strings.HasPrefix(h.Name, "capture-set/") { continue }
		name := strings.TrimPrefix(h.Name, "capture-set/")
		files[name], err = io.ReadAll(input)
		if err != nil { t.Fatal(err) }
	}
	a, err := dumpformat.ReadNativeCaptureSet(context.Background(), bytes.NewReader(files["capture-set.json"]), bytes.NewReader(files["head.tar"]), func(_ context.Context, ordinal uint64) (io.ReadCloser, error) {
		raw, ok := files[fmt.Sprintf("parts/%020d.part", ordinal)]
		if !ok { return nil, os.ErrNotExist }
		return io.NopCloser(bytes.NewReader(raw)), nil
	}, dumpformat.NativeCaptureSetOptions{})
	if err != nil { t.Fatal(err) }
	t.Cleanup(a.ReleaseNativeDecoded)
	return a
}

func nativeVerifierProducer(t *testing.T, a *dumpformat.Archive) (*NativeArchiveStream, *nativeFrames) {
	t.Helper()
	owner, release := a.NativeDeltaAccountingScope()
	f, err := os.CreateTemp(t.TempDir(), "native-verifier-*.frames")
	if err != nil { release(); t.Fatal(err) }
	s := &NativeArchiveStream{archive: owner, release: release, file: f, path: f.Name(), when: time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC), files: map[string]dumpformat.File{}, nativeCurrent: map[string]checkpointRecord{}, revisions: map[string]model.Revision{}}
	t.Cleanup(func() { if err := s.Close(); err != nil { t.Error(err) } })
	b, err := newNativeFrames(context.Background(), s)
	if err != nil { t.Fatal(err) }
	return s, b
}

func TestNativeVerifierAuthenticProducerRejectsChangedIntermediatePreimages(t *testing.T) {
	a := nativeVerifierArchive(t)
	prepared, err := a.PrepareNativeDelta(context.Background())
	if err != nil { t.Fatal(err) }
	for _, attack := range []string{"physical-count", "physical-hash", "physical-schema", "converted-before", "commit-id", "sequence", "independent-head"} {
		t.Run(attack, func(t *testing.T) {
			_, b := nativeVerifierProducer(t, a)
			changed := false
			err := prepared.Visit(context.Background(), dumpformat.NativeDeltaVisitor{
				Commit: func(c dumpformat.NativeDeltaRowCommit) error {
					if attack == "converted-before" && !changed && c.Sequence > 1 && c.Sequence < b.stream.header.Head {
						// A newly inserted metadata row has no Before. Choose an
						// actual intermediate preimage rather than remarshal an
						// unchanged After row and claim a mutation occurred.
						for _, change := range c.Changes {
							if change.Before == nil { continue }
							original := bytes.Clone(change.Before.Body)
							change.Before.Body = append(change.Before.Body, ' ')
							changed = !bytes.Equal(original, change.Before.Body)
							t.Logf("NATIVE_PREIMAGE_MUTATION attack=converted-before sequence=%d original=%s changed=%s", c.Sequence, nativeSHA(original), nativeSHA(change.Before.Body))
							break
						}
					} else if c.Sequence == 2 && attack != "independent-head" && attack != "converted-before" {
						switch attack {
						case "commit-id": c.CommitID = "different-intermediate-commit"; changed = true
						case "sequence": c.Sequence++; changed = true
						default:
							for _, change := range c.Changes {
								if change.After == nil { continue }
								meta, _, err := a.DecodeNativeRowRecord(*change.After)
								if err != nil { return err }
								if meta == nil { continue }
								switch attack {
								case "physical-count": meta.RowCount++
								case "physical-hash": value := strings.Repeat("1", 64); meta.PhysicalTableSHA256 = &value
								case "physical-schema": meta.SchemaID = strings.Repeat("2", 64)
								}
								change.After.Body, err = json.Marshal(meta)
								if err != nil { return err }
								changed = true; break
							}
						}
					}
					return b.commit(c)
				},
				TableState: b.checkpoint,
				File: b.captureFile,
			})
            if err == nil && attack == "independent-head" {
				for key, value := range b.head { value.hash = strings.Repeat("3", 64); b.head[key] = value; changed = true; break }
			}
			if err == nil { err = b.finish() }
			if !changed { t.Fatal("intermediate preimage attack did not change its authentic input", attack) }
			switch attack {
			case "physical-schema", "converted-before":
				// These attacks fail in the genuine canonical native decoder before
				// transcript reconciliation. Require its exact typed cause so a
				// budget, I/O or unrelated refusal cannot satisfy the mutation test.
				want := "physical metadata/schema differs"
				if attack == "converted-before" { want = "noncanonical body" }
				var refusal *dumpformat.NativeRowRecordError
				if !errors.As(err, &refusal) || refusal.Reason != want { t.Fatal("missing exact native preimage refusal", attack, err) }
			default:
				want := "physical commit census differs from independently authenticated preimages"
				if attack == "commit-id" || attack == "sequence" { want = "native ordered graph differs" }
				if attack == "independent-head" { want = "exact transcript HEAD differs from independent checkpoint" }
				var refusal ErrCheckpoint
				if !errors.As(err, &refusal) || refusal.Reason != want { t.Fatal("missing exact transcript preimage refusal", attack, err) }
			}
		})
	}
}

func TestNativeVerifierProducerCloseReleasesPrivateDiskAndAccounting(t *testing.T) {
	a := nativeVerifierArchive(t)
	prepared, err := a.PrepareNativeDelta(context.Background())
	if err != nil { t.Fatal(err) }
	before, _ := a.NativeDecodedUsage()
	s, b := nativeVerifierProducer(t, a)
	if err := prepared.Visit(context.Background(), dumpformat.NativeDeltaVisitor{Commit: b.commit, TableState: b.checkpoint, File: b.captureFile}); err != nil { t.Fatal(err) }
	if err := b.finish(); err != nil { t.Fatal(err) }
	path := s.path
	if _, err := s.file.Seek(0, io.SeekStart); err != nil { t.Fatal(err) }
	// Retain a real decoded body frame as Next does. No test checkpoint is
	// issued: this checks the actual private transfer allocation and its owner.
	for {
		frame, releaseFrame, err := s.readFrame(context.Background(), s.file, NativeArchiveLimits())
		if err != nil { t.Fatal("actual spooled body frame missing", err) }
		if frame.Chunk != nil && len(frame.Chunk.Data) > 0 { s.pullRelease = releaseFrame; break }
		releaseFrame()
	}
	live, _ := a.NativeDecodedUsage()
	batch := live.CurrentCategories["offline_batches"] - before.CurrentCategories["offline_batches"]
	if live.CurrentCategories["offline_batches"] <= before.CurrentCategories["offline_batches"] || batch > uint64(nativeFrameBytes*8+1024) || live.PeakRetainedBatchBytes < batch { t.Fatal("real retained stream frame missing from bounded batch accounting", live) }
	ctx, cancel := context.WithCancel(context.Background()); cancel()
	if _, _, err := s.readFrame(ctx, s.file, NativeArchiveLimits()); !errors.Is(err, context.Canceled) { t.Fatal("cancelled source pull", err) }
	if err := s.Close(); err != nil { t.Fatal(err) }
	if err := s.Close(); err != nil { t.Fatal("idempotent close", err) }
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) { t.Fatal("private spool survived Close", err) }
	if s.archive != nil || s.file != nil || s.nativeCurrent != nil || s.revisions != nil || s.files != nil { t.Fatal("closed source retained released candidates") }
	after, _ := a.NativeDecodedUsage()
	if before.CurrentAccountedDecodedBytes != after.CurrentAccountedDecodedBytes || !reflect.DeepEqual(before.CurrentCategories, after.CurrentCategories) || !reflect.DeepEqual(before.CurrentCharges, after.CurrentCharges) { t.Fatal("private source reservations survived Close", before, after) }
}
