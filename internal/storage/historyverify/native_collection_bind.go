package historyverify

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/nativeprojection"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// Bind is the second, final-profile boundary. The existing host first verifies
// its explicit source mappings and prepares the builtin provider from Owner's
// authenticated current rows. Here the private planner independently rebuilds
// DerivedDigest, and every fixed admission/source/policy/clock/target/provider
// field is checked before the transcript is bound. Seal later accepts only the
// same full profile and this actual immutable Plan. No source is reopened.
func (c *NativeCollection) Bind(ctx context.Context, profile driver.RestoreImportProfile) (_ *NativeArchiveStream, _ nativeprojection.Plan, _ driver.RestoreImportProfile, err error) {
	if c == nil || c.closed || c.failed || !c.finished || c.bound || profile.SourcePlanes == nil {
		return nil, nativeprojection.Plan{}, driver.RestoreImportProfile{}, ErrCheckpoint{"collection final bind is unavailable"}
	}
	defer func() {
		if err != nil {
			c.failed = true
		}
	}()
	if ctx == nil {
		return nil, nativeprojection.Plan{}, driver.RestoreImportProfile{}, ErrCheckpoint{"missing collection verification context"}
	}
	if err := ctx.Err(); err != nil {
		return nil, nativeprojection.Plan{}, driver.RestoreImportProfile{}, err
	}
	owner := c.Owner()
	work, release := owner.NativeDeltaAccountingScope()
	defer release()
	if err = func() error {
		plan, releasePlan, err := profile.SourcePlanes.PlanWithAccounting(nativeprojection.ProfileAccounting(work))
		if err != nil {
			return err
		}
		defer func() { plan = driver.SourcePlanePlanV1{}; releasePlan() }()
		if len(plan.Providers) != 1 {
			return ErrCheckpoint{"collection requires one checked provider"}
		}
		fixed, err := nativeCollectionAdmissionHash(work, NativeCollectionAdmission{Profile: profile, Target: plan.Target, Provider: plan.Providers[0]})
		if err != nil {
			return err
		}
		if fixed != c.fixedHash || c.admission.Provider.LogicalImageSHA256 != "" && (c.admission.Provider.LogicalImageSHA256 != plan.Providers[0].LogicalImageSHA256 || c.admission.Provider.LogicalImageSize != plan.Providers[0].LogicalImageSize) {
			return ErrCheckpoint{"collection fixed admission/provider differs"}
		}
		return nil
	}(); err != nil {
		return nil, nativeprojection.Plan{}, driver.RestoreImportProfile{}, err
	}
	wantDerived, wantHistory := profile.DerivedDigest, profile.HistoryDigest
	profile.HistoryDigest = ""
	if profile.DerivedDigest == "" {
		profile.DerivedDigest = strings.Repeat("0", 64)
	}
	derived, err := c.planner.DerivedDigest(ctx, profile)
	if err != nil {
		return nil, nativeprojection.Plan{}, driver.RestoreImportProfile{}, err
	}
	if wantDerived != "" && wantDerived != derived || c.wantDerived != "" && c.wantDerived != derived {
		return nil, nativeprojection.Plan{}, driver.RestoreImportProfile{}, ErrCheckpoint{"collection rebuilt derived digest differs"}
	}
	profile.DerivedDigest = derived
	profile, releaseProfile, err := nativeprojection.DetachedProfile(owner, profile)
	if err != nil {
		return nil, nativeprojection.Plan{}, driver.RestoreImportProfile{}, err
	}
	keepProfile := false
	defer func() {
		if !keepProfile {
			profile = driver.RestoreImportProfile{}
			releaseProfile()
		}
	}()
	if err = owner.ChargeNativeDeltaJSON("collection_source_order", c.binding.Sources); err != nil {
		return nil, nativeprojection.Plan{}, driver.RestoreImportProfile{}, err
	}
	order := make([]driver.NativeHistoryBoundary, 0, 2)
	for _, source := range c.binding.Sources {
		order = append(order, driver.NativeHistoryBoundary{SourceStoreID: source.SourceStoreID, Project: source.SourceProject})
	}
	s := c.stream
	s.header = driver.OfflineHistoryStreamHeader{Version: driver.OfflineHistoryStreamVersion, Source: driver.NativeTableHistorySource, Verified: true, Complete: c.complete, First: 1, Head: c.commits, NativeBoundaries: c.boundaries, NativeSourceCount: 2, NativeSourceOrderSHA256: driver.NativeSourceOrderSHA256(order)}
	if err = work.ChargeNativeDeltaBytes("collection_sorted_head_index", uint64(len(c.head))*32); err != nil {
		return nil, nativeprojection.Plan{}, driver.RestoreImportProfile{}, err
	}
	s.head, err = convertedRoot(c.head)
	if err != nil {
		return nil, nativeprojection.Plan{}, driver.RestoreImportProfile{}, err
	}
	if err = owner.ChargeNativeDeltaBytes("verifier_preparation_hash", 64); err != nil {
		return nil, nativeprojection.Plan{}, driver.RestoreImportProfile{}, err
	}
	s.profileHash, err = nativePreparationHash(owner, profile)
	if err != nil {
		return nil, nativeprojection.Plan{}, driver.RestoreImportProfile{}, err
	}
	s.digest, err = s.bindCollectionSpool(ctx)
	if err != nil {
		return nil, nativeprojection.Plan{}, driver.RestoreImportProfile{}, err
	}
	if wantHistory != "" && wantHistory != s.digest {
		return nil, nativeprojection.Plan{}, driver.RestoreImportProfile{}, ErrCheckpoint{"collection transcript digest differs"}
	}
	end := driver.OfflineHistoryStreamEnd{Commits: c.commits, Tables: c.tables, DecodedBytes: c.decoded}
	b := &nativeFrames{ctx: ctx, stream: s, digest: sha256.New()}
	if err = b.frame(driver.OfflineHistoryStreamFrame{Kind: "end", End: &end}, nil); err != nil {
		return nil, nativeprojection.Plan{}, driver.RestoreImportProfile{}, err
	}
	if err = s.file.Sync(); err != nil {
		return nil, nativeprojection.Plan{}, driver.RestoreImportProfile{}, err
	}
	if _, err = s.file.Seek(0, io.SeekStart); err != nil {
		return nil, nativeprojection.Plan{}, driver.RestoreImportProfile{}, err
	}
	profile.HistoryDigest = s.digest
	actual, err := c.planner.Build(ctx, profile)
	if err != nil {
		return nil, nativeprojection.Plan{}, driver.RestoreImportProfile{}, err
	}
	c.bound = true
	c.head, c.ids = nil, nil
	c.releaseProfile, keepProfile = releaseProfile, true
	return s, actual, profile, nil
}

// Only verifier-authored private frames are scanned here. This binds the final
// header without a fourth traversal of either original capture. The receiving
// validator independently hashes these exact stored bytes during consumption.
func (s *NativeArchiveStream) bindCollectionSpool(ctx context.Context) (_ string, err error) {
	input, err := os.Open(s.path)
	if err != nil {
		return "", err
	}
	defer func() { err = errors.Join(err, input.Close()) }()
	owner, release := s.archive.NativeDeltaAccountingScope()
	defer release()
	if err = owner.ChargeNativeDeltaJSON("collection_header_encoding", s.header); err != nil {
		return "", err
	}
	raw, err := json.Marshal(s.header)
	if err != nil {
		return "", err
	}
	h := sha256.New()
	h.Write([]byte(driver.OfflineHistoryStreamVersion + "\n"))
	h.Write(raw)
	h.Write([]byte{'\n'})
	if err = owner.ChargeNativeDeltaBytes("collection_spool_hash_buffer", 32<<10); err != nil {
		return "", err
	}
	buffer := make([]byte, 32<<10)
	for {
		if err = ctx.Err(); err != nil {
			return "", err
		}
		var prefix [8]byte
		n, e := io.ReadFull(input, prefix[:])
		if n == 0 && e == io.EOF {
			break
		}
		if e != nil {
			return "", e
		}
		size := binary.BigEndian.Uint64(prefix[:])
		if size == 0 || size > nativeFrameBytes {
			return "", ErrCheckpoint{"invalid collection spool frame length"}
		}
		copied, e := io.CopyBuffer(h, io.LimitReader(input, int64(size)), buffer)
		if e != nil {
			return "", e
		}
		if uint64(copied) != size {
			return "", io.ErrUnexpectedEOF
		}
		h.Write([]byte{'\n'})
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func (c *NativeCollection) spoolOriginal(name string, raw []byte) error {
	want := c.binding.Sources[c.next]
	var expected *dumpformat.NativeCaptureFile
	for i := range want.Files {
		if want.Files[i].Name == name {
			expected = &want.Files[i]
			break
		}
	}
	if expected == nil || uint64(len(raw)) != expected.Bytes || nativeSHA(raw) != expected.SHA256 {
		return ErrCheckpoint{"collection original differs from ordered input ledger"}
	}
	if err := c.Owner().ChargeNativeDeltaBytes("collection_spooled_original_ledger", uint64(len(name))*4+512); err != nil {
		return err
	}
	qualified := dumpformat.NativeCollectionFileName(c.next, name)
	if _, duplicate := c.stream.files[qualified]; duplicate {
		return ErrCheckpoint{"collection original is repeated"}
	}
	path := filepath.Join(c.dir, qualified)
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	_, writeErr := f.Write(raw)
	if err = errors.Join(writeErr, f.Close()); err != nil {
		return err
	}
	c.stream.files[qualified] = dumpformat.File{SHA256: expected.SHA256, Size: int64(expected.Bytes)}
	return nil
}

// VisitOriginalFiles exposes one bounded verified private spool buffer at a
// time. It never reopens an original source. The callback borrows the bytes;
// receiving must charge any retained transaction clone to the session owner.
func (s *NativeArchiveStream) VisitOriginalFiles(ctx context.Context, visit func(string, []byte) error) error {
	if s == nil || s.closed || s.digest == "" || s.originalDir == "" || visit == nil {
		return ErrCheckpoint{"collection original spool unavailable"}
	}
	owner, release := s.archive.NativeDeltaAccountingScope()
	defer release()
	if err := owner.ChargeNativeDeltaBytes("collection_sorted_original_paths", uint64(len(s.files))*256); err != nil {
		return err
	}
	names := make([]string, 0, len(s.files))
	for name := range s.files {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if err := s.visitOriginalFile(ctx, name, visit); err != nil {
			return err
		}
	}
	return nil
}

func (s *NativeArchiveStream) visitOriginalFile(ctx context.Context, name string, visit func(string, []byte) error) (err error) {
	if err = ctx.Err(); err != nil {
		return err
	}
	work, release := s.archive.NativeDeltaAccountingScope()
	defer release()
	want := s.files[name]
	if want.Size <= 0 || uint64(want.Size) > uint64(^uint(0)>>1) {
		return ErrCheckpoint{"invalid collection original size"}
	}
	if err = work.ChargeNativeDeltaBytes("collection_original_staging_buffer", uint64(want.Size)); err != nil {
		return err
	}
	f, err := os.Open(filepath.Join(s.originalDir, name))
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, f.Close()) }()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() != want.Size {
		return ErrCheckpoint{"collection original spool shape differs"}
	}
	raw := make([]byte, int(want.Size))
	if _, err = io.ReadFull(f, raw); err != nil {
		return err
	}
	var extra [1]byte
	if n, e := f.Read(extra[:]); n != 0 || e != io.EOF || nativeSHA(raw) != want.SHA256 {
		return ErrCheckpoint{"collection original spool changed"}
	}
	return visit(name, raw)
}
