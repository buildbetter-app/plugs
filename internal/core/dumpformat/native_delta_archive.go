package dumpformat

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
)

type nativeDeltaAttachment struct {
	root      NativeCaptureSet
	rootBytes []byte
	open      NativeDeltaPartOpener
	proof     NativeDeltaVerification
	budget    *NativeDeltaBudget
	recheck   func(context.Context) error
}

type NativeCaptureSetOptions struct {
	DecodedLimitBytes uint64
	// Empty means the default or an explicit library option, as appropriate.
	DecodedLimitOrigin string
}

type nativeDeltaAccountingContextKey struct{}

// WithNativeDeltaAccounting shares this verified archive's cumulative budget
// with subsequent reconstruction/recovery in the same receiving operation.
func (a *Archive) WithNativeDeltaAccounting(ctx context.Context) context.Context {
	if a == nil || a.delta == nil {
		return ctx
	}
	return context.WithValue(ctx, nativeDeltaAccountingContextKey{}, a.delta.budget)
}

// ReadNativeCaptureSet authenticates the entire stream before returning an
// archive. Root/head are bounded; parts are opened one at a time. The portable
// Read entry point refuses the same head's explicit v2 marker.
func ReadNativeCaptureSet(ctx context.Context, rootInput, headInput io.Reader, open NativeDeltaPartOpener, options NativeCaptureSetOptions) (*Archive, error) {
	rootBytes, err := io.ReadAll(io.LimitReader(rootInput, NativeDeltaRootBytes+1))
	if err != nil {
		return nil, err
	}
	if len(rootBytes) > NativeDeltaRootBytes {
		return nil, fmt.Errorf("native delta root limit")
	}
	var root NativeCaptureSet
	if err := decodeNativeDeltaJSON(rootBytes, &root); err != nil {
		return nil, err
	}
	if root.Format != NativeCaptureSetFormat || root.Contract != NativeDeltaFormat || root.PartCount == 0 || root.StreamBytes == 0 || !validDeltaSHA256(root.FinalPartSHA256) || !validDeltaSHA256(root.StreamSHA256) || root.HeadArchive.Size <= 0 || root.HeadArchive.Size > MaxArchiveBytes || !validDeltaSHA256(root.HeadArchive.SHA256) {
		return nil, fmt.Errorf("native delta root binding differs")
	}
	head, err := io.ReadAll(io.LimitReader(headInput, root.HeadArchive.Size+1))
	if err != nil {
		return nil, err
	}
	if int64(len(head)) != root.HeadArchive.Size || digest(head) != root.HeadArchive.SHA256 {
		return nil, fmt.Errorf("native delta head attachment differs")
	}
	budget := NewNativeDeltaBudget(options.DecodedLimitBytes)
	if shared, ok := ctx.Value(nativeDeltaAccountingContextKey{}).(*NativeDeltaBudget); ok {
		if options.DecodedLimitBytes != 0 && options.DecodedLimitBytes != shared.limit {
			return nil, fmt.Errorf("native delta recovery decoded limit differs")
		}
		budget = shared
	} else if options.DecodedLimitOrigin != "" {
		budget.origin = options.DecodedLimitOrigin
	}
	if err := budget.ChargeBytes("head_tar", uint64(len(head))); err != nil {
		return nil, err
	}
	// Reserve the complete raw input/member payload and retained-tar copies
	// before the legacy bounded head reader allocates them. This is a logical
	// conservative duplicate charge, not a claim about allocator overhead.
	if err := budget.ChargeBytes("head_tar_copies", 2*uint64(len(head))); err != nil {
		return nil, err
	}
	a, err := readArchive(bytes.NewReader(head), true)
	if err != nil {
		return nil, err
	}
	projects := []string{}
	for _, p := range a.manifest.Projects {
		projects = append(projects, p.Prefix)
	}
	if a.manifest.StoreID != root.StoreID || !equalJSON(projects, root.Projects) || len(projects) != 1 || !validCommitID(root.Head) {
		return nil, fmt.Errorf("native delta root/head identity differs")
	}
	parts, err := NewNativeDeltaPartReader(ctx, root.PartCount, root.FinalPartSHA256, root.StreamSHA256, open)
	if err != nil {
		return nil, err
	}
	defer parts.Close()
	h, proof, err := reconstructNativeDelta(ctx, parts, root, budget)
	if err != nil {
		return nil, err
	}
	if !parts.Verified() || parts.StreamBytes() != root.StreamBytes {
		return nil, fmt.Errorf("native delta stream commitment incomplete")
	}
	p, err := NativeSchemaForVersion(a.manifest.MainVersion, a.manifest.IgnoredVersion)
	if err != nil {
		return nil, err
	}
	h.SchemaProfile = &p
	h.Sources = append([]Disposition{}, a.manifest.Sources...)
	h.deltaVerified = true
	if err := validateNativeHistory(h); err != nil {
		return nil, err
	}
	if err := bindNativePortableCensus(a, h, false); err != nil {
		return nil, err
	}
	if err := bindNativeCurrent(h, a.byKind, false); err != nil {
		return nil, err
	}
	if err := validateNativeEventsAttachment(a, h, false); err != nil {
		return nil, err
	}
	a.nativeHistories = []NativeHistory{h}
	a.sha256 = digest(rootBytes)
	a.delta = &nativeDeltaAttachment{root: root, rootBytes: rootBytes, open: open, proof: proof, budget: budget}
	return a, nil
}

// ReadNativeCaptureSetDirectory accepts only the canonical fixed layout. A
// descriptor-scoped root plus same-file checks reject links and path races.
func ReadNativeCaptureSetDirectory(ctx context.Context, path string, options NativeCaptureSetOptions) (*Archive, error) {
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("native capture set requires a regular directory")
	}
	root, err := os.OpenRoot(path)
	if err != nil {
		return nil, err
	}
	defer root.Close()
	f, err := root.Open(".")
	if err != nil {
		return nil, err
	}
	actual, err := f.Stat()
	_ = f.Close()
	if err != nil || !os.SameFile(info, actual) {
		return nil, fmt.Errorf("native capture directory changed")
	}
	openRegular := func(name string) (*os.File, error) { return nativeDeltaRegularFile(root, name) }
	manifest, err := openRegular("capture-set.json")
	if err != nil {
		return nil, err
	}
	defer manifest.Close()
	raw, err := io.ReadAll(io.LimitReader(manifest, NativeDeltaRootBytes+1))
	if err != nil || len(raw) > NativeDeltaRootBytes {
		return nil, fmt.Errorf("native delta root limit")
	}
	var binding NativeCaptureSet
	if err := decodeNativeDeltaJSON(raw, &binding); err != nil {
		return nil, err
	}
	head, err := openRegular("head.tar")
	if err != nil {
		return nil, err
	}
	defer head.Close()
	dir, err := root.Open(".")
	if err != nil {
		return nil, err
	}
	entries, err := dir.ReadDir(-1)
	_ = dir.Close()
	if err != nil {
		return nil, err
	}
	if len(entries) != 3 {
		return nil, fmt.Errorf("native capture set directory census differs")
	}
	for _, entry := range entries {
		if entry.Name() != "capture-set.json" && entry.Name() != "head.tar" && entry.Name() != "parts" {
			return nil, fmt.Errorf("native capture set has extra entry")
		}
	}
	partInfo, err := root.Lstat("parts")
	if err != nil || !partInfo.IsDir() || partInfo.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("native parts requires a directory")
	}
	partDir, err := root.Open("parts")
	if err != nil {
		return nil, err
	}
	defer partDir.Close()
	var count uint64
	for {
		entries, err := partDir.ReadDir(256)
		if err != nil && err != io.EOF {
			return nil, err
		}
		for _, entry := range entries {
			name := entry.Name()
			if len(name) != 25 || filepath.Ext(name) != ".part" {
				return nil, fmt.Errorf("native delta part name differs")
			}
			ordinal, err := strconv.ParseUint(name[:20], 10, 64)
			if err != nil || ordinal == 0 || ordinal > binding.PartCount || fmt.Sprintf("%020d.part", ordinal) != name || entry.Type()&os.ModeSymlink != 0 || entry.IsDir() {
				return nil, fmt.Errorf("native delta part census differs")
			}
			if err := deltaAddCount(&count, 1); err != nil {
				return nil, err
			}
		}
		if err == io.EOF {
			break
		}
	}
	if count != binding.PartCount {
		return nil, fmt.Errorf("native delta part count differs")
	}
	// Reopen a descriptor-scoped root for each future evidence-retention read;
	// the archive owns no live source descriptors after this function returns.
	open := func(ctx context.Context, ordinal uint64) (io.ReadCloser, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		again, err := os.OpenRoot(path)
		if err != nil {
			return nil, err
		}
		defer again.Close()
		f, err := again.Open(".")
		if err != nil {
			return nil, err
		}
		current, err := f.Stat()
		_ = f.Close()
		if err != nil || !os.SameFile(info, current) {
			return nil, fmt.Errorf("native capture directory changed")
		}
		return nativeDeltaRegularFile(again, fmt.Sprintf("parts/%020d.part", ordinal))
	}
	a, err := ReadNativeCaptureSet(ctx, bytes.NewReader(raw), head, open, options)
	if err != nil {
		return nil, err
	}
	a.delta.recheck = func(ctx context.Context) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		again, err := os.OpenRoot(path)
		if err != nil {
			return err
		}
		defer again.Close()
		d, err := again.Open(".")
		if err != nil {
			return err
		}
		defer d.Close()
		current, err := d.Stat()
		if err != nil || !os.SameFile(info, current) {
			return fmt.Errorf("native capture directory changed")
		}
		entries, err := d.ReadDir(-1)
		if err != nil || len(entries) != 3 {
			return fmt.Errorf("native capture directory census changed")
		}
		for _, bound := range []struct {
			name string
			size int64
			hash string
		}{{"capture-set.json", int64(len(raw)), digest(raw)}, {"head.tar", binding.HeadArchive.Size, binding.HeadArchive.SHA256}} {
			f, err := nativeDeltaRegularFile(again, bound.name)
			if err != nil {
				return err
			}
			h := sha256.New()
			n, readErr := io.Copy(h, io.LimitReader(f, bound.size+1))
			closeErr := f.Close()
			if readErr != nil || closeErr != nil || n != bound.size || hex.EncodeToString(h.Sum(nil)) != bound.hash {
				return fmt.Errorf("native capture original changed: %s", bound.name)
			}
		}
		return nil
	}
	if err := a.delta.recheck(ctx); err != nil {
		return nil, err
	}
	return a, nil
}

func nativeDeltaRegularFile(root *os.Root, name string) (*os.File, error) {
	before, err := root.Lstat(name)
	if err != nil {
		return nil, err
	}
	if !before.Mode().IsRegular() || before.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("native capture input is not a regular file")
	}
	if filepath.Dir(name) != "." {
		parent, err := root.Lstat(filepath.Dir(name))
		if err != nil || !parent.IsDir() || parent.Mode()&os.ModeSymlink != 0 {
			return nil, fmt.Errorf("native capture parent is not a directory")
		}
	}
	f, err := root.Open(name)
	if err != nil {
		return nil, err
	}
	after, err := f.Stat()
	if err != nil || !os.SameFile(before, after) {
		_ = f.Close()
		return nil, fmt.Errorf("native capture input changed")
	}
	return f, nil
}

func (a *Archive) NativeDeltaVerification() (NativeDeltaVerification, bool) {
	if a == nil || a.delta == nil {
		return NativeDeltaVerification{}, false
	}
	p := a.delta.proof
	p.Usage = a.delta.budget.Usage()
	p.Commits = append([]NativeDeltaCommitProof{}, p.Commits...)
	for i := range p.Commits {
		tables := map[string]NativeDeltaTableDigest{}
		for k, v := range p.Commits[i].Tables {
			tables[k] = v
		}
		p.Commits[i].Tables = tables
	}
	return p, true
}
func (a *Archive) NativeDeltaRoot() ([]byte, bool) {
	if a == nil || a.delta == nil {
		return nil, false
	}
	return append([]byte(nil), a.delta.rootBytes...), true
}
func (a *Archive) ChargeNativeDeltaJSON(phase string, value any) error {
	if a == nil || a.delta == nil {
		return nil
	}
	return a.delta.budget.ChargeJSON(phase, value)
}

// ChargeNativeDeltaBytes admits a size computed without materializing a
// lowered aggregate. Legacy archives have no delta decoded-size budget.
func (a *Archive) ChargeNativeDeltaBytes(phase string, size uint64) error {
	if a == nil || a.delta == nil {
		return nil
	}
	return a.delta.budget.ChargeBytes(phase, size)
}

// NativeSourceHistoriesChecked accounts a detached full history view before
// copying it. Import/verification callers use this when they need table rows.
func (a *Archive) NativeSourceHistoriesChecked() ([]NativeHistory, error) {
	if a != nil && a.delta != nil {
		for _, h := range a.nativeHistories {
			if err := a.ChargeNativeDeltaJSON("detached_history_views", h); err != nil {
				return nil, err
			}
		}
	}
	return a.NativeSourceHistories(), nil
}

func (a *Archive) HasNativeSourceHistories() bool { return a != nil && len(a.nativeHistories) > 0 }

// NativeHistoryMetadata is the small detached source/census view. Querying
// identities, coverage or schemas must not copy every historical table.
type NativeHistoryMetadata struct {
	StoreID         string                 `json:"store_id"`
	Projects        []string               `json:"projects"`
	Complete        bool                   `json:"complete"`
	CommitCount     int                    `json:"commit_count"`
	ImageCount      int                    `json:"image_count"`
	Gaps            []NativeHistoryGap     `json:"gaps"`
	SchemaProfile   *NativeSchemaProfile   `json:"schema_profile,omitempty"`
	HistoryCoverage *NativeHistoryCoverage `json:"history_coverage,omitempty"`
}

func (a *Archive) NativeSourceHistoryMetadata() ([]NativeHistoryMetadata, error) {
	result := []NativeHistoryMetadata{}
	if a == nil {
		return result, nil
	}
	for _, h := range a.nativeHistories {
		value := NativeHistoryMetadata{h.StoreID, h.Projects, h.Complete, len(h.Commits), len(h.Images), h.Gaps, h.SchemaProfile, h.HistoryCoverage}
		if err := a.ChargeNativeDeltaJSON("detached_history_metadata", value); err != nil {
			return nil, err
		}
		metadata := h
		metadata.Commits = nil
		metadata.Images = nil
		metadata.Ignore = NativeTable{}
		metadata.Current = nil
		metadata.Sources = nil
		copy := cloneNativeDeltaHistory(metadata)
		value.Projects = copy.Projects
		value.Gaps = copy.Gaps
		value.SchemaProfile = copy.SchemaProfile
		value.HistoryCoverage = copy.HistoryCoverage
		result = append(result, value)
	}
	return result, nil
}

// VisitNativeCaptureFiles reauthenticates every original while retaining only
// one bounded part. The caller stages blobs; publication follows terminal hash
// verification. A later input change aborts that transaction.
func (a *Archive) VisitNativeCaptureFiles(ctx context.Context, visit func(string, []byte) error) error {
	if a == nil || a.delta == nil || visit == nil {
		return fmt.Errorf("native delta attachment unavailable")
	}
	if a.delta.recheck != nil {
		if err := a.delta.recheck(ctx); err != nil {
			return err
		}
	}
	if err := visit("capture-set.json", a.delta.rootBytes); err != nil {
		return err
	}
	if err := visit("head.tar", a.tarBytes); err != nil {
		return err
	}
	var previous string
	var streamBytes uint64
	// Validate complete framing again, independently of the blob visitor.
	reader, err := NewNativeDeltaPartReader(ctx, a.delta.root.PartCount, a.delta.root.FinalPartSHA256, a.delta.root.StreamSHA256, func(ctx context.Context, n uint64) (io.ReadCloser, error) {
		f, err := a.delta.open(ctx, n)
		if err != nil {
			return nil, err
		}
		raw, readErr := io.ReadAll(io.LimitReader(f, NativeDeltaPartBytes+NativeDeltaPartHeaderBytes+1))
		closeErr := f.Close()
		if readErr != nil {
			return nil, readErr
		}
		if closeErr != nil {
			return nil, closeErr
		}
		if len(raw) > NativeDeltaPartBytes+NativeDeltaPartHeaderBytes {
			return nil, fmt.Errorf("native delta part size exceeds file bound")
		}
		previous = digest(raw)
		if err := visit(fmt.Sprintf("parts/%020d.part", n), raw); err != nil {
			return nil, err
		}
		return io.NopCloser(bytes.NewReader(raw)), nil
	})
	if err != nil {
		return err
	}
	defer reader.Close()
	_, err = io.Copy(io.Discard, reader)
	streamBytes = reader.StreamBytes()
	if err != nil {
		return err
	}
	if !reader.Verified() || previous != a.delta.root.FinalPartSHA256 || streamBytes != a.delta.root.StreamBytes {
		return fmt.Errorf("native delta retained originals differ")
	}
	if a.delta.recheck != nil {
		return a.delta.recheck(ctx)
	}
	return nil
}
