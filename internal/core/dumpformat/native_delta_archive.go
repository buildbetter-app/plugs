package dumpformat

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
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
	prepared  *nativeDeltaPreparation
	// usePreparedReadback reuses the authenticated retained staging view when
	// recovery needs to read back after the original source reader is closed.
	usePreparedReadback bool
}

type NativeCaptureSetOptions struct {
	DecodedLimitBytes uint64
	// Empty means the default or an explicit library option, as appropriate.
	DecodedLimitOrigin string
	// PrepareReceiving retains the authenticated row deltas, final checkpoints
	// and originals from this read for restart recovery. It changes no checks.
	PrepareReceiving bool
}

type nativeDeltaAccountingContextKey struct{}

// NativeDeltaBudgetFromContext returns the optional operation budget. Callers
// must own and release their child scopes; no budget preserves existing paths.
func NativeDeltaBudgetFromContext(ctx context.Context) *NativeDeltaBudget {
	budget, _ := ctx.Value(nativeDeltaAccountingContextKey{}).(*NativeDeltaBudget)
	return budget
}

// WithNativeDeltaBudget carries an existing sequential operation's optional
// budget. It changes no payload, source identity, admission or ceiling.
func WithNativeDeltaBudget(ctx context.Context, budget *NativeDeltaBudget) context.Context {
	if budget == nil {
		return ctx
	}
	return context.WithValue(ctx, nativeDeltaAccountingContextKey{}, budget)
}

// WithNativeDeltaAccounting shares this verified archive's live reservation budget
// with subsequent reconstruction/recovery in the same receiving operation.
func (a *Archive) WithNativeDeltaAccounting(ctx context.Context) context.Context {
	if a.decodedBudget() == nil {
		return ctx
	}
	return context.WithValue(ctx, nativeDeltaAccountingContextKey{}, a.decodedBudget())
}

// ReadNativeCaptureSet authenticates the entire stream before returning an
// archive. Root/head are bounded; parts are opened one at a time. The portable
// Read entry point refuses the same head's explicit v2 marker.
func ReadNativeCaptureSet(ctx context.Context, rootInput, headInput io.Reader, open NativeDeltaPartOpener, options NativeCaptureSetOptions) (*Archive, error) {
	budget := NewNativeDeltaBudget(options.DecodedLimitBytes)
	if shared, ok := ctx.Value(nativeDeltaAccountingContextKey{}).(*NativeDeltaBudget); ok {
		if options.DecodedLimitBytes != 0 && options.DecodedLimitBytes != shared.limit {
			return nil, fmt.Errorf("native delta recovery decoded limit differs")
		}
		budget = shared
	} else if options.DecodedLimitOrigin != "" {
		budget.origin = options.DecodedLimitOrigin
	}
	// An unsuccessful read releases only its own reservations, including when
	// recovery shares the outer receiving budget.
	owner := budget.Scope()
	success := false
	defer func() {
		if !success {
			owner.Release()
		}
	}()
	rootBytes, err := nativeDeltaReadBounded(rootInput, NativeDeltaRootBytes, owner, "retained_capture_root")
	if err != nil {
		return nil, err
	}
	var root NativeCaptureSet
	if err := decodeNativeDeltaJSONWithBudget(rootBytes, &root, owner); err != nil {
		return nil, err
	}
	if root.Format != NativeCaptureSetFormat || root.Contract != NativeDeltaFormat || root.PartCount == 0 || root.StreamBytes == 0 || !validDeltaSHA256(root.FinalPartSHA256) || !validDeltaSHA256(root.StreamSHA256) || root.HeadArchive.Size <= 0 || root.HeadArchive.Size > MaxArchiveBytes || !validDeltaSHA256(root.HeadArchive.SHA256) {
		return nil, fmt.Errorf("native delta root binding differs")
	}
	head, err := nativeDeltaReadBounded(headInput, uint64(root.HeadArchive.Size), owner, "head_tar")
	if err != nil {
		return nil, err
	}
	if int64(len(head)) != root.HeadArchive.Size || digest(head) != root.HeadArchive.SHA256 {
		return nil, fmt.Errorf("native delta head attachment differs")
	}
	a, err := readArchiveData(head, true, owner)
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
	preparation := &nativeDeltaPreparation{owner: owner}
	var staged *NativeDeltaPrepared
	var callbacks *nativeDeltaRowCallbacks
	readOpen := open
	if options.PrepareReceiving {
		staged = newNativeDeltaPrepared(owner)
		defer func() {
			if !success {
				staged.Close()
			}
		}()
		callbacks = staged.callbacks()
		readOpen = nativeDeltaFileOpener(open, staged.budget, staged.retainFile)
		if err := staged.retainFile("capture-set.json", rootBytes); err != nil {
			return nil, err
		}
		if err := staged.retainFile("head.tar", head); err != nil {
			return nil, err
		}
	}
	parts, err := NewNativeDeltaPartReader(ctx, root.PartCount, root.FinalPartSHA256, root.StreamSHA256, readOpen)
	if err != nil {
		return nil, err
	}
	defer parts.Close()
	h, proof, err := reconstructNativeDeltaCallbacks(ctx, parts, root, owner, nil, callbacks, true)
	if err != nil {
		return nil, err
	}
	if !parts.Verified() || parts.StreamBytes() != root.StreamBytes {
		return nil, fmt.Errorf("native delta stream commitment incomplete")
	}
	if err := parts.Close(); err != nil {
		return nil, err
	}
	p, err := NativeSchemaForVersion(a.manifest.MainVersion, a.manifest.IgnoredVersion)
	if err != nil {
		return nil, err
	}
	if err := owner.ChargeJSON("retained_schema_profile", p); err != nil {
		return nil, err
	}
	if err := owner.ChargeJSON("retained_source_census", a.manifest.Sources); err != nil {
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
	a.delta = &nativeDeltaAttachment{root: root, rootBytes: rootBytes, open: open, proof: proof, budget: owner, prepared: preparation}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if staged != nil {
		staged.ready = true
		preparation.view = staged
	}
	success = true
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
	budget := NewNativeDeltaBudget(options.DecodedLimitBytes)
	if shared, ok := ctx.Value(nativeDeltaAccountingContextKey{}).(*NativeDeltaBudget); ok {
		if options.DecodedLimitBytes != 0 && options.DecodedLimitBytes != shared.limit {
			return nil, fmt.Errorf("native delta recovery decoded limit differs")
		}
		budget = shared
	} else if options.DecodedLimitOrigin != "" {
		budget.origin = options.DecodedLimitOrigin
	}
	directory := budget.Scope()
	success := false
	defer func() {
		if !success {
			directory.Release()
		}
	}()
	raw, err := nativeDeltaReadBounded(manifest, NativeDeltaRootBytes, directory, "directory_capture_root")
	if err != nil {
		return nil, err
	}
	var binding NativeCaptureSet
	if err := decodeNativeDeltaJSONWithBudget(raw, &binding, directory, "directory_capture_binding"); err != nil {
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
	ctx = context.WithValue(ctx, nativeDeltaAccountingContextKey{}, directory)
	a, err := ReadNativeCaptureSet(ctx, bytes.NewReader(raw), head, open, options)
	if err != nil {
		return nil, err
	}
	// The directory owner retains the root binding used by recheck, as well
	// as the archive. Flatten the successful child so one release owns both.
	// A receiving preparation is an independently releasable child. Keep its
	// owner nested until that view ends rather than flattening a live child.
	if a.delta.prepared.view == nil {
		a.delta.budget.Retain()
	}
	a.nativeBudget, a.delta.budget = directory, directory
	a.delta.prepared.owner = directory
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
	success = true
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

// HasNativeDelta checks the authenticated format without cloning root bytes.
func (a *Archive) HasNativeDelta() bool { return a != nil && a.delta != nil }

func (a *Archive) NativeDeltaRoot() ([]byte, bool) {
	if a == nil || a.delta == nil {
		return nil, false
	}
	return append([]byte(nil), a.delta.rootBytes...), true
}
func (a *Archive) ChargeNativeDeltaJSON(phase string, value any) error {
	if a.decodedBudget() == nil {
		return nil
	}
	return a.decodedBudget().ChargeJSON(phase, value)
}

// ReleaseNativeDecoded drops a complete archive's receiving ownership. Call
// only after its source views and every conversion child are no longer live;
// the released archive must not be used for further conversion or readback.
func (a *Archive) ReleaseNativeDecoded() {
	a.ClearNativeDeltaPreparation()
	if budget := a.decodedBudget(); budget != nil {
		budget.Release()
	}
}

// ChargeNativeDeltaBytes admits a size computed without materializing a
// lowered aggregate. V1 and v2 receiving share the same held-byte ceiling.
func (a *Archive) ChargeNativeDeltaBytes(phase string, size uint64) error {
	if a.decodedBudget() == nil {
		return nil
	}
	return a.decodedBudget().ChargeBytes(phase, size)
}

// NativeDeltaAccountingScope owns temporary conversion reservations. Its
// shallow archive shares immutable source data and the operation's high-water
// mark. Release only after all values built through this scope are discarded.
func (a *Archive) NativeDeltaAccountingScope() (*Archive, func()) {
	if a.decodedBudget() == nil {
		return a, func() {}
	}
	copy := *a
	copy.nativeBudget = a.decodedBudget().Scope()
	if a.delta != nil {
		attachment := *a.delta
		attachment.budget = copy.nativeBudget
		copy.delta = &attachment
	}
	return &copy, copy.nativeBudget.Release
}

func WithNativeDeltaAccountingScope(ctx context.Context) (context.Context, func()) {
	if budget, ok := ctx.Value(nativeDeltaAccountingContextKey{}).(*NativeDeltaBudget); ok {
		scope := budget.Scope()
		return context.WithValue(ctx, nativeDeltaAccountingContextKey{}, scope), scope.Release
	}
	return ctx, func() {}
}

// ReleaseNativeDeltaBytes replaces an explicitly reserved rolling object.
func (a *Archive) ReleaseNativeDeltaBytes(phase string, size uint64) {
	if a.decodedBudget() != nil {
		a.decodedBudget().ReleaseBytes(phase, size)
	}
}

// VisitNativeDeltaImages reauthenticates the exact original stream, presenting
// one ephemeral normalized state at a time and the tables changed from the
// previous output state (which can differ from the physical first parent).
// A callback must neither mutate nor retain image tables. Candidate batches or
// readback results become usable only after verified terminal EOF returns nil.
func (a *Archive) VisitNativeDeltaImages(ctx context.Context, visit func(NativeHistoryImage, []string) error) error {
	if a == nil || a.delta == nil || visit == nil {
		return fmt.Errorf("native delta image stream unavailable")
	}
	return a.visitNativeDelta(ctx, visit, nil)
}

func (a *Archive) visitNativeDelta(ctx context.Context, visit func(NativeHistoryImage, []string) error, rows *nativeDeltaRowCallbacks) error {
	if a == nil || a.delta == nil {
		return fmt.Errorf("native delta stream unavailable")
	}
	// Prepared staging retains authenticated row/checkpoint callbacks but not
	// full commit table images. Use it only for row-change readback, whose
	// contract does not request complete table states. Complete table-state and
	// image visitors replay authenticated original parts so omitted commit
	// states cannot be mistaken for an empty baseline.
	if a.delta.usePreparedReadback {
		if a.delta.prepared == nil || a.delta.prepared.view == nil || !a.delta.prepared.view.ready {
			return fmt.Errorf("native delta readback preparation unavailable")
		}
		prepared := a.delta.prepared.view
		if visit == nil && rows != nil && rows.tableState == nil {
			return prepared.Visit(ctx, NativeDeltaVisitor{Commit: rows.commit, Checkpoint: rows.checkpoint})
		}
		// Complete states are reconstructed from the retained original parts,
		// not the checkpoint-only staging callbacks. The usual replay still
		// validates terminal EOF and every original stream commitment.
		return a.visitNativeDeltaOpen(ctx, visit, rows, prepared.openPart)
	}
	return a.visitNativeDeltaOpen(ctx, visit, rows, a.delta.open)
}

func (a *Archive) visitNativeDeltaOpen(ctx context.Context, visit func(NativeHistoryImage, []string) error, rows *nativeDeltaRowCallbacks, open NativeDeltaPartOpener) error {
	if a == nil || a.delta == nil {
		return fmt.Errorf("native delta stream unavailable")
	}
	if a.delta.recheck != nil {
		if err := a.delta.recheck(ctx); err != nil {
			return err
		}
	}
	parts, err := NewNativeDeltaPartReader(ctx, a.delta.root.PartCount, a.delta.root.FinalPartSHA256, a.delta.root.StreamSHA256, open)
	if err != nil {
		return err
	}
	defer parts.Close()
	replay := a.delta.budget.Scope()
	defer replay.Release()
	_, proof, err := reconstructNativeDeltaCallbacks(ctx, parts, a.delta.root, replay, visit, rows, true, a.nativeHistories[0].Commits)
	if err != nil {
		return err
	}
	for _, phase := range []string{"replay_actual_proof_bytes", "replay_expected_proof_bytes"} {
		if err := replay.ChargeJSON(phase, proof.Commits); err != nil {
			return err
		}
	}
	if !parts.Verified() || parts.StreamBytes() != a.delta.root.StreamBytes || proof.Totals != a.delta.proof.Totals || proof.HeadRootSHA256 != a.delta.proof.HeadRootSHA256 || proof.CurrentRootSHA256 != a.delta.proof.CurrentRootSHA256 || !equalJSON(proof.Commits, a.delta.proof.Commits) {
		return fmt.Errorf("native delta replay commitment differs")
	}
	if err := parts.Close(); err != nil {
		return err
	}
	if a.delta.recheck != nil {
		if err := a.delta.recheck(ctx); err != nil {
			return err
		}
	}
	return ctx.Err()
}

// NativeSourceHistoriesChecked reserves a detached source view before copying.
// For authenticated v2, Images contains HEAD only; use VisitNativeDeltaImages
// for history. Import/readback owners release their conversion scope afterward.
func (a *Archive) NativeSourceHistoriesChecked() ([]NativeHistory, error) {
	if a.decodedBudget() != nil {
		for _, h := range a.nativeHistories {
			if err := a.ChargeNativeDeltaJSON("detached_history_views", h); err != nil {
				return nil, err
			}
		}
	}
	return a.NativeSourceHistories(), nil
}

func (a *Archive) HasNativeSourceHistories() bool {
	return a != nil && (len(a.nativeHistories) > 0 || a.HasNativeCaptureCollection())
}

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
	if a.HasNativeCaptureCollection() {
		if err := a.ChargeNativeDeltaJSON("detached_history_metadata", a.collection.metadata); err != nil {
			return nil, err
		}
		if err := a.ChargeNativeDeltaJSON("detached_history_metadata_encoding", a.collection.metadata); err != nil {
			return nil, err
		}
		raw, err := json.Marshal(a.collection.metadata)
		if err != nil {
			return nil, err
		}
		if err := decodeJSON(raw, &result); err != nil {
			return nil, err
		}
		return result, nil
	}
	for _, h := range a.nativeHistories {
		value := NativeHistoryMetadata{h.StoreID, h.Projects, h.Complete, len(h.Commits), int(h.VerifiedImageCount()), h.Gaps, h.SchemaProfile, h.HistoryCoverage}
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
