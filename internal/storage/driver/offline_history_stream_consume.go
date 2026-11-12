package driver

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"hash"
	"io"
	"math"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

// PrepareOfflineHistoryStream never calls the aggregate builder/digester. All
// frames are staged synchronously under backpressure; no prefetch or goroutine
// outlives it. Source and stage ownership transfer at entry. Any failure closes
// the source and aborts staging, preserving both primary and cleanup errors.
func PrepareOfflineHistoryStream(ctx context.Context, request OfflineHistoryStreamRequest, source OfflineHistoryStreamSource, stage OfflineHistoryStreamStage) (prepared *PreparedOfflineHistoryStream, err error) {
	sourceClosed := false
	defer func() {
		if source != nil && !sourceClosed {
			err = errors.Join(err, source.Close())
		}
		if err != nil && stage != nil {
			err = errors.Join(err, stage.Abort())
			prepared = nil
		}
	}()
	if source == nil || stage == nil {
		return nil, streamError("source and private staging are required")
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	if request.Limits.MaxChunkBytes <= 0 || request.Limits.MaxFrameBytes <= 0 || request.Limits.MaxChunkBytes > request.Limits.MaxFrameBytes {
		return nil, streamError("positive per-pull frame/chunk limits are required")
	}
	if !streamText(request.Header.Source) || len(request.Header.Source) > request.Limits.MaxFrameBytes || request.Header.Version != OfflineHistoryStreamVersion {
		return nil, streamError("invalid raw header identity/version/metadata bound")
	}
	raw, err := json.Marshal(request.Header)
	if err != nil || len(raw) > request.Limits.MaxFrameBytes {
		return nil, streamError("header exceeds metadata pull bound")
	}
	var header OfflineHistoryStreamHeader
	if err = json.Unmarshal(raw, &header); err != nil {
		return nil, err
	}
	binding, err := prepareStreamBinding(request, header)
	if err != nil {
		return nil, err
	}
	if err = stage.Begin(ctx, binding); err != nil {
		return nil, err
	}
	v := streamValidator{header: header, nextGap: 1, digest: sha256.New()}
	v.digest.Write([]byte(OfflineHistoryStreamVersion + "\n"))
	streamHashJSON(v.digest, raw)
	for {
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		frame, pullErr := source.Next(ctx, request.Limits)
		if err = ctx.Err(); err != nil {
			return nil, errors.Join(err, pullErr)
		}
		if pullErr != nil {
			if pullErr != io.EOF || !v.ended || !emptyStreamFrame(frame) {
				return nil, errors.Join(streamError("pull failed or ended without a clean terminal EOF"), pullErr)
			}
			break
		}
		if v.ended {
			return nil, streamError("frame after terminal completion")
		}
		// Reject decoded chunks before encoding; metadata and the canonical
		// frame are bounded too. Detached frames tolerate producer buffer reuse.
		if frame.Chunk != nil && len(frame.Chunk.Data) > request.Limits.MaxChunkBytes {
			return nil, streamError("decoded chunk exceeds pull bound")
		}
		if err = streamRawMetadata(frame, request.Limits.MaxFrameBytes); err != nil {
			return nil, err
		}
		raw, err = json.Marshal(frame)
		if err != nil || len(raw) > request.Limits.MaxFrameBytes {
			return nil, streamError("frame exceeds metadata pull bound")
		}
		var detached OfflineHistoryStreamFrame
		if err = json.Unmarshal(raw, &detached); err != nil {
			return nil, err
		}
		if len(raw) > v.stats.PeakFrameBytes {
			v.stats.PeakFrameBytes = len(raw)
		}
		if err = v.accept(detached, raw); err != nil {
			return nil, err
		}
		if detached.Kind != "end" {
			streamHashJSON(v.digest, raw)
			if err = stage.Stage(ctx, v.revision(), detached); err != nil {
				return nil, err
			}
			if err = ctx.Err(); err != nil {
				return nil, err
			}
		}
	}
	digest := hex.EncodeToString(v.digest.Sum(nil))
	if digest != binding.Profile.HistoryDigest {
		return nil, streamError("stream transcript differs from admitted history digest")
	}
	// Closing is part of validation, before a publish-capable token exists.
	sourceClosed = true
	if err = source.Close(); err != nil {
		return nil, err
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	requestHash, err := binding.Profile.RequestHash()
	if err != nil {
		return nil, err
	}
	completion := OfflineHistoryStreamCompletion{Meta: binding.Meta, HistorySHA256: digest, RequestHash: requestHash, Stats: v.stats}
	return &PreparedOfflineHistoryStream{session: &offlineHistoryStreamSession{stage: stage, binding: binding, completion: completion}}, nil
}

func prepareStreamBinding(r OfflineHistoryStreamRequest, h OfflineHistoryStreamHeader) (OfflineHistoryStreamBinding, error) {
	if !r.Current.Head.IsZero() || !r.Current.HistoryFloor.IsZero() {
		return OfflineHistoryStreamBinding{}, ErrRestoreTargetNotEmpty
	}
	var p RestoreImportProfile
	var err error
	if r.Profile.SourcePlanes != nil {
		p, err = PrepareSourcePlaneHistoryProfile(r.Profile, r.Current, r.Lifetime)
	} else {
		p, err = r.Profile.DetachedCoreOnly()
	}
	if err != nil {
		return OfflineHistoryStreamBinding{}, err
	}
	if p.NativeDeltaHistory != nil {
		return OfflineHistoryStreamBinding{}, streamError("native delta mode provenance requires the capture-set importer")
	}
	if h.Version != OfflineHistoryStreamVersion || !h.Verified || !streamText(h.Source) || h.First == 0 || h.Head < h.First || p.HistoryMode == RestoreHistoryModeSnapshot || p.HistoryDigest == "" || r.When.IsZero() || p.HistoryTime != r.When.UTC().Format(time.RFC3339Nano) {
		return OfflineHistoryStreamBinding{}, streamError("invalid verified range/profile/time")
	}
	partial, bounded := h.First > 1, h.NativeBoundaries > 0
	ordered := h.NativeSourceOrderSHA256 != ""
	if ordered && (h.Source != NativeTableHistorySource || p.SourcePlanes == nil || h.NativeSourceCount != 2 || !importDigest(h.NativeSourceOrderSHA256) || partial || h.BaselineVerified) || !ordered && h.NativeSourceCount != 0 {
		return OfflineHistoryStreamBinding{}, streamError("invalid native collection source order")
	}
	if bounded && (h.Source != NativeTableHistorySource || p.SourcePlanes == nil || p.HistoryMode != RestoreHistoryModeFull || h.Complete || partial || h.BaselineVerified) {
		return OfflineHistoryStreamBinding{}, streamError("invalid native source boundary policy")
	}
	if h.Source == NativeTableHistorySource && p.SourcePlanes == nil {
		return OfflineHistoryStreamBinding{}, streamError("native table history requires joint source-plane publication")
	}
	if partial {
		if p.HistoryMode != RestoreHistoryModePartial || h.Complete || !h.BaselineVerified || bounded {
			return OfflineHistoryStreamBinding{}, streamError("partial history requires a proven baseline and exact prefix gaps")
		}
	} else if !h.Complete && !bounded || h.BaselineVerified {
		return OfflineHistoryStreamBinding{}, streamError("full history requires complete coverage or native boundaries")
	}
	units := h.Head - h.First + 1
	extra := uint64(1)
	if partial {
		extra++
	}
	if units > math.MaxUint64-extra {
		return OfflineHistoryStreamBinding{}, streamError("history revision exhausted")
	}
	meta, err := NewOfflineRestoreMeta(r.Current, p.SourceStoreID)
	if err != nil {
		return OfflineHistoryStreamBinding{}, err
	}
	meta.Head, meta.HistoryFloor = model.Natural64(units+1), model.Natural64(0)
	if partial {
		meta.Head, meta.HistoryFloor = model.Natural64(units+2), model.Natural64(1)
	}
	return OfflineHistoryStreamBinding{Current: r.Current, Meta: meta, Profile: p, Header: h, When: r.When, Lifetime: r.Lifetime}, nil
}

func streamText(s string) bool {
	return s != "" && utf8.ValidString(s) && strings.TrimSpace(s) == s && !strings.ContainsRune(s, '\x00')
}

// Check ORIGINAL Go strings before encoding/json can replace invalid UTF-8.
// A fixed number of individually bounded fields also bounds the temporary JSON
// allocation, even if a faulty trusted source violates its pull contract. Image
// byte chunks are opaque and may split inside a UTF-8 sequence.
func streamRawMetadata(f OfflineHistoryStreamFrame, limit int) error {
	texts := []string{f.Kind}
	if f.Gap != nil {
		texts = append(texts, f.Gap.Reason)
	}
	if f.Boundary != nil {
		texts = append(texts, f.Boundary.SourceStoreID, f.Boundary.Project, f.Boundary.BoundarySHA256)
	}
	if f.Commit != nil {
		texts = append(texts, f.Commit.SourceStoreID, f.Commit.Project)
		if f.Commit.Physical != nil {
			texts = append(texts, f.Commit.Physical.Root, f.Commit.Physical.TablesSHA256)
		}
	}
	if f.ID != nil {
		texts = append(texts, f.ID.Kind, f.ID.Value)
	}
	if f.Table != nil {
		t := f.Table
		texts = append(texts, t.Name, t.Kind, t.Ref.ID, t.Ref.Key, string(t.Ref.Branch))
		for _, image := range []*OfflineHistoryStreamImage{t.Before, t.After} {
			if image != nil {
				texts = append(texts, image.SHA256)
			}
		}
		for _, physical := range []*OfflineHistorySourceTable{t.PhysicalBefore, t.PhysicalAfter} {
			if physical != nil {
				texts = append(texts, physical.SchemaSHA256, physical.TableSHA256)
			}
		}
	}
	if f.Chunk != nil {
		texts = append(texts, f.Chunk.Image)
	}
	if f.CommitEnd != nil {
		texts = append(texts, f.CommitEnd.SHA256)
	}
	for _, text := range texts {
		if !utf8.ValidString(text) || len(text) > limit {
			return streamError("raw metadata is invalid UTF-8 or exceeds pull bound")
		}
	}
	return nil
}

func streamHashJSON(h hash.Hash, raw []byte) {
	h.Write(raw)
	h.Write([]byte{'\n'})
}

func emptyStreamFrame(f OfflineHistoryStreamFrame) bool {
	return f.Kind == "" && streamPayloads(f) == 0
}

func streamPayloads(f OfflineHistoryStreamFrame) int {
	n := 0
	for _, present := range []bool{f.Gap != nil, f.Boundary != nil, f.Commit != nil, f.ID != nil, f.Table != nil, f.Chunk != nil, f.CommitEnd != nil, f.End != nil} {
		if present {
			n++
		}
	}
	return n
}

type streamValidator struct {
	header       OfflineHistoryStreamHeader
	digest       hash.Hash
	stats        OfflineHistoryStreamStats
	nextGap      uint64
	started      bool
	baselineDone bool
	ended        bool
	boundary     *NativeHistoryBoundary
	boundaries   uint64
	boundaryUsed uint64
	commit       *OfflineHistoryStreamCommit
	commitHash   hash.Hash
	ids          [3]uint64
	idKind       int
	tables       uint64
	previousKey  string
	table        *OfflineHistoryStreamTable
	images       [2]hash.Hash
	imageBytes   [2]uint64
	imageSide    int
	lastSource   string
	lastSequence uint64
	sourceOrder  hash.Hash
	sourceSeen   map[string]bool

	physicalName   string
	physicalTables uint64
	physical       [2]OfflineHistorySourceTable
	physicalSet    [2]bool
}

func (v *streamValidator) revision() model.Revision {
	rev := v.stats.Commits
	if v.header.First > 1 {
		rev++
	}
	return model.Natural64(rev)
}

func (v *streamValidator) accept(f OfflineHistoryStreamFrame, raw []byte) error {
	wantPayloads := 1
	if f.Kind == "table-end" {
		wantPayloads = 0
	}
	if streamPayloads(f) != wantPayloads {
		return streamError("frame has missing or multiple payloads")
	}
	switch f.Kind {
	case "gap":
		g := f.Gap
		if g == nil || v.started || v.header.First == 1 || g.First != v.nextGap || g.Last < g.First || g.Last >= v.header.First || !streamText(g.Reason) {
			return streamError("gap is not an exact ordered partial prefix")
		}
		v.nextGap = g.Last + 1
	case "boundary":
		b := f.Boundary
		if b == nil || v.commit != nil || v.header.NativeBoundaries == 0 || v.boundaries == v.header.NativeBoundaries || !streamText(b.SourceStoreID) || !streamText(b.Project) || b.VerifiedCommitCount == 0 || !importDigest(b.BoundarySHA256) {
			return streamError("invalid native source boundary")
		}
		if v.boundary != nil && (v.boundaryUsed != v.boundary.VerifiedCommitCount || v.header.NativeSourceOrderSHA256 == "" && streamSourceKey(b.SourceStoreID, b.Project) <= streamSourceKey(v.boundary.SourceStoreID, v.boundary.Project)) {
			return streamError("native boundary count/order differs from verified commits")
		}
		v.boundary, v.boundaryUsed = b, 0
		v.boundaries++
	case "commit":
		if f.Commit == nil || v.commit != nil {
			return streamError("nested or missing commit")
		}
		if err := v.beginCommit(f.Commit); err != nil {
			return err
		}
		v.commitHash = sha256.New()
		v.commitHash.Write([]byte("plug-offline-history-commit/1\n"))
		streamHashJSON(v.commitHash, raw)
	case "id":
		if f.ID == nil || v.commit == nil || v.table != nil || v.tables != 0 || !streamText(f.ID.Value) {
			return streamError("provenance outside commit prelude")
		}
		index := -1
		switch f.ID.Kind {
		case "commit":
			index = 0
		case "parent":
			index = 1
		case "event":
			index = 2
		}
		if index < v.idKind || index < 0 {
			return streamError("provenance category order changed")
		}
		counts := v.idCounts()
		for i := 0; i < index; i++ {
			if v.ids[i] != counts[i] {
				return streamError("missing ordered provenance")
			}
		}
		if v.ids[index] == counts[index] {
			return streamError("extra provenance")
		}
		v.idKind = index
		v.ids[index]++
		streamHashJSON(v.commitHash, raw)
	case "table":
		if f.Table == nil || v.commit == nil || v.table != nil || v.ids != v.idCounts() {
			return streamError("table outside commit or before complete provenance")
		}
		if err := v.beginTable(f.Table); err != nil {
			return err
		}
		streamHashJSON(v.commitHash, raw)
	case "chunk":
		if f.Chunk == nil || v.table == nil {
			return streamError("image bytes outside a table")
		}
		return v.chunk(f.Chunk)
	case "table-end":
		if v.table == nil {
			return streamError("table end without table")
		}
		for i, image := range []*OfflineHistoryStreamImage{v.table.Before, v.table.After} {
			if image != nil && (v.imageBytes[i] != image.Bytes || hex.EncodeToString(v.images[i].Sum(nil)) != image.SHA256) {
				return streamError("converted table image size/hash mismatch")
			}
		}
		v.table, v.images = nil, [2]hash.Hash{}
	case "commit-end":
		if f.CommitEnd == nil || v.commit == nil || v.table != nil || v.tables != v.commit.Tables || v.ids != v.idCounts() || hex.EncodeToString(v.commitHash.Sum(nil)) != f.CommitEnd.SHA256 {
			return streamError("commit count/provenance/hash mismatch")
		}
		if v.commit.Baseline {
			v.baselineDone = true
		}
		v.commit, v.commitHash = nil, nil
	case "end":
		if v.header.NativeSourceOrderSHA256 != "" && (uint64(len(v.sourceSeen)) != v.header.NativeSourceCount || v.sourceOrder == nil || hex.EncodeToString(v.sourceOrder.Sum(nil)) != v.header.NativeSourceOrderSHA256) {
			return streamError("native source ordinal commitment differs")
		}
		if f.End == nil || v.commit != nil || !v.started || v.stats.Commits != v.header.Head-v.header.First+1 || v.header.First > 1 && !v.baselineDone || v.boundaries != v.header.NativeBoundaries || v.boundary != nil && v.boundaryUsed != v.boundary.VerifiedCommitCount {
			return streamError("terminal before complete range/boundaries")
		}
		if f.End.Commits != v.stats.Commits || f.End.Tables != v.stats.Tables || f.End.DecodedBytes != v.stats.DecodedBytes {
			return streamError("terminal counts differ from actual pulls")
		}
		v.ended = true
	default:
		return streamError("unknown frame kind")
	}
	return nil
}

func streamSourceKey(store, project string) string { return store + "\x00" + project }

func (v *streamValidator) beginCommit(c *OfflineHistoryStreamCommit) error {
	if v.header.First > 1 && !v.started && v.nextGap != v.header.First {
		return streamError("partial prefix gaps do not reach baseline")
	}
	if c.Baseline {
		if v.started || v.header.First == 1 || c.Sequence != v.header.First-1 || c.SourceStoreID != "" || c.Project != "" || c.SourceSequence != 0 || c.CommitIDs != 0 || c.ParentIDs != 0 || c.EventIDs != 0 || c.Physical != nil {
			return streamError("invalid proven baseline")
		}
	} else {
		if v.header.First > 1 && !v.baselineDone || v.stats.Commits == v.header.Head-v.header.First+1 || c.Sequence != v.header.First+v.stats.Commits || c.CommitIDs == 0 && c.EventIDs == 0 || c.Tables == 0 && c.CommitIDs == 0 {
			return streamError("missing, extra, unordered or provenance-free commit")
		}
		if c.SourceStoreID != "" || c.Project != "" || c.SourceSequence != 0 {
			if !streamText(c.SourceStoreID) || !streamText(c.Project) || c.SourceSequence == 0 {
				return streamError("incomplete original source scope")
			}
		}
		if v.header.Source == NativeTableHistorySource {
			if c.CommitIDs != 1 || c.EventIDs != 0 || !streamText(c.SourceStoreID) || !streamText(c.Project) || c.Physical == nil {
				return streamError("native commit lost physical provenance")
			}
			key := streamSourceKey(c.SourceStoreID, c.Project)
			if key != v.lastSource {
				if v.header.NativeSourceOrderSHA256 != "" {
					if v.sourceSeen == nil {
						v.sourceSeen = map[string]bool{}
						v.sourceOrder = nativeSourceOrderHasher()
					}
					if v.sourceSeen[key] || uint64(len(v.sourceSeen)) >= v.header.NativeSourceCount {
						return streamError("native collection repeats or exceeds source blocks")
					}
					v.sourceSeen[key] = true
					addNativeSourceOrder(v.sourceOrder, c.SourceStoreID, c.Project)
				} else if key <= v.lastSource {
					return streamError("native source blocks are not ordered")
				}
				v.lastSource, v.lastSequence = key, 0
			}
			if v.lastSequence == math.MaxUint64 || c.SourceSequence != v.lastSequence+1 {
				return streamError("native source sequence is not contiguous")
			}
			v.lastSequence = c.SourceSequence
		}
		if c.Physical != nil && (!streamText(c.Physical.Root) || !importDigest(c.Physical.TablesSHA256)) {
			return streamError("invalid physical commit commitment")
		}
		if v.header.NativeBoundaries != 0 {
			b := v.boundary
			if b != nil && v.header.NativeSourceOrderSHA256 != "" && (c.SourceStoreID != b.SourceStoreID || c.Project != b.Project) && v.boundaryUsed == b.VerifiedCommitCount {
				v.boundary = nil
				b = nil
			}
			if b == nil && v.header.NativeSourceOrderSHA256 == "" || b != nil && (c.SourceStoreID != b.SourceStoreID || c.Project != b.Project || v.boundaryUsed == b.VerifiedCommitCount || c.SourceSequence != v.boundaryUsed+1) {
				return streamError("commit differs from active native boundary")
			}
			if b != nil {
				v.boundaryUsed++
			}
		}
		v.stats.Commits++
	}
	v.started, v.commit = true, c
	v.ids, v.idKind, v.tables, v.previousKey = [3]uint64{}, 0, 0, ""
	v.physicalName, v.physicalTables = "", 0
	v.physical, v.physicalSet = [2]OfflineHistorySourceTable{}, [2]bool{}
	return nil
}

func (v *streamValidator) idCounts() [3]uint64 {
	return [3]uint64{v.commit.CommitIDs, v.commit.ParentIDs, v.commit.EventIDs}
}

func (v *streamValidator) beginTable(t *OfflineHistoryStreamTable) error {
	if !streamText(t.Name) || !streamText(t.Kind) || t.Ref.Validate() != nil || !t.Ref.Branch.IsZero() || t.Ref.ID == OfflineHistoryEvidenceID || t.Before == nil && t.After == nil || v.tables == v.commit.Tables || v.stats.Tables == math.MaxUint64 {
		return streamError("invalid converted table identity/count/presence")
	}
	key, err := EncodeLockKey(LockKey{Kind: LockRecord, Ref: &t.Ref})
	if err != nil {
		return err
	}
	key = t.Name + "\x00" + key
	if key <= v.previousKey {
		return streamError("duplicate or unordered table")
	}
	if v.commit.Baseline && (t.Before != nil || t.After == nil) {
		return streamError("baseline must contain only proven complete after images")
	}
	for _, image := range []*OfflineHistoryStreamImage{t.Before, t.After} {
		if image != nil && (image.Bytes == 0 || !importDigest(image.SHA256)) {
			return streamError("invalid converted image commitment")
		}
	}
	for _, physical := range []*OfflineHistorySourceTable{t.PhysicalBefore, t.PhysicalAfter} {
		if physical != nil && (!importDigest(physical.TableSHA256) || physical.Present && !importDigest(physical.SchemaSHA256) || !physical.Present && (physical.Rows != 0 || physical.SchemaSHA256 != "")) {
			return streamError("invalid physical table presence/schema/count/hash")
		}
	}
	if v.header.Source == NativeTableHistorySource && (t.Kind != "beads-archive" || !strings.HasPrefix(t.Ref.ID, NativeTableHistoryRecordPrefix) || t.PhysicalAfter == nil) {
		return streamError("native table evidence lost its inert identity/physical commitment")
	}
	var physical [2]OfflineHistorySourceTable
	var physicalSet [2]bool
	for i, value := range []*OfflineHistorySourceTable{t.PhysicalBefore, t.PhysicalAfter} {
		if value != nil {
			physical[i], physicalSet[i] = *value, true
		}
	}
	// Name+record ordering makes all records for a physical table contiguous.
	// Retain only its value copies, never a map or borrowed producer pointers.
	if t.Name == v.physicalName {
		if physical != v.physical || physicalSet != v.physicalSet {
			return streamError("inconsistent repeated physical table commitments")
		}
	} else {
		if v.physicalTables == math.MaxUint64 || v.commit.Physical != nil && v.physicalTables >= v.commit.Physical.Tables {
			return streamError("referenced physical tables exceed physical census")
		}
		v.physicalTables++
	}
	v.physicalName, v.physical, v.physicalSet = t.Name, physical, physicalSet
	v.table, v.previousKey = t, key
	v.tables++
	v.stats.Tables++
	v.images, v.imageBytes, v.imageSide = [2]hash.Hash{sha256.New(), sha256.New()}, [2]uint64{}, 0
	if t.Before == nil {
		v.imageSide = 1
	}
	return nil
}

func (v *streamValidator) chunk(c *OfflineHistoryStreamChunk) error {
	side := -1
	if c.Image == "before" {
		side = 0
	} else if c.Image == "after" {
		side = 1
	}
	if side < v.imageSide || side < 0 || len(c.Data) == 0 {
		return streamError("empty or unordered converted image chunk")
	}
	if side == 1 && v.table.Before != nil && v.imageBytes[0] != v.table.Before.Bytes {
		return streamError("after image began before complete preimage")
	}
	image := []*OfflineHistoryStreamImage{v.table.Before, v.table.After}[side]
	if image == nil || c.Offset != v.imageBytes[side] || uint64(len(c.Data)) > image.Bytes-v.imageBytes[side] || uint64(len(c.Data)) > math.MaxUint64-v.stats.DecodedBytes {
		return streamError("converted image offset/size overflow or absent image")
	}
	v.imageSide = side
	v.images[side].Write(c.Data)
	v.imageBytes[side] += uint64(len(c.Data))
	v.stats.DecodedBytes += uint64(len(c.Data))
	if len(c.Data) > v.stats.PeakChunkBytes {
		v.stats.PeakChunkBytes = len(c.Data)
	}
	return nil
}
