package driver

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

// The lazy fixture holds one frame and reuses its chunk buffer. Its history is
// deliberately much larger than the pull bound; no []OfflineHistory.Batches or
// accumulated frame/image history exists in this successful behavior fixture.
type streamingFixture struct {
	commits, sequence uint64
	frames            []OfflineHistoryStreamFrame // bounded metadata for ONE commit
	at, bodyOffset    int
	imageSide         int
	chunking          bool
	buffer            []byte
	closed, pulls     int
	maxChunk          int
	beforeNext        func(*streamingFixture) error
	closeErr          error
}

const fixtureImageBytes = 4096
const fixtureChunkBytes = 97

func fixtureSHA(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

func fixtureBodyChunk(sequence uint64, table int, offset, n int) []byte {
	prefix := fmt.Sprintf(`{"commit":%d,"table":%d,"rows":[{"n":9007199254740993,"v":"`, sequence, table)
	suffix := `"}]}`
	out := bytes.Repeat([]byte{'x'}, n)
	for i := range out {
		position := offset + i
		if position < len(prefix) {
			out[i] = prefix[position]
		} else if position >= fixtureImageBytes-len(suffix) {
			out[i] = suffix[position-(fixtureImageBytes-len(suffix))]
		}
	}
	return out
}

func fixtureImage(sequence uint64, table int) *OfflineHistoryStreamImage {
	h := sha256.New()
	for offset := 0; offset < fixtureImageBytes; offset += fixtureChunkBytes {
		n := min(fixtureChunkBytes, fixtureImageBytes-offset)
		h.Write(fixtureBodyChunk(sequence, table, offset, n))
	}
	return &OfflineHistoryStreamImage{Bytes: fixtureImageBytes, SHA256: hex.EncodeToString(h.Sum(nil))}
}

func fixtureTable(sequence uint64, table int) OfflineHistoryStreamFrame {
	physical := func(seq uint64) *OfflineHistorySourceTable {
		return &OfflineHistorySourceTable{Present: true, Rows: 1, SchemaSHA256: fixtureSHA([]byte("source-schema")), TableSHA256: fixtureSHA([]byte(fmt.Sprintf("physical-table:%d:%d", seq, table)))}
	}
	x := &OfflineHistoryStreamTable{Name: fmt.Sprintf("table-%02d", table), Ref: model.RefByID(fmt.Sprintf("table-record-%02d", table), model.Natural64(0)), Kind: "beads-archive", After: fixtureImage(sequence, table), PhysicalAfter: physical(sequence)}
	if sequence > 1 {
		previous := fixturePrevious(sequence)
		x.Before, x.PhysicalBefore = fixtureImage(previous, table), physical(previous)
	}
	return OfflineHistoryStreamFrame{Kind: "table", Table: x}
}

func fixturePrevious(sequence uint64) uint64 {
	previous := sequence - 1
	if previous%5 == 0 {
		previous--
	}
	return previous
}

func fixtureMetadata(sequence uint64) []OfflineHistoryStreamFrame {
	tables := uint64(2)
	if sequence%5 == 0 {
		tables = 0 // explicit empty physical commit, retained without invented rows
	}
	c := &OfflineHistoryStreamCommit{Sequence: sequence, SourceSequence: sequence, SourceStoreID: "original", Project: "pl", CommitIDs: 1, Tables: tables, Physical: &OfflineHistorySourceCommit{Root: fmt.Sprintf("root-%d", sequence), Tables: tables, TablesSHA256: fixtureSHA([]byte(fmt.Sprintf("physical-tables:%d", sequence)))}}
	if sequence > 1 {
		c.ParentIDs = 2
	}
	frames := []OfflineHistoryStreamFrame{{Kind: "commit", Commit: c}, {Kind: "id", ID: &OfflineHistoryStreamID{Kind: "commit", Value: fmt.Sprintf("commit-%d", sequence)}}}
	if sequence > 1 {
		frames = append(frames, OfflineHistoryStreamFrame{Kind: "id", ID: &OfflineHistoryStreamID{Kind: "parent", Value: fmt.Sprintf("commit-%d", sequence-1)}}, OfflineHistoryStreamFrame{Kind: "id", ID: &OfflineHistoryStreamID{Kind: "parent", Value: "second-parent"}})
	}
	for table := 0; table < int(tables); table++ {
		frames = append(frames, fixtureTable(sequence, table), OfflineHistoryStreamFrame{Kind: "table-end"})
	}
	commitHash := sha256.New()
	commitHash.Write([]byte("plug-offline-history-commit/1\n"))
	for _, frame := range frames {
		if frame.Kind != "table-end" {
			data, _ := json.Marshal(frame)
			commitHash.Write(append(data, '\n'))
		}
	}
	return append(frames, OfflineHistoryStreamFrame{Kind: "commit-end", CommitEnd: &OfflineHistoryStreamCommitEnd{SHA256: hex.EncodeToString(commitHash.Sum(nil))}})
}

func fixtureTotals(commits uint64) OfflineHistoryStreamEnd {
	var result OfflineHistoryStreamEnd
	result.Commits = commits
	for sequence := uint64(1); sequence <= commits; sequence++ {
		if sequence%5 == 0 {
			continue
		}
		result.Tables += 2
		result.DecodedBytes += 2 * fixtureImageBytes
		if sequence > 1 {
			result.DecodedBytes += 2 * fixtureImageBytes
		}
	}
	return result
}

func (s *streamingFixture) Next(ctx context.Context, limits OfflineHistoryStreamLimits) (OfflineHistoryStreamFrame, error) {
	s.pulls++
	if err := ctx.Err(); err != nil {
		return OfflineHistoryStreamFrame{}, err
	}
	if s.beforeNext != nil {
		if err := s.beforeNext(s); err != nil {
			return OfflineHistoryStreamFrame{}, err
		}
	}
	if len(s.frames) == 0 && s.sequence < s.commits {
		s.sequence++
		s.frames, s.at = fixtureMetadata(s.sequence), 0
	}
	if len(s.frames) == 0 {
		if s.sequence == s.commits {
			s.sequence++
			end := fixtureTotals(s.commits)
			return OfflineHistoryStreamFrame{Kind: "end", End: &end}, nil
		}
		return OfflineHistoryStreamFrame{}, io.EOF
	}
	f := s.frames[s.at]
	if s.chunking {
		table := s.frames[s.at-1].Table
		tableNumber := 0
		if table.Name == "table-01" {
			tableNumber = 1
		}
		n := min(fixtureChunkBytes, fixtureImageBytes-s.bodyOffset)
		if n > limits.MaxChunkBytes {
			return OfflineHistoryStreamFrame{}, streamError("fixture chunk cannot fit requested pull")
		}
		sequence, side := s.sequence, "after"
		if s.imageSide == 0 {
			sequence, side = fixturePrevious(sequence), "before"
		}
		data := fixtureBodyChunk(sequence, tableNumber, s.bodyOffset, n)
		s.buffer = append(s.buffer[:0], data...)
		f = OfflineHistoryStreamFrame{Kind: "chunk", Chunk: &OfflineHistoryStreamChunk{Image: side, Offset: uint64(s.bodyOffset), Data: s.buffer}}
		s.bodyOffset += n
		s.maxChunk = max(s.maxChunk, len(data))
		if s.bodyOffset == fixtureImageBytes {
			s.bodyOffset = 0
			if s.imageSide == 0 {
				s.imageSide = 1
			} else {
				s.chunking = false
			}
		}
		return f, nil
	}
	s.at++
	if f.Table != nil {
		s.chunking, s.bodyOffset, s.imageSide = true, 0, 1
		if f.Table.Before != nil {
			s.imageSide = 0
		}
	}
	if s.at == len(s.frames) {
		s.frames = nil
	}
	return f, nil
}

func (s *streamingFixture) Close() error {
	s.closed++
	clear(s.buffer)
	return s.closeErr
}

// Independently hashes each staged converted image using only two hash states;
// captures provenance order and physical fields without constructing replay plans.
type streamingStageFixture struct {
	binding                        OfflineHistoryStreamBinding
	source                         *streamingFixture
	staged, aborted, published     int
	beginErr, stageErr, publishErr error
	beforeStage                    func(OfflineHistoryStreamFrame)
	active                         *OfflineHistoryStreamTable
	hashes                         [2]hash.Hash
	bytes                          [2]uint64
	sequence                       uint64
	parents                        []string
	tables, emptyCommits           uint64
	lastRevision                   model.Revision
}

func (s *streamingStageFixture) Begin(_ context.Context, b OfflineHistoryStreamBinding) error {
	s.binding = b
	return s.beginErr
}

func (s *streamingStageFixture) Stage(_ context.Context, revision model.Revision, f OfflineHistoryStreamFrame) error {
	s.staged++
	s.lastRevision = revision
	if s.beforeStage != nil {
		s.beforeStage(f)
	}
	if s.stageErr != nil {
		return s.stageErr
	}
	switch f.Kind {
	case "commit":
		s.sequence, s.parents = f.Commit.Sequence, nil
		if f.Commit.Tables == 0 {
			s.emptyCommits++
		}
	case "id":
		if f.ID.Kind == "parent" {
			s.parents = append(s.parents, f.ID.Value)
		}
	case "table":
		s.active, s.hashes, s.bytes = f.Table, [2]hash.Hash{sha256.New(), sha256.New()}, [2]uint64{}
		if f.Table.PhysicalAfter != nil && f.Table.After != nil && f.Table.PhysicalAfter.TableSHA256 == f.Table.After.SHA256 {
			return errors.New("physical and converted hashes were conflated")
		}
	case "chunk":
		i := 1
		if f.Chunk.Image == "before" {
			i = 0
		}
		s.hashes[i].Write(f.Chunk.Data)
		s.bytes[i] += uint64(len(f.Chunk.Data))
	case "table-end":
		for i, image := range []*OfflineHistoryStreamImage{s.active.Before, s.active.After} {
			if image != nil && (s.bytes[i] != image.Bytes || hex.EncodeToString(s.hashes[i].Sum(nil)) != image.SHA256) {
				return errors.New("independent stage image mismatch")
			}
		}
		s.tables++
		s.active, s.hashes = nil, [2]hash.Hash{}
	case "commit-end":
		if s.sequence > 1 && len(s.parents) > 0 && !reflect.DeepEqual(s.parents, []string{fmt.Sprintf("commit-%d", s.sequence-1), "second-parent"}) {
			return errors.New("parent order changed")
		}
	}
	return nil
}

func (s *streamingStageFixture) Publish(_ context.Context, completion OfflineHistoryStreamCompletion, h ChangesetHeader) (model.Changeset, error) {
	if s.source != nil && s.source.closed != 1 {
		return model.Changeset{}, errors.New("publication before source cleanup")
	}
	s.published++
	return model.Changeset{Revision: completion.Meta.Head, Time: h.Time, Principal: h.Principal}, s.publishErr
}

func (s *streamingStageFixture) Abort() error { s.aborted++; return nil }

func fixtureRequest(t *testing.T, commits uint64) OfflineHistoryStreamRequest {
	t.Helper()
	header := OfflineHistoryStreamHeader{Version: OfflineHistoryStreamVersion, Source: "contract-table-history", Verified: true, Complete: true, First: 1, Head: commits}
	when := time.Date(2026, 11, 10, 23, 17, 0, 123, time.UTC)
	p := replayProfile(RestoreHistoryModeFull)
	p.HistoryTime = when.Format(time.RFC3339Nano)
	limits := OfflineHistoryStreamLimits{MaxFrameBytes: 2048, MaxChunkBytes: fixtureChunkBytes}
	digest := sha256.New()
	digest.Write([]byte(OfflineHistoryStreamVersion + "\n"))
	raw, _ := json.Marshal(header)
	digest.Write(append(raw, '\n'))
	source := &streamingFixture{commits: commits}
	for {
		f, err := source.Next(context.Background(), limits)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if f.Kind != "end" {
			raw, _ = json.Marshal(f)
			digest.Write(append(raw, '\n'))
		}
	}
	p.HistoryDigest = hex.EncodeToString(digest.Sum(nil))
	return OfflineHistoryStreamRequest{Current: Meta{StoreID: "target", Incarnation: "old"}, Profile: p, Header: header, When: when, Limits: limits}
}

func streamReceipt(t *testing.T, p *PreparedOfflineHistoryStream, r OfflineHistoryStreamRequest) ChangesetHeader {
	t.Helper()
	c, err := p.Completion()
	if err != nil {
		t.Fatal(err)
	}
	requestHash, _ := r.Profile.RequestHash()
	return ChangesetHeader{Kind: model.ChangesetRestore, Method: r.Profile.Method, Principal: r.Profile.Admission.Actor, IdempotencyKey: r.Profile.IdempotencyKey, RequestHash: requestHash, Time: r.When, Incarnation: c.Meta.Incarnation, Branch: model.Natural64(0), Response: model.Commit{Revision: c.Meta.Head}}
}

func TestOfflineHistoryStreamLazyOrderedTablesHashesAndAtomicReceipt(t *testing.T) {
	r := fixtureRequest(t, 37)
	source := &streamingFixture{commits: 37}
	stage := &streamingStageFixture{source: source}
	lastPull := 0
	stage.beforeStage = func(OfflineHistoryStreamFrame) {
		if source.pulls != lastPull+1 {
			t.Fatalf("staging lost pull backpressure: prior=%d next=%d", lastPull, source.pulls)
		}
		lastPull = source.pulls
	}
	p, err := PrepareOfflineHistoryStream(context.Background(), r, source, stage)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	c, _ := p.Completion()
	want := fixtureTotals(37)
	if c.Stats.Commits != want.Commits || c.Stats.Tables != want.Tables || c.Stats.DecodedBytes != want.DecodedBytes || c.Stats.PeakChunkBytes > r.Limits.MaxChunkBytes || c.Stats.PeakFrameBytes > r.Limits.MaxFrameBytes || c.Stats.DecodedBytes <= uint64(r.Limits.MaxFrameBytes) || c.Meta.Head != model.Natural64(38) || !c.Meta.HistoryFloor.IsZero() || c.HistorySHA256 != r.Profile.HistoryDigest || stage.tables != want.Tables || stage.emptyCommits != 7 || stage.published != 0 || source.closed != 1 {
		t.Fatalf("lazy stream lost ordering/counts/bounds/metadata: %+v stage=%+v", c, stage)
	}
	copyOfToken := *p
	h := streamReceipt(t, p, r)
	entry, err := p.Commit(context.Background(), h)
	if err != nil || entry.Revision != c.Meta.Head || stage.published != 1 || stage.aborted != 0 || !reflect.DeepEqual(entry.Principal, r.Profile.Admission.Actor) {
		t.Fatalf("atomic receipt publication failed: %+v %v", entry, err)
	}
	if _, err := copyOfToken.Commit(context.Background(), h); !errors.Is(err, ErrOfflineHistoryStream) || stage.published != 1 {
		t.Fatal("copied consumed token republished", err)
	}
}

type frameSourceFixture struct {
	frames         []OfflineHistoryStreamFrame
	at, closed     int
	errAt          int
	pullErr        error
	closeErr       error
	withErrorFrame bool
	beforeNext     func()
}

func (s *frameSourceFixture) Next(context.Context, OfflineHistoryStreamLimits) (OfflineHistoryStreamFrame, error) {
	if s.beforeNext != nil {
		s.beforeNext()
	}
	if s.pullErr != nil && s.at == s.errAt {
		if s.withErrorFrame {
			return s.frames[s.at], s.pullErr
		}
		return OfflineHistoryStreamFrame{}, s.pullErr
	}
	if s.at == len(s.frames) {
		return OfflineHistoryStreamFrame{}, io.EOF
	}
	f := s.frames[s.at]
	s.at++
	return f, nil
}

func (s *frameSourceFixture) Close() error { s.closed++; return s.closeErr }

func fixtureFrames(t *testing.T, commits uint64) []OfflineHistoryStreamFrame {
	t.Helper()
	source := &streamingFixture{commits: commits}
	var frames []OfflineHistoryStreamFrame
	for {
		f, err := source.Next(context.Background(), OfflineHistoryStreamLimits{MaxFrameBytes: 2048, MaxChunkBytes: fixtureChunkBytes})
		if err == io.EOF {
			return frames
		}
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := json.Marshal(f)
		var detached OfflineHistoryStreamFrame
		if err := json.Unmarshal(raw, &detached); err != nil {
			t.Fatal(err)
		}
		frames = append(frames, detached)
	}
}

func TestOfflineHistoryStreamFailuresAbortWithoutPublishToken(t *testing.T) {
	fault := errors.New("source/stage/cleanup fault")
	tests := []struct {
		name string
		edit func(*OfflineHistoryStreamRequest, *frameSourceFixture, *streamingStageFixture)
		want error
	}{
		{"valid-prefix-without-terminal", func(_ *OfflineHistoryStreamRequest, s *frameSourceFixture, _ *streamingStageFixture) {
			s.frames = s.frames[:len(s.frames)-1]
		}, ErrOfflineHistoryStream},
		{"error-after-valid-prefix", func(_ *OfflineHistoryStreamRequest, s *frameSourceFixture, _ *streamingStageFixture) {
			s.errAt, s.pullErr = len(s.frames)-1, fault
		}, fault},
		{"data-and-error", func(_ *OfflineHistoryStreamRequest, s *frameSourceFixture, _ *streamingStageFixture) {
			s.errAt, s.pullErr, s.withErrorFrame = 1, fault, true
		}, fault},
		{"end-and-error", func(_ *OfflineHistoryStreamRequest, s *frameSourceFixture, _ *streamingStageFixture) {
			s.errAt, s.pullErr, s.withErrorFrame = len(s.frames)-1, fault, true
		}, fault},
		{"data-and-EOF", func(_ *OfflineHistoryStreamRequest, s *frameSourceFixture, _ *streamingStageFixture) {
			s.errAt, s.pullErr, s.withErrorFrame = len(s.frames)-1, io.EOF, true
		}, ErrOfflineHistoryStream},
		{"early-EOF", func(_ *OfflineHistoryStreamRequest, s *frameSourceFixture, _ *streamingStageFixture) {
			s.frames = s.frames[:3]
		}, ErrOfflineHistoryStream},
		{"extra-end", func(_ *OfflineHistoryStreamRequest, s *frameSourceFixture, _ *streamingStageFixture) {
			s.frames = append(s.frames, s.frames[len(s.frames)-1])
		}, ErrOfflineHistoryStream},
		{"extra-after-end", func(_ *OfflineHistoryStreamRequest, s *frameSourceFixture, _ *streamingStageFixture) {
			s.frames = append(s.frames, s.frames[0])
		}, ErrOfflineHistoryStream},
		{"multiple-payloads", func(_ *OfflineHistoryStreamRequest, s *frameSourceFixture, _ *streamingStageFixture) {
			s.frames[0].End = &OfflineHistoryStreamEnd{}
		}, ErrOfflineHistoryStream},
		{"metadata-bound", func(r *OfflineHistoryStreamRequest, s *frameSourceFixture, _ *streamingStageFixture) {
			s.frames[1].ID.Value = strings.Repeat("i", r.Limits.MaxFrameBytes)
		}, ErrOfflineHistoryStream},
		{"chunk-bound", func(r *OfflineHistoryStreamRequest, s *frameSourceFixture, _ *streamingStageFixture) {
			s.frames[3].Chunk.Data = bytes.Repeat([]byte{'a'}, r.Limits.MaxChunkBytes+1)
		}, ErrOfflineHistoryStream},
		{"chunk-hash", func(_ *OfflineHistoryStreamRequest, s *frameSourceFixture, _ *streamingStageFixture) {
			s.frames[3].Chunk.Data[0] ^= 1
		}, ErrOfflineHistoryStream},
		{"chunk-offset", func(_ *OfflineHistoryStreamRequest, s *frameSourceFixture, _ *streamingStageFixture) {
			s.frames[3].Chunk.Offset++
		}, ErrOfflineHistoryStream},
		{"missing-chunk", func(_ *OfflineHistoryStreamRequest, s *frameSourceFixture, _ *streamingStageFixture) {
			s.frames = append(s.frames[:3], s.frames[4:]...)
		}, ErrOfflineHistoryStream},
		{"commit-order", func(_ *OfflineHistoryStreamRequest, s *frameSourceFixture, _ *streamingStageFixture) {
			s.frames[0].Commit.Sequence++
		}, ErrOfflineHistoryStream},
		{"missing-provenance", func(_ *OfflineHistoryStreamRequest, s *frameSourceFixture, _ *streamingStageFixture) {
			s.frames[0].Commit.ParentIDs++
		}, ErrOfflineHistoryStream},
		{"physical-count", func(_ *OfflineHistoryStreamRequest, s *frameSourceFixture, _ *streamingStageFixture) {
			s.frames[0].Commit.Physical.Tables++
		}, ErrOfflineHistoryStream},
		{"terminal-count", func(_ *OfflineHistoryStreamRequest, s *frameSourceFixture, _ *streamingStageFixture) {
			s.frames[len(s.frames)-1].End.Tables++
		}, ErrOfflineHistoryStream},
		{"transcript", func(r *OfflineHistoryStreamRequest, _ *frameSourceFixture, _ *streamingStageFixture) {
			r.Profile.HistoryDigest = strings.Repeat("d", 64)
		}, ErrOfflineHistoryStream},
		{"header-bound", func(r *OfflineHistoryStreamRequest, _ *frameSourceFixture, _ *streamingStageFixture) {
			r.Header.Source = strings.Repeat("s", r.Limits.MaxFrameBytes)
		}, ErrOfflineHistoryStream},
		{"unverified", func(r *OfflineHistoryStreamRequest, _ *frameSourceFixture, _ *streamingStageFixture) {
			r.Header.Verified = false
		}, ErrOfflineHistoryStream},
		{"unproven-full", func(r *OfflineHistoryStreamRequest, _ *frameSourceFixture, _ *streamingStageFixture) {
			r.Header.Complete = false
		}, ErrOfflineHistoryStream},
		{"zero-bound", func(r *OfflineHistoryStreamRequest, _ *frameSourceFixture, _ *streamingStageFixture) {
			r.Limits.MaxChunkBytes = 0
		}, ErrOfflineHistoryStream},
		{"admitted-time", func(r *OfflineHistoryStreamRequest, _ *frameSourceFixture, _ *streamingStageFixture) {
			r.When = r.When.Add(time.Second)
		}, ErrOfflineHistoryStream},
		{"target-not-empty", func(r *OfflineHistoryStreamRequest, _ *frameSourceFixture, _ *streamingStageFixture) {
			r.Current.Head = model.Natural64(1)
		}, ErrRestoreTargetNotEmpty},
		{"target-lifetime", func(_ *OfflineHistoryStreamRequest, _ *frameSourceFixture, s *streamingStageFixture) {
			s.beginErr = ErrConflict
		}, ErrConflict},
		{"staging", func(_ *OfflineHistoryStreamRequest, _ *frameSourceFixture, s *streamingStageFixture) {
			s.stageErr = fault
		}, fault},
		{"cleanup", func(_ *OfflineHistoryStreamRequest, s *frameSourceFixture, _ *streamingStageFixture) {
			s.closeErr = fault
		}, fault},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := fixtureRequest(t, 1)
			source := &frameSourceFixture{frames: fixtureFrames(t, 1)}
			stage := &streamingStageFixture{}
			tt.edit(&r, source, stage)
			p, err := PrepareOfflineHistoryStream(context.Background(), r, source, stage)
			if !errors.Is(err, tt.want) || p != nil || source.closed != 1 || stage.aborted != 1 || stage.published != 0 {
				t.Fatalf("failure allowed completion/publication or lost error/cleanup: p=%v err=%v closed=%d abort=%d publish=%d", p, err, source.closed, stage.aborted, stage.published)
			}
		})
	}
}

func TestOfflineHistoryStreamCancellationAndCleanupOwnership(t *testing.T) {
	for _, where := range []string{"before-entry", "during-pull", "during-stage", "after-terminal"} {
		t.Run(where, func(t *testing.T) {
			r := fixtureRequest(t, 1)
			source := &frameSourceFixture{frames: fixtureFrames(t, 1)}
			stage := &streamingStageFixture{}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch where {
			case "before-entry":
				cancel()
			case "during-pull":
				source.beforeNext = cancel
			case "during-stage":
				stage.beforeStage = func(OfflineHistoryStreamFrame) { cancel() }
			case "after-terminal":
				source.beforeNext = func() {
					if source.at == len(source.frames) {
						cancel()
					}
				}
			}
			p, err := PrepareOfflineHistoryStream(ctx, r, source, stage)
			if !errors.Is(err, context.Canceled) || p != nil || source.closed != 1 || stage.aborted != 1 || stage.published != 0 {
				t.Fatalf("cancellation escaped cleanup/poison: p=%v err=%v stage=%+v", p, err, stage)
			}
		})
	}
	var zero PreparedOfflineHistoryStream
	if _, err := zero.Commit(context.Background(), ChangesetHeader{}); !errors.Is(err, ErrOfflineHistoryStream) {
		t.Fatal("zero token published", err)
	}
	r := fixtureRequest(t, 1)
	source := &streamingFixture{commits: 1}
	stage := &streamingStageFixture{}
	p, err := PrepareOfflineHistoryStream(context.Background(), r, source, stage)
	if err != nil {
		t.Fatal(err)
	}
	if p.Close() != nil || p.Close() != nil || stage.aborted != 1 || source.closed != 1 {
		t.Fatal("unfinished prepared stream did not release resources exactly once")
	}
}

func TestOfflineHistoryStreamReceiptBindingAndIndeterminateRecovery(t *testing.T) {
	for _, fault := range []string{"principal", "time", "incarnation", "request", "key", "head", "pending", "cancel", "indeterminate"} {
		t.Run(fault, func(t *testing.T) {
			r := fixtureRequest(t, 1)
			source := &streamingFixture{commits: 1}
			stage := &streamingStageFixture{source: source}
			p, err := PrepareOfflineHistoryStream(context.Background(), r, source, stage)
			if err != nil {
				t.Fatal(err)
			}
			h := streamReceipt(t, p, r)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch fault {
			case "principal":
				h.Principal.ID = "source-author"
			case "time":
				h.Time = h.Time.Add(time.Second)
			case "incarnation":
				h.Incarnation = "other"
			case "request":
				h.RequestHash = strings.Repeat("d", 64)
			case "key":
				h.IdempotencyKey = "different-original-key"
			case "head":
				h.Response.Revision = model.Natural64(99)
			case "pending":
				h.ResponsePending = true
			case "cancel":
				cancel()
			case "indeterminate":
				stage.publishErr = ErrIndeterminate{IdempotencyKey: h.IdempotencyKey}
			}
			_, err = p.Commit(ctx, h)
			if err == nil || stage.aborted != 1 || fault != "indeterminate" && stage.published != 0 {
				t.Fatalf("unbound receipt escaped single-use abort: err=%v stage=%+v", err, stage)
			}
			if fault == "indeterminate" {
				var unknown ErrIndeterminate
				if !errors.As(err, &unknown) || unknown.IdempotencyKey != r.Profile.IdempotencyKey || stage.published != 1 {
					t.Fatal("original uncertain keyed receipt identity lost", err)
				}
			}
			if _, err = p.Commit(context.Background(), h); !errors.Is(err, ErrOfflineHistoryStream) {
				t.Fatal("consumed receipt retried", err)
			}
		})
	}
}

// Rebind a small edited fixture independently of the consumer's validator.
// This lets policy failures be exercised with an otherwise authentic transcript.
func bindFixtureFrames(t *testing.T, r *OfflineHistoryStreamRequest, frames []OfflineHistoryStreamFrame) {
	t.Helper()
	transcript := sha256.New()
	transcript.Write([]byte(OfflineHistoryStreamVersion + "\n"))
	raw, _ := json.Marshal(r.Header)
	transcript.Write(append(raw, '\n'))
	var commit hash.Hash
	var totals OfflineHistoryStreamEnd
	for i := range frames {
		f := &frames[i]
		switch f.Kind {
		case "commit":
			commit = sha256.New()
			commit.Write([]byte("plug-offline-history-commit/1\n"))
			if !f.Commit.Baseline {
				totals.Commits++
			}
		case "table":
			totals.Tables++
		case "chunk":
			totals.DecodedBytes += uint64(len(f.Chunk.Data))
		case "commit-end":
			f.CommitEnd.SHA256 = hex.EncodeToString(commit.Sum(nil))
		case "end":
			*f.End = totals
		}
		raw, err := json.Marshal(f)
		if err != nil {
			t.Fatal(err)
		}
		if f.Kind == "commit" || f.Kind == "id" || f.Kind == "table" {
			commit.Write(append(bytes.Clone(raw), '\n'))
		}
		if f.Kind != "end" {
			transcript.Write(append(raw, '\n'))
		}
	}
	r.Profile.HistoryDigest = hex.EncodeToString(transcript.Sum(nil))
}

func TestOfflineHistoryStreamPartialBaselineGapsAndFloor(t *testing.T) {
	r := fixtureRequest(t, 1)
	r.Header.Complete, r.Header.First, r.Header.Head, r.Header.BaselineVerified = false, 9, 9, true
	r.Profile.HistoryMode = RestoreHistoryModePartial
	frames := fixtureFrames(t, 1)
	frames[0].Commit.Sequence, frames[0].Commit.SourceSequence = 9, 9
	prelude := []OfflineHistoryStreamFrame{
		{Kind: "gap", Gap: &SourceHistoryGap{First: 1, Last: 4, Reason: "unavailable"}},
		{Kind: "gap", Gap: &SourceHistoryGap{First: 5, Last: 8, Reason: "not-retained"}},
		{Kind: "commit", Commit: &OfflineHistoryStreamCommit{Baseline: true, Sequence: 8}},
		{Kind: "commit-end", CommitEnd: &OfflineHistoryStreamCommitEnd{}},
	}
	frames = append(prelude, frames...)
	bindFixtureFrames(t, &r, frames)
	source := &frameSourceFixture{frames: frames}
	stage := &streamingStageFixture{}
	var gaps []SourceHistoryGap
	var revisions []model.Revision
	stage.beforeStage = func(f OfflineHistoryStreamFrame) {
		if f.Gap != nil {
			gaps = append(gaps, *f.Gap)
		}
		if f.Commit != nil {
			revisions = append(revisions, stage.lastRevision)
		}
	}
	p, err := PrepareOfflineHistoryStream(context.Background(), r, source, stage)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	c, _ := p.Completion()
	if c.Meta.Head != model.Natural64(3) || c.Meta.HistoryFloor != model.Natural64(1) || c.Stats.Commits != 1 || !reflect.DeepEqual(revisions, []model.Revision{model.Natural64(1), model.Natural64(2)}) || !reflect.DeepEqual(gaps, []SourceHistoryGap{*prelude[0].Gap, *prelude[1].Gap}) || source.closed != 1 {
		t.Fatalf("partial metadata was fabricated or aggregated incorrectly: %+v gaps=%+v revisions=%v", c, gaps, revisions)
	}
	// The exact, authenticated same stream with a hole in its declared prefix
	// must refuse, even though its transcript binding matches that bad input.
	frames[1].Gap.First++
	bindFixtureFrames(t, &r, frames)
	badSource, badStage := &frameSourceFixture{frames: frames}, &streamingStageFixture{}
	if token, err := PrepareOfflineHistoryStream(context.Background(), r, badSource, badStage); !errors.Is(err, ErrOfflineHistoryStream) || token != nil || badStage.aborted != 1 {
		t.Fatal("authenticated gap hole admitted", err)
	}
}

func TestOfflineHistoryStreamNativeBoundariesAndTargetLifetime(t *testing.T) {
	for _, fault := range []string{"none", "count", "source", "lifetime", "absence"} {
		t.Run(fault, func(t *testing.T) {
			r := fixtureRequest(t, 1)
			r.Header.Source, r.Header.Complete, r.Header.NativeBoundaries = NativeTableHistorySource, false, 1
			r.Current.Incarnation, r.Lifetime = "old-incarnation", CheckpointLifetimeProcess
			profile := sourcePlaneTestProfile()
			binding, err := NewSourcePlaneImportBindingV1(sourcePlaneTestPlan())
			if err != nil {
				t.Fatal(err)
			}
			profile.SourcePlanes, profile.HistoryMode, profile.HistoryTime = &binding, RestoreHistoryModeFull, r.Profile.HistoryTime
			r.Profile = profile
			frames := fixtureFrames(t, 1)
			for i := range frames {
				if frames[i].Table != nil {
					frames[i].Table.Ref.ID = NativeTableHistoryRecordPrefix + frames[i].Table.Ref.ID
				}
			}
			boundary := &NativeHistoryBoundary{SourceStoreID: "original", Project: "pl", VerifiedCommitCount: 1, BoundarySHA256: strings.Repeat("e", 64)}
			frames = append([]OfflineHistoryStreamFrame{{Kind: "boundary", Boundary: boundary}}, frames...)
			switch fault {
			case "count":
				boundary.VerifiedCommitCount++
			case "source":
				boundary.Project = "other"
			case "lifetime":
				r.Lifetime = CheckpointLifetimeRestart
			case "absence":
				frames[3].Table.PhysicalAfter.Present = false // count/schema contradict absence
			}
			bindFixtureFrames(t, &r, frames)
			source, stage := &frameSourceFixture{frames: frames}, &streamingStageFixture{}
			p, err := PrepareOfflineHistoryStream(context.Background(), r, source, stage)
			if fault != "none" {
				if err == nil || p != nil || source.closed != 1 || stage.aborted != 1 {
					t.Fatal("native source boundary/lifetime bypass", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()
			c, _ := p.Completion()
			if c.Meta.Head != model.Natural64(2) || !c.Meta.HistoryFloor.IsZero() || stage.binding.Header.Complete || stage.binding.Header.NativeBoundaries != 1 || source.closed != 1 {
				t.Fatal("unknown native prefix became fabricated history/completeness", c)
			}
		})
	}
}

func TestOfflineHistoryStreamDetachesReusedMetadataAndAdmission(t *testing.T) {
	r := fixtureRequest(t, 1)
	r.Profile.Admission.Actor.Caveats = map[string]any{"purpose": "import"}
	source := &streamingFixture{commits: 1}
	source.beforeNext = func(s *streamingFixture) error {
		if s.pulls == 4 { // producer reuses metadata AFTER its table was pulled
			s.frames[0].Commit.Tables = 99
			s.frames[2].Table.After.Bytes = 1
		}
		return nil
	}
	stage := &streamingStageFixture{source: source}
	p, err := PrepareOfflineHistoryStream(context.Background(), r, source, stage)
	if err != nil {
		t.Fatal(err)
	}
	h := streamReceipt(t, p, r)
	h.Principal.Caveats = map[string]any{"purpose": "import"}
	r.Profile.Admission.Actor.Caveats["purpose"] = "changed-after-preparation"
	if _, err := p.Commit(context.Background(), h); err != nil || stage.published != 1 {
		t.Fatal("borrowed metadata/admission mutation changed binding", err)
	}
}

func TestOfflineHistoryStreamRejectsOriginalInvalidUTF8WithBoundTranscript(t *testing.T) {
	for _, field := range []string{"header", "source", "project", "root", "id", "name", "kind", "ref"} {
		t.Run(field, func(t *testing.T) {
			r := fixtureRequest(t, 1)
			frames := fixtureFrames(t, 1)
			bad := "original-\xff"
			switch field {
			case "header":
				r.Header.Source = bad
			case "source":
				frames[0].Commit.SourceStoreID = bad
			case "project":
				frames[0].Commit.Project = bad
			case "root":
				frames[0].Commit.Physical.Root = bad
			case "id":
				frames[1].ID.Value = bad
			case "name":
				frames[2].Table.Name = bad
			case "kind":
				frames[2].Table.Kind = bad
			case "ref":
				frames[2].Table.Ref.ID = bad
			}
			// encoding/json silently substitutes U+FFFD. Deliberately bind that
			// exact transcript: a digest mismatch cannot mask this regression.
			bindFixtureFrames(t, &r, frames)
			source, stage := &frameSourceFixture{frames: frames}, &streamingStageFixture{}
			p, err := PrepareOfflineHistoryStream(context.Background(), r, source, stage)
			if !errors.Is(err, ErrOfflineHistoryStream) || p != nil || stage.aborted != 1 || stage.published != 0 || source.closed != 1 || field != "header" && !strings.Contains(err.Error(), "UTF-8") {
				t.Fatalf("raw identity normalized before validation: field=%s token=%v err=%v", field, p, err)
			}
		})
	}
}
