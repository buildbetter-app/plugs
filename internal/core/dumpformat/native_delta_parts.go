package dumpformat

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
)

// NativeDeltaPartFormat and the limits apply to the explicitly selected v2
// capture set only. The portable archive reader keeps its original limits.
const NativeDeltaPartPayloadLimit = 64 << 20
const NativeDeltaPartHeaderLimit = 4096

type nativeDeltaPartHeader struct {
	Format             string `json:"format"`
	Sequence           uint64 `json:"sequence"`
	PreviousPartSHA256 any    `json:"previous_part_sha256"`
	PayloadBytes       uint64 `json:"payload_bytes"`
	PayloadSHA256      string `json:"payload_sha256"`
}

// NativeDeltaPartOpener opens only the next authenticated ordinal, not a path
// supplied by a stream record. The directory adapter owns path validation.
type NativeDeltaPartOpener func(context.Context, uint64) (io.ReadCloser, error)

// NativeDeltaPartReader streams payloads across bounded part files. Hashes are
// checked as bytes pass; an EOF is returned only after both final commitments
// match. Closing early never reports a verified stream.
type NativeDeltaPartReader struct {
	ctx                    context.Context
	open                   NativeDeltaPartOpener
	count, sequence        uint64
	finalPart, finalStream string
	previous               string
	stream                 hash.Hash
	part                   io.ReadCloser
	input                  *bufio.Reader
	partHash, payloadHash  hash.Hash
	header                 nativeDeltaPartHeader
	remaining              uint64
	verified, closed       bool
	failure                error
	payloadLimit           uint64
	streamBytes            uint64
}

func NewNativeDeltaPartReader(ctx context.Context, count uint64, finalPart, finalStream string, open NativeDeltaPartOpener) (*NativeDeltaPartReader, error) {
	if ctx == nil || open == nil || count == 0 || !validDeltaSHA256(finalPart) || !validDeltaSHA256(finalStream) {
		return nil, fmt.Errorf("invalid native delta part binding")
	}
	return &NativeDeltaPartReader{ctx: ctx, open: open, count: count, finalPart: finalPart, finalStream: finalStream, stream: sha256.New(), payloadLimit: NativeDeltaPartPayloadLimit}, nil
}

func validDeltaSHA256(value string) bool {
	raw, err := hex.DecodeString(value)
	return err == nil && len(raw) == sha256.Size && hex.EncodeToString(raw) == value
}

func (r *NativeDeltaPartReader) beginPart() error {
	r.sequence++
	f, err := r.open(r.ctx, r.sequence)
	if err != nil {
		return fmt.Errorf("open native delta part %d: %w", r.sequence, err)
	}
	if f == nil {
		return fmt.Errorf("open native delta part %d returned no file", r.sequence)
	}
	r.part, r.input = f, bufio.NewReaderSize(f, NativeDeltaPartHeaderLimit+1)
	line, err := r.input.ReadSlice('\n')
	if err != nil || len(line) > NativeDeltaPartHeaderLimit {
		return fmt.Errorf("native delta part %d header limit or truncation", r.sequence)
	}
	var header nativeDeltaPartHeader
	if err := decodeNativeDeltaJSON(line, &header); err != nil {
		return fmt.Errorf("native delta part %d header: %w", r.sequence, err)
	}
	if header.Format != NativeDeltaPartFormat || header.Sequence != r.sequence || header.PayloadBytes == 0 || header.PayloadBytes > r.payloadLimit || r.sequence < r.count && header.PayloadBytes != r.payloadLimit || !validDeltaSHA256(header.PayloadSHA256) {
		return fmt.Errorf("native delta part %d header binding differs", r.sequence)
	}
	if r.sequence == 1 {
		if header.PreviousPartSHA256 != nil {
			return fmt.Errorf("first native delta part has a predecessor")
		}
	} else if previous, ok := header.PreviousPartSHA256.(string); !ok || previous != r.previous {
		return fmt.Errorf("native delta part %d predecessor differs", r.sequence)
	}
	r.header, r.remaining = header, header.PayloadBytes
	r.partHash, r.payloadHash = sha256.New(), sha256.New()
	_, _ = r.partHash.Write(line)
	return nil
}

func (r *NativeDeltaPartReader) finishPart() error {
	if _, err := r.input.ReadByte(); err != io.EOF {
		if err != nil {
			return fmt.Errorf("native delta part %d trailing read: %w", r.sequence, err)
		}
		return fmt.Errorf("native delta part %d has unbound trailing bytes", r.sequence)
	}
	if hex.EncodeToString(r.payloadHash.Sum(nil)) != r.header.PayloadSHA256 {
		return fmt.Errorf("native delta part %d payload digest differs", r.sequence)
	}
	if err := r.part.Close(); err != nil {
		r.part = nil
		return fmt.Errorf("close native delta part %d: %w", r.sequence, err)
	}
	r.part = nil
	r.previous = hex.EncodeToString(r.partHash.Sum(nil))
	if r.sequence == r.count {
		if r.previous != r.finalPart || hex.EncodeToString(r.stream.Sum(nil)) != r.finalStream {
			return fmt.Errorf("native delta final part or stream digest differs")
		}
		r.verified = true
	}
	return nil
}

func (r *NativeDeltaPartReader) Read(p []byte) (int, error) {
	if r.closed {
		return 0, fmt.Errorf("native delta part reader is closed")
	}
	if r.failure != nil {
		return 0, r.failure
	}
	if len(p) == 0 {
		return 0, nil
	}
	for !r.verified {
		if err := r.ctx.Err(); err != nil {
			r.failure = err
			return 0, err
		}
		if r.part == nil {
			if err := r.beginPart(); err != nil {
				r.failure = err
				return 0, err
			}
		}
		if r.remaining == 0 {
			if err := r.finishPart(); err != nil {
				r.failure = err
				return 0, err
			}
			continue
		}
		if uint64(len(p)) > r.remaining {
			p = p[:int(r.remaining)]
		}
		n, err := r.input.Read(p)
		if n > 0 {
			if err := deltaAddCount(&r.streamBytes, uint64(n)); err != nil {
				r.failure = err
				return 0, err
			}
			_, _ = r.payloadHash.Write(p[:n])
			_, _ = r.partHash.Write(p[:n])
			_, _ = r.stream.Write(p[:n])
			r.remaining -= uint64(n)
		}
		if err != nil {
			if err == io.EOF {
				err = io.ErrUnexpectedEOF
			}
			r.failure = fmt.Errorf("native delta part %d payload: %w", r.sequence, err)
			return n, r.failure
		}
		return n, nil
	}
	return 0, io.EOF
}

func (r *NativeDeltaPartReader) Verified() bool      { return r.verified && r.failure == nil }
func (r *NativeDeltaPartReader) StreamBytes() uint64 { return r.streamBytes }

func (r *NativeDeltaPartReader) Close() error {
	if r.closed {
		return r.failure
	}
	r.closed = true
	var err error
	if r.part != nil {
		err = r.part.Close()
		r.part = nil
	}
	if !r.Verified() {
		err = errors.Join(err, r.failure, fmt.Errorf("native delta stream not completely verified"))
	}
	return err
}
