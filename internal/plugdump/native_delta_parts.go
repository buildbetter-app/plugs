package plugdump

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
)

// Payloads spool to an owned file. No part-set or full stream is buffered.
// The test-only smaller part size exercises rollover without changing the wire
// maximum; production always selects NativeDeltaPartBytes.
type nativeDeltaParts struct {
	directory    string
	scratch      string
	limit        uint64
	sequence     uint64
	previous     *string
	stream       hash.Hash
	streamBytes  uint64
	file         *os.File
	payloadHash  hash.Hash
	payloadBytes uint64
}

func newNativeDeltaParts(directory, scratch string, limit uint64) (*nativeDeltaParts, error) {
	if limit == 0 || limit > NativeDeltaPartBytes {
		return nil, fmt.Errorf("invalid native part limit")
	}
	if err := os.Mkdir(directory, 0700); err != nil {
		return nil, err
	}
	return &nativeDeltaParts{directory: directory, scratch: scratch, limit: limit, stream: sha256.New()}, nil
}

func (p *nativeDeltaParts) Close() error {
	if p.file == nil {
		return nil
	}
	name := p.file.Name()
	err := p.file.Close()
	p.file = nil
	return errors.Join(err, os.Remove(name))
}

func (p *nativeDeltaParts) start() error {
	if p.file != nil {
		return nil
	}
	file, err := os.CreateTemp(p.scratch, "payload-*")
	if err != nil {
		return err
	}
	p.file = file
	p.payloadHash = sha256.New()
	p.payloadBytes = 0
	return nil
}

func (p *nativeDeltaParts) Write(data []byte) (int, error) {
	written := 0
	for len(data) > 0 {
		if err := p.start(); err != nil {
			return written, err
		}
		next := uint64(len(data))
		if next > p.limit-p.payloadBytes {
			next = p.limit - p.payloadBytes
		}
		if ^uint64(0)-p.streamBytes < next {
			return written, ErrSourceHistoryUnavailable{Reason: "table-size-limit"}
		}
		piece := data[:int(next)]
		n, err := p.file.Write(piece)
		if n > 0 {
			_, _ = p.payloadHash.Write(piece[:n])
			_, _ = p.stream.Write(piece[:n])
			p.payloadBytes += uint64(n)
			p.streamBytes += uint64(n)
			written += n
			data = data[n:]
		}
		if err != nil {
			return written, err
		}
		if n != int(next) {
			return written, io.ErrShortWrite
		}
		if p.payloadBytes == p.limit {
			if err := p.flush(); err != nil {
				return written, err
			}
		}
	}
	return written, nil
}

func (p *nativeDeltaParts) flush() (resultErr error) {
	if p.file == nil || p.payloadBytes == 0 {
		return nil
	}
	if p.sequence == ^uint64(0) {
		return ErrSourceHistoryUnavailable{Reason: "table-size-limit"}
	}
	sequence := p.sequence + 1
	header, err := marshal(NativeDeltaPartHeader{Format: NativeDeltaPartFormat, Sequence: sequence, PreviousPartSHA256: p.previous, PayloadBytes: p.payloadBytes, PayloadSHA256: hex.EncodeToString(p.payloadHash.Sum(nil))})
	if err != nil {
		return err
	}
	if len(header) > NativeDeltaPartHeaderBytes {
		return fmt.Errorf("native part header limit")
	}
	if _, err := p.file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	output, err := os.OpenFile(filepath.Join(p.directory, fmt.Sprintf("%020d.part", sequence)), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, output.Close()) }()
	whole := sha256.New()
	writer := io.MultiWriter(output, whole)
	if _, err := writer.Write(header); err != nil {
		return err
	}
	count, err := io.Copy(writer, p.file)
	if err != nil {
		return err
	}
	if uint64(count) != p.payloadBytes {
		return fmt.Errorf("native part payload changed")
	}
	if err := output.Sync(); err != nil {
		return err
	}
	value := hex.EncodeToString(whole.Sum(nil))
	p.sequence = sequence
	p.previous = &value
	return p.Close()
}

func (p *nativeDeltaParts) finish() (uint64, string, string, uint64, error) {
	if err := p.flush(); err != nil {
		return 0, "", "", 0, err
	}
	if p.previous == nil {
		return 0, "", "", 0, fmt.Errorf("empty native stream")
	}
	return p.sequence, *p.previous, hex.EncodeToString(p.stream.Sum(nil)), p.streamBytes, nil
}

func (p *nativeDeltaParts) record(value any) error {
	if _, err := nativeDeltaJSONSize(value, NativeDeltaRecordBytes-1); err != nil {
		return err
	}
	data, err := marshal(value)
	if err != nil {
		return err
	}
	if len(data) > NativeDeltaRecordBytes {
		return ErrSourceHistoryUnavailable{Reason: "table-size-limit"}
	}
	_, err = p.Write(data)
	return err
}
