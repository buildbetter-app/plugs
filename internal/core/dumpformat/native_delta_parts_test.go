package dumpformat

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"testing"
)

type deltaPartFile struct {
	*bytes.Reader
	closed *int
}

func (p deltaPartFile) Close() error { (*p.closed)++; return nil }

func deltaTestParts(t *testing.T, payload []byte, width int) ([][]byte, string) {
	t.Helper()
	parts := [][]byte{}
	var previous any
	for len(payload) != 0 {
		n := min(width, len(payload))
		piece := payload[:n]
		header := nativeDeltaPartHeader{NativeDeltaPartFormat, uint64(len(parts) + 1), previous, uint64(n), digest(piece)}
		raw, err := json.Marshal(header)
		if err != nil {
			t.Fatal(err)
		}
		raw = append(append(raw, '\n'), piece...)
		parts = append(parts, raw)
		previous, payload = digest(raw), payload[n:]
	}
	return parts, previous.(string)
}

func TestNativeDeltaPartsPreserveRecordsAcrossEveryBoundary(t *testing.T) {
	// Includes a split UTF-8 code point and split JSON string/number tokens.
	// Transport framing must not decode, normalize or truncate any payload.
	want := []byte("{\"type\":\"row\",\"label\":\"\u03bb\",\"n\":9007199254740993}\n{\"type\":\"end\"}\n")
	for _, width := range []int{1, 7, 31, len(want)} {
		t.Run(fmt.Sprint(width), func(t *testing.T) {
			parts, last := deltaTestParts(t, want, width)
			opened, closed := 0, 0
			r, err := NewNativeDeltaPartReader(context.Background(), uint64(len(parts)), last, digest(want), func(_ context.Context, ordinal uint64) (io.ReadCloser, error) {
				if ordinal != uint64(opened+1) || opened != closed {
					t.Fatal("parts were opened out of sequence or retained simultaneously")
				}
				opened++
				return deltaPartFile{bytes.NewReader(parts[ordinal-1]), &closed}, nil
			})
			if err != nil {
				t.Fatal(err)
			}
			r.payloadLimit = uint64(width) // Exercise the same framing at small part boundaries.
			var got bytes.Buffer
			if _, err = io.CopyBuffer(&got, r, make([]byte, 3)); err != nil {
				t.Fatal(err)
			}
			if !r.Verified() || !bytes.Equal(got.Bytes(), want) || opened != len(parts) || closed != opened {
				t.Fatal("payload/complete part verification/lifetime differs")
			}
			if err := r.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestNativeDeltaPartsCannotQualifyIncompleteOrChangedInput(t *testing.T) {
	want := []byte("{\"type\":\"header\"}\n{\"type\":\"end\"}\n")
	for _, mode := range []string{"truncated", "changed-payload", "trailing-part-bytes", "missing-part", "wrong-final-stream", "early-close"} {
		t.Run(mode, func(t *testing.T) {
			parts, last := deltaTestParts(t, want, 9)
			stream := digest(want)
			switch mode {
			case "truncated":
				parts[0] = parts[0][:len(parts[0])-1]
			case "changed-payload":
				parts[0][len(parts[0])-1] ^= 1
			case "trailing-part-bytes":
				parts[0] = append(parts[0], 'x')
			case "wrong-final-stream":
				stream = strings.Repeat("f", 64)
			}
			closed := 0
			r, err := NewNativeDeltaPartReader(context.Background(), uint64(len(parts)), last, stream, func(_ context.Context, ordinal uint64) (io.ReadCloser, error) {
				if mode == "missing-part" && ordinal == 2 {
					return nil, fmt.Errorf("original part unavailable")
				}
				return deltaPartFile{bytes.NewReader(parts[ordinal-1]), &closed}, nil
			})
			if err != nil {
				t.Fatal(err)
			}
			r.payloadLimit = 9
			if mode != "early-close" {
				if _, err := io.Copy(io.Discard, r); err == nil {
					t.Fatal("incomplete or changed original qualified")
				}
			}
			if r.Verified() || r.Close() == nil {
				t.Fatal("unverified stream reported a successful close")
			}
		})
	}
}
