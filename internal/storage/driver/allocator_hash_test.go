package driver

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestConfiguredHashProfilePreservesZeroProbabilityJSON(t *testing.T) {
	a := NewHashAllocator("demo", 3, 8, 0.25)
	a.MaxCollisionProb = 0
	data, err := json.Marshal(a)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil || string(fields["max_collision_prob"]) != "0" {
		t.Fatalf("explicit zero probability omitted: %s %v", data, err)
	}
	var decoded OfflineAllocator
	if err := json.Unmarshal(data, &decoded); err != nil || decoded.MaxCollisionProb != 0 {
		t.Fatalf("zero profile round trip: %+v %v", decoded, err)
	}
	// A nonempty store with a zero collision budget uses the maximum width.
	id, err := decoded.MintHashID(HashInput{Title: "zero probability"}, 1, func(string) (bool, error) { return false, nil })
	if err != nil || len(strings.TrimPrefix(id, "demo-")) != 8 {
		t.Fatalf("zero policy lost after serialization: %s %v", id, err)
	}
}

func TestHashCandidateMatchesPinnedJiraVector(t *testing.T) {
	input := HashInput{
		Title:       "Fix login",
		Description: "Details",
		Creator:     "jira-import",
		Timestamp:   time.Date(2024, 1, 2, 3, 4, 5, 6*1_000_000, time.UTC),
	}
	want := map[uint32]string{
		3: string([]byte{98, 100}) + "-vju",
		4: string([]byte{98, 100}) + "-8d8e",
		5: string([]byte{98, 100}) + "-bi3tk",
		6: string([]byte{98, 100}) + "-8bi3tk",
		7: string([]byte{98, 100}) + "-r5sr6bm",
		8: string([]byte{98, 100}) + "-8r5sr6bm",
	}
	for length, expected := range want {
		if got, err := HashCandidate(string([]byte{98, 100}), input, length, 0); err != nil || got != expected {
			t.Errorf("length %d: got %q err=%v want %q", length, got, err, expected)
		}
	}
}

func TestHashAllocatorAdaptiveLengthAndTenNonceRetry(t *testing.T) {
	a := NewHashAllocator("demo", 3, 10, 0.25)
	input := HashInput{Title: "same", Creator: "actor", Timestamp: time.Unix(0, 123).UTC()}
	var tried []string
	got, err := a.MintHashID(input, 100, func(candidate string) (bool, error) {
		tried = append(tried, candidate)
		// The first ten candidates at the adaptive starting length are
		// occupied, forcing the source's ten-nonce then next-length path.
		return len(tried) <= 10, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(tried) != 11 {
		t.Fatalf("collision attempts=%d, want exactly ten retries plus one candidate", len(tried))
	}
	if len(strings.TrimPrefix(got, "demo-")) != 4 {
		t.Fatalf("adaptive candidate=%q, want length 4 after ten length-3 collisions", got)
	}
}

func TestHashAllocatorRejectsIncompleteRestoredPolicy(t *testing.T) {
	a := OfflineAllocator{Prefix: "demo", Mode: AllocatorHash, HashLength: 8}
	_, err := a.MintHashID(HashInput{Title: "x"}, 0, func(string) (bool, error) { return false, nil })
	if !errors.Is(err, ErrUnsupported) {
		t.Fatalf("incomplete hash policy error=%v", err)
	}
}

func TestChildNumberRepairsStaleCounterButIgnoresGrandchildren(t *testing.T) {
	if got, err := ChildNumber("demo-1", "demo-1.8"); err != nil || got != 8 {
		t.Fatalf("direct child=%d err=%v", got, err)
	}
	if got, err := ChildNumber("demo-1", "demo-1.8.2"); err == nil || got != 0 {
		t.Fatalf("grandchild accepted: %d err=%v", got, err)
	}
	if _, err := ChildNumber("demo-1", "demo-1.18446744073709551616"); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("overflow error=%v", err)
	}
}
