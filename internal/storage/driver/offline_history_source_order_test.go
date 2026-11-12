package driver

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"testing"
)

func ordinalCommitFrames(store, project string, global, local uint64) []OfflineHistoryStreamFrame {
	c := OfflineHistoryStreamCommit{SourceStoreID: store, Project: project, Sequence: global, SourceSequence: local, CommitIDs: 1, Physical: &OfflineHistorySourceCommit{Root: "original-root", TablesSHA256: fixtureSHA([]byte(store))}}
	frames := []OfflineHistoryStreamFrame{{Kind: "commit", Commit: &c}, {Kind: "id", ID: &OfflineHistoryStreamID{Kind: "commit", Value: "SAME-RAW-COMMIT-ID"}}}
	h := sha256.New()
	h.Write([]byte("plug-offline-history-commit/1\n"))
	for _, frame := range frames {
		raw, _ := json.Marshal(frame)
		streamHashJSON(h, raw)
	}
	return append(frames, OfflineHistoryStreamFrame{Kind: "commit-end", CommitEnd: &OfflineHistoryStreamCommitEnd{SHA256: hex.EncodeToString(h.Sum(nil))}})
}

func validateOrdinalFixture(header OfflineHistoryStreamHeader, frames []OfflineHistoryStreamFrame) error {
	v := streamValidator{header: header, nextGap: 1}
	for _, frame := range frames {
		raw, _ := json.Marshal(frame)
		if err := v.accept(frame, raw); err != nil {
			return err
		}
	}
	return nil
}

func TestNativeCollectionOrdinalOrderKeepsEqualRawCommitIDsDistinct(t *testing.T) {
	order := []NativeHistoryBoundary{{SourceStoreID: "z-source", Project: "ops"}, {SourceStoreID: "a-source", Project: "sv2"}}
	h := OfflineHistoryStreamHeader{Version: OfflineHistoryStreamVersion, Source: NativeTableHistorySource, Verified: true, Complete: true, First: 1, Head: 2, NativeSourceCount: 2, NativeSourceOrderSHA256: NativeSourceOrderSHA256(order)}
	frames := append(ordinalCommitFrames("z-source", "ops", 1, 1), ordinalCommitFrames("a-source", "sv2", 2, 1)...)
	frames = append(frames, OfflineHistoryStreamFrame{Kind: "end", End: &OfflineHistoryStreamEnd{Commits: 2}})
	if err := validateOrdinalFixture(h, frames); err != nil {
		t.Fatal("ordinal concatenation/equal raw IDs refused", err)
	}
	for _, attack := range []string{"wrong-order-commitment", "source-count", "repeated-source-block", "legacy-order"} {
		t.Run(attack, func(t *testing.T) {
			candidate := h
			body := frames
			switch attack {
			case "wrong-order-commitment":
				candidate.NativeSourceOrderSHA256 = NativeSourceOrderSHA256([]NativeHistoryBoundary{order[1], order[0]})
			case "source-count":
				candidate.NativeSourceCount = 1
			case "repeated-source-block":
				body = append(ordinalCommitFrames("z-source", "ops", 1, 1), ordinalCommitFrames("z-source", "ops", 2, 1)...)
				body = append(body, frames[len(frames)-1])
			case "legacy-order":
				candidate.NativeSourceCount, candidate.NativeSourceOrderSHA256 = 0, ""
			}
			if err := validateOrdinalFixture(candidate, body); err == nil {
				t.Fatal("changed ordinal authority accepted")
			}
		})
	}
}

func TestNativeCollectionMixedBoundaryAppliesOnlyToItsSource(t *testing.T) {
	order := []NativeHistoryBoundary{{SourceStoreID: "z-source", Project: "ops", VerifiedCommitCount: 1, BoundarySHA256: fixtureSHA([]byte("boundary"))}, {SourceStoreID: "a-source", Project: "sv2"}}
	h := OfflineHistoryStreamHeader{Version: OfflineHistoryStreamVersion, Source: NativeTableHistorySource, Verified: true, First: 1, Head: 2, NativeBoundaries: 1, NativeSourceCount: 2, NativeSourceOrderSHA256: NativeSourceOrderSHA256(order)}
	frames := []OfflineHistoryStreamFrame{{Kind: "boundary", Boundary: &order[0]}}
	frames = append(frames, ordinalCommitFrames("z-source", "ops", 1, 1)...)
	frames = append(frames, ordinalCommitFrames("a-source", "sv2", 2, 1)...)
	frames = append(frames, OfflineHistoryStreamFrame{Kind: "end", End: &OfflineHistoryStreamEnd{Commits: 2}})
	if err := validateOrdinalFixture(h, frames); err != nil {
		t.Fatal(err)
	}
	order[0].VerifiedCommitCount = 2
	if err := validateOrdinalFixture(h, frames); err == nil {
		t.Fatal("incomplete source boundary accepted")
	}
}
