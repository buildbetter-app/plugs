package reverseconverter

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"testing"
)

// Genuine public SQLSource output: producer33890d8, RunsOn41334286284.
// Its verified suffix does not establish complete-root source history.
func TestVerifiedPublicPartialArchiveReverseDisclosesHistoryLoss(t *testing.T) {
	data, err := os.ReadFile("../../integration/offlinehistory/testdata/dolt-source-partial.tar")
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(data)
	if hex.EncodeToString(sum[:]) != "b56f1de4a36c22cca379647d5fe1b2962e4358d9976923e76007ea84b7712582" {
		t.Fatal("public producer archive changed")
	}
	result, err := ConvertArchive(data)
	if err != nil {
		t.Fatalf("validated public partial source cannot reverse: %v", err)
	}
	if len(result.JSONL) == 0 || len(result.Report.LostPostSwitchHistory) == 0 {
		t.Fatal("head-state reverse must disclose unreplayed source history")
	}
}

// This is a canonical-consumer contract fixture, not real complete-root proof.
func TestCheckedHistoryHeadReverseDoesNotClaimReplay(t *testing.T) {
	data, err := os.ReadFile("../../integration/offlinehistory/testdata/full.tar")
	if err != nil {
		t.Fatal(err)
	}
	result, err := ConvertArchive(data)
	if err != nil {
		t.Fatalf("canonical checked history cannot reverse its head: %v", err)
	}
	if len(result.JSONL) == 0 || len(result.Report.LostPostSwitchHistory) == 0 {
		t.Fatal("head-state conversion must disclose that source history is not replayed")
	}
}

// The actual supported exporter ran against public Dolt source actions. This
// checks its exact bytes, never a consumer-generated complete-root substitute.
func TestPublicDoltCompleteRootArchiveHeadReverse(t *testing.T) {
	data, err := os.ReadFile("../../integration/converterroundtrip/testdata/public-complete-root-990eb800.tar")
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(data)
	if hex.EncodeToString(sum[:]) != "a2d933a7a54b53303d33a20d5b96eaac0a85c079c5b08cae553aa2a654d820b5" {
		t.Fatal("public complete-root source bytes changed")
	}
	result, err := ConvertArchive(data)
	if err != nil {
		t.Fatalf("genuine public complete-root head cannot reverse: %v", err)
	}
	if len(result.JSONL) == 0 || len(result.Report.LostPostSwitchHistory) == 0 {
		t.Fatal("head-state reverse must disclose unreplayed complete source history")
	}
}
