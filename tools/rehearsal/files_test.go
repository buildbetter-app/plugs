package rehearsal

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestGateFileRequiresBoundedCanonicalSingleObjectAndExpectedHead(t *testing.T) {
	if err := ensureWorkRoot(); err != nil {
		t.Fatal(err)
	}
	dir, err := os.MkdirTemp(workRoot, "plug-swarm-i8-evidence-test-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Error(err)
		}
	})
	evidence := completeGateEvidence(t, time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC))
	data, err := json.Marshal(evidence)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "evidence.json")
	write := func(data []byte) {
		t.Helper()
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	write(data)
	loaded, err := LoadGateEvidence(path, evidence.SourceSHA, evidence.SourceTree)
	if err != nil || len(EvaluateGate(time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC), loaded)) != 0 {
		t.Fatalf("valid evidence refused: %v", err)
	}
	for _, value := range [][]byte{append(append([]byte(nil), data...), []byte(" {}")...), []byte(`{"extra":true}`), []byte("null"), []byte(strings.Repeat(" ", (1<<20)+1))} {
		write(value)
		if _, err := LoadGateEvidence(path, evidence.SourceSHA, evidence.SourceTree); err == nil {
			t.Fatal("invalid evidence accepted")
		}
	}
	write(data)
	if _, err := LoadGateEvidence(path, strings.Repeat("c", 40), evidence.SourceTree); err == nil {
		t.Fatal("old head accepted")
	}
	if _, err := LoadGateEvidence(path, evidence.SourceSHA, "HEAD"); err == nil {
		t.Fatal("unbound tree accepted")
	}
	link := filepath.Join(dir, "link.json")
	if err := os.Symlink(path, link); err != nil {
		t.Fatal(err)
	}
	for _, invalid := range []string{link, dir, filepath.Join(dir, "missing.json"), "evidence.json", filepath.Dir(workRoot) + "/evidence.json"} {
		if _, err := LoadGateEvidence(invalid, evidence.SourceSHA, evidence.SourceTree); err == nil {
			t.Fatal("unsafe path accepted")
		}
	}
}
