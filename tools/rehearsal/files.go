package rehearsal

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

const workRoot = "/tmp/plug-swarm/work"

func ensureWorkRoot() error {
	canonical, err := filepath.EvalSymlinks("/tmp")
	if err != nil || canonical != "/tmp" {
		return fmt.Errorf("disposable parent must be a canonical directory")
	}
	for _, path := range []string{filepath.Dir(workRoot), workRoot} {
		if err := os.Mkdir(path, 0700); err != nil && !os.IsExist(err) {
			return fmt.Errorf("disposable work directory unavailable")
		}
		info, err := os.Lstat(path)
		if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("disposable work root must be a real directory")
		}
	}
	return nil
}

// LoadGateEvidence accepts bounded, sanitized evidence from a trusted host.
// The independently selected expected head prevents an old evidence file from
// certifying a new candidate. Receipt contents still require external review.
func LoadGateEvidence(path, expectedSHA, expectedTree string) (GateEvidence, error) {
	var evidence GateEvidence
	if !sha40.MatchString(expectedSHA) || !sha40.MatchString(expectedTree) {
		return evidence, fmt.Errorf("full expected candidate SHA and tree required")
	}
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || !strings.HasPrefix(path, workRoot+"/") {
		return evidence, fmt.Errorf("gate evidence must be a canonical disposable work file")
	}
	canonical, err := filepath.EvalSymlinks(path)
	if err != nil || canonical != path {
		return evidence, fmt.Errorf("gate evidence symlinks are refused")
	}
	before, err := os.Lstat(path)
	if err != nil || !before.Mode().IsRegular() || before.Size() > 1<<20 {
		return evidence, fmt.Errorf("gate evidence must be a bounded regular file")
	}
	file, err := os.Open(path)
	if err != nil {
		return evidence, fmt.Errorf("gate evidence unavailable")
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() > 1<<20 {
		return evidence, fmt.Errorf("gate evidence must be a bounded regular file")
	}
	data, err := io.ReadAll(io.LimitReader(file, (1<<20)+1))
	if err != nil || len(data) > 1<<20 {
		return evidence, fmt.Errorf("gate evidence exceeds bound")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&evidence) != nil || decoder.Decode(new(any)) != io.EOF {
		return evidence, fmt.Errorf("gate evidence must contain exactly one valid object")
	}
	if evidence.SourceSHA != expectedSHA || evidence.SourceTree != expectedTree {
		return evidence, fmt.Errorf("gate evidence does not match the expected candidate")
	}
	return evidence, nil
}
