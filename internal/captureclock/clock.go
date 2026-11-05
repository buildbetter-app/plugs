// Package captureclock supplies explicitly opted-in capture clocks. It does
// not replace elapsed-time/deadline clocks or change ordinary command policy.
package captureclock

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	FileEnv   = "BEADS_CAPTURE_CLOCK_FILE"
	PublicEnv = "BEADS_CAPTURE_CLOCK_PUBLIC"
	Version   = "beads-capture-clock/1"
	maxBytes  = 4096
)

// Snapshot binds an observed clock input to its exact bytes. Operators publish
// a new file by atomic rename only while commands/queries are quiescent. Each
// decision reads the file again, including in an already-running HTTP/SQL server.
type Snapshot struct {
	Version string    `json:"version"`
	Step    string    `json:"step"`
	GoTime  time.Time `json:"go_time"`
	SQLTime time.Time `json:"sql_time"`
	Path    string    `json:"path"`
	SHA256  string    `json:"sha256"`
	Input   []byte    `json:"input_bytes"`
}

// Read returns nil only when both capture inputs are genuinely absent. Invalid
// or partial opt-in refuses; it never silently falls back to ordinary time.
func Read() (*Snapshot, error) {
	path, hasFile := os.LookupEnv(FileEnv)
	public, hasPublic := os.LookupEnv(PublicEnv)
	if !hasFile && !hasPublic {
		return nil, nil
	}
	if !hasFile || path == "" || !hasPublic || public != "1" {
		return nil, fmt.Errorf("capture clock requires %s=1 and an explicit %s", PublicEnv, FileEnv)
	}
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return nil, fmt.Errorf("capture clock requires a canonical absolute file path")
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil || resolved != path {
		return nil, fmt.Errorf("capture clock file must exist without symlink aliases")
	}
	parent, err := os.Lstat(filepath.Dir(path))
	if err != nil || !parent.IsDir() || parent.Mode().Perm()&0077 != 0 {
		return nil, fmt.Errorf("capture clock parent must be a private directory")
	}
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
		return nil, fmt.Errorf("capture clock input must be a private regular file")
	}
	// Keep resolution inside the validated private parent even if the input
	// changes between validation and opening. SameFile below also binds the inode.
	file, err := os.OpenInRoot(filepath.Dir(path), filepath.Base(path))
	if err != nil {
		return nil, fmt.Errorf("open capture clock: %w", err)
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil || !os.SameFile(info, opened) {
		return nil, fmt.Errorf("capture clock input changed while opening")
	}
	data, err := io.ReadAll(io.LimitReader(file, maxBytes+1))
	if err != nil || len(data) > maxBytes {
		return nil, fmt.Errorf("capture clock input is unreadable or exceeds %d bytes", maxBytes)
	}
	snapshot, err := parse(data)
	if err != nil {
		return nil, err
	}
	snapshot.Path = path
	hash := sha256.Sum256(data)
	snapshot.SHA256 = hex.EncodeToString(hash[:])
	snapshot.Input = data
	return snapshot, nil
}

// Now is used only at the named native decision/wall-time seams. Monotonic
// deadlines, sleeps, transport timeouts and other command behavior stay intact.
// A malformed step is fatal to the capture, rather than an uncontrolled read.
func Now() time.Time {
	snapshot, err := Read()
	if err != nil {
		panic(err)
	}
	if snapshot != nil {
		return snapshot.GoTime
	}
	return time.Now()
}

// SQLNow is installed around the genuine capture SQL server's entire lifetime.
// That companion always requires opt-in, and never uses an ordinary-time fallback.
func SQLNow() time.Time {
	snapshot, err := Read()
	if err != nil {
		panic(err)
	}
	if snapshot == nil {
		panic("capture SQL server lost its explicit clock input")
	}
	return snapshot.SQLTime
}

func parse(data []byte) (*Snapshot, error) {
	if !utf8.Valid(data) {
		return nil, fmt.Errorf("capture clock input must be valid UTF-8")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	first, err := decoder.Token()
	if err != nil || first != json.Delim('{') {
		return nil, fmt.Errorf("capture clock must be an object")
	}
	values := map[string]string{}
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return nil, fmt.Errorf("capture clock invalid field: %w", err)
		}
		key, ok := token.(string)
		if !ok || (key != "version" && key != "step" && key != "go_time" && key != "sql_time") {
			return nil, fmt.Errorf("capture clock unknown field %q", token)
		}
		if _, exists := values[key]; exists {
			return nil, fmt.Errorf("capture clock duplicate field %s", key)
		}
		value, err := decoder.Token()
		text, ok := value.(string)
		if err != nil || !ok || text == "" {
			return nil, fmt.Errorf("capture clock %s must be a nonempty string", key)
		}
		values[key] = text
	}
	if end, err := decoder.Token(); err != nil || end != json.Delim('}') {
		return nil, fmt.Errorf("capture clock invalid object end")
	}
	if _, err := decoder.Token(); err != io.EOF {
		return nil, fmt.Errorf("capture clock trailing input")
	}
	if len(values) != 4 || values["version"] != Version {
		return nil, fmt.Errorf("capture clock requires version %s and all four fields", Version)
	}
	if len(values["step"]) > 128 || strings.ContainsAny(values["step"], "\x00\r\n\t") {
		return nil, fmt.Errorf("capture clock invalid step identity")
	}
	parseInstant := func(key string) (time.Time, error) {
		value := values[key]
		instant, err := time.Parse(time.RFC3339Nano, value)
		if err != nil || !strings.HasSuffix(value, "Z") || instant.Format(time.RFC3339Nano) != value {
			return time.Time{}, fmt.Errorf("capture clock %s must be canonical RFC3339 UTC", key)
		}
		return instant, nil
	}
	goTime, err := parseInstant("go_time")
	if err != nil {
		return nil, err
	}
	sqlTime, err := parseInstant("sql_time")
	if err != nil {
		return nil, err
	}
	if sqlTime.Nanosecond()%1000 != 0 {
		return nil, fmt.Errorf("capture clock sql_time must be exactly representable in microseconds")
	}
	return &Snapshot{Version: Version, Step: values["step"], GoTime: goTime, SQLTime: sqlTime}, nil
}
