package captureclock

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const validInput = `{"version":"beads-capture-clock/1","step":"before","go_time":"2026-11-03T12:00:00.123456789Z","sql_time":"2026-11-03T12:00:00.123456Z"}`

func TestCaptureClockStrictInput(t *testing.T) {
	baseline, err := parse([]byte(validInput))
	if err != nil || baseline.GoTime.Nanosecond() != 123456789 || baseline.SQLTime.Nanosecond() != 123456000 {
		t.Fatalf("independent valid clock inputs: %+v %v", baseline, err)
	}
	cases := []struct{ name, input, reason string }{
		{"null", "null", "must be an object"},
		{"array", "[]", "must be an object"},
		{"missing", strings.Replace(validInput, `"step":"before",`, "", 1), "all four fields"},
		{"duplicate", strings.Replace(validInput, `"step":"before"`, `"step":"before","step":"after"`, 1), "duplicate field step"},
		{"folded", strings.Replace(validInput, `"step"`, `"STEP"`, 1), "unknown field"},
		{"unknown", strings.Replace(validInput, `"step"`, `"other"`, 1), "unknown field"},
		{"empty", strings.Replace(validInput, `"before"`, `""`, 1), "nonempty string"},
		{"null-value", strings.Replace(validInput, `"before"`, `null`, 1), "nonempty string"},
		{"number", strings.Replace(validInput, `"before"`, `12`, 1), "nonempty string"},
		{"nested", strings.Replace(validInput, `"before"`, `{}`, 1), "nonempty string"},
		{"version", strings.Replace(validInput, Version, "beads-capture-clock/2", 1), "requires version"},
		{"trailing", validInput + `{}`, "trailing input"},
		{"offset", strings.Replace(validInput, "123456789Z", "123456789+00:00", 1), "canonical RFC3339 UTC"},
		{"rounding", strings.Replace(validInput, "123456Z", "123456001Z", 1), "exactly representable in microseconds"},
		{"noncanonical", strings.Replace(validInput, "123456Z", "123456000Z", 1), "canonical RFC3339 UTC"},
		{"step-newline", strings.Replace(validInput, `"before"`, `"before\nafter"`, 1), "invalid step"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := parse([]byte(tc.input)); err == nil || !strings.Contains(err.Error(), tc.reason) {
				t.Fatalf("want specific %q refusal, got %v", tc.reason, err)
			}
		})
	}
}

func unset(t *testing.T, key string) {
	t.Helper()
	value, ok := os.LookupEnv(key)
	if err := os.Unsetenv(key); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		var err error
		if ok {
			err = os.Setenv(key, value)
		} else {
			err = os.Unsetenv(key)
		}
		if err != nil {
			t.Error(err)
		}
	})
}

func TestCaptureClockOptInAndStep(t *testing.T) {
	unset(t, FileEnv)
	unset(t, PublicEnv)
	start := time.Now()
	if input, err := Read(); input != nil || err != nil {
		t.Fatalf("legacy absence: %+v %v", input, err)
	}
	if now := Now(); now.Before(start) || now.After(time.Now()) {
		t.Fatalf("ordinary clock: %s", now)
	}
	t.Setenv(PublicEnv, "1")
	if _, err := Read(); err == nil {
		t.Fatal("partial opt-in accepted")
	}
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "clock.json")
	t.Setenv(FileEnv, path)
	if _, err := Read(); err == nil {
		t.Fatal("missing input accepted")
	}
	if err := os.WriteFile(path, []byte(validInput), 0600); err != nil {
		t.Fatal(err)
	}
	input, err := Read()
	if err != nil {
		t.Fatal(err)
	}
	if input.SHA256 != fmt.Sprintf("%x", sha256.Sum256([]byte(validInput))) || !bytes.Equal(input.Input, []byte(validInput)) {
		t.Fatal("clock evidence did not bind exact input bytes")
	}
	if Now() != input.GoTime || SQLNow() != input.SQLTime {
		t.Fatal("independent clocks were conflated")
	}
	next := strings.ReplaceAll(validInput, "before", "after")
	next = strings.ReplaceAll(next, "12:00:00", "12:00:01")
	stage := filepath.Join(dir, "next.json")
	if err := os.WriteFile(stage, []byte(next), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(stage, path); err != nil {
		t.Fatal(err)
	}
	after, err := Read()
	if err != nil || after.Step != "after" || after.SHA256 == input.SHA256 {
		t.Fatalf("step identity: %+v %v", after, err)
	}
	if Now() != input.GoTime.Add(time.Second) || SQLNow() != input.SQLTime.Add(time.Second) {
		t.Fatal("live readers did not observe atomic step")
	}
	if err := os.WriteFile(path, []byte(`null`), 0600); err != nil {
		t.Fatal(err)
	}
	func() {
		defer func() {
			if recover() == nil {
				t.Error("malformed active step fell back to uncontrolled time")
			}
		}()
		_ = Now()
	}()
}

func TestCaptureClockPathAndOptInRefusals(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "clock.json")
	if err := os.WriteFile(path, []byte(validInput), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv(FileEnv, path)
	t.Setenv(PublicEnv, "1")
	if _, err := Read(); err != nil {
		t.Fatalf("valid baseline: %v", err)
	}
	for _, marker := range []string{"", "0", "true"} {
		t.Run("public="+marker, func(t *testing.T) {
			t.Setenv(PublicEnv, marker)
			if _, err := Read(); err == nil {
				t.Fatal("invalid public opt-in accepted")
			}
		})
	}
	t.Run("empty-file", func(t *testing.T) {
		t.Setenv(FileEnv, "")
		if _, err := Read(); err == nil {
			t.Fatal("empty input became absence")
		}
	})
	t.Run("relative", func(t *testing.T) {
		t.Setenv(FileEnv, "clock.json")
		if _, err := Read(); err == nil {
			t.Fatal("relative path accepted")
		}
	})
	t.Run("symlink", func(t *testing.T) {
		link := filepath.Join(dir, "alias.json")
		if err := os.Symlink(path, link); err != nil {
			t.Fatal(err)
		}
		t.Setenv(FileEnv, link)
		if _, err := Read(); err == nil {
			t.Fatal("symlink alias accepted")
		}
	})
	t.Run("file-mode", func(t *testing.T) {
		if err := os.Chmod(path, 0644); err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := os.Chmod(path, 0600); err != nil {
				t.Error(err)
			}
		}()
		if _, err := Read(); err == nil {
			t.Fatal("public clock input accepted")
		}
	})
	t.Run("directory-mode", func(t *testing.T) {
		if err := os.Chmod(dir, 0755); err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := os.Chmod(dir, 0700); err != nil {
				t.Error(err)
			}
		}()
		if _, err := Read(); err == nil {
			t.Fatal("public clock parent accepted")
		}
	})
	t.Run("oversize", func(t *testing.T) {
		if err := os.WriteFile(path, bytes.Repeat([]byte(" "), maxBytes+1), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := Read(); err == nil {
			t.Fatal("unbounded input accepted")
		}
	})
}
