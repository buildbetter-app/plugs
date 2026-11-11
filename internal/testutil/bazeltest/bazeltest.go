// Package bazeltest resolves declared native test runfiles.
package bazeltest

import (
	"fmt"
	"os"
	"path/filepath"
)

func IsBazel() bool {
	return os.Getenv("TEST_SRCDIR") != ""
}

func runfilesDir() string {
	dir := os.Getenv("RUNFILES_DIR")
	if dir == "" {
		dir = os.Getenv("TEST_SRCDIR")
	}
	if dir == "" {
		return ""
	}
	if abs, err := filepath.Abs(dir); err == nil {
		return abs
	}
	return dir
}

func Runfile(path string) (string, error) {
	rf := runfilesDir()
	if rf == "" {
		return "", fmt.Errorf("runfile %q: not running under bazel", path)
	}
	full := filepath.Join(rf, filepath.FromSlash(path))
	if _, err := os.Stat(full); err != nil {
		return "", fmt.Errorf("runfile %q: %w", path, err)
	}
	return full, nil
}

func RunfileEnv(key string) (string, error) {
	val := os.Getenv(key)
	if val == "" {
		return "", fmt.Errorf("%s is not set: declare the file as data of this go_test and set env = {%q: \"$(rlocationpath <label>)\"}", key, key)
	}
	if filepath.IsAbs(val) {
		if _, err := os.Stat(val); err != nil {
			return "", fmt.Errorf("%s=%q is not usable: %w", key, val, err)
		}
		return val, nil
	}
	path, err := Runfile(val)
	if err != nil {
		return "", fmt.Errorf("%s: %w", key, err)
	}
	return path, nil
}
