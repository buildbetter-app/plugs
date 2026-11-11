//go:build unix

package httpclient

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Bazel's mount namespace can expose a filesystem root owned by nobody.
// These CA fixtures require a trusted enclosing filesystem, independently of
// that host layout. Model ownership only above the test's private directory;
// every mode, symlink, leaf and directory inside the fixture remains real.
// Ownership-refusal tests can overlay this baseline through the same stat seam.
func trustCAFixtureAncestors(t *testing.T, dir string) {
	t.Helper()
	physical, err := filepath.EvalSymlinks(dir)
	if err != nil {
		t.Fatal(err)
	}
	ancestors := map[string]bool{}
	for parent := filepath.Dir(physical); ; parent = filepath.Dir(parent) {
		ancestors[parent] = true
		if filepath.Dir(parent) == parent {
			break
		}
	}
	previous := statCAPathFn
	t.Cleanup(func() { statCAPathFn = previous })
	statCAPathFn = func(path string) (caFileStat, error) {
		fi, err := previous(path)
		if err == nil && ancestors[path] {
			fi.uid = uint32(os.Geteuid())
			fi.gid = uint32(os.Getegid())
		}
		return fi, err
	}
}

func TestCheckCAFilePermissionsRefusesUntrustedFilesystemRoot(t *testing.T) {
	dir := secureTempDir(t)
	path := filepath.Join(dir, "ca.pem")
	if err := os.WriteFile(path, []byte("fixture"), 0o600); err != nil {
		t.Fatal(err)
	}
	previous := statCAPathFn
	t.Cleanup(func() { statCAPathFn = previous })
	statCAPathFn = func(path string) (caFileStat, error) {
		fi, err := previous(path)
		if path == "/" {
			fi.uid = 65534
			if os.Geteuid() == 65534 {
				fi.uid++
			}
		}
		return fi, err
	}
	if _, _, err := checkCAFilePermissions(path); err == nil || !strings.Contains(err.Error(), "directory / is owned by uid") {
		t.Fatalf("untrusted filesystem root accepted or wrong refusal: %v", err)
	}
}
