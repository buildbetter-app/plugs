package plugdump

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestNativeCaptureRequiresPublicDeclaration(t *testing.T) {
	_, files, err := (SQLSource{}).NativeInventory(context.Background(), NativeCapture{})
	if err == nil || !strings.Contains(err.Error(), "explicit --plug-capture-public") || len(files) != 0 {
		t.Fatalf("unapproved public census: %v", err)
	}
}

func TestUniqueNativePathsPreservesFirstSeenDistinctPaths(t *testing.T) {
	input := []string{
		"/workspace/.beads/config.local.yaml",
		"/workspace/.beads/config.yaml",
		"/workspace/.beads/config.local.yaml",
		"/workspace/.beads/metadata.json",
		"/workspace/.beads/config.yaml",
	}
	got := uniqueNativePaths(input)
	want := []string{
		"/workspace/.beads/config.local.yaml",
		"/workspace/.beads/config.yaml",
		"/workspace/.beads/metadata.json",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("uniqueNativePaths() = %v, want %v", got, want)
	}
}

func TestNativeFileObservations(t *testing.T) {
	root := t.TempDir()
	raw := []byte("#" + strings.Repeat(" public ", 300) + "\nissue-prefix: native\npublic_number: 9007199254740993\n")
	path := filepath.Join(root, "config.yaml")
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	value, err := observeNativePath(path, 0, &nativeCaptureBudget{})
	if err != nil {
		t.Fatal(err)
	}
	encoded := value["bytes"].(map[string]string)["$bytes"]
	decoded, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil || !bytes.Equal(decoded, raw) || value["sha256"] != fmt.Sprintf("%x", sha256.Sum256(raw)) || value["state"] != "file" {
		t.Fatal("native file bytes/hash changed")
	}
	absent, err := observeNativePath(filepath.Join(root, "absent"), 0, &nativeCaptureBudget{})
	if err != nil || absent["state"] != "absent" {
		t.Fatal("actual absence not observed")
	}
	directory, err := observeNativePath(root, 0, &nativeCaptureBudget{})
	if err != nil || directory["state"] != "directory" || len(directory["entries"].([]any)) != 1 {
		t.Fatal("present directory falsely omitted")
	}
	if err := validateSecretReferences(value); err != nil {
		t.Fatal(err)
	}
}

func TestNativeYAMLCommentOnlyDocumentEmitsObject(t *testing.T) {
	for name, raw := range map[string][]byte{
		"empty":        nil,
		"comment-only": []byte("# comment-only native config\n# preserved exactly\n"),
	} {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			path := filepath.Join(root, "config.yaml")
			if err := os.WriteFile(path, raw, 0600); err != nil {
				t.Fatal(err)
			}

			value, err := observeNativePath(path, 0, &nativeCaptureBudget{})
			if err != nil {
				t.Fatal(err)
			}
			values, ok := value["values"].(map[string]any)
			if !ok || values == nil || len(values) != 0 {
				t.Fatalf("empty YAML values = %#v, want explicit empty object", value["values"])
			}
			encoded := value["bytes"].(map[string]string)["$bytes"]
			decoded, err := base64.StdEncoding.DecodeString(encoded)
			if err != nil || !bytes.Equal(decoded, raw) || value["sha256"] != fmt.Sprintf("%x", sha256.Sum256(raw)) {
				t.Fatal("empty YAML raw bytes or hash changed")
			}
		})
	}
}

func TestNativeYAMLExplicitNullDoesNotBecomeObject(t *testing.T) {
	for _, raw := range []string{"null\n", "~\n", "!!null \"\"\n"} {
		t.Run(strings.TrimSpace(raw), func(t *testing.T) {
			root := t.TempDir()
			path := filepath.Join(root, "config.yaml")
			if err := os.WriteFile(path, []byte(raw), 0600); err != nil {
				t.Fatal(err)
			}
			value, err := observeNativePath(path, 0, &nativeCaptureBudget{})
			if err != nil {
				t.Fatal(err)
			}
			if _, ok := value["values"]; ok {
				t.Fatalf("explicit null emitted values: %#v", value["values"])
			}
		})
	}
}

func TestNativeYAMLNonObjectsRemainRefused(t *testing.T) {
	for name, raw := range map[string]string{
		"scalar":   "hello\n",
		"sequence": "- hello\n",
		"invalid":  "key: [unterminated\n",
	} {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			path := filepath.Join(root, "config.yaml")
			if err := os.WriteFile(path, []byte(raw), 0600); err != nil {
				t.Fatal(err)
			}
			if value, err := observeNativePath(path, 0, &nativeCaptureBudget{}); err == nil || value != nil {
				t.Fatalf("non-object YAML was captured: value=%#v err=%v", value, err)
			}
		})
	}
}

func TestNativeCaptureRefusesUnboundedOrUnsafeSources(t *testing.T) {
	for _, name := range []string{"symlink", "engine-directory", "credential-yaml", "duplicate-json", "oversized", "relative"} {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			path := filepath.Join(root, "source")
			switch name {
			case "symlink":
				if err := os.Symlink(root, path); err != nil {
					t.Fatal(err)
				}
			case "engine-directory":
				path = root
				if err := os.Mkdir(filepath.Join(root, ".dolt"), 0700); err != nil {
					t.Fatal(err)
				}
			case "credential-yaml":
				path += ".yaml"
				if err := os.WriteFile(path, []byte("password: do-not-capture\n"), 0600); err != nil {
					t.Fatal(err)
				}
			case "duplicate-json":
				path += ".json"
				if err := os.WriteFile(path, []byte(`{"x":1,"x":2}`), 0600); err != nil {
					t.Fatal(err)
				}
			case "oversized":
				if err := os.WriteFile(path, make([]byte, (1<<20)+1), 0600); err != nil {
					t.Fatal(err)
				}
			case "relative":
				path = "relative"
			}
			if value, err := observeNativePath(path, 0, &nativeCaptureBudget{}); err == nil || value != nil {
				t.Fatal("unsupported source became a public capture")
			}
		})
	}
}

func TestNativeEnvironmentCredentialReferences(t *testing.T) {
	values := nativePublicValues(map[string]any{"BD_PASSWORD": "must-not-leak", "BEADS_DIR": "/public"})
	data, err := marshal(values)
	if err != nil || bytes.Contains(data, []byte("must-not-leak")) || values["BEADS_DIR"] != "/public" {
		t.Fatal("public environment classification changed source or leaked a credential")
	}
	if err := validateSecretReferences(values); err != nil {
		t.Fatal(err)
	}
}
