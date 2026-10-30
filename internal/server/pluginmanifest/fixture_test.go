package pluginmanifest_test

import (
	"context"
	"embed"
	"errors"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/server/pluginmanifest"
	"github.com/buildbetter-app/plugs/internal/server/pluginmanifest/fixture"
	sdk "github.com/buildbetter-app/plugs/sdk/go"
)

// These are the same source bytes compiled into the fixture library. Tests
// commit them into a disposable local repository; no fetch or build is run.
//
//go:embed fixture/BUILD.bazel fixture/native.go fixture/plug-plugin.json
var sourceFiles embed.FS

func git(t *testing.T, root string, args ...string) string {
	t.Helper()
	argv := []string{"-c", "core.hooksPath=" + os.DevNull, "-c", "commit.gpgsign=false", "-C", root}
	command := exec.Command("git", append(argv, args...)...)
	for _, value := range os.Environ() {
		if !strings.HasPrefix(value, "GIT_") {
			command.Env = append(command.Env, value)
		}
	}
	command.Env = append(command.Env, "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL="+os.DevNull,
		"GIT_AUTHOR_NAME=Fixture", "GIT_AUTHOR_EMAIL=fixture@example.test", "GIT_COMMITTER_NAME=Fixture", "GIT_COMMITTER_EMAIL=fixture@example.test",
		"GIT_AUTHOR_DATE=2001-01-01T00:00:00Z", "GIT_COMMITTER_DATE=2001-01-01T00:00:00Z")
	out, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("local fixture git %s: %v\n%s", args[0], err, out)
	}
	return strings.TrimSpace(string(out))
}

func checkout(t *testing.T) pluginmanifest.Checkout {
	t.Helper()
	return checkoutFormat(t, "sha1")
}

func checkoutFormat(t *testing.T, format string) pluginmanifest.Checkout {
	t.Helper()
	root := t.TempDir()
	if err := fs.WalkDir(sourceFiles, "fixture", func(name string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		data, err := sourceFiles.ReadFile(name)
		if err != nil {
			return err
		}
		return os.WriteFile(filepath.Join(root, strings.TrimPrefix(name, "fixture/")), data, 0o644)
	}); err != nil {
		t.Fatal(err)
	}
	git(t, root, "init", "-q", "--template=", "--initial-branch=main", "--object-format="+format)
	return commit(t, pluginmanifest.Checkout{Root: root})
}

func commit(t *testing.T, source pluginmanifest.Checkout) pluginmanifest.Checkout {
	t.Helper()
	git(t, source.Root, "add", "--all")
	git(t, source.Root, "commit", "-q", "--no-verify", "-m", "static source fixture")
	source.Commit = git(t, source.Root, "rev-parse", "HEAD")
	return source
}

func write(t *testing.T, source pluginmanifest.Checkout, name string, data []byte) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(source.Root, name), data, 0o644); err != nil {
		t.Fatal(err)
	}
}

func load(t *testing.T, source pluginmanifest.Checkout) *pluginmanifest.Bundle {
	t.Helper()
	bundle, err := pluginmanifest.Load(context.Background(), source)
	if err != nil {
		t.Fatal(err)
	}
	return bundle
}

func natives(t *testing.T) []sdk.Native {
	t.Helper()
	value, err := fixture.Natives()
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func requireCode(t *testing.T, err error, code string) {
	t.Helper()
	var failure *pluginmanifest.Error
	if !errors.As(err, &failure) || failure.Code != code {
		t.Fatalf("got %v, want plugin bundle refusal %s", err, code)
	}
}
