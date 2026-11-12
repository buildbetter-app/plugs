package recipehooks

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"os"
	"path/filepath"
	"testing"
)

type fixtureServer struct {
	root, marker string
	stops        int
	stopErr      error
}

func (s *fixtureServer) Stop(context.Context) error {
	s.stops++
	if s.stopErr != nil {
		return s.stopErr
	}
	if s.marker != "" {
		return os.WriteFile(filepath.Join(s.root, "stopped"), []byte(s.marker), 0o600)
	}
	return nil
}

func (s *fixtureServer) SourceRoot() (string, error) {
	if s.stops != 1 || s.stopErr != nil {
		return "", errors.New("source was not quiesced exactly once")
	}
	return s.root, nil
}

func destination(t *testing.T) string {
	t.Helper()
	if err := os.MkdirAll("/tmp/plug-swarm", 0o700); err != nil {
		t.Fatal(err)
	}
	parent, err := os.MkdirTemp("/tmp/plug-swarm", "public-recipe-hooks-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(parent) })
	return filepath.Join(parent, "retained")
}

func TestRetainedStoreHookDefaultOff(t *testing.T) {
	flags := RegisterFlags(flag.NewFlagSet("public-recipe", flag.ContinueOnError))
	if flags.Options().RetainStore != "" {
		t.Fatal("handoff is enabled by default")
	}
	source := t.TempDir()
	archive := filepath.Join(source, "output.tar")
	want := []byte{0, 1, 2, 255, '\n'}
	if err := os.WriteFile(archive, want, 0o600); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(archive)
	if err != nil {
		t.Fatal(err)
	}
	hooks, err := Open(flags.Options())
	if err != nil {
		t.Fatal(err)
	}
	server := &fixtureServer{root: source}
	retained, err := hooks.Handoff(context.Background(), server)
	if err != nil || retained || server.stops != 0 {
		t.Fatalf("disabled hook touched lifecycle: retained=%t stops=%d error=%v", retained, server.stops, err)
	}
	got, err := os.ReadFile(archive)
	after, statErr := os.Stat(archive)
	if err != nil || statErr != nil || !bytes.Equal(got, want) || !before.ModTime().Equal(after.ModTime()) || !os.SameFile(before, after) {
		t.Fatal("disabled hook changed output bytes or file identity")
	}
	if entries, err := os.ReadDir(source); err != nil || len(entries) != 1 {
		t.Fatal("disabled hook changed the source tree")
	}
}

func TestRetainedStoreHookCopiesQuiescedRoot(t *testing.T) {
	source := t.TempDir()
	if err := os.Mkdir(filepath.Join(source, "database"), 0o700); err != nil {
		t.Fatal(err)
	}
	want := []byte("public synthetic source\x00all bytes retained\xff")
	if err := os.WriteFile(filepath.Join(source, "database", "rows"), want, 0o600); err != nil {
		t.Fatal(err)
	}
	path := destination(t)
	set := flag.NewFlagSet("public-recipe", flag.ContinueOnError)
	flags := RegisterFlags(set)
	if err := set.Parse([]string{"-plug-recipe-retain-store", path}); err != nil {
		t.Fatal(err)
	}
	hooks, err := Open(flags.Options())
	if err != nil {
		t.Fatal(err)
	}
	server := &fixtureServer{root: source, marker: "written only when stopped"}
	retained, err := hooks.Handoff(context.Background(), server)
	if err != nil || !retained || server.stops != 1 {
		t.Fatalf("handoff retained=%t stops=%d error=%v", retained, server.stops, err)
	}
	got, err := os.ReadFile(filepath.Join(path, "database", "rows"))
	if err != nil || !bytes.Equal(got, want) {
		t.Fatal("handoff lost source bytes")
	}
	marker, err := os.ReadFile(filepath.Join(path, "stopped"))
	if err != nil || string(marker) != server.marker {
		t.Fatal("copy did not occur after quiescence")
	}
	if err := os.RemoveAll(source); err != nil {
		t.Fatal(err)
	}
	if got, err := os.ReadFile(filepath.Join(path, "database", "rows")); err != nil || !bytes.Equal(got, want) {
		t.Fatal("normal source cleanup destroyed the retained root")
	}
}

func TestRetainedStoreHookRejectsDestinationsBeforeStop(t *testing.T) {
	path := destination(t)
	parent := filepath.Dir(path)
	if err := os.Mkdir(filepath.Join(parent, "existing"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(parent, filepath.Join(parent, "alias")); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{
		"relative", "/tmp/plug-swarm", "/tmp/plug-swarm-other/new",
		parent + "/../escape", filepath.Join(parent, "existing"), filepath.Join(parent, "alias", "new"),
	} {
		t.Run(bad, func(t *testing.T) {
			if _, err := Open(Options{RetainStore: bad}); err == nil {
				t.Fatalf("accepted invalid destination %s", bad)
			}
		})
	}
	hooks, err := Open(Options{RetainStore: path})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("caller-owned existing bytes"), 0o600); err != nil {
		t.Fatal(err)
	}
	server := &fixtureServer{root: t.TempDir()}
	if _, err := hooks.Handoff(context.Background(), server); err == nil || server.stops != 0 {
		t.Fatal("late destination collision was not refused before stopping")
	}
	if got, _ := os.ReadFile(path); string(got) != "caller-owned existing bytes" {
		t.Fatal("destination collision changed caller data")
	}
}

func TestRetainedStoreHookFailureCleanup(t *testing.T) {
	t.Run("stop refusal", func(t *testing.T) {
		path := destination(t)
		hooks, err := Open(Options{RetainStore: path})
		if err != nil {
			t.Fatal(err)
		}
		server := &fixtureServer{root: t.TempDir(), stopErr: errors.New("stop refused")}
		if _, err := hooks.Handoff(context.Background(), server); err == nil {
			t.Fatal("failed quiescence produced a handoff")
		}
		if _, err := os.Lstat(path); !os.IsNotExist(err) {
			t.Fatal("failed quiescence left a destination")
		}
	})
	t.Run("copy refusal", func(t *testing.T) {
		path := destination(t)
		source, outside := t.TempDir(), t.TempDir()
		if err := os.WriteFile(filepath.Join(outside, "private"), []byte("must not copy"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(source, "a"), []byte("partial copy"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(outside, filepath.Join(source, "z")); err != nil {
			t.Fatal(err)
		}
		hooks, err := Open(Options{RetainStore: path})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := hooks.Handoff(context.Background(), &fixtureServer{root: source}); err == nil {
			t.Fatal("copy followed a source symlink")
		}
		if _, err := os.Lstat(path); !os.IsNotExist(err) {
			t.Fatal("failed copy left a partial destination")
		}
		if got, _ := os.ReadFile(filepath.Join(outside, "private")); string(got) != "must not copy" {
			t.Fatal("copy refusal touched an outside source")
		}
	})
}
