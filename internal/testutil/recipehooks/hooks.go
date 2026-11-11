// Package recipehooks contains the opt-in retained-store input for the two
// public source recipes. It is used by tests, never by a Beads command.
package recipehooks

import (
	"context"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// Options is a test input. Empty options leave the recipe and cleanup alone.
type Options struct{ RetainStore string }

type Flags struct{ retain *string }

// RegisterFlags is called only by the recipe's _test.go entrypoint.
func RegisterFlags(set *flag.FlagSet) Flags {
	return Flags{set.String("plug-recipe-retain-store", "", "fresh export-point source root under /tmp/plug-swarm (caller owns cleanup)")}
}

func (f Flags) Options() Options { return Options{RetainStore: *f.retain} }

type Hooks struct{ options Options }

// Open never inspects paths or changes state when the input is off.
func Open(options Options) (*Hooks, error) {
	if options.RetainStore != "" {
		if err := validateDestination(options.RetainStore); err != nil {
			return nil, err
		}
	}
	return &Hooks{options: options}, nil
}

// QuiescentSource is implemented by the existing isolated test-server handle.
// SourceRoot must refuse a running server; callers close their SQL clients first.
type QuiescentSource interface {
	Stop(context.Context) error
	SourceRoot() (string, error)
}

// Handoff stops only the recipe-owned server and copies its complete data root.
// It leaves the server stopped; a recipe with later refusal controls can resume
// through its existing Start interface. Ordinary test cleanup remains registered.
func (h *Hooks) Handoff(ctx context.Context, server QuiescentSource) (bool, error) {
	if h.options.RetainStore == "" {
		return false, nil
	}
	destination := h.options.RetainStore
	if err := validateDestination(destination); err != nil {
		return false, err
	}
	if server == nil {
		return false, fmt.Errorf("recipe handoff needs its isolated server")
	}
	if err := server.Stop(ctx); err != nil {
		return false, fmt.Errorf("quiesce recipe source: %w", err)
	}
	source, err := server.SourceRoot()
	if err != nil {
		return false, err
	}
	rel, err := filepath.Rel(source, destination)
	if err != nil || rel == "." || (rel != ".." && !strings.HasPrefix(rel, ".."+string(os.PathSeparator))) {
		return false, fmt.Errorf("recipe handoff destination overlaps its source")
	}
	if err := os.Mkdir(destination, 0o700); err != nil {
		return false, err
	}
	if err := copyRoot(source, destination); err != nil {
		_ = os.RemoveAll(destination) // Only the new directory created above.
		return false, err
	}
	return true, nil
}

func validateDestination(path string) error {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || !strings.HasPrefix(path, "/tmp/plug-swarm/") {
		return fmt.Errorf("recipe handoff requires a fresh canonical path under /tmp/plug-swarm")
	}
	parent := filepath.Dir(path)
	resolved, err := filepath.EvalSymlinks(parent)
	if err != nil || resolved != parent {
		return fmt.Errorf("recipe handoff parent must exist without symlink aliases")
	}
	info, err := os.Stat(parent)
	if err != nil || !info.IsDir() || info.Mode().Perm()&0o077 != 0 {
		return fmt.Errorf("recipe handoff parent must be a private directory")
	}
	if _, err := os.Lstat(path); !os.IsNotExist(err) {
		return fmt.Errorf("recipe handoff destination must not already exist")
	}
	return nil
}

func copyRoot(source, destination string) error {
	root, err := os.OpenRoot(source)
	if err != nil {
		return err
	}
	defer func() { _ = root.Close() }()
	return fs.WalkDir(root.FS(), ".", func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil || path == "." {
			return walkErr
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		target := filepath.Join(destination, path)
		if entry.IsDir() {
			return os.Mkdir(target, info.Mode().Perm())
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("recipe handoff refuses nonregular source entry %s", path)
		}
		input, err := root.Open(path)
		if err != nil {
			return err
		}
		defer input.Close()
		output, err := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_WRONLY, info.Mode().Perm()) //nolint:gosec // G304: WalkDir-relative name inside the fresh validated private destination; exclusive creation never replaces a file.
		if err != nil {
			return err
		}
		_, copyErr := io.Copy(output, input)
		closeErr := output.Close()
		if copyErr != nil {
			return copyErr
		}
		return closeErr
	})
}
