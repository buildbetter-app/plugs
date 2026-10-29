package pluginmanifest

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

func localGit(ctx context.Context, root string, args ...string) ([]byte, error) {
	argv := []string{"--no-optional-locks", "--no-replace-objects", "-c", "core.fsmonitor=false", "-c", "core.hooksPath=" + os.DevNull, "-C", root}
	command := exec.CommandContext(ctx, "git", append(argv, args...)...)
	for _, value := range os.Environ() {
		if !strings.HasPrefix(value, "GIT_") && !strings.HasPrefix(value, "LC_ALL=") {
			command.Env = append(command.Env, value)
		}
	}
	command.Env = append(command.Env, "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL="+os.DevNull, "GIT_TERMINAL_PROMPT=0", "LC_ALL=C")
	out, err := command.Output()
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		// Git diagnostics can contain repository configuration values. Keep
		// those out of local plan results; no source contents are necessary.
		return nil, refuse("checkout_invalid", "local git %s failed", args[0])
	}
	return out, nil
}

func checkHead(ctx context.Context, source Checkout) error {
	top, err := localGit(ctx, source.Root, "rev-parse", "--show-toplevel")
	if err != nil {
		return err
	}
	if filepath.Clean(strings.TrimSpace(string(top))) != source.Root {
		return refuse("checkout_invalid", "source must be the checkout root")
	}
	head, err := localGit(ctx, source.Root, "rev-parse", "--verify", "HEAD^{commit}")
	if err != nil {
		return err
	}
	if strings.TrimSpace(string(head)) != source.Commit {
		return refuse("pin_mismatch", "checkout HEAD differs from the pinned commit")
	}
	return nil
}

func checkTree(ctx context.Context, source Checkout, files []File, objects map[string]string) error {
	out, err := localGit(ctx, source.Root, "ls-tree", "-r", "-z", "--full-tree", source.Commit, "--")
	if err != nil {
		return err
	}
	actual := map[string]File{}
	for _, file := range files {
		actual[file.Path] = file
	}
	for _, line := range strings.Split(string(out), "\x00") {
		if line == "" {
			continue
		}
		meta, name, ok := strings.Cut(line, "\t")
		parts := strings.Split(meta, " ")
		if !ok || len(parts) != 3 || !sourcePath(name) || parts[1] != "blob" || (parts[0] != "100644" && parts[0] != "100755") {
			return refuse("containment", "commit contains a symlink, submodule or unsupported source entry")
		}
		file, exists := actual[name]
		if !exists || objects[name] != parts[2] || file.Executable != (parts[0] == "100755") {
			return refuse("source_changed", "source %q differs from the pinned commit", name)
		}
		delete(actual, name)
	}
	if len(actual) != 0 {
		return refuse("source_changed", "checkout contains source outside the pinned commit")
	}
	return nil
}
