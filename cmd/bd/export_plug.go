package main

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	beads "github.com/steveyegge/beads"
	"github.com/steveyegge/beads/internal/formula"
	beadsgit "github.com/steveyegge/beads/internal/git"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/steveyegge/beads/internal/atomicfile"
	"github.com/steveyegge/beads/internal/plugdump"
	"github.com/steveyegge/beads/internal/storage"
)

func runPlugExport(cmd *cobra.Command) error {
	if !readonlyMode {
		return HandleErrorRespectJSON("plug-dump/1 requires --readonly and a disposable offline capture")
	}
	if exportPlugInventory == "" {
		return HandleErrorRespectJSON("plug-dump/1 requires --plug-inventory; ordinary JSONL is not a source census")
	}
	if exportAll || exportIncludeInfra || exportScrub || exportNoMemories || exportIncludeMemories || len(exportExcludeOwners) != 0 || exportVerbose {
		return HandleErrorRespectJSON("plug-dump/1 refuses JSONL selection flags; it must account for every source family")
	}
	if jsonOutput {
		return HandleErrorRespectJSON("plug-dump/1 is a binary archive; --json is unsupported")
	}
	if usesProxiedServer() || storeIsRemoteBackend(store) || store == nil {
		return HandleErrorRespectJSON("plug-dump/1 requires an offline capture with the existing raw database read capability; proxied/remote stores are unsupported")
	}
	accessor, ok := storage.UnwrapStore(store).(storage.RawDBTxAccessor)
	if !ok {
		return HandleErrorRespectJSON("store cannot expose the complete paired census read set")
	}
	file, err := os.Open(exportPlugInventory)
	if err != nil {
		return HandleErrorRespectJSON("cannot read plug inventory")
	}
	defer file.Close()
	inventory, err := plugdump.ReadInventory(file)
	if err != nil {
		return HandleErrorRespectJSON("invalid plug inventory: %v", err)
	}
	ctx := rootCtx
	if ctx == nil {
		ctx = context.Background()
	}
	var data []byte
	err = accessor.WithRawDBTx(ctx, &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelSerializable}, func(tx *sql.Tx) error {
		var buildErr error
		data, buildErr = plugdump.Build(ctx, plugdump.SQLSource{DB: tx}, inventory)
		return buildErr
	})
	if err != nil {
		return HandleErrorRespectJSON("plug-dump/1 refused: %v", err)
	}
	if err := writePlugExport(cmd, data, exportOutput); err != nil {
		return HandleErrorRespectJSON("plug-dump/1 refused: %v", err)
	}
	return nil
}

// Shared command boundary used by disposable source fixtures. Validation is
// complete before any stdout byte or destination file can be changed.
func exportPlugFromSource(ctx context.Context, cmd *cobra.Command, source plugdump.Source, inventory plugdump.Inventory, output string) error {
	data, err := plugdump.Build(ctx, source, inventory)
	if err != nil {
		return err
	}
	return writePlugExport(cmd, data, output)
}

// Publish only after the source transaction and its connection are released.
func writePlugExport(cmd *cobra.Command, data []byte, output string) error {
	if output != "" {
		return atomicfile.WriteFile(output, data, 0o600)
	}
	var writer io.Writer = cmd.OutOrStdout()
	n, err := writer.Write(data)
	if err != nil {
		return fmt.Errorf("write archive: %w", err)
	}
	if n != len(data) {
		return io.ErrShortWrite
	}
	return nil
}

// The native producer uses the existing driver read scope. All filesystem
// source resolution happens through the same workspace/configuration helpers
// as ordinary commands, and engine directories are only located, never read.
func runNativePlugInventory(cmd *cobra.Command) error {
	if !readonlyMode || !exportPlugCapturePublic {
		return HandleErrorRespectJSON("beads-inventory/1 requires --readonly and explicit --plug-capture-public")
	}
	if exportOutput == "" || exportPlugInventory != "" || jsonOutput || exportAll || exportIncludeInfra || exportScrub || exportNoMemories || exportIncludeMemories || len(exportExcludeOwners) != 0 || exportVerbose {
		return HandleErrorRespectJSON("native inventory requires only --output and public capture flags")
	}
	if store == nil || usesProxiedServer() || storeIsRemoteBackend(store) {
		return HandleErrorRespectJSON("native inventory requires a local supported store")
	}
	source := storage.UnwrapStore(store)
	accessor, ok := source.(storage.RawDBTxAccessor)
	if !ok {
		return HandleErrorRespectJSON("store cannot expose a scoped native inventory")
	}
	locator, ok := source.(storage.StoreLocator)
	if !ok {
		return HandleErrorRespectJSON("store cannot locate its native source")
	}
	output, err := filepath.Abs(exportOutput)
	if err != nil {
		return err
	}
	parent, err := filepath.EvalSymlinks(filepath.Dir(output))
	if err != nil {
		return HandleErrorRespectJSON("native inventory output parent cannot be resolved")
	}
	output = filepath.Join(parent, filepath.Base(output))
	info, err := os.Lstat(parent)
	if err != nil || !info.IsDir() || info.Mode().Perm()&0077 != 0 {
		return HandleErrorRespectJSON("native inventory output parent must be an existing private directory (0700)")
	}
	for _, path := range []string{output, output + ".sources"} {
		if _, err := os.Lstat(path); !os.IsNotExist(err) {
			return HandleErrorRespectJSON("native inventory refuses existing output or sidecars")
		}
	}
	cwd, err := os.Getwd()
	if err != nil {
		return err
	}
	home, err := os.UserHomeDir()
	if err != nil || !filepath.IsAbs(home) {
		return HandleErrorRespectJSON("native inventory requires an absolute user home")
	}
	userConfig, err := os.UserConfigDir()
	if err != nil || !filepath.IsAbs(userConfig) {
		return HandleErrorRespectJSON("native inventory requires an absolute native config root")
	}
	active := beads.FindBeadsDir()
	if active == "" {
		return HandleErrorRespectJSON("native inventory cannot resolve active beads directory")
	}
	active, err = filepath.Abs(active)
	if err != nil {
		return err
	}
	project := beadsgit.GetRepoRoot()
	if project == "" {
		project = cwd
	}
	hooks, err := beadsgit.GetGitHooksDir()
	if err != nil {
		return HandleErrorRespectJSON("native inventory cannot resolve git hooks: %v", err)
	}
	machine, err := os.Hostname()
	if err != nil {
		return err
	}
	paths := map[string][]string{
		"user-legacy-config":     {filepath.Join(home, ".beads", "config.yaml")},
		"user-xdg-config":        {filepath.Join(home, ".config", "bd", "config.yaml")},
		"user-native-config":     {filepath.Join(userConfig, "bd", "config.yaml")},
		"project-config":         {filepath.Join(project, ".beads", "config.yaml")},
		"beads-dir-config":       {filepath.Join(active, "config.yaml")},
		"project-local-config":   {filepath.Join(active, "config.local.yaml"), filepath.Join(project, ".beads", "config.local.yaml")},
		"metadata-json":          {filepath.Join(active, "metadata.json")},
		"legacy-config-json":     {filepath.Join(active, "config.json"), filepath.Join(home, ".beads", "config.json")},
		"beads-hooks":            {filepath.Join(active, "hooks")},
		"legacy-hooks":           {filepath.Join(active, "hooks.json"), filepath.Join(home, ".beads", "hooks")},
		"git-hooks":              {hooks},
		"external-hook-managers": {filepath.Join(project, ".husky"), filepath.Join(project, "lefthook.yml"), filepath.Join(project, "lefthook.yaml"), filepath.Join(project, ".pre-commit-config.yaml")},
		"routes-jsonl":           {filepath.Join(active, "routes.jsonl")},
		"formulas":               formula.DefaultSearchPaths(),
		"formula-search-paths":   formula.DefaultSearchPaths(),
		"issues-jsonl":           {filepath.Join(active, "issues.jsonl")},
		"interactions-jsonl":     {filepath.Join(active, "interactions.jsonl")},
	}
	environment := map[string]any{}
	for _, entry := range os.Environ() {
		key, value, ok := strings.Cut(entry, "=")
		if ok && (strings.HasPrefix(key, "BD_") || strings.HasPrefix(key, "BEADS_") || strings.HasPrefix(key, "DOLT_") || key == "GT_ROOT" || key == "HOME" || strings.HasPrefix(key, "XDG_")) {
			environment[key] = value
		}
	}
	flags := map[string]any{}
	cmd.Flags().Visit(func(flag *pflag.Flag) { flags[flag.Name] = flag.Value.String() })
	capture := plugdump.NativeCapture{Public: true, StoreDir: locator.CLIDir(), BeadsDir: active, Machine: machine, SidecarDir: output + ".sources", Paths: paths, Environment: environment, Flags: flags}
	capture.SourcePlaneAddressContract = plugdump.SourcePlaneAddressContract
	ctx := rootCtx
	if ctx == nil {
		ctx = context.Background()
	}
	var inventory plugdump.Inventory
	var sidecars map[string][]byte
	err = accessor.WithRawDBTx(ctx, &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelSerializable}, func(tx *sql.Tx) error {
		var readErr error
		inventory, sidecars, readErr = (plugdump.SQLSource{DB: tx}).NativeInventory(ctx, capture)
		return readErr
	})
	if err != nil {
		return HandleErrorRespectJSON("native inventory refused: %v", err)
	}
	data, err := plugdump.MarshalNativeInventory(inventory)
	if err != nil {
		return err
	}
	staging, err := os.MkdirTemp(parent, ".native-inventory-")
	if err != nil {
		return err
	}
	defer func() { _ = os.RemoveAll(staging) }()
	for name, body := range sidecars {
		if err := os.WriteFile(filepath.Join(staging, name), body, 0600); err != nil {
			return err
		}
	}
	if err := os.Rename(staging, capture.SidecarDir); err != nil {
		return err
	}
	if err := atomicfile.WriteFile(output, data, 0600); err != nil {
		_ = os.RemoveAll(capture.SidecarDir)
		return err
	}
	return nil
}
