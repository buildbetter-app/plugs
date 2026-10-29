package main

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"

	"github.com/spf13/cobra"
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
	accessor, ok := storage.UnwrapStore(store).(storage.RawDBAccessor)
	if !ok || accessor.DB() == nil {
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
	tx, err := accessor.DB().BeginTx(ctx, &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelRepeatableRead})
	if err != nil {
		return HandleErrorRespectJSON("cannot begin read-only frozen-source transaction: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := exportPlugFromSource(ctx, cmd, plugdump.SQLSource{DB: tx}, inventory, exportOutput); err != nil {
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
