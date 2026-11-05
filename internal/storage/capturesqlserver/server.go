// Package capturesqlserver is the capture-only clock wrapper around the pinned
// upstream Dolt SQL server. It does not implement or rewrite SQL expressions.
package capturesqlserver

import (
	"context"
	"fmt"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/sqlserver"
	"github.com/dolthub/dolt/go/cmd/dolt/doltversion"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/libraries/utils/svcs"
	gmssql "github.com/dolthub/go-mysql-server/sql"
	"github.com/steveyegge/beads/internal/captureclock"
)

// VersionInfo describes the independently bound companion and its unchanged
// upstream engine inputs. The module versions are pinned in go.mod/go.sum.
type VersionInfo struct {
	Program       string `json:"program"`
	ClockContract string `json:"clock_contract"`
	DoltVersion   string `json:"dolt_version"`
	DoltModule    string `json:"dolt_module"`
	GMSModule     string `json:"go_mysql_server_module"`
}

func Version() VersionInfo {
	return VersionInfo{
		Program:       "plug-capture-sql-server",
		ClockContract: captureclock.Version,
		DoltVersion:   doltversion.Version,
		DoltModule:    "v0.40.5-0.20260806213044-796d07497741",
		GMSModule:     "v0.20.1-0.20260805191915-e5eafe0da809",
	}
}

// Run forwards the native SQL-server arguments unchanged. The existing engine
// hook supplies each actual query context, including later connections/steps.
// This companion is a separately identified artifact, never a relabeled release
// Dolt executable or a claim of controlled embedded-driver SQL time.
func Run(ctx context.Context, args []string) error {
	clock, err := captureclock.Read()
	if err != nil {
		return err
	}
	if clock == nil {
		return fmt.Errorf("capture SQL server requires an explicit public clock file")
	}
	restoreIO := cli.InitIO()
	defer restoreIO()
	// The upstream dolt entrypoint resolves --data-dir before loading dEnv.
	// StartServer uses dEnv.FS for database discovery and creation; forwarding
	// the flag alone leaves those operations rooted in the process directory.
	dataDir, err := sqlserver.GetDataDirPreStart(filesys.LocalFS, args)
	if err != nil {
		return fmt.Errorf("capture SQL server data directory: %w", err)
	}
	var dataFS filesys.Filesys = filesys.LocalFS
	if dataDir != "" {
		dataFS, err = filesys.LocalFS.WithWorkingDir(dataDir)
		if err != nil {
			return fmt.Errorf("capture SQL server data directory: %w", err)
		}
	}
	dEnv := env.LoadWithoutDB(ctx, env.GetCurrentUserHomeDir, dataFS, doltdb.LocalDirDoltDB, doltversion.Version)
	if dEnv.CfgLoadErr != nil {
		return fmt.Errorf("capture SQL server config: %w", dEnv.CfgLoadErr)
	}
	controller := svcs.NewController()
	finished := make(chan struct{})
	defer close(finished)
	go func() {
		select {
		case <-ctx.Done():
			controller.Stop()
		case <-finished:
		}
	}()
	return gmssql.RunWithNowFunc(captureclock.SQLNow, func() error {
		return sqlserver.StartServer(ctx, doltversion.Version, "dolt sql-server", args, dEnv, filesys.LocalFS, controller)
	})
}
