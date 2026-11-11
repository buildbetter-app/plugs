// The capture companion runs the existing pinned Dolt SQL server with its
// supported query-clock hook. Its bytes/version must be admitted separately
// alongside bd; it is not the ordinary pinned release Dolt executable.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/steveyegge/beads/internal/storage/capturesqlserver"
)

func main() {
	if len(os.Args) == 2 && os.Args[1] == "--capture-version" {
		if err := json.NewEncoder(os.Stdout).Encode(capturesqlserver.Version()); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	if len(os.Args) == 2 && (os.Args[1] == "--help" || os.Args[1] == "-h") {
		fmt.Println("plug-capture-sql-server: capture-only pinned Dolt SQL server")
		fmt.Println("Requires BEADS_CAPTURE_CLOCK_PUBLIC=1 and BEADS_CAPTURE_CLOCK_FILE=/private/clock.json.")
		fmt.Println("Clock contract: beads-capture-clock/1; publish steps by atomic rename while quiescent.")
		fmt.Println("Other arguments are forwarded to the unchanged upstream dolt sql-server parser.")
		fmt.Println("--capture-version prints the companion and pinned upstream version bindings.")
		return
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := capturesqlserver.Run(ctx, os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
