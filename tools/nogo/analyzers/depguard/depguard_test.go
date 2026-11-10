package depguard_test

import (
	"strings"
	"testing"

	"github.com/steveyegge/beads/tools/nogo/analyzers/depguard"
	"github.com/steveyegge/beads/tools/nogo/internal/analyzertest"
)

// The rules come from the repository's .golangci.yml.

func TestFilesGlobsMatchRepositoryRelativeNames(t *testing.T) {
	got := analyzertest.RunInRepo(t, depguard.Analyzer, "internal/workapi", map[string]string{
		"w.go": "package workapi\n\nimport _ \"github.com/spf13/cobra\"\n",
	})
	if len(got) != 1 || !strings.HasPrefix(got[0].Message, "import 'github.com/spf13/cobra' is not allowed from list 'workapi-frontend-boundary'") {
		t.Fatalf("diagnostics = %+v, want the workapi-frontend-boundary denial", got)
	}
}

// "$all" is "**/*.go": golangci-lint hands depguard absolute paths, so it
// covers files at the repository root too.
func TestAllCoversTheRepositoryRoot(t *testing.T) {
	got := analyzertest.RunInRepo(t, depguard.Analyzer, "", map[string]string{
		"root.go": "package beads\n\nimport _ \"github.com/dolthub/dolt/go/libraries/doltcore/doltdb\"\n",
	})
	if len(got) != 1 || !strings.Contains(got[0].Message, "'dolt-storage-boundary'") {
		t.Fatalf("diagnostics = %+v, want the dolt-storage-boundary denial", got)
	}
}

func TestNativeCaptureDriverEngineAllowance(t *testing.T) {
	got := analyzertest.RunInRepo(t, depguard.Analyzer, "third_party/dolt-driver", map[string]string{
		"capture.go": "package embedded\n\nimport _ \"github.com/dolthub/dolt/go/libraries/doltcore/doltdb\"\n",
	})
	analyzertest.Equal(t, got, nil)
}

func TestNativeCaptureDriverRefusesUnlistedEngineImports(t *testing.T) {
	for _, imp := range []string{
		"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/unlisted",
		"github.com/dolthub/dolt/go/libraries/doltcore/doltdblookalike",
		"github.com/dolthub/dolt/go/libraries/doltcore/unlisted",
	} {
		t.Run(imp, func(t *testing.T) {
			got := analyzertest.RunInRepo(t, depguard.Analyzer, "third_party/dolt-driver", map[string]string{
				"capture.go": "package embedded\n\nimport _ \"" + imp + "\"\n",
			})
			if len(got) != 1 || !strings.Contains(got[0].Message, "'native-capture-driver-engine-boundary'") {
				t.Fatalf("diagnostics = %+v, want the exact-import capture denial", got)
			}
		})
	}
}

func TestNativeCaptureAllowanceDoesNotReachOtherPackages(t *testing.T) {
	for _, dir := range []string{
		"backend",
		"internal/plugdump",
		"cmd/plug-capture-sql-server",
		"tools/native-physical-capture",
		"third_party/unrelated",
		"third_party/dolt-driver-sibling",
		"third_party/dolt-driver/nested",
		"internal/plugdump/third_party/dolt-driver",
		"prefix/third_party/dolt-driver",
		"internal/storage/new_adapter",
		"internal/storage/embeddeddolt/nested",
		"internal/doltserver/nested",
		"prefix/internal/storage/physicalcapture",
	} {
		t.Run(dir, func(t *testing.T) {
			for _, imp := range []string{
				"github.com/dolthub/dolt/go/libraries/doltcore/doltdb",
				"github.com/dolthub/driver/v2",
				"github.com/dolthub/go-mysql-server/sql",
			} {
				got := analyzertest.RunInRepo(t, depguard.Analyzer, dir, map[string]string{
					"capture.go": "package outsider\n\nimport _ \"" + imp + "\"\n",
				})
				if len(got) != 1 || !strings.Contains(got[0].Message, "'dolt-storage-boundary'") {
					t.Fatalf("import %s: diagnostics = %+v, want the non-producer engine denial", imp, got)
				}
			}
		})
	}
}

func TestPinnedLegacyAdapterImportsRemainAllowed(t *testing.T) {
	for dir, imports := range map[string][]string{
		"internal/doltserver": {
			"github.com/dolthub/dolt/go/libraries/doltcore/servercfg",
			"github.com/dolthub/dolt/go/libraries/utils/filesys",
		},
		"internal/storage/capturesqlserver": {
			"github.com/dolthub/dolt/go/cmd/dolt/cli",
			"github.com/dolthub/dolt/go/cmd/dolt/commands/sqlserver",
			"github.com/dolthub/dolt/go/cmd/dolt/doltversion",
			"github.com/dolthub/dolt/go/libraries/doltcore/doltdb",
			"github.com/dolthub/dolt/go/libraries/doltcore/env",
			"github.com/dolthub/dolt/go/libraries/utils/filesys",
			"github.com/dolthub/dolt/go/libraries/utils/svcs",
			"github.com/dolthub/go-mysql-server/sql",
		},
		"internal/storage/dbproxy/server": {
			"github.com/dolthub/dolt/go/libraries/doltcore/servercfg",
			"github.com/dolthub/dolt/go/libraries/utils/filesys",
		},
		"internal/storage/embeddeddolt": {
			"github.com/dolthub/driver/v2",
		},
		"internal/storage/issueops": {
			"github.com/dolthub/go-mysql-server/sql",
		},
		"internal/storage/physicalcapture": {
			"github.com/dolthub/driver/v2",
		},
		"internal/storage/sqlclass": {
			"github.com/dolthub/vitess/go/vt/sqlparser",
		},
	} {
		t.Run(dir, func(t *testing.T) {
			for _, imp := range imports {
				got := analyzertest.RunInRepo(t, depguard.Analyzer, dir, map[string]string{
					"adapter.go": "package adapter\n\nimport _ \"" + imp + "\"\n",
				})
				analyzertest.Equal(t, got, nil)
			}
		})
	}
}

func TestLegacyAdaptersRefuseNewImports(t *testing.T) {
	for _, dir := range []string{
		"internal/doltserver",
		"internal/storage/capturesqlserver",
		"internal/storage/dbproxy/server",
		"internal/storage/embeddeddolt",
		"internal/storage/issueops",
		"internal/storage/physicalcapture",
		"internal/storage/sqlclass",
	} {
		t.Run(dir, func(t *testing.T) {
			got := analyzertest.RunInRepo(t, depguard.Analyzer, dir, map[string]string{
				"new.go": "package adapter\n\nimport _ \"github.com/dolthub/dolt/go/unlisted\"\n",
			})
			if len(got) != 1 || !strings.Contains(got[0].Message, "'pinned-native-") {
				t.Fatalf("diagnostics = %+v, want an untupled adapter import denial", got)
			}
		})
	}
}

// beadserrors' strict rule allows only $gostd.
func TestGoStdAllowList(t *testing.T) {
	got := analyzertest.RunInRepo(t, depguard.Analyzer, "beadserrors", map[string]string{
		"e.go": "package beadserrors\n\nimport (\n\t_ \"errors\"\n\t_ \"net/http\"\n\t_ \"github.com/steveyegge/beads/internal/types\"\n)\n",
	})
	if len(got) != 1 || !strings.HasPrefix(got[0].Message, "import 'github.com/steveyegge/beads/internal/types' is not allowed from list 'beadserrors-leaf'") {
		t.Fatalf("diagnostics = %+v, want only the internal/types denial", got)
	}
}
