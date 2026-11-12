package graphcertfixture

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
)

// This target stops at the unchanged capture-set reader. It opens no target
// store and runs no import, plan, RPC, Docker container or native executable.
// Independent sizes and read modes report every refusal the fail-fast reader
// permits us to observe without bypassing a check or repairing an input.
func TestNativeCaptureFixtureAdmission(t *testing.T) {
	for _, count := range []int{3, 5000} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			capture, _ := Capture(t, "../converterroundtrip/testdata/public-native-delta-small-0e4d5da4.tar.gz", count)
			root, err := os.ReadFile(filepath.Join(capture, "capture-set.json"))
			if err != nil {
				t.Fatal(err)
			}
			for _, prepare := range []bool{false, true} {
				t.Run(fmt.Sprintf("prepared=%t", prepare), func(t *testing.T) {
					budget := dumpformat.NewNativeDeltaBudget(0)
					ctx := dumpformat.WithNativeDeltaBudget(context.Background(), budget)
					started := time.Now()
					archive, readErr := dumpformat.ReadNativeCaptureSetDirectory(ctx, capture, dumpformat.NativeCaptureSetOptions{PrepareReceiving: prepare})
					elapsed := time.Since(started)
					refusals := []string{}
					actual := 0
					if readErr != nil {
						refusals = append(refusals, readErr.Error())
						t.Errorf("unchanged capture reader refused: %v", readErr)
					} else {
						actual = len(archive.Records("bead"))
						if actual != count || archive.SHA256() != Hash(root) {
							t.Errorf("reader count/root binding differs: count=%d root=%s", actual, archive.SHA256())
						}
						archive.ReleaseNativeDecoded()
					}
					usage := budget.Usage()
					if usage.CurrentAccountedDecodedBytes != 0 {
						t.Errorf("reader retained resources after release/refusal: %d", usage.CurrentAccountedDecodedBytes)
					}
					t.Logf("fixture_admission=%s", JSON(t, map[string]any{"current_items": count, "actual_items": actual, "root_sha256": Hash(root), "prepare_receiving": prepare, "reader_refusals": refusals, "elapsed_ms": float64(elapsed) / float64(time.Millisecond), "decoded_usage_after_release": usage}))
				})
			}
		})
	}
}
