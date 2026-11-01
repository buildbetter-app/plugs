package census

import (
	"encoding/json"
	"flag"
	"io"
	"os"
)

// RunPaired is a read-only CLI. Archive measurement stays on the owner host;
// reconciliation accepts only sanitized counts/digests and closed block codes.
// It does not open, discover or migrate a database. BLOCK exits 3, malformed
// input exits 1, and an invalid invocation exits 2.
func RunPaired(args []string, stdout, stderr io.Writer) int {
	f := flag.NewFlagSet("paired_census", flag.ContinueOnError)
	f.SetOutput(io.Discard)
	mode := f.String("mode", "", "archive, reconcile or behaviors")
	archive := f.String("archive", "", "explicit source archive (owner host only)")
	inventory := f.String("inventory", "", "sanitized declared store inventory")
	observations := f.String("observations", "", "sanitized measurements and typed blocks")
	root := f.String("source-root", "", "explicit source checkout for behavior declarations")
	fail := func(code BlockCode, status int) int {
		_, _ = io.WriteString(stderr, CensusError{Code: code}.Error()+"\n")
		return status
	}
	if f.Parse(args) != nil || f.NArg() != 0 {
		return fail(MeasurementInvalid, 2)
	}
	var output any
	exitCode := 0
	switch *mode {
	case "archive":
		if *archive == "" || *inventory != "" || *observations != "" || *root != "" {
			return fail(MeasurementInvalid, 2)
		}
		input, e := os.Open(*archive)
		if e != nil {
			return fail(ArchiveInvalid, 1)
		}
		capture, e := MeasureArchive(input)
		closeErr := input.Close()
		if e != nil || closeErr != nil {
			return fail(ArchiveInvalid, 1)
		}
		output = capture.Summary()
	case "reconcile":
		if *inventory == "" || *observations == "" || *archive != "" || *root != "" {
			return fail(MeasurementInvalid, 2)
		}
		load := func(file string, target any) bool {
			input, e := os.Open(file)
			if e != nil {
				return false
			}
			e = DecodePairedJSON(input, target)
			closeErr := input.Close()
			return e == nil && closeErr == nil
		}
		var declared StoreInventory
		var input PairedInput
		if !load(*inventory, &declared) || !load(*observations, &input) {
			return fail(MeasurementInvalid, 1)
		}
		result, e := Reconcile(declared, input)
		if e != nil {
			return fail(MeasurementInvalid, 1)
		}
		output = result
		if result.Status == "BLOCK" {
			exitCode = 3
		}
	case "behaviors":
		if *root == "" || *archive != "" || *inventory != "" || *observations != "" {
			return fail(MeasurementInvalid, 2)
		}
		result, e := MeasureBehaviors(os.DirFS(*root))
		if e != nil {
			return fail(MeasurementInvalid, 1)
		}
		output = result
		exitCode = 3 // Declaration/reference inventory is not complete-card parity.
	default:
		return fail(MeasurementInvalid, 2)
	}
	if json.NewEncoder(stdout).Encode(output) != nil {
		return fail(MeasurementInvalid, 1)
	}
	return exitCode
}
