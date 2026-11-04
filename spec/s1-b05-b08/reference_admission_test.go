package s1b05b08

import (
	_ "embed"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

//go:embed prepared_reference_test.py
var preparedAdapter []byte

//go:embed reference_inputs.py
var referenceInputs []byte

//go:embed reference_admission_test.py
var referenceAdmissionTests []byte

//go:embed record_reference.py
var referenceRecorder []byte

//go:embed project_reference.py
var referenceProjector []byte

func TestReferenceAdmissionFixtures(t *testing.T) {
	root := t.TempDir()
	for name, source := range map[string][]byte{
		"prepared_reference_test.py": preparedAdapter, "reference_inputs.py": referenceInputs,
		"reference_admission_test.py": referenceAdmissionTests, "record_reference.py": referenceRecorder,
		"project_reference.py": referenceProjector,
	} {
		if err := os.WriteFile(filepath.Join(root, name), source, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	command := exec.Command("python3", "-B", filepath.Join(root, "reference_admission_test.py"))
	command.Env = []string{"PATH=" + os.Getenv("PATH"), "PYTHONNOUSERSITE=1", "PYTHONDONTWRITEBYTECODE=1", "LC_ALL=C.UTF-8"}
	output, err := command.CombinedOutput()
	t.Logf("admission fixtures only, no reference capture:\n%s", output)
	if err != nil {
		t.Fatalf("reference admission regression failed: %v", err)
	}
}
