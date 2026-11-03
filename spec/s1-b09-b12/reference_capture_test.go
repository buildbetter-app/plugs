package s1b09b12

import "testing"

func TestCapturePublicReference(t *testing.T) {
	runOwnedCaptureScript(t, "reference_capture.py", true)
}
