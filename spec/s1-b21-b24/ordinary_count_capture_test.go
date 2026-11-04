package s1b21b24

import "testing"

func TestCapturePublicOrdinaryCountReference(t *testing.T) {
	runOrdinaryCountScript(t, "ordinary_count_capture.py", true)
}
