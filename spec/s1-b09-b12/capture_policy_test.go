package s1b09b12

import "testing"

func TestPublicReferenceCapturePolicy(t *testing.T) {
	t.Log("fixture-only capture/retention regressions; no source engine is executed")
	runOwnedCaptureScript(t, "reference_capture_policy_test.py", false)
}
