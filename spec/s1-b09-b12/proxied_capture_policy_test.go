package s1b09b12

import "testing"

func TestProxiedCapturePolicy(t *testing.T) {
	t.Log("synthetic archive/stream policy fixtures only; no reference runtime or network")
	runOwnedProxiedCapture(t, "proxied_reference_capture_policy_test.py", false)
}
