package s1b09b12

import "testing"

func TestCapturePublicProxiedReference(t *testing.T) {
	runOwnedProxiedCapture(t, "reference_proxied_capture.py", true)
}
