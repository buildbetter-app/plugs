package s1b21b24

import "testing"

func TestOrdinaryCountCapturePolicy(t *testing.T) {
	t.Log("synthetic archive, configuration and framing fixtures only; no reference runtime")
	runOrdinaryCountScript(t, "ordinary_count_capture_policy_test.py", false)
}
