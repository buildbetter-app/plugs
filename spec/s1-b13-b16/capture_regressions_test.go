package s1b13b16

import (
	_ "embed"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

//go:embed reference_capture.py
var captureSource []byte

//go:embed capture_regressions_test.py
var captureTests []byte

func runCaptureRegression(t *testing.T, name string) {
	t.Helper()
	root := t.TempDir()
	source := filepath.Join(root, "spec", "s1-b13-b16", "reference_capture.py")
	harness := filepath.Join(root, "capture_regressions_test.py")
	for path, content := range map[string][]byte{source: captureSource, harness: captureTests} {
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, content, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	for _, name := range []string{"prepare-public-reference.py", "prepare-public-bundle.py"} {
		path := filepath.Join(os.Getenv("TEST_SRCDIR"), os.Getenv("TEST_WORKSPACE"), "scripts", "ci", name)
		content, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		destination := filepath.Join(root, "scripts", "ci", name)
		if err := os.MkdirAll(filepath.Dir(destination), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(destination, content, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	command := exec.Command("python3", "-B", harness, source, name)
	command.Env = []string{
		"PATH=" + os.Getenv("PATH"), "HOME=" + root,
		"PYTHONNOUSERSITE=1", "PYTHONDONTWRITEBYTECODE=1", "LC_ALL=C.UTF-8",
	}
	output, err := command.CombinedOutput()
	t.Logf("%s", output)
	if err != nil {
		t.Fatalf("synthetic capture regression failed: %v", err)
	}
}

func TestCaptureTransport(t *testing.T) {
	for _, name := range []string{
		"test_exact_api_uses_github_token",
		"test_exact_api_can_use_gh_token",
		"test_runtime_token_is_not_a_github_api_credential",
		"test_signed_object_has_no_ambient_credential",
		"test_public_web_route_has_no_ambient_credential",
		"test_api_redirect_to_object_strips_authorization",
		"test_public_redirect_stays_credential_free",
		"test_unsupported_routes_refuse_before_transport",
		"test_redirect_to_unknown_origin_refuses_before_second_request",
		"test_redirect_downgrade_refuses_before_second_request",
		"test_public_redirect_cannot_acquire_api_authority",
		"test_redirect_loop_is_bounded_and_never_carries_ambient_auth",
		"test_supplied_route_failure_does_not_fall_back_or_log_credentials",
	} {
		t.Run(name, func(t *testing.T) { runCaptureRegression(t, "CaptureTransportTests."+name) })
	}
}

func TestCaptureExtraction(t *testing.T) {
	for _, name := range []string{
		"test_zip_stripped_modes_reconstructed_from_verified_tar",
		"test_prefixed_zip_special_modes_are_not_restored",
		"test_archive_digest_mismatch_never_grants_execute",
		"test_loose_binary_hash_mismatch_never_grants_execute",
		"test_tar_hash_member_set_and_modes_refuse_before_execute",
		"test_zip_links_devices_duplicates_and_traversal_refuse",
		"test_source_pin_manifest_and_resolver_refusals_never_grant_execute",
	} {
		t.Run(name, func(t *testing.T) { runCaptureRegression(t, "CaptureExtractionTests."+name) })
	}
}

func TestPreparedCaptureConsumer(t *testing.T) {
	for _, name := range []string{
		"test_producer_route_reaches_consumer_with_verified_local_inputs",
		"test_original_mutation_after_validation_preserves_private_source",
		"test_original_replacement_after_validation_preserves_private_source",
		"test_pinned_source_links_resolve_only_inside_private_snapshot",
		"test_prepared_recorder_cannot_reopen_ambient_git_state",
		"test_inconsistent_snapshot_refuses_before_execute_or_capture",
		"test_snapshot_rejects_external_git_state_before_execute",
		"test_malformed_duplicate_missing_and_mixed_flags_refuse",
		"test_untrusted_local_paths_refuse_before_execute_or_capture",
		"test_altered_zip_refuses_without_network_or_execute",
		"test_altered_source_and_incomplete_identity_refuse_before_execute",
		"test_prepared_recorder_assertions_remain_fail_closed",
		"test_legacy_default_and_artifact_url_routes_are_unchanged",
	} {
		t.Run(name, func(t *testing.T) { runCaptureRegression(t, "PreparedConsumerTests."+name) })
	}
}
