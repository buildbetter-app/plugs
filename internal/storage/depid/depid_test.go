package depid

import "testing"

func TestNewPinnedVectors(t *testing.T) {
	for _, tc := range []struct {
		issue, target, want string
	}{
		{"issue-1", "issue-2", "91990ee3-275c-59c5-9291-30f8a8732228"},
		{"mybd-7x76", "mybd-kd8a", "c77e0366-7f45-5f8c-9f12-2c51e0a0c5fd"},
		{"a", "external:https://x/y", "0ac7aa98-536c-5bfa-9e11-df69cc67438a"},
	} {
		if got := New(tc.issue, tc.target); got != tc.want {
			t.Errorf("New(%q, %q) = %q, want %q", tc.issue, tc.target, got, tc.want)
		}
	}
}

func TestNewEndpointIdentityExcludesTypeButSeparatesPairs(t *testing.T) {
	if New("source", "target") != New("source", "target") {
		t.Fatal("same endpoint pair changed identity")
	}
	seen := map[string]struct{}{}
	for _, pair := range [][2]string{{"ab", "c"}, {"a", "bc"}, {"a", "b"}, {"b", "a"}} {
		id := New(pair[0], pair[1])
		if _, exists := seen[id]; exists {
			t.Fatalf("endpoint pair collision for %v: %s", pair, id)
		}
		seen[id] = struct{}{}
	}
}
