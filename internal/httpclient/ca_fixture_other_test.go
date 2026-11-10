//go:build !unix

package httpclient

import "testing"

func trustCAFixtureAncestors(t *testing.T, _ string) {
	t.Helper()
}
