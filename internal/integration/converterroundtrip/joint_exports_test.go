package converterroundtrip

import (
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"net/http"
	"testing"
)

// Test exports let the external composition controls use the same strictly
// admitted archive and explicit plan without adding product fixture APIs.
func JointFixtureForTest(t *testing.T) []byte { return jointSourceFixture(t) }
func JointProfileForTest(t *testing.T, data []byte, store driver.Store, lease driver.ActiveLease, owner model.PrincipalRef) driver.RestoreImportProfile {
	return jointOwnedProfile(t, data, jointProfile(t, data, store, lease), owner)
}
func JointPostgresDSNForTest(t *testing.T) string { return identityPostgresDSN(t) }
func JointHTTPForTest(t *testing.T, handler http.Handler, method string, key int, params map[string]any) []byte {
	return sourceHTTPTokenCall(t, handler, "", method, key, params, "").Result
}
func JointAssertSourceCommentForTest(t *testing.T, result []byte, p driver.RestoreImportProfile, report Report) {
	assertSourceCommentAttribution(t, result, p, report)
}
