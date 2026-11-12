package driver

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"hash"
)

func nativeSourceOrderHasher() hash.Hash {
	h := sha256.New()
	h.Write([]byte("plug-native-source-order/1\n"))
	return h
}
func addNativeSourceOrder(h hash.Hash, store, project string) {
	raw, _ := json.Marshal(struct {
		Store   string `json:"store"`
		Project string `json:"project"`
	}{store, project})
	streamHashJSON(h, raw)
}

// NativeSourceOrderSHA256 commits only the explicit source block order. Counts,
// local sequences, boundaries and physical commits remain separately checked
// by the existing streaming validator and verifier-issued checkpoint.
func NativeSourceOrderSHA256(sources []NativeHistoryBoundary) string {
	h := nativeSourceOrderHasher()
	for _, source := range sources {
		addNativeSourceOrder(h, source.SourceStoreID, source.Project)
	}
	return hex.EncodeToString(h.Sum(nil))
}
