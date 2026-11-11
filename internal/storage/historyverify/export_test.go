package historyverify

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// MintForTesting is compiled only into this package's test variant. It cannot
// be imported by production storage/receiving code. This intentionally permits
// synthetic reconciliation results for real-engine mechanics, never native or
// receiving verification credit. Real verifier issuance is layer 3.
func MintForTesting(request driver.OfflineHistoryStreamRequest, head, current []driver.Record, aux Auxiliary, physical bool) (Checkpoint, error) {
	root := func(records []driver.Record) (Root, error) {
		r := append([]driver.Record(nil), records...)
		key := func(v driver.Record) string {
			k, _ := driver.EncodeLockKey(driver.LockKey{Kind: driver.LockRecord, Ref: &v.Ref})
			return k
		}
		sort.Slice(r, func(i, j int) bool { return key(r[i]) < key(r[j]) })
		h := NewRootHasher()
		for _, v := range r {
			sum := sha256.Sum256(v.Body)
			if err := h.Add(v.Ref, v.Kind, uint64(len(v.Body)), hex.EncodeToString(sum[:])); err != nil {
				return Root{}, err
			}
		}
		return h.Root(), nil
	}
	h, err := root(head)
	if err != nil {
		return Checkpoint{}, err
	}
	c, err := root(current)
	if err != nil {
		return Checkpoint{}, err
	}
	a, err := AuxiliaryRoot(aux)
	if err != nil {
		return Checkpoint{}, err
	}
	return mint(request, h, c, a, physical)
}
