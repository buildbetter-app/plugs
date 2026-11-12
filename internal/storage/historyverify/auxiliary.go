package historyverify

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"

 "github.com/buildbetter-app/plugs/internal/core/dumpformat"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// Auxiliary describes the one current image's other engine writes. Computing a
// root does not issue a seal. Receiving must bind this exact logical projection
// to its independently rebuilt DerivedDigest, never reinterpret that digest as
// the record-root or projection-root preimage defined here.
type Auxiliary struct {
	Derived    map[string]driver.LockState
	Allocators map[string]driver.OfflineAllocator
	Parents    map[string]driver.ParentCounter
	Blobs      map[driver.Hash][]byte
	Bumps      map[string]driver.LockKey
}

func AuxiliaryRoot(a Auxiliary) (Root, error) {
 return auxiliaryRoot(a, nil)
}

// originals is private verifier output from successful reauthentication. It
// adds the exact raw capture blob membership without retaining its bodies.
func auxiliaryRoot(a Auxiliary, originals map[string]dumpformat.File) (Root, error) {
	// These keys are already retained by the ordinary transaction's one current
	// image, not by historical replay. No history bodies or mapping list enter it.
	type part struct {
		kind, key string
		data      []byte
	}
	parts := make([]part, 0, len(a.Derived)+len(a.Allocators)+len(a.Parents)+len(a.Blobs)+len(a.Bumps))
	add := func(kind, key string, v any) error {
		raw, err := json.Marshal(v)
		if err != nil {
			return err
		}
		sum := sha256.Sum256(raw)
		parts = append(parts, part{kind, key, sum[:]})
		return nil
	}
	for key, r := range a.Derived {
		var value []byte
		if r.Derived != nil {
			value = r.Derived.Value
		}
		if err := add("derived", key, struct {
			Key     driver.LockKey
			Present bool
			Value   []byte
		}{r.Key, r.Present, value}); err != nil {
			return Root{}, err
		}
	}
	for key, v := range a.Allocators {
		if err := add("allocator", key, v); err != nil {
			return Root{}, err
		}
	}
	for _, v := range a.Parents {
		if err := add("parent", v.Parent, struct {
			Parent string
			Last   uint64
		}{v.Parent, v.LastChild}); err != nil {
			return Root{}, err
		}
	}
 type blob struct { Bytes int64; SHA256 string }
 blobs := map[driver.Hash]blob{}
 for key, value := range a.Blobs {
  sum := sha256.Sum256(value)
  if string(key) != hex.EncodeToString(sum[:]) { return Root{}, ErrCheckpoint{"staged blob differs from content address"} }
  blobs[key] = blob{int64(len(value)), string(key)}
 }
 for _, file := range originals {
  key := driver.Hash(file.SHA256)
  value := blob{file.Size, file.SHA256}
  if old, exists := blobs[key]; exists && old != value { return Root{}, ErrCheckpoint{"original blob differs from receiving image"} }
  blobs[key] = value
 }
 for key, value := range blobs { if err := add("blob", string(key), value); err != nil { return Root{}, err } }

	for key, v := range a.Bumps {
		if err := add("bump", key, v); err != nil {
			return Root{}, err
		}
	}
	sort.Slice(parts, func(i, j int) bool {
		if parts[i].kind != parts[j].kind {
			return parts[i].kind < parts[j].kind
		}
		return parts[i].key < parts[j].key
	})
	h := sha256.New()
	_, _ = h.Write([]byte("plug-history-current-projection/1\n"))
	for _, p := range parts {
		raw, err := json.Marshal(struct{ Kind, Key, SHA256 string }{p.kind, p.key, hex.EncodeToString(p.data)})
		if err != nil {
			return Root{}, err
		}
		_, _ = h.Write(raw)
		_, _ = h.Write([]byte{'\n'})
	}
	return Root{uint64(len(parts)), hex.EncodeToString(h.Sum(nil))}, nil
}

func (p Checkpoint) CheckAuxiliary(actual Root) error {
	if p.verified == nil || p.verified.auxiliary != actual {
		return ErrCheckpoint{"staged current projection differs from verified derived image"}
	}
	return nil
}
