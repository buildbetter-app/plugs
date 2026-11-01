package webui

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"path"
	"sort"
	"strings"
)

// FrameBundleDigest returns the canonical digest to pin when packaging a
// prebuilt bundle. Admission compares the supplied pin with a detached copy of
// the complete asset inventory; it never builds JavaScript or refreshes a pin.
func FrameBundleDigest(assets map[string][]byte) (string, error) {
	if _, ok := assets["index.html"]; !ok {
		return "", fmt.Errorf("webui: frame bundle requires index.html")
	}
	type file struct {
		Path   string `json:"path"`
		Size   int    `json:"size"`
		SHA256 string `json:"sha256"`
	}
	files := make([]file, 0, len(assets))
	total := 0
	for name, data := range assets {
		if !frameAssetPath(name) {
			return "", fmt.Errorf("webui: invalid frame asset path %q", name)
		}
		if len(data) > maxFrameAssetBytes || total > maxFrameBundleBytes-len(data) {
			return "", fmt.Errorf("webui: frame bundle exceeds its asset size limit")
		}
		total += len(data)
		digest := sha256.Sum256(data)
		files = append(files, file{Path: name, Size: len(data), SHA256: hex.EncodeToString(digest[:])})
	}
	sort.Slice(files, func(i, j int) bool { return files[i].Path < files[j].Path })
	canonical, err := json.Marshal(struct {
		Format string `json:"format"`
		Files  []file `json:"files"`
	}{"plug-ui-bundle/1", files})
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(canonical)
	return hex.EncodeToString(digest[:]), nil
}

func frameAssetPath(name string) bool {
	return name != "" && name != "." && !strings.HasPrefix(name, "/") &&
		path.Clean(name) == name && !strings.HasPrefix(name, "../") &&
		plainPath("/"+name)
}
