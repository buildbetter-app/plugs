package rehearsal

import (
	"archive/tar"
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestValidateSavedImageAcceptsPinnedOCIManifestDigest(t *testing.T) {
	fixture := writeOCIValidationArchive(t)
	bundle := fixture.bundle
	bundle.ImageDigest = fixture.manifestDigest
	if err := validateSavedImage(bundle); err != nil {
		t.Fatalf("valid selected manifest pin refused: %v", err)
	}
}

func TestValidateSavedImageRefusesPinnedBlobWithDifferentManifestConfig(t *testing.T) {
	fixture := writeOCIValidationArchive(t)
	bundle := fixture.bundle
	bundle.ImageDigest = fixture.alternateManifestDigest
	if err := validateSavedImage(bundle); err == nil {
		t.Fatal("archive accepted a pinned manifest blob with a different selected config")
	}
}

func TestValidateSavedImageRefusesPinNamingNonConfigBlob(t *testing.T) {
	fixture := writeOCIValidationArchive(t)
	bundle := fixture.bundle
	bundle.ImageDigest = fixture.layerDigest
	if err := validateSavedImage(bundle); err == nil {
		t.Fatal("archive accepted a layer digest as the image manifest pin")
	}
}

type ociValidationFixture struct {
	bundle                  RuntimeBundle
	manifestDigest          string
	alternateManifestDigest string
	layerDigest             string
}

func writeOCIValidationArchive(t *testing.T) ociValidationFixture {
	t.Helper()
	bundle := RuntimeBundle{
		Role:             "old",
		SourceSHA:        strings.Repeat("a", 40),
		SourceTree:       strings.Repeat("b", 40),
		ExecutableDigest: strings.Repeat("c", 64),
		ImagePath:        filepath.Join(t.TempDir(), "image.tar"),
	}
	layer := []byte("synthetic OCI layer")
	layerDigest := digestBytes(layer)
	configBody := map[string]any{
		"config": map[string]any{"Labels": map[string]string{
			"plug.source.sha": bundle.SourceSHA, "plug.source.tree": bundle.SourceTree,
			"plug.edition": bundle.Role, "plug.executable.sha256": bundle.ExecutableDigest,
		}},
		"rootfs": map[string]any{"type": "layers", "diff_ids": []string{"sha256:" + layerDigest}},
	}
	configRaw := marshalOCIJSON(t, configBody)
	configDigest := digestBytes(configRaw)
	alternateBody := map[string]any{"alternate": true, "config": configBody["config"], "rootfs": configBody["rootfs"]}
	alternateRaw := marshalOCIJSON(t, alternateBody)
	alternateConfigDigest := digestBytes(alternateRaw)
	manifestBody := map[string]any{
		"schemaVersion": 2,
		"config":        map[string]string{"mediaType": "application/vnd.oci.image.config.v1+json", "digest": "sha256:" + configDigest},
		"layers":        []map[string]string{{"mediaType": "application/vnd.oci.image.layer.v1.tar", "digest": "sha256:" + layerDigest}},
	}
	manifestRaw := marshalOCIJSON(t, manifestBody)
	manifestDigest := digestBytes(manifestRaw)
	alternateManifestBody := map[string]any{
		"schemaVersion": 2,
		"config":        map[string]string{"mediaType": "application/vnd.oci.image.config.v1+json", "digest": "sha256:" + alternateConfigDigest},
		"layers":        []map[string]string{{"mediaType": "application/vnd.oci.image.layer.v1.tar", "digest": "sha256:" + layerDigest}},
	}
	alternateManifestRaw := marshalOCIJSON(t, alternateManifestBody)
	alternateManifestDigest := digestBytes(alternateManifestRaw)
	indexRaw := marshalOCIJSON(t, map[string]any{
		"schemaVersion": 2,
		"manifests": []map[string]any{
			{"mediaType": "application/vnd.oci.image.manifest.v1+json", "digest": "sha256:" + manifestDigest, "platform": map[string]string{"os": "linux", "architecture": "amd64"}},
			{"mediaType": "application/vnd.oci.image.manifest.v1+json", "digest": "sha256:" + alternateManifestDigest, "platform": map[string]string{"os": "linux", "architecture": "arm64"}},
		},
	})
	archive := new(bytes.Buffer)
	writer := tar.NewWriter(archive)
	writeOCIEntry(t, writer, "index.json", indexRaw)
	writeOCIEntry(t, writer, "blobs/sha256/"+manifestDigest, manifestRaw)
	writeOCIEntry(t, writer, "blobs/sha256/"+alternateManifestDigest, alternateManifestRaw)
	writeOCIEntry(t, writer, "blobs/sha256/"+configDigest, configRaw)
	writeOCIEntry(t, writer, "blobs/sha256/"+alternateConfigDigest, alternateRaw)
	writeOCIEntry(t, writer, "blobs/sha256/"+layerDigest, layer)
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(bundle.ImagePath, archive.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
	return ociValidationFixture{bundle: bundle, manifestDigest: manifestDigest, alternateManifestDigest: alternateManifestDigest, layerDigest: layerDigest}
}

func marshalOCIJSON(t *testing.T, value any) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func writeOCIEntry(t *testing.T, writer *tar.Writer, name string, content []byte) {
	t.Helper()
	if err := writer.WriteHeader(&tar.Header{Name: name, Mode: 0600, Size: int64(len(content))}); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write(content); err != nil {
		t.Fatal(err)
	}
}
