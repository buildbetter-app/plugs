package s1b01b04

import (
	_ "embed"
	"fmt"
	"reflect"
	"testing"
)

//go:embed native_b02_inputs.json
var nativeB02ContractInputsBytes []byte

// validateB02InputContract is the pure source-contract check shared by the
// ordinary contract target and the separate physical recorder. It has no
// filesystem, process, server, database, or admission side effects.
func validateB02InputContract(inputs nativeInputs) error {
	want := nativeInputs{
		Format:         "plug.s1.b02.native-inputs.v1",
		Consumer:       "1a9bb22bc5eb688e40a929453e5623c118e4f18f",
		ConsumerTree:   "94a3f363770bbf04e9ca533d8c0a4b3bf4c84042",
		ConsumerParent: "d6e4fec401a3d3d0a350e59a1149677bc3b62f63",
		HelperSource:   "43c0f2410ccb24062b8bd1bd073aeb29ef21288f",
		HelperTree:     "05153e1c051b1887fa875b53739adc036ae8cf16",
		Source:         "c8eb3ba7d4177e111a6f5c8aebe06b5d0b1d5724",
		SourceTree:     "930405e863284a3955cbd35e73a6097b606b91a5",
		SourceManifest: "e01a584042b4a2c32ea3cbcbdd55069f7c23bc2b853f8734857c5082e828f15d",
		SourceEntries:  5141,
		BinarySHA256:   "b734da8ba8b6f50d7fd32239e405b8f8ed9d171b7be5fbbccb63a923d257a123",
		BinaryBytes:    161299592,
		BuildRun:       "41800330346",
		Profile:        "plug-public-native/1",
		ManifestSHA256: "ac028e4b1be9609415dde0d9f2a30e12f37f3be5f3f02e4a89b356749fc4e25a",
		WorkflowSHA:    "d758fdf42a426e9e2a4b707dbcebb22ec68b264e",
		Descriptor:     "41800330346:13307042737:ac028e4b1be9609415dde0d9f2a30e12f37f3be5f3f02e4a89b356749fc4e25a",
		ArtifactDigest: "sha256:28609d7c072c281a748cb53a361d9a3ffada826d6f2e8866a3b1e62b23d5ffc4",
		ArtifactSize:   111646601,
		TrustedPreparation: nativeTrustedPreparation{
			Control:               "2d2164ba3d8e456c9aba15fbeb8cae4edc755997",
			ControlTree:           "ae226eed2e9a85d4da19958db7a3520ebfe9cf05",
			ControlBranch:         "swarm/native-preparer-c8eb-rebind-20261104",
			HelperSource:          "70230ccd92597a96000f90223c5d89b103e25426",
			HelperSHA256:          "6a84191fd016ddcb637cb5a470e88727176a70e8b5c6e8e6c69f33db6a1559eb",
			ReadinessReviewSHA256: "d860d82915eb728aa8ee2b33bbadfb7f5e33df59369ff7de24d430aad58eeba6",
			Scope:                 "Fixed reviewed control only. Each consumer invocation must supply its own preparation and four matching exports. publicnative helper43 attribution remains separate.",
		},
		CapabilityBoundary: map[string]string{
			"activeEphemeral":   "c8eb exporter refuses every nonempty ephemeral-plane family (labels, dependencies, events, comments and child counters); ephemeral fields on durable issue rows remain opaque source fields and do not gain live workflow semantics",
			"waitsFor":          "c8eb dependency exporter accepts only blocks,parent-child,related,discovered-from; waits-for and its gate metadata have no accepted native archive path",
			"conditionalBlocks": "c8eb dependency exporter accepts no conditional-blocks edge; B02 conditional outcomes remain native-only and public replay is separately labeled",
			"customStatuses":    "c8eb exporter refuses nonempty custom_statuses/custom_types; category-outcomes retains the real native command and expected dump refusal",
		},
		ProducerBinding: nativeProducerBinding{
			Commit:              "c8eb3ba7d4177e111a6f5c8aebe06b5d0b1d5724",
			Tree:                "930405e863284a3955cbd35e73a6097b606b91a5",
			Parent:              "305fec3afac83224f46c54f982959fd99db136fa",
			DependencyPath:      "internal/plugdump/map.go",
			DependencyBlob:      "106adb8fa19ae96117e27ef3291014706640e10c",
			DependencySHA256:    "9549c124151d1a74b2d8115bda0f9cacd83b4a50d6a252e08a2aafd0de50f872",
			DependencyLines:     "244-267",
			DependencyGuard:     "dependencies rows accept only blocks,parent-child,related,discovered-from; every other type returns unsupported dependency type",
			FamilyBuildPath:     "internal/plugdump/build.go",
			FamilyBuildBlob:     "9f2c8a8d8e1f93bfe3e91e51370a3ce617f1d140",
			FamilyBuildLines:    "54-57",
			FamilyCatalogPath:   "internal/plugdump/catalog.go",
			FamilyCatalogBlob:   "c12556ed36823875ddd0e8ceeeb6e98532bcb6ea",
			FamilyCatalogSHA256: "808d49d37ee63991715568067d69a48587d213b79d5ce1938ca0991f892c51fb",
			FamilyCatalogLines:  "59-70",
			RefusalTestPath:     "internal/plugdump/plugdump_test.go",
			RefusalTestBlob:     "f5f271045849cf6ea09ef20911003278b167d253",
			RefusalTestSHA256:   "9f4d844799a670724a9002f38349e07010105bd8aa931ce78f9dc42e3ca8fd4",
			RefusalTestLines:    "594-619",
			FamilyRefusalGuard:  "every family classified refuse-if-nonempty is populated with one row and must return nonempty family <name> with zero archive bytes",
		},
	}
	if inputs.Format != want.Format || inputs.Consumer != want.Consumer || inputs.ConsumerTree != want.ConsumerTree || inputs.ConsumerParent != want.ConsumerParent ||
		inputs.HelperSource != want.HelperSource || inputs.HelperTree != want.HelperTree || inputs.Source != want.Source || inputs.SourceTree != want.SourceTree ||
		inputs.SourceManifest != want.SourceManifest || inputs.SourceEntries != want.SourceEntries || inputs.BinarySHA256 != want.BinarySHA256 ||
		inputs.BinaryBytes != want.BinaryBytes || inputs.BuildRun != want.BuildRun || inputs.Profile != want.Profile || inputs.ManifestSHA256 != want.ManifestSHA256 ||
		inputs.WorkflowSHA != want.WorkflowSHA || inputs.Descriptor != want.Descriptor || inputs.ArtifactDigest != want.ArtifactDigest || inputs.ArtifactSize != want.ArtifactSize ||
		inputs.TrustedPreparation != want.TrustedPreparation || !reflect.DeepEqual(inputs.CapabilityBoundary, want.CapabilityBoundary) ||
		inputs.ProducerBinding != want.ProducerBinding {
		return fmt.Errorf("B02 native input contract drifted: got=%+v", inputs)
	}
	return nil
}

// This ordinary target exercises the same pure contract validator as the
// physical recorder. It intentionally contains no recorder, server, database,
// executable, or trusted-admission setup.
func TestB02NativeInputContract(t *testing.T) {
	var inputs nativeInputs
	nativeDecode(t, nativeB02ContractInputsBytes, &inputs)
	if err := validateB02InputContract(inputs); err != nil {
		t.Fatal(err)
	}
}
