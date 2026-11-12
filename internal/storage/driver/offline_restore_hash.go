package driver

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"io"
)

func hashRestoreImportProfile(detached RestoreImportProfile, accounting RestoreImportProfileAccounting) (string, error) {
	h := sha256.New()
	if err := writeRestoreImportProfileJSON(h, detached, accounting); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// writeRestoreImportProfileJSON writes the exact compact JSON of an already
// detached profile. The canonical source plan is the final field in the closed
// profile schema. Stream its standard padded base64 directly into the hash,
// without another plan-sized JSON buffer. All validation and normalization
// still happen at RequestHash's shared detachment boundary.
func writeRestoreImportProfileJSON(out io.Writer, detached RestoreImportProfile, accounting RestoreImportProfileAccounting) error {
	binding := detached.SourcePlanes
	detached.SourcePlanes = nil
	if err := writeRestoreHashJSON(out, "restore_profile_hash_metadata", detached, binding != nil, accounting); err != nil {
		return err
	}
	if binding == nil {
		return nil
	}
	if err := writeRestoreHashBytes(out, []byte(`,"sourcePlanes":{"version":`)); err != nil {
		return err
	}
	if err := writeRestoreHashJSON(out, "restore_profile_hash_version", binding.Version, false, accounting); err != nil {
		return err
	}
	if err := writeRestoreHashBytes(out, []byte(`,"canonicalPlan":"`)); err != nil {
		return err
	}
	if err := writeRestoreHashBase64(out, binding.CanonicalPlan, accounting); err != nil {
		return err
	}
	if err := writeRestoreHashBytes(out, []byte(`","jointPlanDigest":`)); err != nil {
		return err
	}
	if err := writeRestoreHashJSON(out, "restore_profile_hash_digest", binding.JointPlanDigest, false, accounting); err != nil {
		return err
	}
	return writeRestoreHashBytes(out, []byte(`}}`))
}

func writeRestoreHashJSON(out io.Writer, phase string, value any, omitClosingBrace bool, accounting RestoreImportProfileAccounting) error {
	serializerRelease, err := accounting.reserveJSON(phase+"_serializer", value)
	if err != nil {
		return err
	}
	wireRelease, err := accounting.reserveJSON(phase+"_wire", value)
	if err != nil {
		serializerRelease()
		return err
	}
	defer wireRelease()
	wire, err := json.Marshal(value)
	serializerRelease()
	if err != nil {
		return err
	}
	if omitClosingBrace {
		wire = wire[:len(wire)-1]
	}
	return writeRestoreHashBytes(out, wire)
}

// Encode complete three-byte groups into one fixed output buffer. The final
// group uses the same standard padding as encoding/json's []byte encoder.
// Borrowing the checked input avoids another canonical-plan-sized candidate.
func writeRestoreHashBase64(out io.Writer, canonical []byte, accounting RestoreImportProfileAccounting) error {
	const outputBytes = 1024
	release, err := accounting.reserveBytes("restore_profile_hash_base64", outputBytes)
	if err != nil {
		return err
	}
	defer release()
	var encoded [outputBytes]byte
	for len(canonical) != 0 {
		n := min(len(canonical), len(encoded)/4*3)
		base64.StdEncoding.Encode(encoded[:], canonical[:n])
		if err := writeRestoreHashBytes(out, encoded[:base64.StdEncoding.EncodedLen(n)]); err != nil {
			return err
		}
		canonical = canonical[n:]
	}
	return nil
}

func writeRestoreHashBytes(out io.Writer, value []byte) error {
	n, err := out.Write(value)
	if err == nil && n != len(value) {
		err = io.ErrShortWrite
	}
	return err
}
