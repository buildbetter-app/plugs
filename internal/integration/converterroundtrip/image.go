package converterroundtrip

import "github.com/buildbetter-app/plugs/internal/core/nativeprojection"

type derivedImage = nativeprojection.DerivedImage
type archiveEvidence = nativeprojection.ArchiveEvidence

const ImportedOntologySourceID = nativeprojection.ImportedOntologySourceID

func hashBytes(data []byte) string { return nativeprojection.HashBytes(data) }
func validDigest(value string) bool { return nativeprojection.ValidDigest(value) }
