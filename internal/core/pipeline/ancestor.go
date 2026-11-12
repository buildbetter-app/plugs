package pipeline

import (
	"github.com/buildbetter-app/plugs/internal/core/recordimage"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func AncestorPrefix(descendant string) (string, error) {
	return recordimage.AncestorPrefix(descendant)
}
func AncestorRow(descendant, ancestor string, depth uint64) (driver.DerivedRow, error) {
	return recordimage.AncestorRow(descendant, ancestor, depth)
}
func DecodeAncestorRow(row driver.DerivedRow) (descendant, ancestor string, depth uint64, err error) {
	return recordimage.DecodeAncestorRow(row)
}
