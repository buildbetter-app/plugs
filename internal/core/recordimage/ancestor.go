package recordimage

import (
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// AncestorPrefix is the private, injective descendant prefix shared by closure
// writers and readers. Guest fields and unresolved request aliases never name
// derived rows. Drivers treat the entire key as opaque.
func AncestorPrefix(descendant string) (string, error) {
	if _, err := driver.NewClosureSentinel(descendant); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString([]byte(descendant)) + ".", nil
}

// AncestorRow encodes C(descendant, ancestor)=depth, including the self row.
func AncestorRow(descendant, ancestor string, depth uint64) (driver.DerivedRow, error) {
	if (descendant == ancestor) != (depth == 0) {
		return driver.DerivedRow{}, fmt.Errorf("only the self ancestor has depth zero")
	}
	prefix, err := AncestorPrefix(descendant)
	if err != nil {
		return driver.DerivedRow{}, err
	}
	if _, err := driver.NewClosureSentinel(ancestor); err != nil {
		return driver.DerivedRow{}, err
	}
	return driver.DerivedRow{Key: prefix + base64.RawURLEncoding.EncodeToString([]byte(ancestor)), Value: []byte(strconv.FormatUint(depth, 10))}, nil
}

// DecodeAncestorRow rejects aliases, ambiguous keys and noncanonical depths.
func DecodeAncestorRow(row driver.DerivedRow) (descendant, ancestor string, depth uint64, err error) {
	parts := strings.Split(row.Key, ".")
	if len(parts) != 2 {
		err = fmt.Errorf("invalid ancestor row key")
		return
	}
	d, e1 := base64.RawURLEncoding.DecodeString(parts[0])
	a, e2 := base64.RawURLEncoding.DecodeString(parts[1])
	n, e3 := strconv.ParseUint(string(row.Value), 10, 64)
	if e1 != nil || e2 != nil || e3 != nil {
		err = fmt.Errorf("invalid ancestor row")
		return
	}
	descendant, ancestor, depth = string(d), string(a), n
	canonical, e := AncestorRow(descendant, ancestor, depth)
	if e != nil || canonical.Key != row.Key || string(canonical.Value) != string(row.Value) || ((descendant == ancestor) != (depth == 0)) {
		err = fmt.Errorf("noncanonical ancestor row")
	}
	return
}
