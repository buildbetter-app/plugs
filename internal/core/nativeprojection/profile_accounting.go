package nativeprojection

import (
	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// ProfileAccounting shares the receiving budget through a dependency-neutral
// driver port. Each candidate has a separate owner and an explicit lifetime.
func ProfileAccounting(archive *dumpformat.Archive) driver.RestoreImportProfileAccounting {
	if archive == nil {
		return driver.RestoreImportProfileAccounting{}
	}
	bytes := func(phase string, size uint64) (func(), error) {
		owner, release := archive.NativeDeltaAccountingScope()
		if err := owner.ChargeNativeDeltaBytes(phase, size); err != nil {
			release()
			return nil, err
		}
		return release, nil
	}
	return driver.RestoreImportProfileAccounting{Bytes: bytes, JSON: func(phase string, value any) (func(), error) {
		n, err := dumpformat.NativeDeltaJSONSize(value)
		if err != nil {
			return nil, err
		}
		return bytes(phase, n-1)
	}}
}

// DetachedProfile returns one raw canonical-plan copy and normalized scalar
// output, with cleanup separate from the shorter validation/wire work.
func DetachedProfile(archive *dumpformat.Archive, profile driver.RestoreImportProfile) (driver.RestoreImportProfile, func(), error) {
	return profile.DetachedWithAccounting(ProfileAccounting(archive))
}
