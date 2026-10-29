package model

import (
	"encoding/json"
	"fmt"
	"math/big"
	"strconv"
	"strings"
)

// Natural is the lossless nonnegative JSON-integer wire domain. A zero Go
// value represents zero. It deliberately has no uint32/int64/uint64 ceiling:
// the accepted OpenRPC schemas specify no such maximum. Drivers must check
// conversion to engine-sized integers and may never silently narrow a value.
// Direct casts are checked again by Validate and MarshalJSON.
type Natural string

func Natural64(value uint64) Natural { return Natural(strconv.FormatUint(value, 10)) }

func (value Natural) Canonical() (string, error) {
	text := string(value)
	if text == "" {
		text = "0"
	}
	canonical, err := canonicalInteger([]byte(text))
	if err != nil {
		return "", err
	}
	if strings.HasPrefix(canonical, "-") {
		return "", fmt.Errorf("expected nonnegative integer")
	}
	return canonical, nil
}

func (value Natural) Validate() error {
	_, err := value.Canonical()
	return err
}

func (value Natural) IsZero() bool {
	canonical, err := value.Canonical()
	return err == nil && canonical == "0"
}

func (value Natural) String() string {
	canonical, err := value.Canonical()
	if err != nil {
		return "<invalid integer>"
	}
	return canonical
}

func (value Natural) Uint64() (uint64, error) {
	canonical, err := value.Canonical()
	if err != nil {
		return 0, err
	}
	// Canonical values of at most 20 digits are expanded. An exponent here
	// therefore cannot fit uint64; ParseUint rejects it without allocation.
	return strconv.ParseUint(canonical, 10, 64)
}

func (value Natural) MarshalJSON() ([]byte, error) {
	canonical, err := value.Canonical()
	return []byte(canonical), err
}

func (value *Natural) UnmarshalJSON(data []byte) error {
	canonical, err := canonicalInteger(data)
	if err != nil {
		return err
	}
	decoded := Natural(canonical)
	if err := decoded.Validate(); err != nil {
		return err
	}
	*value = decoded
	return nil
}

// canonicalInteger recognizes mathematical JSON integers, including 1.0,
// 1e0, and large integral exponents, without float64 rounding. It normalizes
// equivalent spellings. Exponents stay symbolic so a short input such as
// 1e1000000000 does not allocate a billion-digit integer.
func canonicalInteger(data []byte) (string, error) {
	text := strings.TrimSpace(string(data))
	if text == "" || !json.Valid([]byte(text)) || (text[0] != '-' && (text[0] < '0' || text[0] > '9')) {
		return "", fmt.Errorf("expected JSON integer")
	}
	negative := strings.HasPrefix(text, "-")
	if negative {
		text = text[1:]
	}
	exponent := new(big.Int)
	if index := strings.IndexAny(text, "eE"); index >= 0 {
		if _, ok := exponent.SetString(text[index+1:], 10); !ok {
			return "", fmt.Errorf("invalid integer exponent")
		}
		text = text[:index]
	}
	fractionDigits := 0
	if index := strings.IndexByte(text, '.'); index >= 0 {
		fractionDigits = len(text) - index - 1
	}
	digits := strings.TrimLeft(strings.ReplaceAll(text, ".", ""), "0")
	if digits == "" {
		return "0", nil
	}
	coefficient := strings.TrimRight(digits, "0")
	trailingZeros := len(digits) - len(coefficient)
	exponent.Sub(exponent, big.NewInt(int64(fractionDigits-trailingZeros)))
	if exponent.Sign() < 0 {
		return "", fmt.Errorf("JSON number is not integral")
	}
	canonical := coefficient
	if exponent.Sign() > 0 {
		if exponent.IsInt64() && exponent.Int64() <= int64(20-len(coefficient)) {
			canonical += strings.Repeat("0", int(exponent.Int64()))
		} else {
			canonical += "e" + exponent.String()
		}
	}
	if negative {
		canonical = "-" + canonical
	}
	return canonical, nil
}

type Revision = Natural
type Branch = Natural
