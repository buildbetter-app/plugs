package workflow

import (
	"bytes"
	"context"
	"encoding/json"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// InspectSourceReverseImageContext shares the caller's admitted decoded budget.
// The compatibility entry below uses the same existing default budget when a
// caller has none; neither entry permits an unlimited inspection.
func InspectSourceReverseImageContext(ctx context.Context, logical []byte) (SourceReverseImage, error) {
	return inspectSourceReverseImage(ctx, logical, decodeSourceReverseImage)
}
func InspectSourceReverseImage(logical []byte) (SourceReverseImage, error) {
	return InspectSourceReverseImageContext(context.Background(), logical)
}

// SourceOriginDigest hashes the immutable member of a logical image borrowed
// from a validated checkpoint envelope. Full inspection proves the canonical
// bytes at import or startup. A byte match reuses that proof without decoding
// the raw records or rebuilding mutable workflow views.
func SourceOriginDigest(ctx context.Context, logical []byte) (string, error) {
	_, _, mutableLimit, aggregateLimit := driver.SourcePlaneReverseImageLimits()
	if uint64(len(logical)) > aggregateLimit {
		return "", sourceRefusal("reverse aggregate byte limit")
	}
	var source []byte
	seen := make(map[string]bool, 5)
	err := reverseObjectSections(ctx, logical, func(key string, value []byte) error {
		if seen[key] || len(value) == 0 {
			return sourceRefusal("duplicate or empty logical section")
		}
		seen[key] = true
		switch key {
		case `"source"`:
			source = value
		case `"runs"`, `"items"`, `"receipts"`, `"audit"`:
			if uint64(len(value)) > mutableLimit {
				return sourceRefusal("reverse mutable section byte limit: " + key)
			}
		default:
			return sourceRefusal("unknown reverse logical section")
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	if len(source) == 0 || !seen[`"runs"`] || !seen[`"items"`] || !seen[`"receipts"`] {
		return "", sourceRefusal("incomplete source-restored logical image")
	}
	digest := sourceImageHash(source)
	return digest, ctx.Err()
}

func decodeSourceReverseImage(raw []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	decoder.DisallowUnknownFields()
	return decoder.Decode(target)
}

// This scanner borrows slices only. No section is decoded, copied or cloned
// until all independent section bounds and the composed aggregate pass.
func admitReverseImage(ctx context.Context, raw []byte) error {
	planLimit, recordsLimit, mutableLimit, aggregateLimit := driver.SourcePlaneReverseImageLimits()
	if uint64(len(raw)) > aggregateLimit {
		return sourceRefusal("reverse aggregate byte limit")
	}
	var planBytes uint64
	return reverseObjectSections(ctx, raw, func(key string, value []byte) error {
		switch key {
		case `"runs"`, `"items"`, `"receipts"`, `"audit"`:
			if uint64(len(value)) > mutableLimit {
				return sourceRefusal("reverse mutable section byte limit: " + key)
			}
		case `"source"`:
			return reverseObjectSections(ctx, value, func(key string, value []byte) error {
				switch key {
				case `"schema"`:
					if len(value) > len(SourceStateSchema)+2 {
						return sourceRefusal("reverse source schema byte limit")
					}
				case `"entities"`, `"relations"`, `"origins"`:
					planBytes += uint64(len(value))
					if planBytes > planLimit {
						return sourceRefusal("reverse plan declarations byte limit")
					}
					return reverseArraySections(ctx, value, func([]byte) error { return nil })
				case `"records"`:
					if uint64(len(value)) > recordsLimit {
						return sourceRefusal("reverse raw section byte limit")
					}
					var rawBytes uint64
					return reverseArraySections(ctx, value, func(record []byte) error {
						return reverseObjectSections(ctx, record, func(key string, value []byte) error {
							if key != `"Raw"` {
								return nil
							}
							if len(value) < 2 || value[0] != '"' || value[len(value)-1] != '"' {
								return sourceRefusal("invalid reverse raw bytes")
							}
							encoded := value[1 : len(value)-1]
							if len(encoded)%4 != 0 || bytes.IndexByte(encoded, '\\') >= 0 {
								return sourceRefusal("noncanonical reverse raw bytes")
							}
							size := uint64(len(encoded)/4) * 3
							for i := len(encoded) - 1; i >= 0 && encoded[i] == '='; i-- {
								if size == 0 {
									return sourceRefusal("invalid reverse raw padding")
								}
								size--
							}
							rawBytes += size
							if rawBytes > uint64(driver.SourcePlaneMaxPlanBytes) {
								return sourceRefusal("source record byte limit")
							}
							return nil
						})
					})
				default:
					return sourceRefusal("unknown reverse source section")
				}
				return nil
			})
		default:
			return sourceRefusal("unknown reverse logical section")
		}
		return nil
	})
}

func reverseObjectSections(ctx context.Context, raw []byte, visit func(string, []byte) error) error {
	raw = bytes.TrimSpace(raw)
	if len(raw) < 2 || raw[0] != '{' || raw[len(raw)-1] != '}' {
		return sourceRefusal("invalid reverse object")
	}
	end := len(raw) - 1
	for i, count := 1, 0; i < end; count++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		for i < end && reverseJSONSpace(raw[i]) {
			i++
		}
		if i == end {
			break
		}
		if count >= driver.SourcePlaneMaxListEntries || raw[i] != '"' {
			return sourceRefusal("invalid reverse object census")
		}
		keyStart := i
		i++
		// Closed canonical keys never contain escapes; refuse before allocating one.
		for i < end && raw[i] != '"' {
			if raw[i] == '\\' {
				return sourceRefusal("noncanonical reverse section key")
			}
			i++
		}
		if i == end {
			return sourceRefusal("unterminated reverse section key")
		}
		i++
		if i-keyStart > len(`"relations"`) {
			return sourceRefusal("unknown reverse section key")
		}
		key := string(raw[keyStart:i])
		for i < end && reverseJSONSpace(raw[i]) {
			i++
		}
		if i == end || raw[i] != ':' {
			return sourceRefusal("missing reverse section value")
		}
		i++
		for i < end && reverseJSONSpace(raw[i]) {
			i++
		}
		start := i
		next, err := reverseValueEnd(ctx, raw, i, end)
		if err != nil {
			return err
		}
		i = next
		if err := visit(key, bytes.TrimSpace(raw[start:i])); err != nil {
			return err
		}
		for i < end && reverseJSONSpace(raw[i]) {
			i++
		}
		if i == end {
			break
		}
		if raw[i] != ',' {
			return sourceRefusal("invalid reverse section separator")
		}
		i++
	}
	return nil
}
func reverseArraySections(ctx context.Context, raw []byte, visit func([]byte) error) error {
	raw = bytes.TrimSpace(raw)
	if bytes.Equal(raw, []byte("null")) {
		return nil
	}
	if len(raw) < 2 || raw[0] != '[' || raw[len(raw)-1] != ']' {
		return sourceRefusal("invalid reverse array")
	}
	end := len(raw) - 1
	for i, count := 1, 0; i < end; count++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		for i < end && reverseJSONSpace(raw[i]) {
			i++
		}
		if i == end {
			break
		}
		if count >= driver.SourcePlaneMaxListEntries {
			return sourceRefusal("reverse array count limit")
		}
		start := i
		next, err := reverseValueEnd(ctx, raw, i, end)
		if err != nil {
			return err
		}
		i = next
		if err := visit(bytes.TrimSpace(raw[start:i])); err != nil {
			return err
		}
		for i < end && reverseJSONSpace(raw[i]) {
			i++
		}
		if i == end {
			break
		}
		if raw[i] != ',' {
			return sourceRefusal("invalid reverse array separator")
		}
		i++
	}
	return nil
}
func reverseJSONSpace(c byte) bool { return c == ' ' || c == '\n' || c == '\r' || c == '\t' }
func reverseValueEnd(ctx context.Context, raw []byte, start, end int) (int, error) {
	depth := 0
	quoted := false
	escaped := false
	for i := start; i < end; i++ {
		// Check cancellation during long raw payloads without allocating tokens.
		if i%(1<<16) == 0 {
			if err := ctx.Err(); err != nil {
				return 0, err
			}
		}
		c := raw[i]
		if quoted {
			if escaped {
				escaped = false
			} else if c == '\\' {
				escaped = true
			} else if c == '"' {
				quoted = false
			}
			continue
		}
		switch c {
		case '"':
			quoted = true
		case '{', '[':
			depth++
		case '}', ']':
			if depth == 0 {
				return i, nil
			}
			depth--
		case ',':
			if depth == 0 {
				return i, nil
			}
		}
	}
	if quoted || depth != 0 || start == end {
		return 0, sourceRefusal("incomplete reverse section")
	}
	return end, nil
}
