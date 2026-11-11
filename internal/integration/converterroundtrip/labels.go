package converterroundtrip

import "github.com/buildbetter-app/plugs/internal/core/nativeprojection"

func exactLabelSet(raw any) ([]string, bool) { return nativeprojection.ExactLabelSet(raw) }
