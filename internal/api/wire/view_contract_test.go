package wire

import (
	"os"
	"path/filepath"
	"testing"
)

func TestB23CountContractCanonicalAndEmbedded(t *testing.T) {
	canonicalPath := filepath.Join(os.Getenv("TEST_SRCDIR"), os.Getenv("TEST_WORKSPACE"), "engdocs/design/plug/contracts/openrpc.json")
	canonical, err := os.ReadFile(canonicalPath)
	if err != nil {
		t.Fatal(err)
	}
	for name, raw := range map[string][]byte{"canonical": canonical, "embedded": catalogJSON} {
		t.Run(name, func(t *testing.T) {
			c, err := ParseCatalog(raw)
			if err != nil {
				t.Fatal(err)
			}
			d, _ := c.Descriptor("view.run")
			for _, params := range []string{`{"definition":{"filter":{}}}`, `{"countOnly":false,"viewId":"saved"}`, `{"countOnly":true,"definition":{"filter":{}}}`} {
				if err := c.ValidateParams(d, []byte(params)); err != nil {
					t.Fatalf("valid count option refused: %s: %v", params, err)
				}
			}
			for _, params := range []string{`{"countOnly":null}`, `{"countOnly":1}`, `{"countOnly":"true"}`, `{"definition":{"filter":{},"countOnly":true}}`} {
				if err := c.ValidateParams(d, []byte(params)); err == nil {
					t.Fatalf("invalid count option admitted: %s", params)
				}
			}
			for _, count := range []string{"", `,"count":0`, `,"count":20000`} {
				if err := c.ValidateResult(d, []byte(`{"items":[],"asOf":{"revision":1}`+count+`}`)); err != nil {
					t.Fatalf("valid ordinary result refused: count=%s: %v", count, err)
				}
			}
			if err := c.ValidateResult(d, []byte(`{"items":[],"asOf":{"revision":1},"count":0,"projection":{"binding":"core","checkpoint":{"incarnation":"inc","revision":1,"index":0}}}`)); err != nil {
				t.Fatalf("count with existing projection metadata refused: %v", err)
			}
			for _, count := range []string{"-1", "20001", "null", "true", `"0"`, "0.5"} {
				if err := c.ValidateResult(d, []byte(`{"items":[],"asOf":{"revision":1},"count":`+count+`}`)); err == nil {
					t.Fatalf("invalid ordinary count admitted: %s", count)
				}
			}
		})
	}
}
