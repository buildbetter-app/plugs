package cp7latency_test

import (
	"os"
	"testing"
)

func TestRealProductCP7100K(t *testing.T) {
	checkDeclaration(t)
	engine := os.Getenv("CP7_LATENCY_ENGINE")
	if engine != "sqlite" && engine != "postgres" {
		t.Fatal("Bazel target must declare its real engine")
	}
	f := newFixture(t, engine, corpusSize)
	for _, divisor := range []int{1, 100, 1000} {
		name := map[int]string{1: "100pct", 100: "1pct", 1000: "0.1pct"}[divisor]
		t.Run(name, func(t *testing.T) {
			for _, op := range []string{"Q4Ready", "Q5ClaimNext", "Q6GroupedCount"} {
				t.Run(op, func(t *testing.T) {
					f.compose(t)
					cold := f.observe(t, engine, divisor, op, "cold", 0)
					for i := 0; i < warmups; i++ {
						f.observe(t, engine, divisor, op, "warmup", i)
					}
					measured := make([]sample, 0, measuredSamples)
					for i := 0; i < measuredSamples; i++ {
						measured = append(measured, f.observe(t, engine, divisor, op, "measured", i))
					}
					result, err := summarize(engine, divisor, op, f.corpusHash, cold, measured)
					if err != nil {
						t.Fatal(err)
					}
					emit(t, "CP7_CASE", result)
				})
			}
		})
	}
}
