package arena

import "testing"

func TestRunScenarios(t *testing.T) {
	for _, sc := range []string{ScenarioBasic, ScenarioOrder, ScenarioRange} {
		for _, n := range []int{MinN, 1000, 12345} {
			res, err := Run(sc, n)
			if err != nil {
				t.Fatalf("%s n=%d: %v", sc, n, err)
			}
			if !res.Verified {
				t.Fatalf("%s n=%d failed checks: %v", sc, n, res.Checks)
			}
			if len(res.Measures) == 0 || len(res.Checks) == 0 {
				t.Fatalf("%s n=%d returned no measures or checks", sc, n)
			}
			for _, m := range res.Measures {
				if m.Ops <= 0 || m.OpsPerSec <= 0 || m.Millis <= 0 {
					t.Fatalf("%s n=%d bad measure %+v", sc, n, m)
				}
			}
		}
	}
}

func TestRunRejectsUnsafeInput(t *testing.T) {
	for _, n := range []int{-1, 0, MinN - 1, MaxN + 1, 10_000_000} {
		if _, err := Run(ScenarioBasic, n); err == nil {
			t.Fatalf("n=%d should be rejected", n)
		}
	}
	if _, err := Run("nope", 1000); err == nil {
		t.Fatal("unknown scenario should be rejected")
	}
}

func TestMaxSizeStaysBounded(t *testing.T) {
	res, err := Run(ScenarioBasic, MaxN)
	if err != nil || !res.Verified {
		t.Fatalf("max n failed: %v %v", err, res.Checks)
	}
}
