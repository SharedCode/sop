package cel

import (
	"testing"
)

func TestBasicCEL(t *testing.T) {
	e, err := NewEvaluator("comparer", "mapX['a'] < mapY['a'] ? -1 : mapX['a'] > mapY['a'] ? 1 : 0")
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	mx := map[string]any{"a": 1}
	my := map[string]any{"a": 2}
	r, _ := e.Evaluate(mx, my)
	if r >= 0 {
		t.Errorf("expected < 1, but got >= 0")
		t.FailNow()
	}
}

func TestBasicCEL2(t *testing.T) {
	e, err := NewEvaluator("comparer", "mapX['b'] < mapY['b'] ? -1 : mapX['b'] > mapY['b'] ? 1 : 0")
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	mx := map[string]any{"a": 1, "b": "foo"}
	my := map[string]any{"a": 2, "b": "foo"}
	r, _ := e.Evaluate(mx, my)
	if r != 0 {
		t.Errorf("expected 0, but got %d", r)
		t.FailNow()
	}
}
