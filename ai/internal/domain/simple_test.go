package domain

import (
	"testing"

	"github.com/sharedcode/sop/ai/internal/embed"
	"github.com/sharedcode/sop/ai/internal/generator"
	"github.com/sharedcode/sop/ai/internal/index"
	"github.com/sharedcode/sop/ai/internal/policy"
)

func TestSimpleDomainPrompt(t *testing.T) {
	d := NewSimple("d", embed.NewSimple("e", 8), index.NewMemory(), policy.NewAllow("p"), map[string]string{"x": "prompt"}, generator.NewMock("g"))
	p, err := d.Prompt("x")
	if err != nil {
		t.Fatal(err)
	}
	if p != "prompt" {
		t.Fatalf("expected prompt")
	}
	p2, _ := d.Prompt("missing")
	if p2 != "" {
		t.Fatalf("expected empty for missing")
	}
}
