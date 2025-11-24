package generator

import (
	"context"
	"testing"

	"github.com/sharedcode/sop/ai/internal/port"
)

func TestMockGenerate(t *testing.T) {
	g := NewMock("m")
	out, err := g.Generate(context.Background(), "hello world", port.GenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if out.Text == "" || out.TokensUsed == 0 {
		t.Fatalf("unexpected output: %+v", out)
	}
}
