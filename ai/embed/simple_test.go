package embed

import "testing"

func TestSimpleEmbedBasic(t *testing.T) {
	e := NewSimple("simple", 32, nil)
	vecs, err := e.EmbedTexts([]string{"hello world", ""})
	if err != nil {
		t.Fatal(err)
	}
	if len(vecs) != 2 {
		t.Fatalf("expected 2 vectors")
	}
	if len(vecs[0]) != 32 {
		t.Fatalf("dimension mismatch")
	}
}
