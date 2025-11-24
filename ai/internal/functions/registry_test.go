package functions

import "testing"

func TestRegistryMissing(t *testing.T) {
	fn := Get("does.not.exist")
	if fn != nil {
		t.Fatalf("expected nil for missing function")
	}
}
