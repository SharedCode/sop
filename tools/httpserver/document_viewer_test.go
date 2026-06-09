package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHandleViewer_ExternalDocIDRedirectsDirectly(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/viewer?docID=https://github.com/SharedCode/sop/README.md&text=hello", nil)
	rec := httptest.NewRecorder()

	handleViewer(rec, req)

	if rec.Code != http.StatusFound {
		t.Fatalf("expected redirect status 302, got %d", rec.Code)
	}

	got := rec.Header().Get("Location")
	if got != "https://github.com/SharedCode/sop/README.md" {
		t.Fatalf("expected direct external redirect without highlight text suffix, got %q", got)
	}
}
