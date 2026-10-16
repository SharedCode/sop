package main

import "testing"

// isSafeRelativeRedirect is a thin alias over internal/netguard.
// IsSafeRelativeRedirect; the exhaustive bypass-attempt suite lives in
// internal/netguard/redirect_test.go so it isn't duplicated here. This is a
// smoke test confirming the wrapper actually delegates.
func TestIsSafeRelativeRedirect_DelegatesToNetguard(t *testing.T) {
	unsafe := []string{"", "https://evil.example/", "//evil.example/", `/\evil.example`}
	for _, target := range unsafe {
		if isSafeRelativeRedirect(target) {
			t.Errorf("isSafeRelativeRedirect(%q) = true, want false", target)
		}
	}

	safe := []string{"/", "/app", "/app?checkout=success&tier=pro"}
	for _, target := range safe {
		if !isSafeRelativeRedirect(target) {
			t.Errorf("isSafeRelativeRedirect(%q) = false, want true", target)
		}
	}
}

func TestRelativeRedirectOrDefault(t *testing.T) {
	const fb = "/app?checkout=success&tier=pro"
	cases := map[string]string{
		"/app?x=1":              "/app?x=1",
		"/billing/done":         "/billing/done",
		"":                      fb,
		"//evil.example/x":      fb,
		"https://evil.example/": fb,
		`/\evil.example`:        fb,
		"javascript:alert(1)":   fb,
	}
	for in, want := range cases {
		if got := relativeRedirectOrDefault(in, fb); got != want {
			t.Errorf("relativeRedirectOrDefault(%q) = %q, want %q", in, got, want)
		}
	}
}
