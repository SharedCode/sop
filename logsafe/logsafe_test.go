package logsafe

import "testing"

func TestS(t *testing.T) {
	tests := []struct{ name, in, want string }{
		{"plain", "hello", "hello"},
		{"empty", "", ""},
		{"newline", "a\nb", `a\nb`},
		{"carriage return", "a\rb", `a\rb`},
		{"crlf", "a\r\nb", `a\r\nb`},
		{"forged line", "ok\n2026-01-01 ERROR forged", `ok\n2026-01-01 ERROR forged`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := S(tt.in); got != tt.want {
				t.Fatalf("S(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

func TestV(t *testing.T) {
	tests := []struct {
		name string
		in   any
		want string
	}{
		{"int", 42, "42"},
		{"nil", nil, "<nil>"},
		{"string with newline", "a\nb", `a\nb`},
		{"error with newline", errString("x\ny"), `x\ny`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := V(tt.in); got != tt.want {
				t.Fatalf("V(%v) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

type errString string

func (e errString) Error() string { return string(e) }
