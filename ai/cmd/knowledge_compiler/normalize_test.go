package main

import "testing"

func TestNormalizeCategoryName_DoesNotRewriteProgrammingLanguageTokens(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "csharp", in: "C#", want: "C#"},
		{name: "cpp", in: "C++", want: "C++"},
		{name: "dotnet", in: ".NET", want: ".NET"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := normalizeCategoryName(tt.in); got != tt.want {
				t.Fatalf("normalizeCategoryName(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}
