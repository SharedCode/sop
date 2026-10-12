package main

import (
	"context"
	"io"
	"strings"
	"testing"
)

func TestRunRefusesIncompleteFlags(t *testing.T) {
	cases := map[string]struct {
		args []string
		want string
	}{
		"no flags":         {nil, "--policy"},
		"no tls cert":      {[]string{"--policy", "p.json", "--tls-key", "k"}, "--tls-cert"},
		"no tls key":       {[]string{"--policy", "p.json", "--tls-cert", "c"}, "--tls-key"},
		"unknown flag":     {[]string{"--nope"}, "nope"},
		"policy not found": {[]string{"--policy", "/nonexistent/p.json", "--tls-cert", "c", "--tls-key", "k"}, "p.json"},
	}
	for name, c := range cases {
		err := run(context.Background(), c.args, io.Discard)
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: err = %v, want one mentioning %q", name, err, c.want)
		}
	}
}
