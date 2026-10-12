// Command sop-k8s-gate is a Kubernetes validating admission webhook that puts
// the verify barrier in front of destructive cluster operations.
//
// A policy file maps Kubernetes operations (a DELETE on a PersistentVolumeClaim,
// say) to runbook steps. The webhook allows such a request only when the steps
// that step depends on are recorded for the trace named in the object's
// joltrin.dev/trace-id annotation. Steps are recorded by POSTing to /commit with
// a bearer token, from a job that holds a credential the workload under control
// does not. See docs/KUBERNETES_GATE.md.
//
//	sop-k8s-gate --policy policy.json --tls-cert tls.crt --tls-key tls.key \
//	    --commit-token-file commit-token
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/sharedcode/joltrin/v5/tools/k8sgate"
)

func run(ctx context.Context, args []string, stderr io.Writer) error {
	fs := flag.NewFlagSet("sop-k8s-gate", flag.ContinueOnError)
	fs.SetOutput(stderr)
	addr := fs.String("addr", ":8443", "address to listen on")
	policy := fs.String("policy", "", "policy JSON file mapping Kubernetes operations to runbook steps (required)")
	runbooks := fs.String("runbooks", "", "runbook JSON file; defaults to the built-in db-maintenance runbook")
	certFile := fs.String("tls-cert", "", "TLS certificate file (required)")
	keyFile := fs.String("tls-key", "", "TLS private key file (required)")
	tokenFile := fs.String("commit-token-file", "", "file holding the bearer token for POST /commit; without it /commit is disabled")
	warnOnly := fs.Bool("warn-only", false, "allow every request and report what would have been blocked as an admission warning")
	if err := fs.Parse(args); err != nil {
		return err
	}
	switch {
	case *policy == "":
		return errors.New("--policy is required")
	case *certFile == "":
		return errors.New("--tls-cert is required")
	case *keyFile == "":
		return errors.New("--tls-key is required")
	}
	gate, err := k8sgate.FromFiles(k8sgate.Files{Runbooks: *runbooks, Policy: *policy, CommitTokenFile: *tokenFile, WarnOnly: *warnOnly, Logger: slog.New(slog.NewJSONHandler(stderr, nil))})
	if err != nil {
		return err
	}
	reloader, err := k8sgate.NewCertReloader(*certFile, *keyFile)
	if err != nil {
		return fmt.Errorf("loading the TLS pair: %w", err)
	}
	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		return err
	}
	fmt.Fprintf(stderr, "sop-k8s-gate listening on %s (warn-only=%v, commit endpoint %s)\n", ln.Addr(), *warnOnly, map[bool]string{true: "enabled", false: "disabled"}[*tokenFile != ""])
	return k8sgate.Serve(ctx, ln, gate.Handler(), reloader)
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stderr); err != nil {
		log.SetFlags(0)
		log.Fatalf("sop-k8s-gate: %v", err)
	}
}
