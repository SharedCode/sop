package k8sgate

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/http"
	"time"
)

// Serve serves h over TLS on ln until ctx is cancelled, then drains in-flight
// requests for a few seconds. The certificate comes from r on every handshake.
func Serve(ctx context.Context, ln net.Listener, h http.Handler, r *CertReloader) error {
	srv := &http.Server{
		Handler:           h,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		TLSConfig: &tls.Config{
			MinVersion:     tls.VersionTLS12,
			GetCertificate: r.GetCertificate,
		},
	}
	errc := make(chan error, 1)
	go func() { errc <- srv.ServeTLS(ln, "", "") }()
	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdown); err != nil {
		return err
	}
	if err := <-errc; err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
