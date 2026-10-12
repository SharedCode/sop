package k8sgate

import (
	"crypto/tls"
	"os"
	"sync"
	"time"
)

// CertReloader serves a certificate read from disk and picks up a new one when
// the files change, which is how cert-manager and the kubelet rotate a mounted
// Secret. A new pair that fails to load is ignored and the last good one keeps
// serving, so a half-written rotation cannot take the webhook down.
type CertReloader struct {
	certFile, keyFile string

	mu      sync.Mutex
	cert    *tls.Certificate
	modTime time.Time
}

// NewCertReloader loads the pair at certFile and keyFile. It fails if the first
// load does.
func NewCertReloader(certFile, keyFile string) (*CertReloader, error) {
	r := &CertReloader{certFile: certFile, keyFile: keyFile}
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}
	r.cert = &cert
	r.modTime = r.newest()
	return r, nil
}

// newest returns the later modification time of the two files, or the zero
// time when either cannot be read.
func (r *CertReloader) newest() time.Time {
	var latest time.Time
	for _, p := range []string{r.certFile, r.keyFile} {
		fi, err := os.Stat(p)
		if err != nil {
			return time.Time{}
		}
		if fi.ModTime().After(latest) {
			latest = fi.ModTime()
		}
	}
	return latest
}

// GetCertificate is a tls.Config.GetCertificate hook.
func (r *CertReloader) GetCertificate(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if mt := r.newest(); !mt.IsZero() && !mt.Equal(r.modTime) {
		r.modTime = mt
		if cert, err := tls.LoadX509KeyPair(r.certFile, r.keyFile); err == nil {
			r.cert = &cert
		}
	}
	return r.cert, nil
}
