package k8sgate

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// writePair writes a self-signed certificate with the given common name and
// returns the serial-independent CN so a test can tell two pairs apart.
func writePair(t *testing.T, certPath, keyPath, cn string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, tpl, tpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatal(err)
	}
}

func commonName(t *testing.T, c *tls.Certificate) string {
	t.Helper()
	leaf, err := x509.ParseCertificate(c.Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	return leaf.Subject.CommonName
}

func bump(t *testing.T, paths ...string) {
	t.Helper()
	later := time.Now().Add(5 * time.Second)
	for _, p := range paths {
		if err := os.Chtimes(p, later, later); err != nil {
			t.Fatal(err)
		}
	}
}

func TestCertReloaderServesTheRotatedCertificate(t *testing.T) {
	dir := t.TempDir()
	cert, key := filepath.Join(dir, "tls.crt"), filepath.Join(dir, "tls.key")
	writePair(t, cert, key, "first")
	r, err := NewCertReloader(cert, key)
	if err != nil {
		t.Fatal(err)
	}
	c, err := r.GetCertificate(nil)
	if err != nil || commonName(t, c) != "first" {
		t.Fatalf("first certificate: %v, %v", c, err)
	}
	writePair(t, cert, key, "second")
	bump(t, cert, key)
	c, err = r.GetCertificate(nil)
	if err != nil || commonName(t, c) != "second" {
		t.Fatalf("after rotation: %v, %v", c, err)
	}
}

func TestCertReloaderKeepsServingWhenTheNewFilesAreBroken(t *testing.T) {
	dir := t.TempDir()
	cert, key := filepath.Join(dir, "tls.crt"), filepath.Join(dir, "tls.key")
	writePair(t, cert, key, "first")
	r, err := NewCertReloader(cert, key)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(cert, []byte("not a certificate"), 0o600); err != nil {
		t.Fatal(err)
	}
	bump(t, cert, key)
	c, err := r.GetCertificate(nil)
	if err != nil || commonName(t, c) != "first" {
		t.Fatalf("a half-written rotation must not break serving: %v, %v", c, err)
	}
}

func TestNewCertReloaderFailsOnMissingFiles(t *testing.T) {
	if _, err := NewCertReloader("/nonexistent/tls.crt", "/nonexistent/tls.key"); err == nil {
		t.Error("NewCertReloader succeeded with no files")
	}
}
