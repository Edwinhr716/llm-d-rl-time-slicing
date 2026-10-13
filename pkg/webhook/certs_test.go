package webhook_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/webhook"
)

// writePair writes a self-signed certificate for cn to dir/tls.crt and dir/tls.key with the
// given modification time.
func writePair(t *testing.T, dir, cn string, mod time.Time) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: cn},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), DNSNames: []string{cn},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	files := map[string][]byte{
		"tls.crt": pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		"tls.key": pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}),
	}
	for name, data := range files {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(path, mod, mod); err != nil {
			t.Fatal(err)
		}
	}
}

func commonName(t *testing.T, loader *webhook.CertLoader) string {
	t.Helper()
	cert, err := loader.GetCertificate(nil)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	return leaf.Subject.CommonName
}

func TestCertLoader_LoadsAndReloads(t *testing.T) {
	dir := t.TempDir()
	start := time.Now().Add(-time.Minute)
	writePair(t, dir, "first", start)
	loader, err := webhook.NewCertLoader(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got := commonName(t, loader); got != "first" {
		t.Errorf("CN = %q", got)
	}
	writePair(t, dir, "second", start.Add(30*time.Second))
	if got := commonName(t, loader); got != "second" {
		t.Errorf("after rotation CN = %q, want second", got)
	}
	// A broken rotation keeps the last good certificate.
	if err := os.WriteFile(filepath.Join(dir, "tls.crt"), []byte("garbage"), 0o600); err != nil {
		t.Fatal(err)
	}
	if got := commonName(t, loader); got != "second" {
		t.Errorf("after a broken rotation CN = %q, want second", got)
	}
}

func TestCertLoader_MissingFiles(t *testing.T) {
	if _, err := webhook.NewCertLoader(t.TempDir()); err == nil {
		t.Error("NewCertLoader on an empty directory succeeded")
	}
}
