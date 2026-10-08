package webhook

import (
	"crypto/tls"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// CertLoader serves the serving certificate from <cert-dir>/tls.crt and tls.key, as mounted from a
// kubernetes.io/tls Secret. It reloads the pair when either file changes, so a rotated Secret is
// picked up without a restart. No cert-manager dependency.
type CertLoader struct {
	certFile, keyFile string

	mu      sync.Mutex
	cert    *tls.Certificate
	certMod time.Time
	keyMod  time.Time
}

// NewCertLoader returns a loader for dir/tls.crt and dir/tls.key and loads them once.
func NewCertLoader(dir string) (*CertLoader, error) {
	l := &CertLoader{certFile: filepath.Join(dir, "tls.crt"), keyFile: filepath.Join(dir, "tls.key")}
	if _, err := l.GetCertificate(nil); err != nil {
		return nil, err
	}
	return l, nil
}

// GetCertificate implements tls.Config.GetCertificate. On a reload error it keeps serving the
// last good certificate.
func (l *CertLoader) GetCertificate(_ *tls.ClientHelloInfo) (*tls.Certificate, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	certInfo, certErr := os.Stat(l.certFile)
	keyInfo, keyErr := os.Stat(l.keyFile)
	if certErr == nil && keyErr == nil && l.cert != nil &&
		certInfo.ModTime().Equal(l.certMod) && keyInfo.ModTime().Equal(l.keyMod) {
		return l.cert, nil
	}
	if pair, err := tls.LoadX509KeyPair(l.certFile, l.keyFile); err == nil {
		l.cert = &pair
		if certErr == nil && keyErr == nil {
			l.certMod, l.keyMod = certInfo.ModTime(), keyInfo.ModTime()
		}
	} else if l.cert == nil {
		return nil, fmt.Errorf("load serving certificate: %w", err)
	}
	return l.cert, nil
}
