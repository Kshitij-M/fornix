package credentials

import (
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/omaveda/fornix/internal/connector"
	"github.com/omaveda/fornix/internal/contracts"
)

var (
	ErrCertificatePolicy  = errors.New("certificate policy rejected the connection")
	ErrCertificateChain   = errors.New("certificate chain is invalid")
	ErrCertificatePin     = errors.New("certificate pin is not trusted")
	ErrCertificateRevoked = errors.New("certificate is revoked")
)

const (
	defaultMTLSTimeout = 10 * time.Second
	maxMTLSTimeout     = 2 * time.Minute
)

// MTLSClientConfig is the process-local input for a deployment transport.
// PEM and private-key bytes must be supplied by the deployment authority and
// are not retained outside the returned TLS client.
type MTLSClientConfig struct {
	Policy        contracts.CertificatePolicy
	ClientCertPEM []byte
	ClientKeyPEM  []byte
	ServerRootPEM []byte
	Client        *http.Client
	Timeout       time.Duration
	Resolver      connector.IPResolver
}

// NewMTLSHTTPClient builds a bounded client with explicit certificate and
// server-pin verification. The returned client can be passed to the existing
// controlled egress adapters; it never follows redirects by default.
func NewMTLSHTTPClient(config MTLSClientConfig) (*http.Client, error) {
	if err := config.Policy.Normalize(); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrCertificatePolicy, err)
	}
	if len(config.ClientCertPEM) == 0 || len(config.ClientKeyPEM) == 0 || len(config.ServerRootPEM) == 0 {
		return nil, fmt.Errorf("%w: client certificate, key, and server roots are required", ErrCertificatePolicy)
	}
	if config.Timeout <= 0 {
		config.Timeout = defaultMTLSTimeout
	}
	if config.Timeout > maxMTLSTimeout {
		return nil, fmt.Errorf("%w: mTLS timeout exceeds two minutes", ErrCertificatePolicy)
	}
	certificate, err := tls.X509KeyPair(config.ClientCertPEM, config.ClientKeyPEM)
	if err != nil {
		return nil, fmt.Errorf("%w: client key pair is invalid", ErrCertificateChain)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(config.ServerRootPEM) {
		return nil, fmt.Errorf("%w: server roots are invalid", ErrCertificateChain)
	}
	client := config.Client
	if client == nil {
		client = &http.Client{}
	} else {
		copyClient := *client
		client = &copyClient
	}
	if config.Timeout > 0 {
		client.Timeout = config.Timeout
	}
	transport := &http.Transport{}
	if existing, ok := client.Transport.(*http.Transport); ok && existing != nil {
		transport = existing.Clone()
	}
	transport.TLSClientConfig = &tls.Config{
		MinVersion:         tls.VersionTLS13,
		Certificates:       []tls.Certificate{certificate},
		RootCAs:            roots,
		ServerName:         config.Policy.ServerName,
		InsecureSkipVerify: true, // VerifyConnection performs the complete check.
		VerifyConnection: func(state tls.ConnectionState) error {
			_, verifyErr := ValidateCertificateChain(state.PeerCertificates, roots, config.Policy, time.Now().UTC())
			return verifyErr
		},
	}
	client.Transport = transport
	if client.CheckRedirect == nil {
		client.CheckRedirect = func(_ *http.Request, _ []*http.Request) error { return ErrCertificatePolicy }
	}
	return client, nil
}

// ValidateCertificateChain validates a peer chain independently of network
// I/O. It is used by VerifyConnection and qualification tests so certificate
// failures are deterministic and inspectable without exposing certificate
// bytes.
func ValidateCertificateChain(chain []*x509.Certificate, roots *x509.CertPool, policy contracts.CertificatePolicy, now time.Time) (contracts.CertificateObservation, error) {
	if err := policy.Normalize(); err != nil {
		return contracts.CertificateObservation{}, fmt.Errorf("%w: %v", ErrCertificatePolicy, err)
	}
	if len(chain) == 0 || len(chain) > policy.MaxChainLength || roots == nil {
		return contracts.CertificateObservation{}, ErrCertificateChain
	}
	leaf := chain[0]
	if leaf == nil {
		return contracts.CertificateObservation{}, ErrCertificateChain
	}
	leafFingerprint := certificateFingerprint(leaf)
	spkiFingerprint := spkiFingerprint(leaf)
	if containsPin(policy.RevokedFingerprints, leafFingerprint) || containsPin(policy.RevokedFingerprints, spkiFingerprint) {
		return contracts.CertificateObservation{}, ErrCertificateRevoked
	}
	if len(policy.CertificateSHA256Pins) > 0 && !containsPin(policy.CertificateSHA256Pins, leafFingerprint) {
		return contracts.CertificateObservation{}, ErrCertificatePin
	}
	if len(policy.SPKISHA256Pins) > 0 && !containsPin(policy.SPKISHA256Pins, spkiFingerprint) {
		return contracts.CertificateObservation{}, ErrCertificatePin
	}
	now = now.UTC()
	skew := time.Duration(policy.ClockSkewSeconds) * time.Second
	if now.Add(skew).Before(leaf.NotBefore) || now.Add(-skew).After(leaf.NotAfter) {
		return contracts.CertificateObservation{}, ErrCertificateChain
	}
	intermediates := x509.NewCertPool()
	for _, certificate := range chain[1:] {
		if certificate == nil {
			return contracts.CertificateObservation{}, ErrCertificateChain
		}
		intermediates.AddCert(certificate)
	}
	verifyNow := now
	if verifyNow.Before(leaf.NotBefore) {
		verifyNow = leaf.NotBefore
	}
	if verifyNow.After(leaf.NotAfter) {
		verifyNow = leaf.NotAfter
	}
	if _, err := leaf.Verify(x509.VerifyOptions{Roots: roots, Intermediates: intermediates, DNSName: policy.ServerName, CurrentTime: verifyNow, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}); err != nil {
		return contracts.CertificateObservation{}, fmt.Errorf("%w: %v", ErrCertificateChain, err)
	}
	observation := contracts.CertificateObservation{
		SchemaVersion: contracts.DeploymentCertificateSchemaVersion,
		ServerName:    policy.ServerName, LeafFingerprint: leafFingerprint,
		SPKIFingerprint: spkiFingerprint, NotBefore: leaf.NotBefore,
		NotAfter: leaf.NotAfter, ChainLength: len(chain), ValidatedAt: now,
	}
	return observation, observation.Normalize()
}

func certificateFingerprint(certificate *x509.Certificate) string {
	digest := sha256.Sum256(certificate.Raw)
	return hex.EncodeToString(digest[:])
}

func spkiFingerprint(certificate *x509.Certificate) string {
	digest := sha256.Sum256(certificate.RawSubjectPublicKeyInfo)
	return hex.EncodeToString(digest[:])
}

func containsPin(pins []string, value string) bool {
	value = strings.ToLower(strings.TrimSpace(value))
	for _, pin := range pins {
		if strings.EqualFold(pin, value) {
			return true
		}
	}
	return false
}
