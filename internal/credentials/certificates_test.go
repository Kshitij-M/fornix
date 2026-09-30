package credentials

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/omaveda/fornix/internal/contracts"
)

func TestDeploymentAuthorityRotationRevocationAndRedaction(t *testing.T) {
	authority := NewTestDeploymentAuthority()
	now := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	authority.SetNow(func() time.Time { return now })
	ref, err := ParseRef("provider/openai")
	if err != nil {
		t.Fatal(err)
	}
	if err := authority.Put("workspace-a", "openai", ref, "model:openai", []byte("secret-a"), time.Minute); err != nil {
		t.Fatal(err)
	}
	resolver := NewManagedSecretResolver(authority)
	resolver.Now = func() time.Time { return now }
	first, err := resolver.ResolveVersioned(context.Background(), "workspace-a", ref, "model:openai")
	if err != nil {
		t.Fatal(err)
	}
	if first.Version != "test-v-1" || first.Secret.Len() != len("secret-a") {
		t.Fatalf("first resolution metadata=%q len=%d", first.Version, first.Secret.Len())
	}
	first.Clear()
	if err := authority.Rotate("workspace-a", "openai", ref, "model:openai", []byte("secret-b"), time.Minute); err != nil {
		t.Fatal(err)
	}
	now = now.Add(1500 * time.Millisecond)
	second, err := resolver.ResolveVersioned(context.Background(), "workspace-a", ref, "model:openai")
	if err != nil || second.Version != "test-v-2" {
		t.Fatalf("rotated resolution=%+v err=%v", second, err)
	}
	second.Clear()
	if _, err := resolver.ResolveVersioned(context.Background(), "workspace-b", ref, "model:openai"); !errors.Is(err, ErrManagedUnavailable) {
		t.Fatalf("cross-workspace resolution=%v, want unavailable", err)
	}
	if err := authority.Revoke("workspace-a", "openai", ref, "model:openai"); err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Second)
	if _, err := resolver.ResolveVersioned(context.Background(), "workspace-a", ref, "model:openai"); !errors.Is(err, ErrManagedDenied) {
		t.Fatalf("revoked resolution=%v, want denied", err)
	}
	observations := authority.Observations()
	if len(observations) < 3 {
		t.Fatalf("observations=%d, want resolution/rotation/revocation evidence", len(observations))
	}
	var sawRotationLag, sawRevocationLag bool
	for _, observation := range observations {
		if err := observation.Normalize(); err != nil || strings.Contains(strings.Join([]string{observation.SourceVersion, observation.PreviousVersion}, " "), "secret") {
			t.Fatalf("invalid or redacted observation=%+v err=%v", observation, err)
		}
		sawRotationLag = sawRotationLag || observation.RotationLagMillis > 0
		sawRevocationLag = sawRevocationLag || observation.RevocationLagMS > 0
	}
	if !sawRotationLag || !sawRevocationLag {
		t.Fatalf("rotation/revocation propagation lag was not observed: %+v", observations)
	}
}

func TestCertificateValidationAndMTLSClientFailClosed(t *testing.T) {
	material := makeTestCertificateMaterial(t)
	policy := contracts.CertificatePolicy{ServerName: "peer.example.test", SPKISHA256Pins: []string{spkiFingerprint(material.serverCert)}}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(material.rootPEM) {
		t.Fatal("append root")
	}
	now := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	observation, err := ValidateCertificateChain([]*x509.Certificate{material.serverCert}, roots, policy, now)
	if err != nil {
		t.Fatalf("valid certificate=%+v err=%v", observation, err)
	}
	if observation.SPKIFingerprint != spkiFingerprint(material.serverCert) {
		t.Fatalf("unexpected SPKI observation=%+v", observation)
	}
	badPin := policy
	badPin.SPKISHA256Pins = []string{strings.Repeat("0", 64)}
	if _, err := ValidateCertificateChain([]*x509.Certificate{material.serverCert}, roots, badPin, now); !errors.Is(err, ErrCertificatePin) {
		t.Fatalf("bad pin=%v, want pin error", err)
	}
	revoked := policy
	revoked.RevokedFingerprints = []string{certificateFingerprint(material.serverCert)}
	if _, err := ValidateCertificateChain([]*x509.Certificate{material.serverCert}, roots, revoked, now); !errors.Is(err, ErrCertificateRevoked) {
		t.Fatalf("revoked certificate=%v, want revoked error", err)
	}
	client, err := NewMTLSHTTPClient(MTLSClientConfig{Policy: policy, ClientCertPEM: material.clientPEM, ClientKeyPEM: material.clientKeyPEM, ServerRootPEM: material.rootPEM})
	if err != nil || client.Transport == nil {
		t.Fatalf("mTLS client=%v err=%v", client, err)
	}
}

type certificateMaterial struct {
	rootPEM, clientPEM, clientKeyPEM []byte
	serverCert                       *x509.Certificate
}

func makeTestCertificateMaterial(t *testing.T) certificateMaterial {
	t.Helper()
	now := time.Date(2026, 9, 26, 11, 0, 0, 0, time.UTC)
	caPublic, caPrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	caTemplate := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "Fornix Test Root"}, NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, caPublic, caPrivate)
	if err != nil {
		t.Fatal(err)
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatal(err)
	}
	serverPublic, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	serverTemplate := &x509.Certificate{SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "peer.example.test"}, DNSNames: []string{"peer.example.test"}, NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour), ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, KeyUsage: x509.KeyUsageDigitalSignature}
	serverDER, err := x509.CreateCertificate(rand.Reader, serverTemplate, caCert, serverPublic, caPrivate)
	if err != nil {
		t.Fatal(err)
	}
	serverCert, err := x509.ParseCertificate(serverDER)
	if err != nil {
		t.Fatal(err)
	}
	clientPublic, clientPrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	clientTemplate := &x509.Certificate{SerialNumber: big.NewInt(3), Subject: pkix.Name{CommonName: "fornix-client"}, NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour), ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}, KeyUsage: x509.KeyUsageDigitalSignature}
	clientDER, err := x509.CreateCertificate(rand.Reader, clientTemplate, caCert, clientPublic, caPrivate)
	if err != nil {
		t.Fatal(err)
	}
	clientKey, err := x509.MarshalPKCS8PrivateKey(clientPrivate)
	if err != nil {
		t.Fatal(err)
	}
	return certificateMaterial{
		rootPEM:      pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}),
		clientPEM:    pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: clientDER}),
		clientKeyPEM: pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: clientKey}),
		serverCert:   serverCert,
	}
}
