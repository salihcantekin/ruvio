package ruvio

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"os"
	"strings"
	"testing"
	"time"
)

// These tests need the TLS servers scripts/run_with_servers.sh starts: RUVIO_TLS_ADDR,
// RUVIO_TLS_CLIENT_AUTH_ADDR and the PEM files RUVIO_TLS_CA, RUVIO_TLS_CLIENT_CERT and
// RUVIO_TLS_CLIENT_KEY. They skip without them.

func tlsEnv(t *testing.T, name string) string {
	t.Helper()
	v := os.Getenv(name)
	if v == "" {
		t.Skip(name + " not set")
	}
	return v
}

func tlsRoots(t *testing.T) *x509.CertPool {
	t.Helper()
	pem, err := os.ReadFile(tlsEnv(t, "RUVIO_TLS_CA"))
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		t.Fatal("no certificate in RUVIO_TLS_CA")
	}
	return pool
}

func tlsConnect(t *testing.T, addrVar string, cfg *tls.Config) (*Client, context.Context, error) {
	t.Helper()
	addr := tlsEnv(t, addrVar)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	c, err := Connect(ctx, Options{Addr: addr, TLS: cfg, CommandTimeout: 10 * time.Second})
	if err == nil {
		t.Cleanup(func() { c.Close() })
	}
	return c, ctx, err
}

func TestTLSVerifiedConnectionRunsCommands(t *testing.T) {
	c, ctx, err := tlsConnect(t, "RUVIO_TLS_ADDR", &tls.Config{RootCAs: tlsRoots(t)})
	if err != nil {
		t.Fatal(err)
	}
	noErr(t, c.Set(ctx, "tls:key", "value"))
	if v, ok, err := c.Get(ctx, "tls:key"); err != nil || !ok || v != "value" {
		t.Fatalf("Get = %q %v %v", v, ok, err)
	}
	big := strings.Repeat("x", 300_000)
	noErr(t, c.Set(ctx, "tls:big", big))
	if v, _, err := c.Get(ctx, "tls:big"); err != nil || v != big {
		t.Fatalf("big value came back wrong: len %d, %v", len(v), err)
	}
	// A blocking command runs on a dedicated TLS connection.
	if _, ok, err := c.BLPop(ctx, time.Second, "tls:missing"); err != nil || ok {
		t.Fatalf("BLPop = %v %v", ok, err)
	}
}

func TestTLSPubSubRunsOverTLS(t *testing.T) {
	c, ctx, err := tlsConnect(t, "RUVIO_TLS_ADDR", &tls.Config{RootCAs: tlsRoots(t)})
	if err != nil {
		t.Fatal(err)
	}
	ps, err := c.PubSub(ctx, 8)
	if err != nil {
		t.Fatal(err)
	}
	defer ps.Close()
	noErr(t, ps.Subscribe(ctx, "tls-ch"))
	if n, err := c.Publish(ctx, "tls-ch", "hi"); err != nil || n < 1 {
		t.Fatalf("Publish = %d %v", n, err)
	}
	select {
	case m := <-ps.Channel():
		if m.Payload != "hi" {
			t.Fatalf("message %+v", m)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no message over TLS")
	}
}

func TestTLSUntrustedCertificateIsRefused(t *testing.T) {
	// The system roots do not know the test CA.
	_, _, err := tlsConnect(t, "RUVIO_TLS_ADDR", &tls.Config{})
	if err == nil || !strings.Contains(err.Error(), "certificate") {
		t.Fatalf("expected a certificate error, got %v", err)
	}
}

func TestTLSNameTheCertificateLacksIsRefused(t *testing.T) {
	_, _, err := tlsConnect(t, "RUVIO_TLS_ADDR", &tls.Config{RootCAs: tlsRoots(t), ServerName: "elsewhere.example"})
	if err == nil {
		t.Fatal("expected a host name error")
	}
}

func TestTLSServerNameChecksADNSNameWhileDialingAnAddress(t *testing.T) {
	c, ctx, err := tlsConnect(t, "RUVIO_TLS_ADDR", &tls.Config{RootCAs: tlsRoots(t), ServerName: "localhost"})
	if err != nil {
		t.Fatal(err)
	}
	noErr(t, c.Ping(ctx))
}

func TestTLSSkippingVerificationIsAnExplicitChoice(t *testing.T) {
	c, ctx, err := tlsConnect(t, "RUVIO_TLS_ADDR", &tls.Config{InsecureSkipVerify: true})
	if err != nil {
		t.Fatal(err)
	}
	noErr(t, c.Ping(ctx))
}

func TestTLSServerThatWantsAClientCertificateGetsOne(t *testing.T) {
	cert, err := tls.LoadX509KeyPair(tlsEnv(t, "RUVIO_TLS_CLIENT_CERT"), tlsEnv(t, "RUVIO_TLS_CLIENT_KEY"))
	if err != nil {
		t.Fatal(err)
	}
	c, ctx, err := tlsConnect(t, "RUVIO_TLS_CLIENT_AUTH_ADDR", &tls.Config{RootCAs: tlsRoots(t), Certificates: []tls.Certificate{cert}})
	if err != nil {
		t.Fatal(err)
	}
	noErr(t, c.Ping(ctx))
}

func TestTLSMissingClientCertificateIsRefused(t *testing.T) {
	c, ctx, err := tlsConnect(t, "RUVIO_TLS_CLIENT_AUTH_ADDR", &tls.Config{RootCAs: tlsRoots(t)})
	if err == nil {
		// With TLS 1.3 the refusal can arrive with the first read.
		err = c.Ping(ctx)
	}
	if err == nil {
		t.Fatal("expected the server to refuse a client without a certificate")
	}
}

func TestTLSPlainTCPToATLSPortFails(t *testing.T) {
	c, ctx, err := tlsConnect(t, "RUVIO_TLS_ADDR", nil)
	if err == nil {
		err = c.Ping(ctx)
	}
	if err == nil {
		t.Fatal("plain TCP to a TLS port must not work")
	}
}
