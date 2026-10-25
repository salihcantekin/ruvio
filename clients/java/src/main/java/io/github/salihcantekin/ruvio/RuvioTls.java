// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Base64;
import java.util.Collection;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.X509ExtendedTrustManager;

/**
 * Builds the {@link SSLContext} for {@link RuvioOptions.Builder#tls(SSLContext)} from PEM files, so a private CA or a client
 * certificate needs no {@code keytool}.
 */
public final class RuvioTls {
    private RuvioTls() {
    }

    /**
     * Builds a TLS context from PEM files.
     *
     * @param caPem a PEM file holding the certificate authorities to trust (only these are trusted), or {@code null} for the
     *     JVM's default certificates
     * @param clientCertPem a PEM file holding the client certificate chain for a server that demands mutual TLS
     *     ({@code tls_client_auth}), or {@code null}; needs {@code clientKeyPem}
     * @param clientKeyPem a PEM file holding the client's unencrypted PKCS#8 private key ({@code BEGIN PRIVATE KEY}, RSA or
     *     EC), or {@code null}
     * @return the context
     * @throws RuvioException when a file cannot be read or holds no certificate or key of the expected kind
     */
    public static SSLContext context(Path caPem, Path clientCertPem, Path clientKeyPem) {
        if ((clientCertPem == null) != (clientKeyPem == null)) {
            throw new RuvioException("clientCertPem and clientKeyPem must be given together");
        }
        try {
            TrustManager[] trust = null;
            if (caPem != null) {
                KeyStore roots = KeyStore.getInstance(KeyStore.getDefaultType());
                roots.load(null, null);
                int n = 0;
                for (Certificate c : certificates(caPem)) {
                    roots.setCertificateEntry("ca-" + n++, c);
                }
                if (n == 0) {
                    throw new RuvioException(caPem + " holds no certificate");
                }
                TrustManagerFactory factory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
                factory.init(roots);
                trust = factory.getTrustManagers();
            }
            KeyManagerFactory keys = null;
            if (clientCertPem != null) {
                Collection<? extends Certificate> chain = certificates(clientCertPem);
                if (chain.isEmpty()) {
                    throw new RuvioException(clientCertPem + " holds no certificate");
                }
                KeyStore store = KeyStore.getInstance(KeyStore.getDefaultType());
                store.load(null, null);
                char[] none = new char[0];
                store.setKeyEntry("client", privateKey(clientKeyPem), none, chain.toArray(new Certificate[0]));
                keys = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                keys.init(store, none);
            }
            SSLContext context = SSLContext.getInstance("TLS");
            context.init(keys == null ? null : keys.getKeyManagers(), trust, null);
            return context;
        } catch (IOException | GeneralSecurityException e) {
            throw new RuvioException("cannot build the TLS context: " + e.getMessage(), e);
        }
    }

    /** A context that accepts any certificate; the host name is not checked either (see {@code tlsInsecure}). */
    static SSLContext insecureContext() {
        try {
            SSLContext context = SSLContext.getInstance("TLS");
            context.init(null, new TrustManager[] {new AcceptAll()}, null);
            return context;
        } catch (GeneralSecurityException e) {
            throw new RuvioException("cannot build the TLS context: " + e.getMessage(), e);
        }
    }

    private static Collection<? extends Certificate> certificates(Path pem) throws IOException, GeneralSecurityException {
        try (InputStream in = Files.newInputStream(pem)) {
            return CertificateFactory.getInstance("X.509").generateCertificates(in);
        }
    }

    private static PrivateKey privateKey(Path pem) throws IOException, GeneralSecurityException {
        String text = Files.readString(pem, StandardCharsets.US_ASCII);
        int begin = text.indexOf("-----BEGIN PRIVATE KEY-----");
        int end = text.indexOf("-----END PRIVATE KEY-----");
        if (begin < 0 || end < 0) {
            throw new RuvioException(pem + " holds no unencrypted PKCS#8 key (BEGIN PRIVATE KEY)");
        }
        String body = text.substring(begin + "-----BEGIN PRIVATE KEY-----".length(), end).replaceAll("\\s", "");
        PKCS8EncodedKeySpec spec = new PKCS8EncodedKeySpec(Base64.getDecoder().decode(body));
        GeneralSecurityException last = null;
        for (String algorithm : new String[] {"RSA", "EC"}) {
            try {
                return KeyFactory.getInstance(algorithm).generatePrivate(spec);
            } catch (GeneralSecurityException e) {
                last = e;
            }
        }
        throw last;
    }

    // An extended trust manager: a plain X509TrustManager would still get the host name checked by the JDK.
    private static final class AcceptAll extends X509ExtendedTrustManager {
        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType) {
        }

        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType) {
        }

        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType, Socket socket) {
        }

        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType, Socket socket) {
        }

        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType, SSLEngine engine) {
        }

        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType, SSLEngine engine) {
        }

        @Override
        public X509Certificate[] getAcceptedIssuers() {
            return new X509Certificate[0];
        }
    }
}
