using System.Security.Cryptography.X509Certificates;

namespace Ruvio.Client;

/// <summary>
/// TLS for every socket a client opens. Set <see cref="RuvioClientOptions.Tls"/> to <c>new RuvioTlsOptions()</c> to turn
/// it on: the server certificate chain is checked against the system trust store and must name the host you connect to.
/// </summary>
public sealed class RuvioTlsOptions
{
    /// <summary>
    /// The name the server certificate must carry, when it differs from the host you connect to (for example a
    /// connection by IP address to a certificate issued for a DNS name). Null uses the host.
    /// </summary>
    public string? ServerName { get; set; }

    /// <summary>
    /// The certificate authorities to trust, for a server whose certificate a private CA signed. When set (and not
    /// empty) only these are trusted instead of the system store, and certificate revocation is not checked. Null uses
    /// the system trust store.
    /// </summary>
    public X509Certificate2Collection? TrustedCertificates { get; set; }

    /// <summary>
    /// Client certificates (with their private keys) for a server that demands mutual TLS (<c>tls_client_auth</c>).
    /// Null sends none.
    /// </summary>
    public X509CertificateCollection? ClientCertificates { get; set; }

    /// <summary>
    /// Accept any server certificate without checking it. This removes the protection TLS gives against someone in
    /// the middle: use it for a throwaway test, never in production. Default false.
    /// </summary>
    public bool InsecureSkipVerify { get; set; }
}
