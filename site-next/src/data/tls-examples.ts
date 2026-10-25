// Connecting to a server that has TLS on (`tls_cert` and `tls_key`), one snippet per client.
// Each one trusts a private CA from `ca.pem`; leave that out to use the system roots.
import type { ClientSlug } from "./client-examples";

export const tlsExamples: Record<ClientSlug, string> = {
  rust: `// Cargo.toml: ruvio-client = { version = "...", features = ["tls"] }
use ruvio_client::{Client, ClientOptions, TlsOptions};

let mut db = Client::connect_with(ClientOptions {
    host: "cache.internal".to_owned(),
    tls: Some(TlsOptions {
        ca_pem: Some(std::fs::read("ca.pem")?),
        ..TlsOptions::default()
    }),
    ..ClientOptions::default()
})?;`,
  csharp: `var roots = new X509Certificate2Collection();
roots.ImportFromPemFile("ca.pem");

await using var connection = await RuvioConnection.ConnectAsync(new RuvioClientOptions
{
    Host = "cache.internal",
    Tls = new RuvioTlsOptions { TrustedCertificates = roots },
});`,
  go: `pem, _ := os.ReadFile("ca.pem")
roots := x509.NewCertPool()
roots.AppendCertsFromPEM(pem)

c, err := ruvio.Connect(ctx, ruvio.Options{
	Addr: "cache.internal:6379",
	TLS:  &tls.Config{RootCAs: roots},
})`,
  java: `RuvioOptions options = RuvioOptions.builder()
        .host("cache.internal")
        .tls(RuvioTls.context(Path.of("ca.pem"), null, null))
        .build();`,
  python: `client = ruvio.Ruvio("cache.internal", 6379, tls=True, tls_ca_file="ca.pem")
# or a URL: ruvios:// (rediss:// is the same)
client = ruvio.Ruvio.from_url("ruvios://cache.internal:6379")`,
  node: `import { createClient } from 'ruvio-client';
import { readFileSync } from 'node:fs';

const c = createClient({ host: 'cache.internal', port: 6379, tls: { ca: readFileSync('ca.pem') } });
// or a URL: ruvios:// (rediss:// is the same)`,
};
