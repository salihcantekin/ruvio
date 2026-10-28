// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio;

import java.io.BufferedReader;
import java.io.InputStreamReader;

/**
 * Probe for {@code scripts/seed_failover.py}: connects with the seeds in {@code RUVIO_SEEDS}, reads
 * {@code seed:name}, waits for a line on stdin (the first server is stopped by then) and reads it again.
 *
 * <pre>
 * mvn -q test-compile
 * scripts/seed_failover.py target/debug/ruvio -- java -cp clients/java/target/classes:clients/java/target/test-classes io.github.salihcantekin.ruvio.SeedProbe
 * </pre>
 */
public final class SeedProbe {
    private SeedProbe() {
    }

    /**
     * Runs the probe.
     *
     * @param args unused
     * @throws Exception when the first read fails
     */
    public static void main(String[] args) throws Exception {
        String seeds = System.getenv("RUVIO_SEEDS");
        if (seeds == null) {
            throw new IllegalStateException("RUVIO_SEEDS is set by the script");
        }
        try (RuvioClient<String, String> client = RuvioClient.connect(RuvioOptions.builder().seeds(seeds.split(",")).build())) {
            String first = client.get("seed:name");
            System.out.println("first " + (first == null ? "" : first));
            System.out.flush();
            new BufferedReader(new InputStreamReader(System.in)).readLine();
            for (int i = 0; i < 50; i++) {
                try {
                    String value = client.get("seed:name");
                    System.out.println("after " + (value == null ? "" : value));
                    System.out.flush();
                    return;
                } catch (RuvioException e) {
                    Thread.sleep(100);
                }
            }
        }
        System.exit(1);
    }
}
