// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Probe for {@code scripts/client_faults.py}: writes {@code fault:<name>:<i>} = {@code i} with
 * durability replicas 1, reads each key back, and keeps going through a failover until a line
 * arrives on stdin. Environment: {@code RUVIO_SEEDS}, {@code RUVIO_PROBE_NAME} and, for TLS,
 * {@code RUVIO_PROBE_CA} (PEM path). Output, one line each: {@code ready}, {@code ack <i>},
 * {@code err <i> <text>}, {@code bad <i> <value>}, {@code done}.
 */
public final class FaultProbe {
    private FaultProbe() {
    }

    /**
     * Runs the probe.
     *
     * @param args unused
     * @throws Exception when the client cannot be built
     */
    public static void main(String[] args) throws Exception {
        String name = System.getenv("RUVIO_PROBE_NAME");
        String ca = System.getenv("RUVIO_PROBE_CA");
        RuvioOptions.Builder builder = RuvioOptions.builder().seeds(System.getenv("RUVIO_SEEDS").split(",")).durability(1);
        if (ca != null && !ca.isEmpty()) {
            builder.tls(RuvioTls.context(Path.of(ca), null, null));
        }
        AtomicBoolean stop = new AtomicBoolean();
        Thread reader = new Thread(() -> {
            try {
                new BufferedReader(new InputStreamReader(System.in)).readLine();
            } catch (java.io.IOException ignored) {
                // stdin closed: stop
            }
            stop.set(true);
        });
        reader.setDaemon(true);
        reader.start();
        try (RuvioClient<String, String> client = RuvioClient.connect(builder.build())) {
            System.out.println("ready");
            System.out.flush();
            for (int i = 1; !stop.get();) {
                String key = "fault:" + name + ":" + i;
                try {
                    client.set(key, Integer.toString(i));
                    String got = client.get(key);
                    if (!Integer.toString(i).equals(got)) {
                        System.out.println("bad " + i + " " + got);
                    }
                    System.out.println("ack " + i);
                    System.out.flush();
                    i++;
                } catch (RuvioException e) {
                    System.out.println("err " + i + " " + e.getClass().getSimpleName());
                    System.out.flush();
                    Thread.sleep(20);
                }
            }
        }
        System.out.println("done");
        System.out.flush();
    }
}
