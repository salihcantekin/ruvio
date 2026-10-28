// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.internal;

import io.github.salihcantekin.ruvio.internal.CommandKeyRules.Rule;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Key positions of every command, looked up by the (case-insensitive) command word straight from the
 * encoded frame. The positions come from the server's command table ({@link CommandKeyRules}); this
 * class adds what is the client's own choice: which multi-key commands are split by slot, and the
 * channel routing of sharded Pub/Sub. A command that is not listed has its single key in the first
 * argument.
 */
public final class ClusterKeys {
    /** Split mode: the command is never split; all keys must share a slot. */
    public static final int NO_SPLIT = 0;
    /** Split mode: split by slot and put the per-key replies back in key order (MGET). */
    public static final int SPLIT_LIST = 1;
    /** Split mode: split by slot and add the integer replies (DEL, EXISTS, UNLINK, TOUCH). */
    public static final int SPLIT_SUM = 2;
    /** Split mode: split by slot and reply OK when every part succeeded (MSET). */
    public static final int SPLIT_VOID = 3;

    /** Server flag bits of the command table; see {@link CommandKeyRules.Rule#flags()}. */
    private static final int FLAG_READONLY = 2;
    private static final int FLAG_BLOCKING = 16;

    /** The routing rule of one command: where its keys are and how a cross-slot call is split. */
    public static final class Spec {
        final String name;
        final byte[] upper;
        final Rule rule;
        /** The first argument is the only key: the router hashes it without filling the key buffer. */
        final boolean first;
        /** Each key is followed by its value (MSET): a split keeps the two together. */
        final boolean pairs;
        final int split;
        /** The server flags the command readonly and not blocking: it may be served by a replica. */
        final boolean read;

        Spec(String name, Rule rule, int split) {
            this.name = name;
            this.upper = name.getBytes(StandardCharsets.US_ASCII);
            this.rule = rule;
            boolean range = rule.kind() == CommandKeyRules.RANGE;
            this.first = range && rule.a() == 1 && rule.b() == 1;
            this.pairs = range && rule.c() == 2;
            this.split = split;
            this.read = (rule.flags() & (FLAG_READONLY | FLAG_BLOCKING)) == FLAG_READONLY;
        }
    }

    private static final String[] NO_WORDS = new String[0];
    private static final Spec DEFAULT = new Spec("", new Rule(CommandKeyRules.RANGE, 1, 1, 1, NO_WORDS, 0), NO_SPLIT);
    private static final Spec[] TABLE = new Spec[1024];
    private static final int MASK = TABLE.length - 1;

    private static void add(Spec s) {
        int h = hash(s.upper, 0, s.upper.length) & MASK;
        while (TABLE[h] != null) {
            h = (h + 1) & MASK;
        }
        TABLE[h] = s;
    }

    static {
        Map<String, Integer> splits = Map.of("MGET", SPLIT_LIST, "MSET", SPLIT_VOID,
                "DEL", SPLIT_SUM, "EXISTS", SPLIT_SUM, "TOUCH", SPLIT_SUM, "UNLINK", SPLIT_SUM);
        // Sharded Pub/Sub goes to the node that owns the channel's slot, so the channels stand in for keys.
        Map<String, Rule> channels = Map.of(
                "SPUBLISH", new Rule(CommandKeyRules.RANGE, 1, 1, 1, NO_WORDS, 0),
                "SSUBSCRIBE", new Rule(CommandKeyRules.RANGE, 1, -1, 1, NO_WORDS, 0),
                "SUNSUBSCRIBE", new Rule(CommandKeyRules.RANGE, 1, -1, 1, NO_WORDS, 0));
        for (Map.Entry<String, Rule> e : CommandKeyRules.RULES.entrySet()) {
            String name = e.getKey();
            add(new Spec(name, channels.getOrDefault(name, e.getValue()), splits.getOrDefault(name, NO_SPLIT)));
        }
    }

    private ClusterKeys() {
    }

    private static int hash(byte[] b, int off, int len) {
        int h = 0;
        for (int i = 0; i < len; i++) {
            h = h * 31 + upper(b[off + i]);
        }
        return h ^ (h >>> 15);
    }

    private static int upper(byte c) {
        return c >= 'a' && c <= 'z' ? c - 32 : c;
    }

    /**
     * Looks up the routing rule of the command word {@code name[off, off + len)}.
     *
     * @param name the array holding the command word
     * @param off where the word starts
     * @param len the word length
     * @return the rule; a default single-key rule for an unlisted command
     */
    public static Spec spec(byte[] name, int off, int len) {
        int h = hash(name, off, len) & MASK;
        for (Spec s; (s = TABLE[h]) != null; h = (h + 1) & MASK) {
            byte[] u = s.upper;
            if (u.length == len) {
                int i = 0;
                while (i < len && u[i] == upper(name[off + i])) {
                    i++;
                }
                if (i == len) {
                    return s;
                }
            }
        }
        return DEFAULT;
    }

    /**
     * Looks up the routing rule of a command word.
     *
     * @param name the command name (any case)
     * @return the rule; a default single-key rule for an unlisted command
     */
    public static Spec spec(String name) {
        byte[] b = name.getBytes(StandardCharsets.UTF_8);
        return spec(b, 0, b.length);
    }

    /**
     * Fills {@code f.keyIdx} with the argument indexes of the command's keys, in argument order. {@code f} must be parsed.
     *
     * @param spec the command's rule
     * @param f the parsed frame
     * @return how many keys there are (0 for keyless commands)
     */
    public static int keys(Spec spec, Frame f) {
        int argc = f.argc;
        if (argc < 2) {
            return 0;
        }
        Rule r = spec.rule;
        switch (r.kind()) {
            case CommandKeyRules.RANGE:
                return range(f, r.a(), r.b() < 0 ? argc + r.b() : Math.min(r.b(), argc - 1), r.c());
            case CommandKeyRules.NUMKEYS:
                return counted(f, r.a(), r.b());
            case CommandKeyRules.KEYWORD_HALF: {
                String word = r.keywords()[0];
                for (int i = 1; i < argc; i++) {
                    if (equalsIgnoreCase(f, i, word)) {
                        return range(f, i + 1, i + (argc - i - 1) / 2, 1);
                    }
                }
                return 0;
            }
            case CommandKeyRules.KEY_AND_STORE: {
                int first = r.a();
                if (first >= argc) {
                    return 0;
                }
                int[] k = f.keyBuffer(1 + (argc - first) / 2);
                int n = 0;
                k[n++] = first;
                for (int i = first + 1; i + 1 < argc; i++) {
                    if (isAny(f, i, r.keywords())) {
                        k[n++] = ++i;
                    }
                }
                return n;
            }
            default:
                return 0;
        }
    }

    /** Arguments {@code from..=last} every {@code step}. */
    private static int range(Frame f, int from, int last, int step) {
        if (last < from) {
            return 0;
        }
        int n = (last - from) / step + 1;
        int[] k = f.keyBuffer(n);
        for (int i = 0; i < n; i++) {
            k[i] = from + i * step;
        }
        return n;
    }

    /** {@code numkeys} at {@code countIndex}, then the keys; {@code before}, when not 0, is one more key (the destination of *STORE). */
    private static int counted(Frame f, int countIndex, int before) {
        int argc = f.argc;
        int prefix = before > 0 && before < argc ? 1 : 0;
        if (countIndex >= argc) {
            return single(f, before, prefix);
        }
        byte[] b = f.out.buf;
        int p = f.off[countIndex];
        int l = f.len[countIndex];
        long count = 0;
        if (l == 0 || l > 9) {
            return single(f, before, prefix);
        }
        for (int i = 0; i < l; i++) {
            int d = b[p + i] - '0';
            if (d < 0 || d > 9) {
                return single(f, before, prefix);
            }
            count = count * 10 + d;
        }
        int available = argc - countIndex - 1;
        int n = (int) Math.min(count, available);
        int[] k = f.keyBuffer(n + prefix);
        if (prefix > 0) {
            k[0] = before;
        }
        for (int i = 0; i < n; i++) {
            k[prefix + i] = countIndex + 1 + i;
        }
        return n + prefix;
    }

    private static int single(Frame f, int at, int n) {
        if (n > 0) {
            f.keyBuffer(1)[0] = at;
        }
        return n;
    }

    private static boolean isAny(Frame f, int arg, String[] words) {
        for (String w : words) {
            if (equalsIgnoreCase(f, arg, w)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Compares an argument with an upper-case word, ignoring case.
     *
     * @param f the parsed frame
     * @param arg the argument index
     * @param word the upper-case word
     * @return {@code true} when equal
     */
    public static boolean equalsIgnoreCase(Frame f, int arg, String word) {
        int l = f.len[arg];
        if (l != word.length()) {
            return false;
        }
        byte[] b = f.out.buf;
        int p = f.off[arg];
        for (int i = 0; i < l; i++) {
            if (upper(b[p + i]) != word.charAt(i)) {
                return false;
            }
        }
        return true;
    }
}
