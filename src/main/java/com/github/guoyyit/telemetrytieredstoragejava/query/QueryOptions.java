package com.github.guoyyit.telemetrytieredstoragejava.query;

/**
 * Per-query execution options: deadline and cooperative cancellation.
 * {@link #check()} is called periodically by the engine; on timeout/cancel it
 * throws, and the engine guarantees no partial migration state or unreclaimed
 * temp files remain (migration IO happens outside query paths entirely).
 */
public class QueryOptions {

    private static final QueryOptions DEFAULT = new QueryOptions(30_000L, null);

    private final long deadlineNanos;
    private final Cancellation cancellation;

    private QueryOptions(long timeoutMs, Cancellation cancellation) {
        this.deadlineNanos = System.nanoTime() + timeoutMs * 1_000_000L;
        this.cancellation = cancellation;
    }

    public static QueryOptions of(Long timeoutMs, Cancellation cancellation) {
        long t = timeoutMs != null && timeoutMs > 0 ? timeoutMs : 30_000L;
        return new QueryOptions(t, cancellation);
    }

    public static QueryOptions ofTimeout(long timeoutMs) {
        return new QueryOptions(timeoutMs, null);
    }

    public static QueryOptions defaults() {
        return DEFAULT;
    }

    public void check() {
        if (cancellation != null && cancellation.isCancelled()) {
            throw new QueryCancelledException("query cancelled");
        }
        if (System.nanoTime() > deadlineNanos) {
            throw new QueryTimeoutException("query exceeded its deadline");
        }
    }
}
