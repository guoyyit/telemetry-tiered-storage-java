package com.github.guoyyit.telemetrytieredstoragejava.query;

import java.util.concurrent.atomic.AtomicBoolean;

/** Cooperative cancellation token for long-running queries. */
public class Cancellation {

    private final AtomicBoolean cancelled = new AtomicBoolean(false);

    public void cancel() {
        cancelled.set(true);
    }

    public boolean isCancelled() {
        return cancelled.get();
    }
}
