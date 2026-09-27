package com.github.guoyyit.telemetrytieredstoragejava.query;

/** Thrown when a query is cancelled via its {@link Cancellation} token. */
public class QueryCancelledException extends RuntimeException {
    public QueryCancelledException(String message) {
        super(message);
    }
}
