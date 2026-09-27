package com.github.guoyyit.telemetrytieredstoragejava.query;

/**
 * Thrown when a query is cancelled or exceeds its timeout.
 */
public class QueryCancelledException extends RuntimeException {

    public QueryCancelledException(String message) {
        super(message);
    }
}
