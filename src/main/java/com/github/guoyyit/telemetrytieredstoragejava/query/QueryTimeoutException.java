package com.github.guoyyit.telemetrytieredstoragejava.query;

/** Thrown when a query exceeds its deadline. No partial state is left behind. */
public class QueryTimeoutException extends RuntimeException {
    public QueryTimeoutException(String message) {
        super(message);
    }
}
