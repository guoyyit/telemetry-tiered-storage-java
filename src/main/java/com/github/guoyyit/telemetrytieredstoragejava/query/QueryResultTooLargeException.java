package com.github.guoyyit.telemetrytieredstoragejava.query;

/** Thrown when a query would materialize more than the configured result bound. */
public class QueryResultTooLargeException extends RuntimeException {
    public QueryResultTooLargeException(String message) {
        super(message);
    }
}
