package com.github.guoyyit.telemetrytieredstoragejava.query;

import com.github.guoyyit.telemetrytieredstoragejava.config.StorageProperties;
import com.github.guoyyit.telemetrytieredstoragejava.model.TelemetrySample;
import com.github.guoyyit.telemetrytieredstoragejava.storage.StorageEngine;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Executes cross-tier queries with timeout and cancellation support.
 *
 * <p>Queries are read-only: they never mutate migration state and never
 * create files, so cancelling or timing out a query cannot leave partial
 * migration state or orphan files behind. A timed-out query is interrupted
 * and its resources are released immediately.
 */
public final class QueryService implements AutoCloseable {

    private final StorageEngine engine;
    private final StorageProperties properties;
    private final ExecutorService executor;

    public QueryService(StorageEngine engine, StorageProperties properties) {
        this.engine = engine;
        this.properties = properties;
        this.executor = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "query-worker");
            t.setDaemon(true);
            return t;
        });
    }

    /** Query with the configured default timeout. */
    public List<TelemetrySample> query(String stream, long fromInclusive, long toExclusive) {
        return query(stream, fromInclusive, toExclusive, properties.getQueryTimeout());
    }

    /**
     * Query with an explicit timeout.
     *
     * @throws QueryCancelledException if the timeout expires or the query is cancelled
     */
    public List<TelemetrySample> query(String stream, long fromInclusive, long toExclusive,
                                       Duration timeout) {
        Future<List<TelemetrySample>> future =
                executor.submit(() -> engine.query(stream, fromInclusive, toExclusive));
        try {
            return future.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            future.cancel(true);
            throw new QueryCancelledException(
                    "query on stream '" + stream + "' timed out after " + timeout);
        } catch (CancellationException e) {
            throw new QueryCancelledException("query on stream '" + stream + "' was cancelled");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            future.cancel(true);
            throw new QueryCancelledException("query on stream '" + stream + "' was interrupted");
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException re) {
                throw re;
            }
            throw new RuntimeException("query failed", cause);
        }
    }

    @Override
    public void close() {
        executor.shutdownNow();
    }
}
