package com.github.guoyyit.telemetrytieredstoragejava.retention;

import com.github.guoyyit.telemetrytieredstoragejava.config.StorageProperties;
import com.github.guoyyit.telemetrytieredstoragejava.storage.StorageEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Applies the configured retention policy: raw hot samples older than
 * {@code rawRetention} are deleted (their cold archive copy, if any, is
 * unaffected), and cold archives whose whole time range is older than
 * {@code downsampledRetention} are deleted. Deletion happens in bounded
 * batches under the engine's write lock, so concurrent writes, queries and
 * migrations never observe torn state or dangling references.
 */
public final class RetentionManager {

    private static final Logger log = LoggerFactory.getLogger(RetentionManager.class);

    private final StorageEngine engine;
    private final StorageProperties properties;
    private final Supplier<Long> clock;
    private ScheduledExecutorService scheduler;

    public RetentionManager(StorageEngine engine, StorageProperties properties) {
        this(engine, properties, System::currentTimeMillis);
    }

    public RetentionManager(StorageEngine engine, StorageProperties properties,
                            Supplier<Long> clock) {
        this.engine = engine;
        this.properties = properties;
        this.clock = clock;
    }

    /** Runs one retention pass. Returns number of deleted samples/files. */
    public int runOnce() {
        long now = clock.get();
        long rawCutoff = now - properties.getRawRetention().toMillis();
        long coldCutoff = now - properties.getDownsampledRetention().toMillis();
        int deleted = 0;
        int batch;
        do {
            batch = engine.deleteExpiredHot(rawCutoff, properties.getMigrationBatchSize());
            deleted += batch;
        } while (batch >= properties.getMigrationBatchSize());
        deleted += engine.deleteExpiredCold(coldCutoff);
        return deleted;
    }

    public synchronized void start() {
        if (scheduler != null) {
            return;
        }
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "retention-manager");
            t.setDaemon(true);
            return t;
        });
        long intervalMillis = Math.max(100, properties.getScanInterval().toMillis());
        scheduler.scheduleWithFixedDelay(() -> {
            try {
                runOnce();
            } catch (Throwable t) {
                log.warn("retention pass failed", t);
            }
        }, intervalMillis, intervalMillis, TimeUnit.MILLISECONDS);
    }

    public synchronized void stop() {
        if (scheduler != null) {
            scheduler.shutdownNow();
            scheduler = null;
        }
    }
}
