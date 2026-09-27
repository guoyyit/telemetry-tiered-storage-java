package com.github.guoyyit.telemetrytieredstoragejava.migration;

import com.github.guoyyit.telemetrytieredstoragejava.config.StorageProperties;
import com.github.guoyyit.telemetrytieredstoragejava.model.TelemetrySample;
import com.github.guoyyit.telemetrytieredstoragejava.storage.StorageEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Periodically moves samples between the hot and cold tiers based on
 * sample age and recent access heat. Cooling is journaled by the engine,
 * so a crash mid-pass is rolled forward on restart.
 */
public final class TieringManager {

    private static final Logger log = LoggerFactory.getLogger(TieringManager.class);

    private final StorageEngine engine;
    private final StorageProperties properties;
    private ScheduledExecutorService scheduler;

    public TieringManager(StorageEngine engine, StorageProperties properties) {
        this.engine = engine;
        this.properties = properties;
    }

    /**
     * Runs one cooling + warming pass. Cooling proceeds in bounded batches
     * so a single pass never holds unbounded memory. Returns the number of
     * migrated samples.
     */
    public int runOnce() {
        int migrated = 0;
        List<TelemetrySample> batch;
        do {
            batch = engine.findCoolingCandidates(properties.getMigrationBatchSize());
            if (!batch.isEmpty()) {
                migrated += engine.migrateToCold(batch);
            }
        } while (batch.size() >= properties.getMigrationBatchSize());
        engine.warmUpHotSamples();
        return migrated;
    }

    public synchronized void start() {
        if (scheduler != null) {
            return;
        }
        engine.resumePendingMigrations();
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "tiering-manager");
            t.setDaemon(true);
            return t;
        });
        long intervalMillis = Math.max(100, properties.getScanInterval().toMillis());
        scheduler.scheduleWithFixedDelay(() -> {
            try {
                runOnce();
            } catch (Throwable t) {
                log.warn("tiering pass failed", t);
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
