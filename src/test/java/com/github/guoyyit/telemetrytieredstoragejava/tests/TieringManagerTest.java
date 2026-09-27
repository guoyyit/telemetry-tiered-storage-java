package com.github.guoyyit.telemetrytieredstoragejava.tests;

import com.github.guoyyit.telemetrytieredstoragejava.config.StorageProperties;
import com.github.guoyyit.telemetrytieredstoragejava.migration.MigrationJournal;
import com.github.guoyyit.telemetrytieredstoragejava.migration.TieringManager;
import com.github.guoyyit.telemetrytieredstoragejava.model.TelemetrySample;
import com.github.guoyyit.telemetrytieredstoragejava.storage.StorageEngine;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * End-to-end tiering: old/idle samples cool down, frequently queried cold
 * samples warm back up, and the hot tier stays within its memory bound.
 */
class TieringManagerTest {

    @TempDir
    Path dataDir;

    private StorageProperties properties() {
        StorageProperties p = new StorageProperties();
        p.setDataDir(dataDir.toString());
        p.setHotMaxAge(Duration.ofMinutes(5));
        p.setCoolAfterIdle(Duration.ZERO);
        p.setMigrationBatchSize(20);
        p.setMaxHotSamples(1_000);
        return p;
    }

    private StorageEngine openEngine(StorageProperties props) {
        StorageEngine engine = new StorageEngine(props);
        engine.recover();
        engine.setMigrationJournal(new MigrationJournal(Path.of(props.getDataDir()).resolve("journal")));
        return engine;
    }

    @Test
    void oldIdleSamplesAreCooledAndStillQueryable() {
        StorageProperties props = properties();
        StorageEngine engine = openEngine(props);
        TieringManager tiering = new TieringManager(engine, props);

        long now = System.currentTimeMillis();
        for (int i = 0; i < 45; i++) {
            engine.write("cpu", now - Duration.ofHours(1).toMillis() + i, Map.of(), i);
        }
        int migrated = tiering.runOnce();
        assertEquals(45, migrated, "all old idle samples should cool, across multiple batches");
        assertEquals(0, engine.hotStore().size());

        List<TelemetrySample> all = engine.query("cpu", 0, now + 1000);
        assertEquals(45, all.size());
        engine.close();
    }

    @Test
    void freshSamplesStayHot() {
        StorageProperties props = properties();
        StorageEngine engine = openEngine(props);
        TieringManager tiering = new TieringManager(engine, props);

        long now = System.currentTimeMillis();
        for (int i = 0; i < 10; i++) {
            engine.write("cpu", now + i, Map.of(), i);
        }
        int migrated = tiering.runOnce();
        assertEquals(0, migrated);
        assertEquals(10, engine.hotStore().size());
        engine.close();
    }

    @Test
    void repeatedlyQueriedColdSamplesWarmBackUp() {
        StorageProperties props = properties();
        StorageEngine engine = openEngine(props);
        TieringManager tiering = new TieringManager(engine, props);

        long now = System.currentTimeMillis();
        long oldTs = now - Duration.ofHours(1).toMillis();
        for (int i = 0; i < 10; i++) {
            engine.write("cpu", oldTs + i, Map.of(), i);
        }
        tiering.runOnce();
        assertEquals(0, engine.hotStore().size());

        // query the cold data twice: access tracker should trigger warming
        engine.query("cpu", oldTs - 1, oldTs + 100);
        engine.query("cpu", oldTs - 1, oldTs + 100);
        int warmed = engine.warmUpHotSamples();
        assertEquals(10, warmed);
        assertEquals(10, engine.hotStore().size());

        // still exactly one logical copy of each sample
        List<TelemetrySample> all = engine.query("cpu", oldTs - 1, oldTs + 100);
        assertEquals(10, all.size());
        engine.close();
    }

    @Test
    void hotTierStaysBoundedUnderHeavyWrites() {
        StorageProperties props = properties();
        props.setMaxHotSamples(100);
        props.setMigrationBatchSize(20);
        StorageEngine engine = openEngine(props);

        long now = System.currentTimeMillis();
        for (int i = 0; i < 500; i++) {
            engine.write("cpu", now - Duration.ofHours(2).toMillis() + i, Map.of(), i);
            assertTrue(engine.hotStore().size() <= 100 + 20,
                    "hot tier exceeded bound: " + engine.hotStore().size());
        }
        // nothing lost: all 500 samples queryable across tiers
        List<TelemetrySample> all = engine.query("cpu", 0, now + 1000);
        assertEquals(500, all.size());
        engine.close();
    }
}
