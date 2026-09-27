package com.github.guoyyit.telemetrytieredstoragejava.tests;

import com.github.guoyyit.telemetrytieredstoragejava.config.StorageProperties;
import com.github.guoyyit.telemetrytieredstoragejava.model.TelemetrySample;
import com.github.guoyyit.telemetrytieredstoragejava.retention.RetentionManager;
import com.github.guoyyit.telemetrytieredstoragejava.storage.StorageEngine;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Retention deletes expired data without losing committed samples or
 * disturbing concurrent writes and migrations.
 */
class RetentionTest {

    @TempDir
    Path dataDir;

    private StorageProperties properties() {
        StorageProperties p = new StorageProperties();
        p.setDataDir(dataDir.toString());
        p.setHotMaxAge(Duration.ofMillis(1));
        p.setCoolAfterIdle(Duration.ZERO);
        p.setMigrationBatchSize(50);
        p.setMaxHotSamples(100_000);
        p.setRawRetention(Duration.ofMinutes(10));
        p.setDownsampledRetention(Duration.ofHours(1));
        return p;
    }

    @Test
    void expiredHotSamplesAreDeletedAndFreshOnesKept() {
        StorageProperties props = properties();
        StorageEngine engine = new StorageEngine(props);
        engine.recover();
        AtomicLong clock = new AtomicLong(System.currentTimeMillis());
        RetentionManager retention = new RetentionManager(engine, props, clock::get);

        long now = clock.get();
        TelemetrySample old = engine.write("cpu", now - Duration.ofHours(2).toMillis(), Map.of(), 1);
        TelemetrySample fresh = engine.write("cpu", now, Map.of(), 2);

        int deleted = retention.runOnce();
        assertEquals(1, deleted);
        List<TelemetrySample> remaining = engine.query("cpu", 0, now + 1000);
        assertEquals(List.of(fresh.getId()),
                remaining.stream().map(TelemetrySample::getId).toList());
        engine.close();
    }

    @Test
    void expiredColdArchivesAreDeletedByDownsampledRetention() {
        StorageProperties props = properties();
        StorageEngine engine = new StorageEngine(props);
        engine.recover();
        AtomicLong clock = new AtomicLong(System.currentTimeMillis());
        RetentionManager retention = new RetentionManager(engine, props, clock::get);

        long now = clock.get();
        // cool two batches: one very old, one recent
        for (int i = 0; i < 10; i++) {
            engine.write("cpu", now - Duration.ofHours(3).toMillis() + i, Map.of(), i);
        }
        engine.migrateToCold(engine.findCoolingCandidates(100));
        for (int i = 0; i < 10; i++) {
            engine.write("cpu", now - Duration.ofMinutes(30).toMillis() + i, Map.of(), 100 + i);
        }
        engine.migrateToCold(engine.findCoolingCandidates(100));
        assertEquals(20, engine.query("cpu", 0, now + 1000).size());

        retention.runOnce();

        List<TelemetrySample> remaining = engine.query("cpu", 0, now + 1000);
        assertEquals(10, remaining.size(), "3h-old archive must be gone, 30m-old kept");
        for (TelemetrySample s : remaining) {
            assertTrue(s.getValue() >= 100);
        }
        engine.close();
    }

    @Test
    void deletionDoesNotLeaveDanglingReferencesAfterRestart() {
        StorageProperties props = properties();
        long now = System.currentTimeMillis();

        StorageEngine engine = new StorageEngine(props);
        engine.recover();
        RetentionManager retention = new RetentionManager(engine, props);
        engine.write("cpu", now - Duration.ofHours(2).toMillis(), Map.of(), 1);
        engine.write("cpu", now, Map.of(), 2);
        retention.runOnce();
        engine.close();

        // restart: WAL replay must not resurrect deleted samples
        StorageEngine recovered = new StorageEngine(props);
        recovered.recover();
        List<TelemetrySample> all = recovered.query("cpu", 0, now + 1000);
        assertEquals(1, all.size());
        assertEquals(2.0, all.get(0).getValue());
        recovered.close();
    }

    @Test
    void retentionRacingWithWritesNeverLosesCommittedSamples() throws Exception {
        StorageProperties props = properties();
        StorageEngine engine = new StorageEngine(props);
        engine.recover();
        AtomicLong clock = new AtomicLong(System.currentTimeMillis());
        RetentionManager retention = new RetentionManager(engine, props, clock::get);

        long base = clock.get();
        // pre-existing expired data
        for (int i = 0; i < 200; i++) {
            engine.write("cpu", base - Duration.ofHours(2).toMillis() + i, Map.of(), i);
        }

        Set<String> freshIds = new HashSet<>();
        ConcurrentLinkedQueue<String> errors = new ConcurrentLinkedQueue<>();
        AtomicBoolean stop = new AtomicBoolean(false);
        CountDownLatch start = new CountDownLatch(1);

        Thread writer = new Thread(() -> {
            await(start);
            for (int i = 0; i < 300; i++) {
                // fresh samples, never expired
                freshIds.add(engine.write("cpu", clock.get() + i, Map.of(), i).getId());
            }
            stop.set(true);
        });
        Thread retentionThread = new Thread(() -> {
            await(start);
            while (!stop.get()) {
                retention.runOnce();
            }
        });
        Thread migrator = new Thread(() -> {
            await(start);
            while (!stop.get()) {
                List<TelemetrySample> batch = engine.findCoolingCandidates(50);
                if (!batch.isEmpty()) {
                    engine.migrateToCold(batch);
                } else {
                    Thread.yield();
                }
            }
        });

        writer.start();
        retentionThread.start();
        migrator.start();
        start.countDown();
        writer.join(30_000);
        retentionThread.join(30_000);
        migrator.join(30_000);

        assertTrue(errors.isEmpty());
        // every fresh (unexpired) sample must still be queryable
        List<TelemetrySample> all = engine.query("cpu", base, base + 10_000_000);
        Set<String> visible = new HashSet<>();
        for (TelemetrySample s : all) {
            visible.add(s.getId());
        }
        for (String id : freshIds) {
            assertTrue(visible.contains(id), "fresh committed sample lost: " + id);
        }
        engine.close();
    }

    private static void await(CountDownLatch latch) {
        try {
            assertTrue(latch.await(10, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }
}
