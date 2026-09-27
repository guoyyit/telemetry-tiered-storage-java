package com.github.guoyyit.telemetrytieredstoragejava.tests;

import com.github.guoyyit.telemetrytieredstoragejava.config.StorageProperties;
import com.github.guoyyit.telemetrytieredstoragejava.model.TelemetrySample;
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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

/**
 * While migration runs concurrently with writes and queries, every query
 * must observe a consistent view: no duplicate samples, and no committed
 * sample missing.
 */
class ConcurrentMigrationTest {

    @TempDir
    Path dataDir;

    private StorageProperties properties() {
        StorageProperties p = new StorageProperties();
        p.setDataDir(dataDir.toString());
        p.setHotMaxAge(Duration.ofMillis(1));
        p.setCoolAfterIdle(Duration.ZERO);
        p.setMigrationBatchSize(50);
        p.setMaxHotSamples(100_000);
        return p;
    }

    @Test
    void queriesNeverSeeDuplicatesOrMissingSamplesDuringMigration() throws Exception {
        StorageEngine engine = new StorageEngine(properties());
        engine.recover();
        long base = System.currentTimeMillis() - 1_000_000;

        int total = 2_000;
        List<String> committedIds = new CopyOnWriteArrayList<>();
        ConcurrentLinkedQueue<String> violations = new ConcurrentLinkedQueue<>();
        AtomicBoolean writingDone = new AtomicBoolean(false);
        CountDownLatch start = new CountDownLatch(1);

        Thread writer = new Thread(() -> {
            await(start);
            for (int i = 0; i < total; i++) {
                TelemetrySample s = engine.write("cpu", base + i, Map.of(), i);
                committedIds.add(s.getId());
            }
            writingDone.set(true);
        });

        Thread migrator = new Thread(() -> {
            await(start);
            while (!writingDone.get() || engine.hotStore().size() > 0) {
                List<TelemetrySample> batch = engine.findCoolingCandidates(50);
                if (!batch.isEmpty()) {
                    engine.migrateToCold(batch);
                } else if (!writingDone.get()) {
                    Thread.yield();
                }
            }
        });

        Thread querier = new Thread(() -> {
            await(start);
            while (!writingDone.get() || engine.hotStore().size() > 0) {
                // snapshot of what was committed before this query started
                List<String> expectedSnapshot = List.copyOf(committedIds);
                List<TelemetrySample> result = engine.query("cpu", base - 1, base + total + 1);
                Set<String> seen = new HashSet<>();
                for (TelemetrySample s : result) {
                    if (!seen.add(s.getId())) {
                        violations.add("duplicate in query result: " + s.getId());
                    }
                }
                for (String id : expectedSnapshot) {
                    if (!seen.contains(id)) {
                        violations.add("committed sample missing from query: " + id);
                        break;
                    }
                }
            }
        });

        writer.start();
        migrator.start();
        querier.start();
        start.countDown();
        writer.join(30_000);
        migrator.join(30_000);
        querier.join(30_000);

        assertTrue(violations.isEmpty(), () -> "consistency violations: " + violations);

        // final state: everything migrated to cold, all still queryable
        assertEquals(0, engine.hotStore().size());
        List<TelemetrySample> all = engine.query("cpu", base - 1, base + total + 1);
        assertEquals(total, all.size());
        assertEquals(new HashSet<>(committedIds),
                new HashSet<>(all.stream().map(TelemetrySample::getId).toList()));
        engine.close();
    }

    @Test
    void concurrentWritersAndMigratorDoNotCorruptEachOther() throws Exception {
        StorageEngine engine = new StorageEngine(properties());
        engine.recover();
        long base = System.currentTimeMillis() - 1_000_000;
        int threads = 4;
        int perThread = 500;
        List<String> allIds = new CopyOnWriteArrayList<>();
        CountDownLatch start = new CountDownLatch(1);

        Thread[] writers = new Thread[threads];
        for (int t = 0; t < threads; t++) {
            final int offset = t * perThread;
            writers[t] = new Thread(() -> {
                await(start);
                for (int i = 0; i < perThread; i++) {
                    allIds.add(engine.write("s" + (offset % 2), base + offset + i, Map.of(), i).getId());
                }
            });
            writers[t].start();
        }

        AtomicBoolean done = new AtomicBoolean(false);
        Thread migrator = new Thread(() -> {
            await(start);
            while (!done.get()) {
                List<TelemetrySample> batch = engine.findCoolingCandidates(50);
                if (!batch.isEmpty()) {
                    engine.migrateToCold(batch);
                } else {
                    Thread.yield();
                }
            }
            // drain the rest
            List<TelemetrySample> batch;
            while (!(batch = engine.findCoolingCandidates(50)).isEmpty()) {
                engine.migrateToCold(batch);
            }
        });
        migrator.start();
        start.countDown();
        for (Thread w : writers) {
            w.join(30_000);
        }
        done.set(true);
        migrator.join(30_000);

        List<TelemetrySample> result = engine.query("s0", base - 1, base + threads * perThread + 1);
        result.addAll(engine.query("s1", base - 1, base + threads * perThread + 1));
        assertEquals(new HashSet<>(allIds),
                new HashSet<>(result.stream().map(TelemetrySample::getId).toList()));
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
