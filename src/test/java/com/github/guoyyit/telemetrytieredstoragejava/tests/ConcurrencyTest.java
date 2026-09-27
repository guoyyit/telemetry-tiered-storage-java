package com.github.guoyyit.telemetrytieredstoragejava.tests;

import com.github.guoyyit.telemetrytieredstoragejava.config.TieredStorageProperties;
import com.github.guoyyit.telemetrytieredstoragejava.engine.TieredStorageEngine;
import com.github.guoyyit.telemetrytieredstoragejava.model.TelemetrySample;
import com.github.guoyyit.telemetrytieredstoragejava.query.QueryOptions;
import com.github.guoyyit.telemetrytieredstoragejava.tests.support.MutableClock;
import com.github.guoyyit.telemetrytieredstoragejava.tests.support.TestSupport;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

/** Requirements 2 & 6: concurrent writes, migrations and queries stay consistent. */
class ConcurrencyTest {

    @TempDir
    Path dir;

    @Test
    void concurrentWritesMigrationsAndQueriesNeverLoseOrDuplicateSamples() throws Exception {
        TieredStorageProperties props = TestSupport.props(dir);
        props.setMigrationBatchSize(20);
        props.setHotMaxAge(Duration.ZERO); // everything is immediately eligible for demotion
        MutableClock clock = MutableClock.utcNow();
        int writers = 4;
        int batchesPerWriter = 25;
        int batchSize = 20;
        int expected = writers * batchesPerWriter * batchSize;

        try (TieredStorageEngine engine = new TieredStorageEngine(props, clock)) {
            ExecutorService pool = Executors.newFixedThreadPool(writers + 3);
            CountDownLatch start = new CountDownLatch(1);
            AtomicBoolean stop = new AtomicBoolean(false);
            ConcurrentLinkedQueue<Throwable> failures = new ConcurrentLinkedQueue<>();
            Set<String> writtenIds = ConcurrentHashMap.newKeySet();

            // Writers
            List<Future<?>> writerFutures = new ArrayList<>();
            for (int w = 0; w < writers; w++) {
                String stream = "s" + (w % 2); // two streams, shared by writers
                writerFutures.add(pool.submit(() -> {
                    await(start);
                    for (int b = 0; b < batchesPerWriter && !stop.get(); b++) {
                        List<TelemetrySample> batch = TestSupport.samples(stream, clock.millis() - 3_600_000 + b * 1000, batchSize);
                        writtenIds.addAll(engine.write(stream, batch));
                    }
                    return null;
                }));
            }
            // Migrator
            pool.submit(() -> {
                await(start);
                while (!stop.get()) {
                    engine.migrateOnce();
                }
                return null;
            });
            // Query verifier: full-range queries must never show duplicates.
            pool.submit(() -> {
                await(start);
                while (!stop.get()) {
                    for (String stream : List.of("s0", "s1")) {
                        List<TelemetrySample> all = engine.query(stream, 0, Long.MAX_VALUE, QueryOptions.defaults());
                        Set<String> seen = new HashSet<>();
                        for (TelemetrySample s : all) {
                            if (!seen.add(s.id())) {
                                failures.add(new AssertionError("duplicate sample in query: " + s.id()));
                                stop.getAndSet(true);
                            }
                        }
                        for (int i = 1; i < all.size(); i++) {
                            if (all.get(i - 1).timestamp() > all.get(i).timestamp()) {
                                failures.add(new AssertionError("query result not time ordered"));
                                stop.getAndSet(true);
                            }
                        }
                    }
                }
                return null;
            });
            // Retention enforcer (nothing expires: retention is far in the future)
            pool.submit(() -> {
                await(start);
                while (!stop.get()) {
                    engine.enforceRetentionOnce();
                }
                return null;
            });

            start.countDown();
            for (Future<?> f : writerFutures) {
                f.get(120, TimeUnit.SECONDS);
            }
            stop.set(true);
            pool.shutdown();
            assertTrue(pool.awaitTermination(60, TimeUnit.SECONDS), "workers did not finish in time");

            assertNull(failures.poll(), "concurrent run produced a failure");
            assertEquals(expected, writtenIds.size());

            // Final verification: every committed sample is queryable exactly once.
            Set<String> remaining = new HashSet<>(writtenIds);
            for (String stream : List.of("s0", "s1")) {
                for (TelemetrySample s : engine.query(stream, 0, Long.MAX_VALUE, QueryOptions.defaults())) {
                    assertTrue(remaining.remove(s.id()), "unexpected or duplicate sample " + s.id());
                }
            }
            assertTrue(remaining.isEmpty(), remaining.size() + " committed samples missing after concurrent migration");
            assertEquals(expected, engine.stats().hotSamples() + engine.stats().coldSamples());
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }
}
