package com.github.guoyyit.telemetrytieredstoragejava.tests;

import com.github.guoyyit.telemetrytieredstoragejava.config.TieredStorageProperties;
import com.github.guoyyit.telemetrytieredstoragejava.engine.TieredStorageEngine;
import com.github.guoyyit.telemetrytieredstoragejava.query.Cancellation;
import com.github.guoyyit.telemetrytieredstoragejava.query.QueryCancelledException;
import com.github.guoyyit.telemetrytieredstoragejava.query.QueryOptions;
import com.github.guoyyit.telemetrytieredstoragejava.query.QueryTimeoutException;
import com.github.guoyyit.telemetrytieredstoragejava.tests.support.MutableClock;
import com.github.guoyyit.telemetrytieredstoragejava.tests.support.TestSupport;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/** Requirement 7: slow queries can be cancelled or time out, leaving no debris. */
class QueryTimeoutTest {

    @TempDir
    Path dir;

    @Test
    void expiredDeadlineAbortsTheQuery() throws Exception {
        TieredStorageProperties props = TestSupport.props(dir);
        try (TieredStorageEngine engine = new TieredStorageEngine(props, MutableClock.utcNow())) {
            engine.write("cpu", TestSupport.samples("cpu", 1_000, 100));
            QueryOptions options = QueryOptions.ofTimeout(1);
            Thread.sleep(5); // let the deadline lapse before the query even starts
            assertThrows(QueryTimeoutException.class,
                    () -> engine.query("cpu", 0, Long.MAX_VALUE, options));
        }
    }

    @Test
    void cancelledQueryAbortsAndLeavesNoDebris() throws Exception {
        TieredStorageProperties props = TestSupport.props(dir);
        MutableClock clock = MutableClock.utcNow();
        try (TieredStorageEngine engine = new TieredStorageEngine(props, clock)) {
            engine.write("cpu", TestSupport.samples("cpu", clock.millis() - 3_600_000, 200));
            engine.migrateOnce();
            TieredStorageEngine.Stats before = engine.stats();

            Cancellation cancellation = new Cancellation();
            cancellation.cancel();
            QueryOptions options = QueryOptions.of(60_000L, cancellation);
            assertThrows(QueryCancelledException.class,
                    () -> engine.query("cpu", 0, Long.MAX_VALUE, options));

            // No partial migration state, no temp files, stats unchanged.
            TieredStorageEngine.Stats after = engine.stats();
            assertEquals(before.hotSamples(), after.hotSamples());
            assertEquals(before.coldBlocks(), after.coldBlocks());
            assertEquals(before.coldSamples(), after.coldSamples());
            try (Stream<Path> walk = Files.walk(dir)) {
                assertEquals(0, walk.filter(p -> p.toString().endsWith(".tmp")).count(),
                        "cancelled query must not leave temp files");
            }
            // The engine is still fully functional.
            assertEquals(200, engine.query("cpu", 0, Long.MAX_VALUE, QueryOptions.defaults()).size());
        }
    }

    @Test
    void timedOutQueryDoesNotAffectConcurrentWriter() throws Exception {
        TieredStorageProperties props = TestSupport.props(dir);
        try (TieredStorageEngine engine = new TieredStorageEngine(props, MutableClock.utcNow())) {
            QueryOptions expired = QueryOptions.ofTimeout(1);
            Thread.sleep(5);
            assertThrows(QueryTimeoutException.class,
                    () -> engine.query("cpu", 0, Long.MAX_VALUE, expired));
            // Writes after a timed-out query proceed normally.
            engine.write("cpu", TestSupport.samples("cpu", 5_000, 10));
            assertEquals(10, engine.query("cpu", 0, Long.MAX_VALUE, QueryOptions.defaults()).size());
        }
    }
}
