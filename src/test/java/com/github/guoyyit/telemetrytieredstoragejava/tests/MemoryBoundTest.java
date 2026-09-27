package com.github.guoyyit.telemetrytieredstoragejava.tests;

import com.github.guoyyit.telemetrytieredstoragejava.config.TieredStorageProperties;
import com.github.guoyyit.telemetrytieredstoragejava.engine.TieredStorageEngine;
import com.github.guoyyit.telemetrytieredstoragejava.model.TelemetrySample;
import com.github.guoyyit.telemetrytieredstoragejava.query.QueryOptions;
import com.github.guoyyit.telemetrytieredstoragejava.query.QueryResultTooLargeException;
import com.github.guoyyit.telemetrytieredstoragejava.tests.support.MutableClock;
import com.github.guoyyit.telemetrytieredstoragejava.tests.support.TestSupport;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/** Requirement 6: hot tier memory stays bounded; result sets stay bounded. */
class MemoryBoundTest {

    @TempDir
    Path dir;

    @Test
    void hotTierNeverExceedsItsBoundUnderSustainedWrites() {
        TieredStorageProperties props = TestSupport.props(dir);
        props.setMaxHotSamples(100);
        props.setMigrationBatchSize(10);
        props.setHotMaxAge(Duration.ofDays(365)); // age-based demotion disabled: only the bound protects us
        MutableClock clock = MutableClock.utcNow();

        try (TieredStorageEngine engine = new TieredStorageEngine(props, clock)) {
            int total = 1_000;
            Set<String> allIds = new HashSet<>();
            for (int batch = 0; batch < total / 50; batch++) {
                List<TelemetrySample> samples = TestSupport.samples("cpu", clock.millis() + batch * 1_000, 50);
                allIds.addAll(engine.write("cpu", samples));
                assertTrue(engine.stats().hotSamples() <= 100,
                        "hot tier exceeded its bound: " + engine.stats().hotSamples());
            }
            // Nothing lost: everything is queryable across tiers.
            List<TelemetrySample> all = engine.query("cpu", 0, Long.MAX_VALUE, QueryOptions.defaults());
            assertEquals(total, all.size());
            Set<String> found = new HashSet<>();
            all.forEach(s -> found.add(s.id()));
            assertEquals(allIds, found);

            // Bound still holds after a restart (cold data stays cold).
            try (TieredStorageEngine reopened = new TieredStorageEngine(props, clock)) {
                assertTrue(reopened.stats().hotSamples() <= 100);
                assertEquals(total, reopened.query("cpu", 0, Long.MAX_VALUE, QueryOptions.defaults()).size());
            }
        }
    }

    @Test
    void resultSetBoundRejectsOversizedQueries() {
        TieredStorageProperties props = TestSupport.props(dir);
        props.setMaxResultSamples(100);
        try (TieredStorageEngine engine = new TieredStorageEngine(props, MutableClock.utcNow())) {
            engine.write("cpu", TestSupport.samples("cpu", 1_000, 500));
            assertThrows(QueryResultTooLargeException.class,
                    () -> engine.query("cpu", 0, Long.MAX_VALUE, QueryOptions.defaults()));
            // A bounded window still works.
            assertEquals(50, engine.query("cpu", 1_000, 1_049, QueryOptions.defaults()).size());
        }
    }
}
