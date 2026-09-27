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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/** Requirements 2 & 3: age/heat based demotion and promotion, cross-tier queries. */
class CrossTierQueryTest {

    @TempDir
    Path dir;

    @Test
    void agedSamplesAreDemotedAndQueriesSpanBothTiers() {
        TieredStorageProperties props = TestSupport.props(dir);
        MutableClock clock = MutableClock.utcNow();
        try (TieredStorageEngine engine = new TieredStorageEngine(props, clock)) {
            long now = clock.millis();
            // Old samples (beyond hotMaxAge=30m) and fresh samples.
            engine.write("cpu", TestSupport.samples("cpu", now - Duration.ofHours(1).toMillis(), 100));
            engine.write("cpu", TestSupport.samples("cpu", now - 1_000, 50));

            int moved = engine.migrateOnce();
            assertEquals(100, moved, "only the aged samples should be demoted");
            TieredStorageEngine.Stats stats = engine.stats();
            assertEquals(50, stats.hotSamples());
            assertEquals(100, stats.coldSamples());
            assertTrue(stats.coldBlocks() >= 1);

            // Cross-tier query: all 150 samples, ordered, no duplicates.
            List<TelemetrySample> all = engine.query("cpu", 0, Long.MAX_VALUE, QueryOptions.defaults());
            assertEquals(150, all.size());
            Set<String> ids = new HashSet<>();
            for (TelemetrySample s : all) {
                assertTrue(ids.add(s.id()), "duplicate sample after migration: " + s.id());
            }
            for (int i = 1; i < all.size(); i++) {
                assertTrue(all.get(i - 1).timestamp() <= all.get(i).timestamp());
            }
        }
    }

    @Test
    void frequentlyAccessedStreamsArePromotedBackToHot() {
        TieredStorageProperties props = TestSupport.props(dir);
        MutableClock clock = MutableClock.utcNow();
        try (TieredStorageEngine engine = new TieredStorageEngine(props, clock)) {
            long now = clock.millis();
            engine.write("gpu", TestSupport.samples("gpu", now - Duration.ofHours(1).toMillis(), 40));
            engine.migrateOnce();
            assertEquals(0, engine.stats().hotSamples());
            assertEquals(40, engine.stats().coldSamples());

            // Build up access heat (threshold is 3) by querying the stream.
            for (int i = 0; i < 3; i++) {
                engine.query("gpu", 0, Long.MAX_VALUE, QueryOptions.defaults());
            }
            int promoted = engine.migrateOnce();
            assertEquals(40, promoted, "hot stream's cold data should be promoted");
            assertEquals(40, engine.stats().hotSamples());
            assertEquals(0, engine.stats().coldSamples());

            // And while the stream is hot, aged samples must not be demoted again.
            assertEquals(0, engine.migrateOnce());
            assertEquals(40, engine.stats().hotSamples());
        }
    }

    @Test
    void migrationIsIdempotentWhenNothingQualifies() {
        TieredStorageProperties props = TestSupport.props(dir);
        MutableClock clock = MutableClock.utcNow();
        try (TieredStorageEngine engine = new TieredStorageEngine(props, clock)) {
            engine.write("mem", TestSupport.samples("mem", clock.millis(), 30));
            assertEquals(0, engine.migrateOnce(), "fresh samples must not be demoted");
            assertEquals(0, engine.migrateOnce());
            assertEquals(30, engine.stats().hotSamples());
        }
    }

    @Test
    void coldBlocksAreCompressedArchivesOrganizedByTimeRange() throws Exception {
        TieredStorageProperties props = TestSupport.props(dir);
        MutableClock clock = MutableClock.utcNow();
        try (TieredStorageEngine engine = new TieredStorageEngine(props, clock)) {
            long now = clock.millis();
            engine.write("cpu", TestSupport.samples("cpu", now - 3_600_000, 120));
            engine.migrateOnce();

            Path coldDir = dir.resolve("cold");
            try (var walk = java.nio.file.Files.walk(coldDir)) {
                List<Path> blocks = walk.filter(p -> p.toString().endsWith(".tsb")).toList();
                assertFalse(blocks.isEmpty(), "demotion must produce archive blocks");
                for (Path block : blocks) {
                    byte[] head = java.nio.file.Files.readAllBytes(block);
                    assertEquals('T', head[0]);
                    assertEquals('S', head[1]);
                    assertEquals('B', head[2]);
                    assertEquals('1', head[3]);
                    // GZIP is the configured compression: flag byte 1.
                    assertEquals(1, head[4], "block should be GZIP compressed");
                    // File name carries the time range: <minTs>-<maxTs>-<seq>.tsb
                    String name = block.getFileName().toString();
                    assertTrue(name.matches("\\d+-\\d+-\\d+\\.tsb"), "unexpected block name: " + name);
                }
            }
        }
    }
}
