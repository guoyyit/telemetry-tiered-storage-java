package com.github.guoyyit.telemetrytieredstoragejava.tests;

import com.github.guoyyit.telemetrytieredstoragejava.config.TieredStorageProperties;
import com.github.guoyyit.telemetrytieredstoragejava.engine.TieredStorageEngine;
import com.github.guoyyit.telemetrytieredstoragejava.model.TelemetrySample;
import com.github.guoyyit.telemetrytieredstoragejava.query.QueryOptions;
import com.github.guoyyit.telemetrytieredstoragejava.tests.support.MutableClock;
import com.github.guoyyit.telemetrytieredstoragejava.tests.support.TestSupport;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/** Requirement 4: configurable retention - raw period, downsampling, expiry deletion. */
class RetentionTest {

    @TempDir
    Path dir;

    private TieredStorageProperties retentionProps() {
        TieredStorageProperties props = TestSupport.props(dir);
        props.setHotMaxAge(Duration.ZERO);              // demote immediately
        props.setRawRetention(Duration.ofHours(1));     // raw kept for 1h
        props.setDownsampledRetention(Duration.ofHours(1)); // downsampled kept 1 more hour
        props.setDownsampleResolution(Duration.ofMinutes(10));
        return props;
    }

    @Test
    void oldRawDataIsDownsampledThenDeleted() {
        TieredStorageProperties props = retentionProps();
        MutableClock clock = MutableClock.utcNow();
        try (TieredStorageEngine engine = new TieredStorageEngine(props, clock)) {
            long t0 = clock.millis();
            // 120 raw samples, 1 per second, two per 10-minute bucket worth of spread.
            engine.write("cpu", TestSupport.samples("cpu", t0, 120));
            clock.advance(Duration.ofSeconds(1)); // all samples now older than the demote cutoff
            engine.migrateOnce();
            assertEquals(120, engine.stats().coldSamples());

            // Advance past raw retention (1h): data must be downsampled.
            clock.advance(Duration.ofHours(1).plusMinutes(1));
            engine.migrateOnce();
            engine.enforceRetentionOnce();

            List<TelemetrySample> afterDownsample = engine.query("cpu", 0, Long.MAX_VALUE, QueryOptions.defaults());
            assertFalse(afterDownsample.isEmpty());
            assertTrue(afterDownsample.size() < 120, "downsampling must reduce sample count");
            assertTrue(afterDownsample.stream().allMatch(TelemetrySample::downsampled),
                    "remaining samples should be marked downsampled");
            // Downsampling aggregates per block: 3 blocks (batch size 50) x at
            // most 2 ten-minute buckets spanned by 120 1ms-apart samples.
            assertTrue(afterDownsample.size() <= 6, "expected per-block bucket samples, got " + afterDownsample.size());
            // Average of values 0..119 in the first bucket (samples 0..119 all in bucket 0 if aligned? not necessarily).
            for (TelemetrySample s : afterDownsample) {
                assertEquals(0, s.timestamp() % Duration.ofMinutes(10).toMillis(),
                        "downsampled timestamps must align to the resolution");
            }

            // Advance past downsampled retention: everything must be deleted.
            clock.advance(Duration.ofHours(1));
            engine.enforceRetentionOnce();
            assertEquals(0, engine.query("cpu", 0, Long.MAX_VALUE, QueryOptions.defaults()).size());
            assertEquals(0, engine.stats().coldSamples());
            assertEquals(0, engine.stats().coldBlocks());
        }
    }

    @Test
    void downsampledAveragesAreCorrect() {
        TieredStorageProperties props = retentionProps();
        MutableClock clock = MutableClock.utcNow();
        try (TieredStorageEngine engine = new TieredStorageEngine(props, clock)) {
            long bucketStart = clock.millis() - clock.millis() % Duration.ofMinutes(10).toMillis();
            engine.write("m", List.of(
                    TestSupport.sample("m", bucketStart + 1_000, 10),
                    TestSupport.sample("m", bucketStart + 2_000, 20),
                    TestSupport.sample("m", bucketStart + 3_000, 30)));
            engine.migrateOnce();
            clock.advance(Duration.ofHours(1).plusSeconds(1));
            engine.enforceRetentionOnce();

            List<TelemetrySample> result = engine.query("m", 0, Long.MAX_VALUE, QueryOptions.defaults());
            assertEquals(1, result.size());
            TelemetrySample agg = result.get(0);
            assertTrue(agg.downsampled());
            assertEquals(bucketStart, agg.timestamp());
            assertEquals(20.0, agg.values().get("v"), 1e-9, "downsampled value must be the bucket average");
        }
    }

    @Test
    void retentionDoesNotTouchFreshOrCommittedConcurrentWrites() throws Exception {
        TieredStorageProperties props = retentionProps();
        MutableClock clock = MutableClock.utcNow();
        try (TieredStorageEngine engine = new TieredStorageEngine(props, clock)) {
            long t0 = clock.millis();
            engine.write("cpu", TestSupport.samples("cpu", t0, 100));
            clock.advance(Duration.ofSeconds(1)); // all samples now older than the demote cutoff
            engine.migrateOnce();
            clock.advance(Duration.ofHours(2).plusMinutes(1)); // everything expired

            // Concurrently write fresh samples while retention deletes the old ones.
            List<TelemetrySample> fresh = TestSupport.samples("cpu", clock.millis(), 50);
            Thread writer = new Thread(() -> engine.write("cpu", fresh));
            writer.start();
            // Retention passes do bounded work per pass; a few passes complete the job.
            for (int i = 0; i < 3; i++) {
                engine.enforceRetentionOnce();
            }
            writer.join();

            List<TelemetrySample> remaining = engine.query("cpu", 0, Long.MAX_VALUE, QueryOptions.defaults());
            Set<String> remainingIds = new HashSet<>();
            remaining.forEach(s -> remainingIds.add(s.id()));
            Set<String> freshIds = new HashSet<>();
            fresh.forEach(s -> freshIds.add(s.id()));
            assertEquals(freshIds, remainingIds, "retention must not lose concurrently committed samples");
        }
    }

    @Test
    void expiredHotSamplesAreDeletedToo() {
        TieredStorageProperties props = retentionProps();
        // Keep data in hot tier: huge hotMaxAge so migration never demotes.
        props.setHotMaxAge(Duration.ofDays(365));
        MutableClock clock = MutableClock.utcNow();
        try (TieredStorageEngine engine = new TieredStorageEngine(props, clock)) {
            engine.write("cpu", TestSupport.samples("cpu", clock.millis(), 30));
            assertEquals(30, engine.stats().hotSamples());
            clock.advance(Duration.ofHours(2).plusMinutes(1)); // beyond raw + downsampled retention
            engine.enforceRetentionOnce();
            assertEquals(0, engine.stats().hotSamples(), "expired hot samples must be deleted");
            assertEquals(0, engine.query("cpu", 0, Long.MAX_VALUE, QueryOptions.defaults()).size());
        }
    }

    @Test
    void noDanglingBlockFilesAfterRetention() throws Exception {
        TieredStorageProperties props = retentionProps();
        MutableClock clock = MutableClock.utcNow();
        try (TieredStorageEngine engine = new TieredStorageEngine(props, clock)) {
            engine.write("cpu", TestSupport.samples("cpu", clock.millis(), 200));
            engine.migrateOnce();
            clock.advance(Duration.ofHours(1).plusMinutes(1));
            engine.enforceRetentionOnce(); // downsample: replace blocks
            clock.advance(Duration.ofHours(1));
            engine.enforceRetentionOnce(); // delete everything

            try (Stream<Path> walk = Files.walk(dir.resolve("cold"))) {
                List<Path> leftovers = walk.filter(p -> p.toString().endsWith(".tsb") || p.toString().endsWith(".tmp")).toList();
                assertEquals(List.of(), leftovers, "no dangling block or tmp files may remain");
            }
            assertEquals(0, engine.stats().coldBlocks(), "registry must not hold dangling references");
        }
    }
}
