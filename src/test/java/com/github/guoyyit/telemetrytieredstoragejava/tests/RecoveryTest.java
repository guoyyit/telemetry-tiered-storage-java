package com.github.guoyyit.telemetrytieredstoragejava.tests;

import com.github.guoyyit.telemetrytieredstoragejava.config.TieredStorageProperties;
import com.github.guoyyit.telemetrytieredstoragejava.engine.TieredStorageEngine;
import com.github.guoyyit.telemetrytieredstoragejava.model.TelemetrySample;
import com.github.guoyyit.telemetrytieredstoragejava.query.QueryOptions;
import com.github.guoyyit.telemetrytieredstoragejava.storage.BlockCodec;
import com.github.guoyyit.telemetrytieredstoragejava.tests.support.MutableClock;
import com.github.guoyyit.telemetrytieredstoragejava.tests.support.TestSupport;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/** Requirement 5: restart recovery, interrupted migrations, no duplicates or losses. */
class RecoveryTest {

    @TempDir
    Path dir;

    @Test
    void crashAfterDemotionRecoversWithoutDuplicatesOrLoss() {
        TieredStorageProperties props = TestSupport.props(dir);
        MutableClock clock = MutableClock.utcNow();
        long now = clock.millis();

        TieredStorageEngine crashed = new TieredStorageEngine(props, clock);
        List<String> ids = crashed.write("cpu", TestSupport.samples("cpu", now - 3_600_000, 120));
        crashed.migrateOnce();
        assertEquals(120, crashed.stats().coldSamples());
        // Crash: no close(), no checkpoint.

        try (TieredStorageEngine recovered = new TieredStorageEngine(props, clock)) {
            List<TelemetrySample> all = recovered.query("cpu", 0, Long.MAX_VALUE, QueryOptions.defaults());
            assertEquals(120, all.size());
            Set<String> recoveredIds = new HashSet<>();
            all.forEach(s -> recoveredIds.add(s.id()));
            assertEquals(new HashSet<>(ids), recoveredIds, "recovery must preserve exactly the committed samples");
            assertEquals(120, recovered.stats().coldSamples());
            assertEquals(0, recovered.stats().hotSamples());

            // And the engine keeps working after recovery.
            recovered.write("cpu", TestSupport.samples("cpu", now, 10));
            assertEquals(130, recovered.query("cpu", 0, Long.MAX_VALUE, QueryOptions.defaults()).size());
        }
    }

    @Test
    void orphanBlockFromInterruptedDemotionIsRemovedOnRecovery() throws Exception {
        TieredStorageProperties props = TestSupport.props(dir);
        MutableClock clock = MutableClock.utcNow();
        long now = clock.millis();

        // Simulate a demotion that wrote its block file but crashed before the
        // ARCHIVE record reached the WAL: write the block file by hand.
        TieredStorageEngine crashed = new TieredStorageEngine(props, clock);
        List<TelemetrySample> batch = TestSupport.samples("cpu", now - 3_600_000, 60);
        List<String> ids = crashed.write("cpu", batch);
        Path streamDir = dir.resolve("cold").resolve("cpu");
        Files.createDirectories(streamDir);
        Files.write(streamDir.resolve((now - 3_600_000) + "-" + (now - 3_600_000 + 59) + "-1.tsb"),
                BlockCodec.encode(batch, props.getCompression()));
        // Crash before the WAL ARCHIVE record: the file is now an orphan.

        try (TieredStorageEngine recovered = new TieredStorageEngine(props, clock)) {
            // Samples are still committed in hot (their PUT records landed).
            List<TelemetrySample> all = recovered.query("cpu", 0, Long.MAX_VALUE, QueryOptions.defaults());
            assertEquals(60, all.size(), "orphan block must not resurrect archived copies");
            Set<String> recoveredIds = new HashSet<>();
            all.forEach(s -> recoveredIds.add(s.id()));
            assertEquals(new HashSet<>(ids), recoveredIds);
            // Orphan file deleted during recovery.
            try (Stream<Path> walk = Files.walk(dir.resolve("cold"))) {
                assertEquals(0, walk.filter(p -> p.toString().endsWith(".tsb")).count(),
                        "orphan block file should have been cleaned up");
            }
        }
    }

    @Test
    void crashAfterPromotionKeepsSamplesExactlyOnce() {
        TieredStorageProperties props = TestSupport.props(dir);
        MutableClock clock = MutableClock.utcNow();
        long now = clock.millis();

        TieredStorageEngine crashed = new TieredStorageEngine(props, clock);
        List<String> ids = crashed.write("gpu", TestSupport.samples("gpu", now - 3_600_000, 40));
        crashed.migrateOnce();
        assertEquals(40, crashed.stats().coldSamples());
        for (int i = 0; i < 3; i++) {
            crashed.query("gpu", 0, Long.MAX_VALUE, QueryOptions.defaults());
        }
        crashed.migrateOnce(); // promotes back to hot
        assertEquals(40, crashed.stats().hotSamples());
        // Crash right after the promotion.

        try (TieredStorageEngine recovered = new TieredStorageEngine(props, clock)) {
            List<TelemetrySample> all = recovered.query("gpu", 0, Long.MAX_VALUE, QueryOptions.defaults());
            assertEquals(40, all.size());
            Set<String> recoveredIds = new HashSet<>();
            all.forEach(s -> recoveredIds.add(s.id()));
            assertEquals(new HashSet<>(ids), recoveredIds);
            assertEquals(0, recovered.stats().coldSamples(), "promoted block must not linger in cold");
        }
    }

    @Test
    void queryResultsAreIdenticalBeforeAndAfterCrash() {
        TieredStorageProperties props = TestSupport.props(dir);
        MutableClock clock = MutableClock.utcNow();
        long now = clock.millis();

        TieredStorageEngine crashed = new TieredStorageEngine(props, clock);
        crashed.write("cpu", TestSupport.samples("cpu", now - 7_200_000, 80));
        crashed.write("cpu", TestSupport.samples("cpu", now - 100, 20));
        crashed.migrateOnce();
        crashed.enforceRetentionOnce(); // also checkpoints
        crashed.write("cpu", TestSupport.samples("cpu", now, 5));
        List<TelemetrySample> beforeCrash = crashed.query("cpu", 0, Long.MAX_VALUE, QueryOptions.defaults());
        // Crash.

        try (TieredStorageEngine recovered = new TieredStorageEngine(props, clock)) {
            List<TelemetrySample> afterRecovery = recovered.query("cpu", 0, Long.MAX_VALUE, QueryOptions.defaults());
            assertEquals(beforeCrash.size(), afterRecovery.size());
            for (int i = 0; i < beforeCrash.size(); i++) {
                assertEquals(beforeCrash.get(i).id(), afterRecovery.get(i).id(),
                        "recovered query results diverge from pre-crash results at position " + i);
            }
        }
    }

    @Test
    void repeatedRestartsAreStable() {
        TieredStorageProperties props = TestSupport.props(dir);
        MutableClock clock = MutableClock.utcNow();
        long now = clock.millis();

        TieredStorageEngine first = new TieredStorageEngine(props, clock);
        first.write("cpu", TestSupport.samples("cpu", now - 3_600_000, 30));
        first.migrateOnce();
        first.close();

        for (int i = 0; i < 3; i++) {
            try (TieredStorageEngine engine = new TieredStorageEngine(props, clock)) {
                assertEquals(30, engine.query("cpu", 0, Long.MAX_VALUE, QueryOptions.defaults()).size(),
                        "restart " + i + " changed the data");
            }
        }
    }
}
