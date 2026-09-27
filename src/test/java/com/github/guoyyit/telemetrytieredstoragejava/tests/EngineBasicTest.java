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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/** Requirement 1: durable writes by stream, unique sample identity, ordered reads. */
class EngineBasicTest {

    @TempDir
    Path dir;

    @Test
    void writesAreQueryableInTimeOrderWithUniqueIds() {
        TieredStorageProperties props = TestSupport.props(dir);
        try (TieredStorageEngine engine = new TieredStorageEngine(props, MutableClock.utcNow())) {
            List<TelemetrySample> batch = TestSupport.samples("cpu", 5_000, 200);
            List<String> ids = engine.write("cpu", batch);
            assertEquals(200, ids.size());
            assertEquals(200, new HashSet<>(ids).size(), "sample ids must be unique");

            List<TelemetrySample> all = engine.query("cpu", 0, Long.MAX_VALUE, QueryOptions.defaults());
            assertEquals(200, all.size());
            for (int i = 1; i < all.size(); i++) {
                assertTrue(all.get(i - 1).timestamp() <= all.get(i).timestamp(), "results must be time ordered");
            }

            List<TelemetrySample> window = engine.query("cpu", 5_100, 5_199, QueryOptions.defaults());
            assertEquals(100, window.size());
            assertEquals(5_100, window.get(0).timestamp());
            assertEquals(5_199, window.get(99).timestamp());
        }
    }

    @Test
    void writesSurviveGracefulRestart() {
        TieredStorageProperties props = TestSupport.props(dir);
        List<String> ids;
        try (TieredStorageEngine engine = new TieredStorageEngine(props, MutableClock.utcNow())) {
            ids = engine.write("disk", TestSupport.samples("disk", 1_000, 100));
        }
        try (TieredStorageEngine reopened = new TieredStorageEngine(props, MutableClock.utcNow())) {
            List<TelemetrySample> all = reopened.query("disk", 0, Long.MAX_VALUE, QueryOptions.defaults());
            assertEquals(100, all.size());
            Set<String> recovered = new HashSet<>();
            all.forEach(s -> recovered.add(s.id()));
            assertEquals(new HashSet<>(ids), recovered, "restart must not lose or duplicate samples");
        }
    }

    @Test
    void writesSurviveCrashWithoutCheckpoint() {
        TieredStorageProperties props = TestSupport.props(dir);
        // Abandon the engine without close(): simulates a process crash. Every
        // committed write was fsynced to the WAL, so recovery must see it.
        TieredStorageEngine crashed = new TieredStorageEngine(props, MutableClock.utcNow());
        List<String> ids = crashed.write("net", TestSupport.samples("net", 2_000, 50));
        // no close(), no checkpoint

        try (TieredStorageEngine recovered = new TieredStorageEngine(props, MutableClock.utcNow())) {
            List<TelemetrySample> all = recovered.query("net", 0, Long.MAX_VALUE, QueryOptions.defaults());
            assertEquals(50, all.size());
            Set<String> recoveredIds = new HashSet<>();
            all.forEach(s -> recoveredIds.add(s.id()));
            assertEquals(new HashSet<>(ids), recoveredIds);
        }
    }

    @Test
    void streamsAreIsolated() {
        TieredStorageProperties props = TestSupport.props(dir);
        try (TieredStorageEngine engine = new TieredStorageEngine(props, MutableClock.utcNow())) {
            engine.write("a", TestSupport.samples("a", 1_000, 10));
            engine.write("b", TestSupport.samples("b", 1_000, 20));
            assertEquals(10, engine.query("a", 0, Long.MAX_VALUE, QueryOptions.defaults()).size());
            assertEquals(20, engine.query("b", 0, Long.MAX_VALUE, QueryOptions.defaults()).size());
            assertEquals(0, engine.query("c", 0, Long.MAX_VALUE, QueryOptions.defaults()).size());
        }
    }
}
