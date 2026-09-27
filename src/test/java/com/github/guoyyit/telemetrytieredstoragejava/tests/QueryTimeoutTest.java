package com.github.guoyyit.telemetrytieredstoragejava.tests;

import com.github.guoyyit.telemetrytieredstoragejava.config.StorageProperties;
import com.github.guoyyit.telemetrytieredstoragejava.model.TelemetrySample;
import com.github.guoyyit.telemetrytieredstoragejava.query.QueryCancelledException;
import com.github.guoyyit.telemetrytieredstoragejava.query.QueryService;
import com.github.guoyyit.telemetrytieredstoragejava.storage.StorageEngine;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Slow queries can be timed out and cancelled; afterwards no partial
 * migration state or orphan intermediate files remain, and the engine
 * keeps serving correct results.
 */
class QueryTimeoutTest {

    @TempDir
    Path dataDir;

    private StorageProperties properties() {
        StorageProperties p = new StorageProperties();
        p.setDataDir(dataDir.toString());
        p.setHotMaxAge(Duration.ofMillis(1));
        p.setCoolAfterIdle(Duration.ZERO);
        p.setMigrationBatchSize(100);
        p.setQueryTimeout(Duration.ofSeconds(10));
        return p;
    }

    @Test
    void queryExceedingTimeoutIsCancelled() {
        StorageProperties props = properties();
        StorageEngine engine = new StorageEngine(props);
        engine.recover();
        long base = System.currentTimeMillis() - 1_000_000;
        for (int i = 0; i < 2000; i++) {
            engine.write("cpu", base + i, Map.of(), i);
        }
        engine.migrateToCold(engine.findCoolingCandidates(2000));

        QueryService service = new QueryService(engine, props);
        assertThrows(QueryCancelledException.class,
                () -> service.query("cpu", 0, Long.MAX_VALUE, Duration.ofNanos(1)));
        service.close();
        engine.close();
    }

    @Test
    void engineStaysConsistentAfterTimeoutAndLeavesNoOrphanFiles() throws IOException {
        StorageProperties props = properties();
        StorageEngine engine = new StorageEngine(props);
        engine.recover();
        long base = System.currentTimeMillis() - 1_000_000;
        for (int i = 0; i < 500; i++) {
            engine.write("cpu", base + i, Map.of(), i);
        }
        engine.migrateToCold(engine.findCoolingCandidates(500));

        QueryService service = new QueryService(engine, props);
        assertThrows(QueryCancelledException.class,
                () -> service.query("cpu", 0, Long.MAX_VALUE, Duration.ofNanos(1)));

        // no leftover tmp files anywhere in the data directory
        try (Stream<Path> files = Files.walk(dataDir)) {
            List<Path> tmpFiles = files.filter(p -> p.getFileName().toString().endsWith(".tmp")).toList();
            assertTrue(tmpFiles.isEmpty(), "orphan tmp files: " + tmpFiles);
        }

        // engine still answers correctly with a generous timeout
        List<TelemetrySample> all = service.query("cpu", 0, Long.MAX_VALUE, Duration.ofSeconds(30));
        assertEquals(500, all.size());
        service.close();
        engine.close();
    }

    @Test
    void fastQueryCompletesWithinTimeout() {
        StorageProperties props = properties();
        StorageEngine engine = new StorageEngine(props);
        engine.recover();
        long now = System.currentTimeMillis();
        engine.write("cpu", now, Map.of(), 42.0);

        QueryService service = new QueryService(engine, props);
        List<TelemetrySample> result =
                service.query("cpu", now - 1, now + 1, Duration.ofSeconds(10));
        assertEquals(1, result.size());
        service.close();
        engine.close();
    }
}
