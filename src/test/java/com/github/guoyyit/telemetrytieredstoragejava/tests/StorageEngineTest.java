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

import static org.junit.jupiter.api.Assertions.*;

/**
 * Core engine behaviour: durable writes, unique ids, cross-tier queries.
 */
class StorageEngineTest {

    @TempDir
    Path dataDir;

    private StorageProperties properties() {
        StorageProperties p = new StorageProperties();
        p.setDataDir(dataDir.toString());
        p.setHotMaxAge(Duration.ofMillis(1));
        p.setCoolAfterIdle(Duration.ZERO);
        p.setMigrationBatchSize(100);
        p.setMaxHotSamples(10_000);
        return p;
    }

    @Test
    void writtenSamplesAreQueryableInTimeOrder() {
        StorageEngine engine = new StorageEngine(properties());
        engine.recover();
        long now = System.currentTimeMillis();
        engine.write("cpu", now + 2, Map.of("host", "a"), 2.0);
        engine.write("cpu", now, Map.of("host", "a"), 0.0);
        engine.write("cpu", now + 1, Map.of("host", "a"), 1.0);

        List<TelemetrySample> result = engine.query("cpu", now, now + 3);
        assertEquals(3, result.size());
        assertEquals(List.of(0.0, 1.0, 2.0), result.stream().map(TelemetrySample::getValue).toList());
        engine.close();
    }

    @Test
    void everySampleGetsAUniqueId() {
        StorageEngine engine = new StorageEngine(properties());
        engine.recover();
        Set<String> ids = new HashSet<>();
        long now = System.currentTimeMillis();
        for (int i = 0; i < 1000; i++) {
            ids.add(engine.write("s", now + i, Map.of(), i).getId());
        }
        assertEquals(1000, ids.size());
        engine.close();
    }

    @Test
    void committedSamplesSurviveRestart() {
        StorageProperties props = properties();
        long now = System.currentTimeMillis();

        StorageEngine first = new StorageEngine(props);
        first.recover();
        TelemetrySample a = first.write("cpu", now, Map.of("host", "a"), 1.0);
        TelemetrySample b = first.write("cpu", now + 1, Map.of("host", "b"), 2.0);
        first.close();

        StorageEngine second = new StorageEngine(props);
        second.recover();
        List<TelemetrySample> result = second.query("cpu", now, now + 2);
        assertEquals(2, result.size());
        assertEquals(Set.of(a.getId(), b.getId()),
                Set.of(result.get(0).getId(), result.get(1).getId()));
        second.close();
    }

    @Test
    void queryMergesHotAndColdTiersWithoutDuplicates() {
        StorageEngine engine = new StorageEngine(properties());
        engine.recover();
        long now = System.currentTimeMillis();
        // old samples -> will be cooled; fresh samples stay hot
        for (int i = 0; i < 10; i++) {
            engine.write("cpu", now - 100_000 + i, Map.of(), i);
        }
        engine.migrateToCold(engine.findCoolingCandidates(100));
        assertEquals(0, engine.hotStore().size());
        for (int i = 0; i < 5; i++) {
            engine.write("cpu", now + i, Map.of(), 100 + i);
        }

        List<TelemetrySample> all = engine.query("cpu", now - 200_000, now + 1000);
        assertEquals(15, all.size());
        Set<String> ids = new HashSet<>();
        for (TelemetrySample s : all) {
            assertTrue(ids.add(s.getId()), "duplicate sample in result: " + s.getId());
        }
        // time ordered
        for (int i = 1; i < all.size(); i++) {
            assertTrue(all.get(i - 1).getTimestamp() <= all.get(i).getTimestamp());
        }
        engine.close();
    }

    @Test
    void migrationIsIdempotentWhenReapplied() {
        StorageEngine engine = new StorageEngine(properties());
        engine.recover();
        long now = System.currentTimeMillis();
        for (int i = 0; i < 5; i++) {
            engine.write("cpu", now - 1000 + i, Map.of(), i);
        }
        var candidates = engine.findCoolingCandidates(100);
        engine.migrateToCold(candidates);
        // re-applying the same batch must not duplicate or lose samples
        engine.migrateToCold(candidates);

        List<TelemetrySample> all = engine.query("cpu", now - 2000, now + 1000);
        assertEquals(5, all.size());
        engine.close();
    }
}
