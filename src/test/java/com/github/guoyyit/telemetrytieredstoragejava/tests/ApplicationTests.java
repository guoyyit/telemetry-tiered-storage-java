package com.github.guoyyit.telemetrytieredstoragejava.tests;

import com.github.guoyyit.telemetrytieredstoragejava.model.TelemetrySample;
import com.github.guoyyit.telemetrytieredstoragejava.query.QueryService;
import com.github.guoyyit.telemetrytieredstoragejava.storage.StorageEngine;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Spring wiring smoke test: the whole engine assembles and serves traffic.
 */
@SpringBootTest
class ApplicationTests {

    @TempDir
    static Path tempDataDir;

    @DynamicPropertySource
    static void storageProps(DynamicPropertyRegistry registry) {
        registry.add("telemetry.storage.data-dir", () -> tempDataDir.toString());
        registry.add("telemetry.storage.scan-interval", () -> "200ms");
    }

    @Autowired
    private StorageEngine engine;

    @Autowired
    private QueryService queryService;

    @Test
    void contextLoadsAndEngineServesWritesAndQueries() {
        long now = System.currentTimeMillis();
        TelemetrySample written = engine.write("cpu", now, Map.of("host", "a"), 1.5);
        assertNotNull(written.getId());
        List<TelemetrySample> result = queryService.query("cpu", now - 1, now + 1);
        assertEquals(1, result.size());
        assertEquals(written.getId(), result.get(0).getId());
    }
}
