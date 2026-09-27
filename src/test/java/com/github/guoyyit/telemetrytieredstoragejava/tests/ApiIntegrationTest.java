package com.github.guoyyit.telemetrytieredstoragejava.tests;

import com.github.guoyyit.telemetrytieredstoragejava.engine.TieredStorageEngine;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/** End-to-end test of the REST API: write, migrate, query across tiers. */
@SpringBootTest
@AutoConfigureMockMvc
class ApiIntegrationTest {

    @TempDir
    static Path tempDir;

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("telemetry.data-dir", () -> tempDir.resolve("data").toString());
        registry.add("telemetry.hot-max-age", () -> "0s");
        registry.add("telemetry.scheduling-enabled", () -> "false");
    }

    @Autowired
    MockMvc mvc;

    @Autowired
    TieredStorageEngine engine;

    @Test
    void writeMigrateQueryRoundTrip() throws Exception {
        mvc.perform(post("/api/telemetry/cpu")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"samples":[
                                  {"timestamp":1000,"values":{"usage":0.5}},
                                  {"timestamp":2000,"values":{"usage":0.7}},
                                  {"timestamp":3000,"values":{"usage":0.9}}
                                ]}"""))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(3));

        // Demote everything to cold via the admin endpoint.
        mvc.perform(post("/api/telemetry/admin/migrate"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$").value(3));
        assertEquals(0, engine.stats().hotSamples());
        assertEquals(3, engine.stats().coldSamples());

        // Cross-tier query through the API.
        mvc.perform(get("/api/telemetry/cpu").param("from", "0").param("to", "5000"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(3))
                .andExpect(jsonPath("$[0].timestamp").value(1000))
                .andExpect(jsonPath("$[2].values.usage").value(0.9));

        mvc.perform(get("/api/telemetry/stats"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.coldSamples").value(3));
    }

    @Test
    void writeRejectsMissingTimestamp() throws Exception {
        mvc.perform(post("/api/telemetry/bad")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"samples\":[{\"values\":{\"v\":1.0}}]}"))
                .andExpect(status().isBadRequest());
    }
}
