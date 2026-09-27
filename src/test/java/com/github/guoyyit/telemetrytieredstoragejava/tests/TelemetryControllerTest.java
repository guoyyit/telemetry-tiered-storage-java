package com.github.guoyyit.telemetrytieredstoragejava.tests;

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

import static org.hamcrest.Matchers.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/**
 * REST API: write samples, read them back time-ordered.
 */
@SpringBootTest
@AutoConfigureMockMvc
class TelemetryControllerTest {

    @TempDir
    static Path tempDataDir;

    @DynamicPropertySource
    static void storageProps(DynamicPropertyRegistry registry) {
        registry.add("telemetry.storage.data-dir", () -> tempDataDir.toString());
    }

    @Autowired
    private MockMvc mvc;

    @Test
    void writeThenQueryRoundTrip() throws Exception {
        long now = System.currentTimeMillis();
        mvc.perform(post("/api/telemetry/cpu")
                        .param("timestamp", String.valueOf(now))
                        .param("value", "3.14")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"host\":\"a\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id", notNullValue()))
                .andExpect(jsonPath("$.stream", is("cpu")))
                .andExpect(jsonPath("$.value", is(3.14)));

        mvc.perform(get("/api/telemetry/cpu")
                        .param("from", String.valueOf(now - 1000))
                        .param("to", String.valueOf(now + 1000)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasSize(1)))
                .andExpect(jsonPath("$[0].dimensions.host", is("a")));
    }
}
