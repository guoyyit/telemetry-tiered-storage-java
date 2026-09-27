package com.github.guoyyit.telemetrytieredstoragejava.tests;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.nio.file.Path;

/**
 * @author Vincent
 * @version 1.0
 */
@SpringBootTest
public class ApplicationTests {

    @TempDir
    static Path tempDir;

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("telemetry.data-dir", () -> tempDir.resolve("data").toString());
        registry.add("telemetry.scheduling-enabled", () -> "false");
    }

    @Test
    public void contentLoad() {
    }
}
