package com.github.guoyyit.telemetrytieredstoragejava.tests.support;

import com.github.guoyyit.telemetrytieredstoragejava.config.TieredStorageProperties;
import com.github.guoyyit.telemetrytieredstoragejava.model.TelemetrySample;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Shared helpers for engine tests. */
public final class TestSupport {

    private TestSupport() {
    }

    /** Properties with small, test-friendly bounds and scheduling irrelevant (engine used directly). */
    public static TieredStorageProperties props(Path dataDir) {
        TieredStorageProperties props = new TieredStorageProperties();
        props.setDataDir(dataDir);
        props.setHotMaxAge(Duration.ofMinutes(30));
        props.setAccessWindow(Duration.ofMinutes(5));
        props.setPromoteHeatThreshold(3);
        props.setMigrationBatchSize(50);
        props.setScanInterval(Duration.ofSeconds(1));
        props.setRawRetention(Duration.ofHours(2));
        props.setDownsampledRetention(Duration.ofHours(2));
        props.setDownsampleResolution(Duration.ofMinutes(10));
        props.setMaxHotSamples(10_000);
        props.setMaxResultSamples(50_000);
        props.setDefaultQueryTimeoutMs(10_000);
        props.setSchedulingEnabled(false);
        return props;
    }

    public static TelemetrySample sample(String stream, long timestamp, double value) {
        return TelemetrySample.of(stream, timestamp, Map.of("v", value));
    }

    public static List<TelemetrySample> samples(String stream, long... timestamps) {
        List<TelemetrySample> result = new ArrayList<>(timestamps.length);
        for (long ts : timestamps) {
            result.add(sample(stream, ts, ts));
        }
        return result;
    }

    /** n samples spaced 1ms apart starting at t0. */
    public static List<TelemetrySample> samples(String stream, long t0, int n) {
        List<TelemetrySample> result = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            result.add(sample(stream, t0 + i, i));
        }
        return result;
    }
}
