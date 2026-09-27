package com.github.guoyyit.telemetrytieredstoragejava.model;

import java.util.Map;
import java.util.UUID;

/**
 * A single telemetry sample belonging to a stream. The id is globally unique
 * and is the deduplication key used to keep query results consistent while
 * samples migrate between tiers.
 */
public record TelemetrySample(String id, String streamId, long timestamp,
                              Map<String, Double> values, boolean downsampled) {

    public static TelemetrySample of(String streamId, long timestamp, Map<String, Double> values) {
        return new TelemetrySample(UUID.randomUUID().toString(), streamId, timestamp, values, false);
    }

    public TelemetrySample withId(String newId) {
        return new TelemetrySample(newId, streamId, timestamp, values, downsampled);
    }

    public TelemetrySample asDownsampled() {
        return new TelemetrySample(id, streamId, timestamp, values, true);
    }
}
