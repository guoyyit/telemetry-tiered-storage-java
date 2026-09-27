package com.github.guoyyit.telemetrytieredstoragejava.model;

import java.util.Map;
import java.util.Objects;

/**
 * A single telemetry sample belonging to a data stream.
 * The id is unique across the whole system.
 */
public final class TelemetrySample {

    private final String id;
    private final String stream;
    private final long timestamp;
    private final Map<String, String> dimensions;
    private final double value;

    public TelemetrySample(String id, String stream, long timestamp,
                           Map<String, String> dimensions, double value) {
        this.id = Objects.requireNonNull(id, "id");
        this.stream = Objects.requireNonNull(stream, "stream");
        this.timestamp = timestamp;
        this.dimensions = dimensions == null ? Map.of() : Map.copyOf(dimensions);
        this.value = value;
    }

    public String getId() {
        return id;
    }

    public String getStream() {
        return stream;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public Map<String, String> getDimensions() {
        return dimensions;
    }

    public double getValue() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TelemetrySample other)) {
            return false;
        }
        return id.equals(other.id);
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }
}
