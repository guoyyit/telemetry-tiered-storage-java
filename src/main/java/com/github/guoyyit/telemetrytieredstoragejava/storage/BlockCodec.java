package com.github.guoyyit.telemetrytieredstoragejava.storage;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.guoyyit.telemetrytieredstoragejava.config.TieredStorageProperties.Compression;
import com.github.guoyyit.telemetrytieredstoragejava.model.TelemetrySample;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Serializes sample batches to bytes and back, optionally compressed.
 * Layout: magic "TSB1" | 1 byte compression flag | payload (JSON, maybe gzipped).
 * The header makes every block self-describing so the compression algorithm
 * can be reconfigured without breaking existing archives.
 */
public final class BlockCodec {

    private static final byte[] MAGIC = {'T', 'S', 'B', '1'};
    private static final byte FLAG_NONE = 0;
    private static final byte FLAG_GZIP = 1;

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final TypeReference<List<TelemetrySample>> SAMPLE_LIST = new TypeReference<>() {
    };

    private BlockCodec() {
    }

    public static byte[] encode(List<TelemetrySample> samples, Compression compression) {
        try {
            byte[] json = MAPPER.writeValueAsBytes(samples);
            ByteArrayOutputStream out = new ByteArrayOutputStream(json.length / 2 + 16);
            out.write(MAGIC, 0, MAGIC.length);
            switch (compression) {
                case NONE -> {
                    out.write(FLAG_NONE);
                    out.write(json, 0, json.length);
                }
                case GZIP -> {
                    out.write(FLAG_GZIP);
                    try (GZIPOutputStream gz = new GZIPOutputStream(out)) {
                        gz.write(json, 0, json.length);
                    }
                }
            }
            return out.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException("failed to encode block", e);
        }
    }

    public static List<TelemetrySample> decode(byte[] data) {
        try {
            if (data.length < MAGIC.length + 1) {
                throw new IOException("block too small: " + data.length + " bytes");
            }
            for (int i = 0; i < MAGIC.length; i++) {
                if (data[i] != MAGIC[i]) {
                    throw new IOException("bad magic: " + new String(data, 0, MAGIC.length, StandardCharsets.US_ASCII));
                }
            }
            byte flag = data[MAGIC.length];
            byte[] json;
            switch (flag) {
                case FLAG_NONE -> {
                    json = new byte[data.length - MAGIC.length - 1];
                    System.arraycopy(data, MAGIC.length + 1, json, 0, json.length);
                }
                case FLAG_GZIP -> {
                    try (GZIPInputStream gz = new GZIPInputStream(
                            new ByteArrayInputStream(data, MAGIC.length + 1, data.length - MAGIC.length - 1))) {
                        json = gz.readAllBytes();
                    }
                }
                default -> throw new IOException("unknown compression flag: " + flag);
            }
            return MAPPER.readValue(json, SAMPLE_LIST);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to decode block", e);
        }
    }
}
