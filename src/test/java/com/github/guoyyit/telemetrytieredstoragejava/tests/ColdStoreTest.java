package com.github.guoyyit.telemetrytieredstoragejava.tests;

import com.github.guoyyit.telemetrytieredstoragejava.model.TelemetrySample;
import com.github.guoyyit.telemetrytieredstoragejava.storage.ColdStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Cold archives are compressed, organised by time range, and expired
 * archives are deleted without touching live ones.
 */
class ColdStoreTest {

    @TempDir
    Path dir;

    private static List<TelemetrySample> batch(String stream, long from, int count) {
        List<TelemetrySample> samples = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            samples.add(new TelemetrySample("id-" + from + "-" + i, stream, from + i,
                    Map.of("host", "h" + (i % 4), "region", "r1"), i * 1.5));
        }
        return samples;
    }

    @Test
    void archivesAreNamedByTimeRangeAndCompressed() throws IOException {
        ColdStore gzipStore = new ColdStore(dir.resolve("gzip"), "gzip");
        gzipStore.appendBatch(batch("cpu", 1000, 200));

        Path archive;
        try (Stream<Path> files = Files.walk(dir.resolve("gzip"))) {
            archive = files.filter(p -> p.getFileName().toString().endsWith(".arc.gz"))
                    .findFirst().orElseThrow();
        }
        String name = archive.getFileName().toString();
        assertTrue(name.startsWith("1000-1200-"), "time range encoded in file name: " + name);

        ColdStore plainStore = new ColdStore(dir.resolve("plain"), "none");
        plainStore.appendBatch(batch("cpu", 1000, 200));
        long plainSize;
        try (Stream<Path> files = Files.walk(dir.resolve("plain"))) {
            plainSize = Files.size(files.filter(p -> p.getFileName().toString().endsWith(".arc"))
                    .findFirst().orElseThrow());
        }
        assertTrue(Files.size(archive) < plainSize,
                "gzip archive (%d) should be smaller than uncompressed (%d)"
                        .formatted(Files.size(archive), plainSize));
    }

    @Test
    void scanOnlyReadsOverlappingArchives() {
        ColdStore store = new ColdStore(dir.resolve("cold"), "gzip");
        store.appendBatch(batch("cpu", 1000, 100));   // [1000, 1100)
        store.appendBatch(batch("cpu", 2000, 100));   // [2000, 2100)
        store.appendBatch(batch("mem", 1000, 50));    // different stream

        List<TelemetrySample> result = store.scan("cpu", 1050, 2050);
        assertEquals(100, result.size()); // 50 from first archive + 50 from second
        for (int i = 1; i < result.size(); i++) {
            assertTrue(result.get(i - 1).getTimestamp() <= result.get(i).getTimestamp());
        }
        assertEquals(List.of(), store.scan("gpu", 0, Long.MAX_VALUE));
    }

    @Test
    void deleteOlderThanOnlyRemovesFullyExpiredArchives() throws IOException {
        ColdStore store = new ColdStore(dir.resolve("cold"), "gzip");
        store.appendBatch(batch("cpu", 1000, 100));  // [1000, 1100)
        store.appendBatch(batch("cpu", 5000, 100));  // [5000, 5100)

        int deleted = store.deleteOlderThan(2000);
        assertEquals(1, deleted);
        assertEquals(0, store.scan("cpu", 0, 2000).size());
        assertEquals(100, store.scan("cpu", 2000, 6000).size());

        // archive partially overlapping the cutoff must survive
        store.appendBatch(batch("cpu", 8000, 1000)); // [8000, 9000)
        deleted = store.deleteOlderThan(8500);
        assertEquals(1, deleted, "only [5000,5100) fully below cutoff");
        assertEquals(1000, store.scan("cpu", 8000, 9000).size());
    }

    @Test
    void leftoverTmpFilesAreCleanedOnOpen() throws IOException {
        Path coldDir = dir.resolve("cold");
        Path streamDir = coldDir.resolve("cpu");
        Files.createDirectories(streamDir);
        Files.writeString(streamDir.resolve("1000-1100-1.arc.gz.tmp"), "partial");

        new ColdStore(coldDir, "gzip"); // open triggers cleanup

        try (Stream<Path> files = Files.walk(coldDir)) {
            assertTrue(files.noneMatch(p -> p.getFileName().toString().endsWith(".tmp")));
        }
    }
}
