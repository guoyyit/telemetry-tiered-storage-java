package com.github.guoyyit.telemetrytieredstoragejava.storage;

import com.github.guoyyit.telemetrytieredstoragejava.model.TelemetrySample;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Cold tier: compressed archive files organised by stream and time range.
 *
 * <p>Layout: {@code <dir>/<stream>/<from>-<to>-<seq>.arc[.gz]}. Each file
 * holds a sorted batch of samples whose timestamps lie inside [from, to).
 * Writes go to a {@code .tmp} file first and are atomically renamed, so a
 * crash can only leave a collectable tmp file behind (cleaned up on open),
 * never a half-valid archive.
 */
public final class ColdStore {

    private static final int MAGIC = 0x54415243; // "TARC"
    private static final int VERSION = 1;
    private static final String TMP_SUFFIX = ".tmp";

    private final Path directory;
    private final boolean gzip;
    private final AtomicLong sequence = new AtomicLong();

    public ColdStore(Path directory, String compression) {
        this.directory = directory;
        this.gzip = !"none".equalsIgnoreCase(compression);
        try {
            Files.createDirectories(directory);
            cleanupTmpFiles();
        } catch (IOException e) {
            throw new UncheckedIOException("failed to init cold store at " + directory, e);
        }
    }

    /** Removes leftover tmp files from a previous crash. */
    private void cleanupTmpFiles() throws IOException {
        try (Stream<Path> streamDirs = Files.list(directory)) {
            for (Path streamDir : streamDirs.filter(Files::isDirectory).toList()) {
                try (Stream<Path> files = Files.list(streamDir)) {
                    for (Path f : files.filter(p -> p.getFileName().toString().endsWith(TMP_SUFFIX)).toList()) {
                        Files.deleteIfExists(f);
                    }
                }
            }
        }
    }

    /**
     * Appends a batch of samples, grouped per stream into one new archive
     * file per stream. Samples inside a file are sorted by timestamp.
     */
    public void appendBatch(List<TelemetrySample> batch) {
        if (batch.isEmpty()) {
            return;
        }
        Map<String, List<TelemetrySample>> perStream = new HashMap<>();
        for (TelemetrySample s : batch) {
            perStream.computeIfAbsent(s.getStream(), k -> new ArrayList<>()).add(s);
        }
        for (List<TelemetrySample> samples : perStream.values()) {
            samples.sort(Comparator.comparingLong(TelemetrySample::getTimestamp)
                    .thenComparing(TelemetrySample::getId));
            writeArchive(samples.get(0).getStream(), samples);
        }
    }

    private void writeArchive(String stream, List<TelemetrySample> samples) {
        long from = samples.get(0).getTimestamp();
        long to = samples.get(samples.size() - 1).getTimestamp() + 1;
        Path streamDir = directory.resolve(stream);
        String fileName = from + "-" + to + "-" + sequence.incrementAndGet()
                + (gzip ? ".arc.gz" : ".arc");
        Path target = streamDir.resolve(fileName);
        Path tmp = streamDir.resolve(fileName + TMP_SUFFIX);
        try {
            Files.createDirectories(streamDir);
            try (OutputStream fileOut = Files.newOutputStream(tmp);
                 OutputStream maybeGzip = gzip ? new GZIPOutputStream(fileOut) : fileOut;
                 DataOutputStream out = new DataOutputStream(maybeGzip)) {
                out.writeInt(MAGIC);
                out.writeInt(VERSION);
                out.writeInt(samples.size());
                for (TelemetrySample s : samples) {
                    writeSample(out, s);
                }
            }
            Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            try {
                Files.deleteIfExists(tmp);
            } catch (IOException suppressed) {
                e.addSuppressed(suppressed);
            }
            throw new UncheckedIOException("failed to write archive " + target, e);
        }
    }

    /** Time-ordered scan of one stream across all overlapping archives. */
    public List<TelemetrySample> scan(String stream, long fromInclusive, long toExclusive) {
        List<TelemetrySample> result = new ArrayList<>();
        for (Path file : archivesOverlapping(stream, fromInclusive, toExclusive)) {
            for (TelemetrySample s : readArchive(file)) {
                if (s.getTimestamp() >= fromInclusive && s.getTimestamp() < toExclusive) {
                    result.add(s);
                }
            }
        }
        result.sort(Comparator.comparingLong(TelemetrySample::getTimestamp)
                .thenComparing(TelemetrySample::getId));
        return result;
    }

    /** Ids already archived for the stream inside the given time range. */
    public Set<String> findExistingIds(String stream, long fromInclusive, long toExclusive) {
        Set<String> ids = new HashSet<>();
        for (Path file : archivesOverlapping(stream, fromInclusive, toExclusive)) {
            for (TelemetrySample s : readArchive(file)) {
                if (s.getTimestamp() >= fromInclusive && s.getTimestamp() < toExclusive) {
                    ids.add(s.getId());
                }
            }
        }
        return ids;
    }

    /** Deletes archives whose entire time range ends before the cutoff. */
    public int deleteOlderThan(long cutoffExclusive) {
        int deleted = 0;
        if (!Files.isDirectory(directory)) {
            return 0;
        }
        try (Stream<Path> streamDirs = Files.list(directory)) {
            for (Path streamDir : streamDirs.filter(Files::isDirectory).toList()) {
                try (Stream<Path> files = Files.list(streamDir)) {
                    for (Path f : files.toList()) {
                        long[] range = parseRange(f.getFileName().toString());
                        if (range != null && range[1] <= cutoffExclusive) {
                            Files.deleteIfExists(f);
                            deleted++;
                        }
                    }
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("failed to delete expired archives", e);
        }
        return deleted;
    }

    private List<Path> archivesOverlapping(String stream, long fromInclusive, long toExclusive) {
        Path streamDir = directory.resolve(stream);
        if (!Files.isDirectory(streamDir)) {
            return List.of();
        }
        List<Path> result = new ArrayList<>();
        try (Stream<Path> files = Files.list(streamDir)) {
            for (Path f : files.toList()) {
                long[] range = parseRange(f.getFileName().toString());
                if (range != null && range[0] < toExclusive && range[1] > fromInclusive) {
                    result.add(f);
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("failed to list archives in " + streamDir, e);
        }
        return result;
    }

    /** Parses "from-to-seq.arc[.gz]" into [from, to]; null if not an archive. */
    private static long[] parseRange(String fileName) {
        String name = fileName;
        if (name.endsWith(TMP_SUFFIX)) {
            return null;
        }
        if (name.endsWith(".gz")) {
            name = name.substring(0, name.length() - 3);
        }
        if (!name.endsWith(".arc")) {
            return null;
        }
        name = name.substring(0, name.length() - 4);
        String[] parts = name.split("-");
        if (parts.length != 3) {
            return null;
        }
        try {
            return new long[]{Long.parseLong(parts[0]), Long.parseLong(parts[1])};
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private List<TelemetrySample> readArchive(Path file) {
        List<TelemetrySample> samples = new ArrayList<>();
        try (InputStream fileIn = Files.newInputStream(file);
             InputStream maybeGzip = file.getFileName().toString().endsWith(".gz")
                     ? new GZIPInputStream(fileIn) : fileIn;
             DataInputStream in = new DataInputStream(maybeGzip)) {
            if (in.readInt() != MAGIC) {
                throw new IOException("bad magic in " + file);
            }
            in.readInt(); // version
            int count = in.readInt();
            for (int i = 0; i < count; i++) {
                samples.add(readSample(in));
            }
        } catch (EOFException e) {
            // tolerate truncated archive: return what was decoded
        } catch (IOException e) {
            throw new UncheckedIOException("failed to read archive " + file, e);
        }
        return samples;
    }

    private static void writeSample(DataOutputStream out, TelemetrySample s) throws IOException {
        out.writeUTF(s.getId());
        out.writeUTF(s.getStream());
        out.writeLong(s.getTimestamp());
        out.writeDouble(s.getValue());
        out.writeInt(s.getDimensions().size());
        for (Map.Entry<String, String> e : s.getDimensions().entrySet()) {
            out.writeUTF(e.getKey());
            out.writeUTF(e.getValue());
        }
    }

    private static TelemetrySample readSample(DataInputStream in) throws IOException {
        String id = in.readUTF();
        String stream = in.readUTF();
        long timestamp = in.readLong();
        double value = in.readDouble();
        int dimCount = in.readInt();
        Map<String, String> dims = new HashMap<>();
        for (int i = 0; i < dimCount; i++) {
            dims.put(in.readUTF(), in.readUTF());
        }
        return new TelemetrySample(id, stream, timestamp, dims, value);
    }
}
