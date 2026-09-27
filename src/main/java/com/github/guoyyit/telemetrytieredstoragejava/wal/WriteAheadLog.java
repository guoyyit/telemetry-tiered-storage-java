package com.github.guoyyit.telemetrytieredstoragejava.wal;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

/**
 * Append-only JSON-lines write ahead log. Every {@link #append} is flushed and
 * fsynced before returning, so a committed record survives a crash. Replay
 * tolerates a torn tail line (the last write may have been interrupted).
 */
public class WriteAheadLog implements Closeable {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Path file;
    private FileChannel channel;
    private boolean closed;

    public WriteAheadLog(Path file) {
        this.file = file;
        try {
            Files.createDirectories(file.getParent());
            this.channel = FileChannel.open(file, StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to open WAL " + file, e);
        }
    }

    /** Append and fsync a record. Must be called under the engine write lock. */
    public synchronized void append(WalRecord record) {
        appendAll(List.of(record));
    }

    /** Append a batch of records with a single fsync. Must be called under the engine write lock. */
    public synchronized void appendAll(List<WalRecord> records) {
        ensureOpen();
        try {
            for (WalRecord record : records) {
                byte[] line = (MAPPER.writeValueAsString(record) + "\n").getBytes(StandardCharsets.UTF_8);
                channel.write(java.nio.ByteBuffer.wrap(line));
            }
            channel.force(false);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to append " + records.size() + " WAL records", e);
        }
    }

    /** Replay all records of the given file, tolerating a torn tail write. */
    public static List<WalRecord> replay(Path file) {
        List<WalRecord> records = new ArrayList<>();
        if (!Files.exists(file)) {
            return records;
        }
        try (BufferedReader reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isBlank()) {
                    continue;
                }
                try {
                    records.add(MAPPER.readValue(line, WalRecord.class));
                } catch (IOException parseFailure) {
                    // Torn or partial tail write: everything after is untrusted.
                    break;
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("failed to replay WAL " + file, e);
        }
        return records;
    }

    /** Close current file and start a fresh empty log (called after a checkpoint). */
    public synchronized void rotate() {
        ensureOpen();
        try {
            channel.close();
            Files.deleteIfExists(file);
            this.channel = FileChannel.open(file, StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to rotate WAL " + file, e);
        }
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("WAL already closed: " + file);
        }
    }

    @Override
    public synchronized void close() {
        if (!closed) {
            closed = true;
            try {
                channel.close();
            } catch (IOException e) {
                throw new UncheckedIOException("failed to close WAL " + file, e);
            }
        }
    }
}
