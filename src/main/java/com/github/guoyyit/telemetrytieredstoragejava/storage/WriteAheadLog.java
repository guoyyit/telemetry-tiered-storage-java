package com.github.guoyyit.telemetrytieredstoragejava.storage;

import com.github.guoyyit.telemetrytieredstoragejava.model.TelemetrySample;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.CRC32;

/**
 * Append-only write ahead log. Every committed mutation is persisted here
 * (and fsynced) before it becomes visible, so a restart can replay it.
 *
 * Record layout: [magic:int][length:int][payload:length bytes][crc32:long]
 * where crc32 covers the payload. A torn tail record (crash mid-write) is
 * detected via magic/length/crc and ignored during replay.
 */
public final class WriteAheadLog implements Closeable {

    /** A replayed WAL record. */
    public sealed interface Record {
        record Put(TelemetrySample sample) implements Record {
        }
        record Delete(String sampleId) implements Record {
        }
    }

    private static final int MAGIC = 0x54574C31; // "TWL1"
    private static final byte TYPE_PUT = 1;
    private static final byte TYPE_DELETE = 2;

    private final Path file;
    private final FileChannel channel;
    private final ByteBuffer writeBuffer = ByteBuffer.allocate(1024 * 1024);

    public WriteAheadLog(Path directory) {
        try {
            Files.createDirectories(directory);
            this.file = directory.resolve("wal.log");
            this.channel = FileChannel.open(file,
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to open WAL in " + directory, e);
        }
    }

    public synchronized void appendPut(TelemetrySample sample) {
        writePutFrame(sample);
        force();
    }

    public synchronized void appendDelete(String sampleId) {
        writeDeleteFrame(sampleId);
        force();
    }

    /**
     * Appends many delete records with a single fsync. Safe because the
     * callers (migration commit, retention) are idempotent: if the process
     * dies before the fsync, recovery simply redoes the operation.
     */
    public synchronized void appendDeleteAll(List<String> sampleIds) {
        for (String id : sampleIds) {
            writeDeleteFrame(id);
        }
        force();
    }

    private void writePutFrame(TelemetrySample sample) {
        writeBuffer.clear();
        writeBuffer.put(TYPE_PUT);
        writeString(writeBuffer, sample.getId());
        writeString(writeBuffer, sample.getStream());
        writeBuffer.putLong(sample.getTimestamp());
        writeBuffer.putDouble(sample.getValue());
        Map<String, String> dims = sample.getDimensions();
        writeBuffer.putInt(dims.size());
        for (Map.Entry<String, String> e : dims.entrySet()) {
            writeString(writeBuffer, e.getKey());
            writeString(writeBuffer, e.getValue());
        }
        appendFrame();
    }

    private void writeDeleteFrame(String sampleId) {
        writeBuffer.clear();
        writeBuffer.put(TYPE_DELETE);
        writeString(writeBuffer, sampleId);
        appendFrame();
    }

    private void force() {
        try {
            channel.force(false);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to fsync WAL", e);
        }
    }

    private void appendFrame() {
        try {
            writeBuffer.flip();
            int payloadLen = writeBuffer.remaining();
            CRC32 crc = new CRC32();
            // CRC over payload bytes.
            ByteBuffer duplicate = writeBuffer.duplicate();
            byte[] payloadBytes = new byte[payloadLen];
            duplicate.get(payloadBytes);
            crc.update(payloadBytes);

            ByteBuffer frame = ByteBuffer.allocate(4 + 4 + payloadLen + 8);
            frame.putInt(MAGIC);
            frame.putInt(payloadLen);
            frame.put(payloadBytes);
            frame.putLong(crc.getValue());
            frame.flip();
            while (frame.hasRemaining()) {
                channel.write(frame);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("failed to append WAL record", e);
        }
    }

    /** Replays all complete records in write order; stops at a torn tail. */
    public List<Record> replay() {
        List<Record> records = new ArrayList<>();
        if (!Files.exists(file)) {
            return records;
        }
        try (DataInputStream in = new DataInputStream(Files.newInputStream(file))) {
            while (true) {
                int magic;
                try {
                    magic = in.readInt();
                } catch (EOFException e) {
                    break;
                }
                if (magic != MAGIC) {
                    break; // torn or corrupt tail: stop replay
                }
                int length = in.readInt();
                if (length < 0 || length > 64 * 1024 * 1024) {
                    break;
                }
                byte[] payload = new byte[length];
                in.readFully(payload);
                long expectedCrc = in.readLong();
                CRC32 crc = new CRC32();
                crc.update(payload);
                if (crc.getValue() != expectedCrc) {
                    break; // torn tail
                }
                records.add(decode(payload));
            }
        } catch (EOFException e) {
            // truncated tail: ignore
        } catch (IOException e) {
            throw new UncheckedIOException("failed to replay WAL " + file, e);
        }
        return records;
    }

    private Record decode(byte[] payload) {
        ByteBuffer buf = ByteBuffer.wrap(payload);
        byte type = buf.get();
        if (type == TYPE_PUT) {
            String id = readString(buf);
            String stream = readString(buf);
            long timestamp = buf.getLong();
            double value = buf.getDouble();
            int dimCount = buf.getInt();
            Map<String, String> dims = new HashMap<>();
            for (int i = 0; i < dimCount; i++) {
                dims.put(readString(buf), readString(buf));
            }
            return new Record.Put(new TelemetrySample(id, stream, timestamp, dims, value));
        }
        if (type == TYPE_DELETE) {
            return new Record.Delete(readString(buf));
        }
        throw new IllegalStateException("unknown WAL record type " + type);
    }

    /** Truncates the log after a checkpoint. */
    public synchronized void truncate() {
        try {
            channel.truncate(0);
            channel.force(false);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to truncate WAL", e);
        }
    }

    @Override
    public synchronized void close() {
        try {
            channel.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void writeString(ByteBuffer buf, String s) {
        byte[] bytes = s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        buf.putInt(bytes.length);
        buf.put(bytes);
    }

    private static String readString(ByteBuffer buf) {
        int len = buf.getInt();
        byte[] bytes = new byte[len];
        buf.get(bytes);
        return new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
    }
}
