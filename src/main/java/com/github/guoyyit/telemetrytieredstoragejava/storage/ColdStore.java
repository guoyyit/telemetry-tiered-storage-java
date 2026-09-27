package com.github.guoyyit.telemetrytieredstoragejava.storage;

import com.github.guoyyit.telemetrytieredstoragejava.config.TieredStorageProperties.Compression;
import com.github.guoyyit.telemetrytieredstoragejava.model.TelemetrySample;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URLEncoder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

/**
 * Cold tier: compressed archive blocks on local disk, one file per block,
 * laid out as {@code cold/<stream>/<minTs>-<maxTs>-<seq>.tsb}. The in-memory
 * registry is rebuilt from snapshot/WAL on restart; files not present in the
 * registry after recovery are orphans of interrupted operations and deleted.
 * All methods are called under the engine lock.
 */
public class ColdStore {

    private static final String BLOCK_SUFFIX = ".tsb";
    private static final String TMP_SUFFIX = ".tmp";

    private final Path dir;
    private final Compression compression;
    private final Map<String, BlockMeta> registry = new LinkedHashMap<>();
    private final AtomicLong seq = new AtomicLong(System.nanoTime());

    public ColdStore(Path dir, Compression compression) {
        this.dir = dir;
        this.compression = compression;
        try {
            Files.createDirectories(dir);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to create cold dir " + dir, e);
        }
    }

    /** Atomically write a block (tmp file + fsync + rename). Returns its metadata. */
    public BlockMeta writeBlock(String streamId, List<TelemetrySample> samples, boolean downsampled) {
        if (samples.isEmpty()) {
            throw new IllegalArgumentException("cannot archive an empty block");
        }
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;
        for (TelemetrySample s : samples) {
            min = Math.min(min, s.timestamp());
            max = Math.max(max, s.timestamp());
        }
        String fileName = streamDirName(streamId) + "/" + min + "-" + max + "-" + seq.getAndIncrement() + BLOCK_SUFFIX;
        Path target = dir.resolve(fileName);
        Path tmp = target.resolveSibling(target.getFileName() + TMP_SUFFIX);
        byte[] payload = BlockCodec.encode(samples, compression);
        try {
            Files.createDirectories(target.getParent());
            try (FileChannel ch = FileChannel.open(tmp, StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
                ch.write(java.nio.ByteBuffer.wrap(payload));
                ch.force(true);
            }
            Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            // Never leave a half-written tmp file behind.
            try {
                Files.deleteIfExists(tmp);
            } catch (IOException ignored) {
                // best effort cleanup
            }
            throw new UncheckedIOException("failed to write block " + fileName, e);
        }
        return new BlockMeta(fileName, streamId, min, max, samples.size(), downsampled);
    }

    public List<TelemetrySample> readBlock(BlockMeta meta) {
        try {
            return BlockCodec.decode(Files.readAllBytes(dir.resolve(meta.fileName())));
        } catch (IOException e) {
            throw new UncheckedIOException("failed to read block " + meta.fileName(), e);
        }
    }

    public void register(BlockMeta meta) {
        registry.put(meta.fileName(), meta);
    }

    public void unregister(String fileName) {
        registry.remove(fileName);
    }

    /** Delete the block file if it exists. Idempotent. */
    public void deleteBlockFile(String fileName) {
        try {
            Files.deleteIfExists(dir.resolve(fileName));
        } catch (IOException e) {
            throw new UncheckedIOException("failed to delete block " + fileName, e);
        }
    }

    /** Registered blocks of a stream ordered by minTs. */
    public List<BlockMeta> blocks(String streamId) {
        List<BlockMeta> result = new ArrayList<>();
        for (BlockMeta meta : registry.values()) {
            if (meta.streamId().equals(streamId)) {
                result.add(meta);
            }
        }
        result.sort(Comparator.comparingLong(BlockMeta::minTs));
        return result;
    }

    public List<BlockMeta> allBlocks() {
        return new ArrayList<>(registry.values());
    }

    public Map<String, BlockMeta> registrySnapshot() {
        return new LinkedHashMap<>(registry);
    }

    public void restoreRegistry(Collection<BlockMeta> metas) {
        registry.clear();
        for (BlockMeta meta : metas) {
            registry.put(meta.fileName(), meta);
        }
    }

    /** Delete block files on disk that are not in the registry (crash leftovers). */
    public int deleteOrphanFiles() {
        int deleted = 0;
        try (Stream<Path> walk = Files.walk(dir)) {
            List<Path> files = walk.filter(Files::isRegularFile).toList();
            for (Path file : files) {
                String rel = dir.relativize(file).toString().replace(file.getFileSystem().getSeparator(), "/");
                boolean orphan = rel.endsWith(TMP_SUFFIX)
                        || (rel.endsWith(BLOCK_SUFFIX) && !registry.containsKey(rel));
                if (orphan) {
                    Files.deleteIfExists(file);
                    deleted++;
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("failed to clean orphan blocks in " + dir, e);
        }
        // Remove now-empty stream directories.
        try (Stream<Path> walk = Files.walk(dir)) {
            for (Path sub : walk.filter(Files::isDirectory).filter(p -> !p.equals(dir)).toList()) {
                try (Stream<Path> children = Files.list(sub)) {
                    if (children.findAny().isEmpty()) {
                        Files.deleteIfExists(sub);
                    }
                }
            }
        } catch (IOException ignored) {
            // best effort: empty dirs are harmless
        }
        return deleted;
    }

    public Path dir() {
        return dir;
    }

    private static String streamDirName(String streamId) {
        return URLEncoder.encode(streamId, StandardCharsets.UTF_8).replace("+", "%20");
    }
}
