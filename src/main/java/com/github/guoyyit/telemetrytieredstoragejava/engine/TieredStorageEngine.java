package com.github.guoyyit.telemetrytieredstoragejava.engine;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.guoyyit.telemetrytieredstoragejava.config.TieredStorageProperties;
import com.github.guoyyit.telemetrytieredstoragejava.model.TelemetrySample;
import com.github.guoyyit.telemetrytieredstoragejava.query.QueryOptions;
import com.github.guoyyit.telemetrytieredstoragejava.query.QueryResultTooLargeException;
import com.github.guoyyit.telemetrytieredstoragejava.storage.BlockMeta;
import com.github.guoyyit.telemetrytieredstoragejava.storage.ColdStore;
import com.github.guoyyit.telemetrytieredstoragejava.storage.HotStore;
import com.github.guoyyit.telemetrytieredstoragejava.wal.WalRecord;
import com.github.guoyyit.telemetrytieredstoragejava.wal.WriteAheadLog;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Core of the tiered storage system.
 *
 * <p>Consistency model: a single read/write lock guards all tier state.
 * Writers and migration state-transitions take the write lock (short, O(batch)
 * critical sections); queries take the read lock and therefore see each sample
 * in exactly one tier. Block file IO happens outside the lock; the WAL is
 * appended inside the write lock so the durable order always matches the
 * in-memory order. Every sample carries a unique id and query merges
 * deduplicate by id, so even a crash in the middle of a migration can never
 * produce duplicates or losses after recovery.</p>
 *
 * <p>Recovery: load snapshot, replay WAL (idempotent handlers), delete orphan
 * block files left by interrupted operations, then resume. Unfinished
 * migrations are either fully replayed (their WAL record landed) or cleanly
 * absent (their block file is removed as an orphan).</p>
 */
public class TieredStorageEngine implements Closeable {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** Durable checkpoint: hot samples + cold block registry. */
    record Snapshot(List<TelemetrySample> hot, List<BlockMeta> blocks) {
    }

    private final TieredStorageProperties props;
    private final Clock clock;
    private final HotStore hot = new HotStore();
    private final ColdStore cold;
    private final AccessTracker access;
    private final WriteAheadLog wal;
    private final Path walFile;
    private final Path snapshotFile;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);
    /** Serializes migration/retention passes so admin triggers cannot race the scheduler. */
    private final ReentrantLock migrationLock = new ReentrantLock();
    private volatile boolean closed;

    public TieredStorageEngine(TieredStorageProperties props) {
        this(props, Clock.systemUTC());
    }

    public TieredStorageEngine(TieredStorageProperties props, Clock clock) {
        this.props = props;
        this.clock = clock;
        Path dataDir = props.getDataDir();
        this.walFile = dataDir.resolve("hot/wal.log");
        this.snapshotFile = dataDir.resolve("snapshot.json");
        try {
            Files.createDirectories(dataDir.resolve("hot"));
        } catch (IOException e) {
            throw new UncheckedIOException("failed to create data dir " + dataDir, e);
        }
        this.cold = new ColdStore(dataDir.resolve("cold"), props.getCompression());
        this.access = new AccessTracker(10_000, props.getAccessWindow());
        recover();
        this.wal = new WriteAheadLog(walFile);
    }

    // ---------------------------------------------------------------- recovery

    private void recover() {
        if (Files.exists(snapshotFile)) {
            try {
                Snapshot snap = MAPPER.readValue(snapshotFile.toFile(), Snapshot.class);
                for (TelemetrySample s : snap.hot()) {
                    hot.put(s);
                }
                cold.restoreRegistry(snap.blocks());
            } catch (IOException e) {
                throw new UncheckedIOException("failed to load snapshot " + snapshotFile, e);
            }
        }
        for (WalRecord record : WriteAheadLog.replay(walFile)) {
            apply(record);
        }
        // Interrupted operations may have left block files that no WAL record
        // refers to; they are not part of any committed state, so delete them.
        cold.deleteOrphanFiles();
    }

    private void apply(WalRecord record) {
        switch (record.type()) {
            case PUT -> hot.put(record.sample());
            case ARCHIVE -> {
                hot.removeAll(record.ids());
                cold.register(record.block());
            }
            case UNARCHIVE -> {
                // Samples travel inside the record: replay must not depend on
                // the block file, which a later DELETE_BLOCK may have removed.
                for (TelemetrySample s : record.samples()) {
                    hot.put(s);
                }
                cold.unregister(record.block().fileName());
            }
            case DELETE_BLOCK -> {
                cold.unregister(record.targetFile());
                cold.deleteBlockFile(record.targetFile());
            }
            case REPLACE -> {
                cold.register(record.block());
                cold.unregister(record.targetFile());
                cold.deleteBlockFile(record.targetFile());
            }
            case DROP -> hot.removeAll(record.ids());
        }
    }

    // ---------------------------------------------------------------- writes

    /** Commit samples to the hot tier. Returns the ids of the committed samples. */
    public List<String> write(String streamId, List<TelemetrySample> samples) {
        if (samples == null || samples.isEmpty()) {
            return List.of();
        }
        lock.writeLock().lock();
        try {
            wal.appendAll(samples.stream().map(WalRecord::put).toList());
            List<String> ids = new ArrayList<>(samples.size());
            for (TelemetrySample s : samples) {
                hot.put(s);
                ids.add(s.id());
            }
            // Safety valve: keep hot memory bounded even between scans.
            evictOverflow();
            return ids;
        } finally {
            lock.writeLock().unlock();
        }
    }

    // ---------------------------------------------------------------- queries

    /** Time-ordered, id-deduplicated view across hot and cold tiers. */
    public List<TelemetrySample> query(String streamId, long from, long to, QueryOptions options) {
        QueryOptions opts = options != null ? options : QueryOptions.ofTimeout(props.getDefaultQueryTimeoutMs());
        opts.check();
        access.recordAccess(streamId);
        lock.readLock().lock();
        try {
            // Hot first: on id overlap the hot copy is the fresher one.
            Map<String, TelemetrySample> dedup = new LinkedHashMap<>();
            for (TelemetrySample s : hot.range(streamId, from, to)) {
                dedup.put(s.id(), s);
                checkResultBound(dedup.size());
            }
            opts.check();
            for (BlockMeta meta : cold.blocks(streamId)) {
                opts.check();
                if (meta.maxTs() < from || meta.minTs() > to) {
                    continue;
                }
                for (TelemetrySample s : cold.readBlock(meta)) {
                    if (s.timestamp() < from || s.timestamp() > to) {
                        continue;
                    }
                    dedup.putIfAbsent(s.id(), s);
                    checkResultBound(dedup.size());
                }
            }
            List<TelemetrySample> result = new ArrayList<>(dedup.values());
            result.sort(Comparator.comparingLong(TelemetrySample::timestamp).thenComparing(TelemetrySample::id));
            return result;
        } finally {
            lock.readLock().unlock();
        }
    }

    private void checkResultBound(int size) {
        if (size > props.getMaxResultSamples()) {
            throw new QueryResultTooLargeException(
                    "query matched more than " + props.getMaxResultSamples() + " samples");
        }
    }

    // ---------------------------------------------------------------- migration

    /**
     * One migration scan pass: demote aged samples of quiet streams, promote
     * cold blocks of hot streams, force-demote on memory overflow.
     * Returns the number of samples moved. Concurrent invocations collapse:
     * the second one returns 0 immediately.
     */
    public int migrateOnce() {
        if (!migrationLock.tryLock()) {
            return 0;
        }
        try {
            int moved = 0;
            long cutoff = clock.millis() - props.getHotMaxAge().toMillis();
            for (String stream : hotStreams()) {
                if (access.heat(stream) >= props.getPromoteHeatThreshold()) {
                    continue; // stream is hot: keep its data in the hot tier
                }
                while (true) {
                    List<TelemetrySample> batch = selectDemotionBatch(stream, cutoff);
                    if (batch.isEmpty()) {
                        break;
                    }
                    demoteBatch(stream, batch);
                    moved += batch.size();
                }
            }
            for (String stream : coldStreams()) {
                if (access.heat(stream) >= props.getPromoteHeatThreshold()) {
                    moved += promoteNewestBatch(stream);
                }
            }
            moved += evictOverflow();
            return moved;
        } finally {
            migrationLock.unlock();
        }
    }

    private List<String> hotStreams() {
        lock.readLock().lock();
        try {
            return new ArrayList<>(hot.streams());
        } finally {
            lock.readLock().unlock();
        }
    }

    private List<String> coldStreams() {
        lock.readLock().lock();
        try {
            return cold.allBlocks().stream().map(BlockMeta::streamId).distinct().sorted().toList();
        } finally {
            lock.readLock().unlock();
        }
    }

    private List<TelemetrySample> selectDemotionBatch(String stream, long cutoff) {
        lock.readLock().lock();
        try {
            List<TelemetrySample> batch = new ArrayList<>();
            for (TelemetrySample s : hot.oldest(stream, props.getMigrationBatchSize())) {
                if (s.timestamp() > cutoff) {
                    break; // oldest-first: everything after is too young
                }
                batch.add(s);
            }
            return batch;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Move one batch hot -> cold. The block file is written and fsynced before
     * the ARCHIVE record lands in the WAL, so a crash either leaves an orphan
     * file (cleaned up on restart) or a fully committed migration.
     */
    private void demoteBatch(String stream, List<TelemetrySample> batch) {
        BlockMeta meta = cold.writeBlock(stream, batch, false);
        List<String> ids = batch.stream().map(TelemetrySample::id).toList();
        lock.writeLock().lock();
        try {
            wal.append(WalRecord.archive(meta, ids));
            hot.removeAll(ids);
            cold.register(meta);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Move the newest cold blocks of a stream back to hot, up to one batch of
     * samples per pass. UNARCHIVE and DELETE_BLOCK are appended in the same
     * write-lock section; a crash between them is reconciled at recovery
     * (samples reappear in hot, the leftover file is removed as an orphan).
     */
    private int promoteNewestBatch(String stream) {
        List<BlockMeta> metas;
        lock.readLock().lock();
        try {
            metas = cold.blocks(stream);
        } finally {
            lock.readLock().unlock();
        }
        int budget = props.getMigrationBatchSize();
        int moved = 0;
        for (int i = metas.size() - 1; i >= 0 && budget > 0; i--) {
            BlockMeta meta = metas.get(i);
            lock.readLock().lock();
            try {
                if (hot.size() + meta.count() > props.getMaxHotSamples()) {
                    break; // promoting would just trigger overflow demotion
                }
            } finally {
                lock.readLock().unlock();
            }
            List<TelemetrySample> samples = cold.readBlock(meta);
            lock.writeLock().lock();
            try {
                wal.appendAll(List.of(WalRecord.unarchive(meta, samples), WalRecord.deleteBlock(meta.fileName())));
                for (TelemetrySample s : samples) {
                    hot.put(s);
                }
                cold.unregister(meta.fileName());
                cold.deleteBlockFile(meta.fileName());
            } finally {
                lock.writeLock().unlock();
            }
            budget -= samples.size();
            moved += samples.size();
        }
        return moved;
    }

    /**
     * Force-demote the globally oldest samples while the hot tier exceeds its
     * configured bound. Reentrant-safe: may be called with the write lock held.
     */
    private int evictOverflow() {
        int moved = 0;
        while (true) {
            String oldestStream;
            int overflow;
            lock.readLock().lock();
            try {
                overflow = hot.size() - props.getMaxHotSamples();
                if (overflow <= 0) {
                    return moved;
                }
                oldestStream = null;
                long oldestTs = Long.MAX_VALUE;
                for (String stream : hot.streams()) {
                    List<TelemetrySample> first = hot.oldest(stream, 1);
                    if (!first.isEmpty() && first.get(0).timestamp() < oldestTs) {
                        oldestTs = first.get(0).timestamp();
                        oldestStream = stream;
                    }
                }
            } finally {
                lock.readLock().unlock();
            }
            if (oldestStream == null) {
                return moved;
            }
            List<TelemetrySample> batch;
            lock.readLock().lock();
            try {
                batch = hot.oldest(oldestStream,
                        Math.min(props.getMigrationBatchSize(), overflow));
            } finally {
                lock.readLock().unlock();
            }
            if (batch.isEmpty()) {
                return moved;
            }
            demoteBatch(oldestStream, batch);
            moved += batch.size();
        }
    }

    // ---------------------------------------------------------------- retention

    /**
     * One retention pass: delete expired blocks and hot samples, downsample
     * raw blocks whose raw retention elapsed. Returns affected sample count.
     */
    public int enforceRetentionOnce() {
        if (!migrationLock.tryLock()) {
            return 0;
        }
        try {
            int affected = 0;
            long rawCutoff = clock.millis() - props.getRawRetention().toMillis();
            long deleteCutoff = rawCutoff - props.getDownsampledRetention().toMillis();

            List<BlockMeta> blocks;
            lock.readLock().lock();
            try {
                blocks = cold.allBlocks();
            } finally {
                lock.readLock().unlock();
            }
            for (BlockMeta meta : blocks) {
                if (meta.maxTs() < deleteCutoff) {
                    // Fully expired: remove the whole block.
                    lock.writeLock().lock();
                    try {
                        wal.append(WalRecord.deleteBlock(meta.fileName()));
                        cold.unregister(meta.fileName());
                        cold.deleteBlockFile(meta.fileName());
                    } finally {
                        lock.writeLock().unlock();
                    }
                    affected += meta.count();
                } else if (!meta.downsampled() && meta.maxTs() < rawCutoff) {
                    // Raw retention elapsed but still within downsampled retention.
                    List<TelemetrySample> samples = cold.readBlock(meta);
                    List<TelemetrySample> downsampled = downsample(meta.streamId(), samples);
                    BlockMeta newMeta = cold.writeBlock(meta.streamId(), downsampled, true);
                    lock.writeLock().lock();
                    try {
                        wal.append(WalRecord.replace(newMeta, meta.fileName()));
                        cold.register(newMeta);
                        cold.unregister(meta.fileName());
                        cold.deleteBlockFile(meta.fileName());
                    } finally {
                        lock.writeLock().unlock();
                    }
                    affected += meta.count();
                }
            }
            affected += dropExpiredHotSamples(deleteCutoff);
            checkpoint();
            return affected;
        } finally {
            migrationLock.unlock();
        }
    }

    /** Delete hot samples past the delete horizon (expired data must go regardless of tier). */
    private int dropExpiredHotSamples(long deleteCutoff) {
        lock.writeLock().lock();
        try {
            int affected = 0;
            for (String stream : hot.streams()) {
                List<String> expired = new ArrayList<>();
                for (TelemetrySample s : hot.oldest(stream, props.getMigrationBatchSize())) {
                    if (s.timestamp() >= deleteCutoff) {
                        break;
                    }
                    expired.add(s.id());
                }
                if (!expired.isEmpty()) {
                    wal.append(WalRecord.drop(expired));
                    hot.removeAll(expired);
                    affected += expired.size();
                }
            }
            return affected;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /** Aggregate raw samples into per-bucket averages at the configured resolution. */
    private List<TelemetrySample> downsample(String streamId, List<TelemetrySample> samples) {
        long resolutionMs = Math.max(1, props.getDownsampleResolution().toMillis());
        Map<Long, Map<String, double[]>> buckets = new TreeMap<>();
        for (TelemetrySample s : samples) {
            long bucket = Math.floorDiv(s.timestamp(), resolutionMs) * resolutionMs;
            Map<String, double[]> fields = buckets.computeIfAbsent(bucket, k -> new TreeMap<>());
            s.values().forEach((field, value) -> {
                double[] sumCount = fields.computeIfAbsent(field, k -> new double[2]);
                sumCount[0] += value;
                sumCount[1] += 1;
            });
        }
        List<TelemetrySample> result = new ArrayList<>(buckets.size());
        buckets.forEach((bucketTs, fields) -> {
            Map<String, Double> averages = new TreeMap<>();
            fields.forEach((field, sumCount) -> averages.put(field, sumCount[0] / sumCount[1]));
            result.add(new TelemetrySample(UUID.randomUUID().toString(), streamId, bucketTs, averages, true));
        });
        return result;
    }

    // ---------------------------------------------------------------- checkpoint & stats

    /** Persist a snapshot of hot + cold registry, then rotate the WAL. */
    public void checkpoint() {
        lock.writeLock().lock();
        try {
            Snapshot snap = new Snapshot(hot.snapshotAll(), cold.allBlocks());
            Files.createDirectories(snapshotFile.getParent());
            Path tmp = snapshotFile.resolveSibling(snapshotFile.getFileName() + ".tmp");
            byte[] json = MAPPER.writeValueAsBytes(snap);
            try (FileChannel ch = FileChannel.open(tmp, StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
                ch.write(java.nio.ByteBuffer.wrap(json));
                ch.force(true);
            }
            Files.move(tmp, snapshotFile, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            wal.rotate();
        } catch (IOException e) {
            throw new UncheckedIOException("failed to checkpoint", e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public Stats stats() {
        lock.readLock().lock();
        try {
            int coldSamples = 0;
            for (BlockMeta meta : cold.allBlocks()) {
                coldSamples += meta.count();
            }
            return new Stats(hot.size(), cold.allBlocks().size(), coldSamples, access.trackedStreams());
        } finally {
            lock.readLock().unlock();
        }
    }

    /** Checkpoint (snapshot + WAL rotate) and close. */
    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        checkpoint();
        wal.close();
    }

    public record Stats(int hotSamples, int coldBlocks, int coldSamples, int trackedStreams) {
    }
}
