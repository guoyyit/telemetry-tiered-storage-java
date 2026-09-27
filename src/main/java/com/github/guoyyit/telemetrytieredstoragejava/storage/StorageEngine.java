package com.github.guoyyit.telemetrytieredstoragejava.storage;

import com.github.guoyyit.telemetrytieredstoragejava.config.StorageProperties;
import com.github.guoyyit.telemetrytieredstoragejava.migration.MigrationJournal;
import com.github.guoyyit.telemetrytieredstoragejava.model.TelemetrySample;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Facade combining the hot and cold tiers. All writes and queries go
 * through this class, which guarantees consistency during migration.
 *
 * <p>Consistency protocol:
 * <ul>
 *   <li>Migration copies samples to the cold tier <em>before</em> removing
 *       them from the hot tier, so a sample is always visible in at least
 *       one tier.</li>
 *   <li>Queries hold the migration read lock for their whole duration, so
 *       the hot-tier commit step of a migration cannot interleave with a
 *       query. Samples visible in both tiers are deduplicated by id.</li>
 *   <li>Every hot-tier mutation is written to the WAL (and fsynced) before
 *       being applied, so committed samples survive a restart.</li>
 *   <li>Migration batches are journaled; pending batches are rolled forward
 *       idempotently on recovery.</li>
 * </ul>
 */
public final class StorageEngine {

    private static final int MAX_TRACKED_COLD_ACCESSES = 10_000;
    private static final int WARM_UP_ACCESS_THRESHOLD = 2;

    private final StorageProperties properties;
    private final HotStore hotStore;
    private final ColdStore coldStore;
    private final WriteAheadLog wal;

    /** Read lock = query in progress; write lock = hot-tier commit of a migration/retention. */
    private final ReentrantReadWriteLock migrationLock = new ReentrantReadWriteLock(true);

    /** Access counts of cold samples, used by the warming heuristic. Bounded. */
    private final Map<String, TrackedColdSample> coldAccesses = new ConcurrentHashMap<>();

    private volatile MigrationJournal journal;

    private record TrackedColdSample(TelemetrySample sample, AtomicInteger accesses) {
    }

    public StorageEngine(StorageProperties properties) {
        this.properties = properties;
        Path root = Path.of(properties.getDataDir());
        this.wal = new WriteAheadLog(root.resolve("wal"));
        this.hotStore = new HotStore();
        this.coldStore = new ColdStore(root.resolve("cold"), properties.getCompression());
    }

    /** Replays the WAL to rebuild the hot tier. Call before serving traffic. */
    public void recover() {
        for (WriteAheadLog.Record record : wal.replay()) {
            if (record instanceof WriteAheadLog.Record.Put put) {
                hotStore.put(put.sample());
            } else if (record instanceof WriteAheadLog.Record.Delete delete) {
                hotStore.remove(delete.sampleId());
            }
        }
    }

    public void setMigrationJournal(MigrationJournal journal) {
        this.journal = journal;
    }

    /**
     * Rolls forward migration batches that were in flight when the process
     * stopped. Idempotent: samples already archived are not re-archived, and
     * queries deduplicate by id in any case.
     */
    public void resumePendingMigrations() {
        MigrationJournal j = journal;
        if (j == null) {
            return;
        }
        for (Map.Entry<Long, List<String>> pending : j.pendingBatches().entrySet()) {
            List<TelemetrySample> stillHot = new ArrayList<>();
            for (String id : pending.getValue()) {
                Optional<TelemetrySample> sample = hotStore.get(id);
                sample.ifPresent(stillHot::add);
            }
            applyMigration(stillHot);
            j.markDone(pending.getKey());
        }
    }

    /** Persists a new sample with a unique id and returns it. */
    public TelemetrySample write(String stream, long timestamp,
                                 Map<String, String> dimensions, double value) {
        TelemetrySample sample = new TelemetrySample(UUID.randomUUID().toString(),
                stream, timestamp, dimensions, value);
        wal.appendPut(sample); // durable before visible
        hotStore.put(sample);
        enforceHotCapacity();
        return sample;
    }

    /** Keeps the hot tier bounded by synchronously cooling the oldest samples. */
    private void enforceHotCapacity() {
        while (hotStore.size() > properties.getMaxHotSamples()) {
            List<TelemetrySample> oldest =
                    hotStore.findOlderThan(Long.MAX_VALUE, properties.getMigrationBatchSize());
            if (oldest.isEmpty()) {
                return;
            }
            migrateToCold(oldest);
        }
    }

    /**
     * Time-ordered query across both tiers. Holds the read lock so a
     * migration commit cannot interleave; duplicates (a sample briefly
     * present in both tiers) are removed by id.
     */
    public List<TelemetrySample> query(String stream, long fromInclusive, long toExclusive) {
        migrationLock.readLock().lock();
        try {
            List<TelemetrySample> hot = hotStore.scan(stream, fromInclusive, toExclusive);
            List<TelemetrySample> cold = coldStore.scan(stream, fromInclusive, toExclusive);
            recordColdAccesses(cold);
            Map<String, TelemetrySample> merged = new LinkedHashMap<>();
            for (TelemetrySample s : hot) {
                merged.put(s.getId(), s);
            }
            for (TelemetrySample s : cold) {
                merged.putIfAbsent(s.getId(), s);
            }
            List<TelemetrySample> result = new ArrayList<>(merged.values());
            result.sort(Comparator.comparingLong(TelemetrySample::getTimestamp)
                    .thenComparing(TelemetrySample::getId));
            return result;
        } finally {
            migrationLock.readLock().unlock();
        }
    }

    /** Cooling candidates according to age and idle time. */
    public List<TelemetrySample> findCoolingCandidates(int limit) {
        return hotStore.findCoolingCandidates(
                properties.getHotMaxAge().toMillis(),
                properties.getCoolAfterIdle().toMillis(),
                limit);
    }

    /**
     * Migrates a batch to the cold tier with journaling. Serialized so
     * concurrent callers (tiering manager, capacity enforcement) cannot
     * interleave batches.
     */
    public synchronized int migrateToCold(List<TelemetrySample> batch) {
        if (batch.isEmpty()) {
            return 0;
        }
        MigrationJournal j = journal;
        Long batchId = null;
        if (j != null) {
            batchId = j.beginBatch(batch.stream().map(TelemetrySample::getId).toList());
        }
        int migrated = applyMigration(batch);
        if (j != null) {
            j.markDone(batchId);
        }
        return migrated;
    }

    /**
     * The actual migration: archive what is not yet archived, then remove
     * from the hot tier under the write lock. Idempotent.
     */
    private int applyMigration(List<TelemetrySample> batch) {
        if (batch.isEmpty()) {
            return 0;
        }
        List<TelemetrySample> toArchive = new ArrayList<>(batch);
        // Idempotency: skip samples already present in the cold tier.
        Map<String, List<TelemetrySample>> perStream = new LinkedHashMap<>();
        for (TelemetrySample s : batch) {
            perStream.computeIfAbsent(s.getStream(), k -> new ArrayList<>()).add(s);
        }
        toArchive.clear();
        for (Map.Entry<String, List<TelemetrySample>> e : perStream.entrySet()) {
            long from = e.getValue().stream().mapToLong(TelemetrySample::getTimestamp).min().orElse(0);
            long to = e.getValue().stream().mapToLong(TelemetrySample::getTimestamp).max().orElse(0) + 1;
            var existing = coldStore.findExistingIds(e.getKey(), from, to);
            for (TelemetrySample s : e.getValue()) {
                if (!existing.contains(s.getId())) {
                    toArchive.add(s);
                }
            }
        }
        coldStore.appendBatch(toArchive);
        migrationLock.writeLock().lock();
        try {
            wal.appendDeleteAll(batch.stream().map(TelemetrySample::getId).toList());
            for (TelemetrySample s : batch) {
                hotStore.remove(s.getId());
            }
        } finally {
            migrationLock.writeLock().unlock();
        }
        return batch.size();
    }

    private void recordColdAccesses(List<TelemetrySample> coldSamples) {
        if (coldSamples.isEmpty()) {
            return;
        }
        if (coldAccesses.size() > MAX_TRACKED_COLD_ACCESSES) {
            coldAccesses.clear(); // bounded memory: drop and re-learn
        }
        for (TelemetrySample s : coldSamples) {
            coldAccesses.compute(s.getId(), (id, tracked) -> {
                if (tracked == null) {
                    return new TrackedColdSample(s, new AtomicInteger(1));
                }
                tracked.accesses().incrementAndGet();
                return tracked;
            });
        }
    }

    /**
     * Moves cold samples that were queried repeatedly back into the hot
     * tier (the hot tier acts as a cache; the cold archive remains the
     * durable copy). Returns the number of warmed samples.
     */
    public int warmUpHotSamples() {
        List<TelemetrySample> toWarm = new ArrayList<>();
        for (Map.Entry<String, TrackedColdSample> e : coldAccesses.entrySet()) {
            if (e.getValue().accesses().get() >= WARM_UP_ACCESS_THRESHOLD
                    && hotStore.get(e.getKey()).isEmpty()) {
                toWarm.add(e.getValue().sample());
            }
        }
        if (toWarm.isEmpty()) {
            return 0;
        }
        migrationLock.writeLock().lock();
        try {
            for (TelemetrySample s : toWarm) {
                wal.appendPut(s);
                hotStore.put(s);
                coldAccesses.remove(s.getId());
            }
        } finally {
            migrationLock.writeLock().unlock();
        }
        return toWarm.size();
    }

    /** Deletes expired hot samples in batches; each batch is one write-lock critical section. */
    public int deleteExpiredHot(long cutoffExclusive, int limit) {
        migrationLock.writeLock().lock();
        try {
            List<TelemetrySample> expired = hotStore.findOlderThan(cutoffExclusive, limit);
            wal.appendDeleteAll(expired.stream().map(TelemetrySample::getId).toList());
            for (TelemetrySample s : expired) {
                hotStore.remove(s.getId());
            }
            return expired.size();
        } finally {
            migrationLock.writeLock().unlock();
        }
    }

    /** Deletes expired cold archives. */
    public int deleteExpiredCold(long cutoffExclusive) {
        return coldStore.deleteOlderThan(cutoffExclusive);
    }

    public HotStore hotStore() {
        return hotStore;
    }

    public ColdStore coldStore() {
        return coldStore;
    }

    public void close() {
        wal.close();
    }
}
