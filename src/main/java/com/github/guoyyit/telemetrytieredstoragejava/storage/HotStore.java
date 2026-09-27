package com.github.guoyyit.telemetrytieredstoragejava.storage;

import com.github.guoyyit.telemetrytieredstoragejava.model.TelemetrySample;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Supplier;

/**
 * In-memory hot tier backed by the write ahead log.
 *
 * <p>Keeps a per-stream time-ordered index so range scans are efficient, and
 * tracks the last access time of every sample so the tiering manager can
 * find cooling candidates. All structures are concurrent; mutating a sample
 * always updates both the id map and the time index atomically enough for
 * readers to never see a sample in neither structure (they may briefly see
 * it in both, which is harmless because reads deduplicate by id at the
 * engine level).
 */
public final class HotStore {

    /** Composite (timestamp, id) key for the per-stream time index. */
    private record TsId(long timestamp, String id) implements Comparable<TsId> {
        @Override
        public int compareTo(TsId other) {
            int c = Long.compare(timestamp, other.timestamp);
            return c != 0 ? c : id.compareTo(other.id);
        }
    }

    private final ConcurrentHashMap<String, TelemetrySample> byId = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ConcurrentSkipListMap<TsId, TelemetrySample>> byStream =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Long> lastAccessMillis = new ConcurrentHashMap<>();
    private final Supplier<Long> clock;

    public HotStore() {
        this(System::currentTimeMillis);
    }

    public HotStore(Supplier<Long> clock) {
        this.clock = clock;
    }

    public void put(TelemetrySample sample) {
        byId.put(sample.getId(), sample);
        byStream.computeIfAbsent(sample.getStream(), s -> new ConcurrentSkipListMap<>())
                .put(new TsId(sample.getTimestamp(), sample.getId()), sample);
        lastAccessMillis.put(sample.getId(), clock.get());
    }

    public Optional<TelemetrySample> get(String id) {
        TelemetrySample sample = byId.get(id);
        if (sample != null) {
            touch(id);
        }
        return Optional.ofNullable(sample);
    }

    public void remove(String id) {
        TelemetrySample removed = byId.remove(id);
        if (removed != null) {
            ConcurrentSkipListMap<TsId, TelemetrySample> index = byStream.get(removed.getStream());
            if (index != null) {
                index.remove(new TsId(removed.getTimestamp(), removed.getId()));
            }
            lastAccessMillis.remove(id);
        }
    }

    /** Time-ordered scan of one stream; touches every returned sample. */
    public List<TelemetrySample> scan(String stream, long fromInclusive, long toExclusive) {
        ConcurrentSkipListMap<TsId, TelemetrySample> index = byStream.get(stream);
        if (index == null) {
            return List.of();
        }
        List<TelemetrySample> result = new ArrayList<>(
                index.subMap(new TsId(fromInclusive, ""), true,
                                new TsId(toExclusive, ""), false)
                        .values());
        long now = clock.get();
        for (TelemetrySample s : result) {
            lastAccessMillis.put(s.getId(), now);
        }
        return result;
    }

    /** Records an access for heat tracking. */
    public void touch(String id) {
        lastAccessMillis.computeIfPresent(id, (k, v) -> clock.get());
    }

    public long lastAccessOf(String id) {
        return lastAccessMillis.getOrDefault(id, 0L);
    }

    /**
     * Returns up to {@code limit} samples eligible for cooling: older than
     * {@code hotMaxAgeMillis} and not accessed for {@code coolAfterIdleMillis}.
     */
    public List<TelemetrySample> findCoolingCandidates(long hotMaxAgeMillis,
                                                       long coolAfterIdleMillis,
                                                       int limit) {
        long now = clock.get();
        List<TelemetrySample> candidates = new ArrayList<>();
        for (TelemetrySample sample : byId.values()) {
            if (candidates.size() >= limit) {
                break;
            }
            long ageMillis = now - sample.getTimestamp();
            long idleMillis = now - lastAccessMillis.getOrDefault(sample.getId(), 0L);
            if (ageMillis >= hotMaxAgeMillis && idleMillis >= coolAfterIdleMillis) {
                candidates.add(sample);
            }
        }
        candidates.sort(Comparator.comparingLong(TelemetrySample::getTimestamp));
        return candidates;
    }

    /**
     * Returns up to {@code limit} samples that were accessed within the last
     * {@code idleMillis}; used by tests and warming heuristics.
     */
    public List<TelemetrySample> findRecentlyAccessed(long idleMillis, int limit) {
        long now = clock.get();
        List<TelemetrySample> result = new ArrayList<>();
        for (TelemetrySample sample : byId.values()) {
            if (result.size() >= limit) {
                break;
            }
            if (now - lastAccessMillis.getOrDefault(sample.getId(), 0L) < idleMillis) {
                result.add(sample);
            }
        }
        return result;
    }

    /** Returns ids of hot samples older than the given timestamp. */
    public List<TelemetrySample> findOlderThan(long timestampExclusive, int limit) {
        List<TelemetrySample> result = new ArrayList<>();
        outer:
        for (ConcurrentSkipListMap<TsId, TelemetrySample> index : byStream.values()) {
            for (TelemetrySample s : index.headMap(new TsId(timestampExclusive, ""), false).values()) {
                result.add(s);
                if (result.size() >= limit) {
                    break outer;
                }
            }
        }
        return result;
    }

    public int size() {
        return byId.size();
    }
}
