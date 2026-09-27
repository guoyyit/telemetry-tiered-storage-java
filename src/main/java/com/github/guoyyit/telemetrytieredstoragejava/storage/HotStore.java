package com.github.guoyyit.telemetrytieredstoragejava.storage;

import com.github.guoyyit.telemetrytieredstoragejava.model.TelemetrySample;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

/**
 * In-memory hot tier: per-stream time-ordered indexes plus a global id index.
 * All mutating methods are called under the engine write lock; readers hold
 * the engine read lock, so this class itself needs no internal locking.
 * The key is (timestamp, id) so samples sharing a timestamp never collide.
 */
public class HotStore {

    /** Lexicographically larger than any real id id (ids are UUIDs). */
    private static final String MAX_ID = "￿￿￿";

    private record Key(long ts, String id) implements Comparable<Key> {
        @Override
        public int compareTo(Key other) {
            int c = Long.compare(ts, other.ts);
            return c != 0 ? c : id.compareTo(other.id);
        }
    }

    private final Map<String, NavigableMap<Key, TelemetrySample>> byStream = new HashMap<>();
    private final Map<String, TelemetrySample> byId = new HashMap<>();

    public void put(TelemetrySample sample) {
        byStream.computeIfAbsent(sample.streamId(), s -> new TreeMap<>())
                .put(new Key(sample.timestamp(), sample.id()), sample);
        byId.put(sample.id(), sample);
    }

    public TelemetrySample remove(String id) {
        TelemetrySample removed = byId.remove(id);
        if (removed != null) {
            NavigableMap<Key, TelemetrySample> index = byStream.get(removed.streamId());
            if (index != null) {
                index.remove(new Key(removed.timestamp(), id));
                if (index.isEmpty()) {
                    byStream.remove(removed.streamId());
                }
            }
        }
        return removed;
    }

    public List<TelemetrySample> removeAll(Collection<String> ids) {
        List<TelemetrySample> removed = new ArrayList<>(ids.size());
        for (String id : ids) {
            TelemetrySample s = remove(id);
            if (s != null) {
                removed.add(s);
            }
        }
        return removed;
    }

    /** Samples of the stream with from <= timestamp <= to, ordered by (timestamp, id). */
    public List<TelemetrySample> range(String streamId, long from, long to) {
        NavigableMap<Key, TelemetrySample> index = byStream.get(streamId);
        if (index == null) {
            return List.of();
        }
        return new ArrayList<>(index.subMap(new Key(from, ""), true,
                new Key(to, MAX_ID), true).values());
    }

    /** Up to limit oldest samples of the stream (demotion candidates). */
    public List<TelemetrySample> oldest(String streamId, int limit) {
        NavigableMap<Key, TelemetrySample> index = byStream.get(streamId);
        if (index == null) {
            return List.of();
        }
        List<TelemetrySample> result = new ArrayList<>(Math.min(limit, index.size()));
        for (TelemetrySample s : index.values()) {
            if (result.size() >= limit) {
                break;
            }
            result.add(s);
        }
        return result;
    }

    public int size() {
        return byId.size();
    }

    public int size(String streamId) {
        NavigableMap<Key, TelemetrySample> index = byStream.get(streamId);
        return index == null ? 0 : index.size();
    }

    public Set<String> streams() {
        return new LinkedHashSet<>(byStream.keySet());
    }

    public List<TelemetrySample> snapshotAll() {
        return new ArrayList<>(byId.values());
    }

    public void clear() {
        byStream.clear();
        byId.clear();
    }
}
