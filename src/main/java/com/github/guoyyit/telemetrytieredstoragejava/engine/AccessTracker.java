package com.github.guoyyit.telemetrytieredstoragejava.engine;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Bounded sliding-window access counter used for promote/demote decisions.
 * Memory is bounded two ways: at most {@code maxStreams} streams are tracked
 * (LRU eviction) and each stream keeps at most {@code maxEventsPerStream}
 * recent access timestamps. All methods are called under the engine lock.
 */
public class AccessTracker {

    private static final int MAX_EVENTS_PER_STREAM = 1024;

    private final long windowMillis;
    private final Map<String, Deque<Long>> events;

    public AccessTracker(int maxStreams, Duration window) {
        this.windowMillis = window.toMillis();
        this.events = new LinkedHashMap<>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, Deque<Long>> eldest) {
                return size() > maxStreams;
            }
        };
    }

    public synchronized void recordAccess(String streamId) {
        long now = System.currentTimeMillis();
        Deque<Long> deque = events.computeIfAbsent(streamId, k -> new ArrayDeque<>());
        evictExpired(deque, now);
        deque.addLast(now);
        while (deque.size() > MAX_EVENTS_PER_STREAM) {
            deque.pollFirst();
        }
    }

    /** Number of accesses inside the sliding window. */
    public synchronized int heat(String streamId) {
        Deque<Long> deque = events.get(streamId);
        if (deque == null) {
            return 0;
        }
        evictExpired(deque, System.currentTimeMillis());
        return deque.size();
    }

    public synchronized void remove(String streamId) {
        events.remove(streamId);
    }

    public synchronized int trackedStreams() {
        return events.size();
    }

    private void evictExpired(Deque<Long> deque, long now) {
        long cutoff = now - windowMillis;
        while (!deque.isEmpty() && deque.peekFirst() < cutoff) {
            deque.pollFirst();
        }
    }
}
