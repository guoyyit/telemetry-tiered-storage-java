package com.github.guoyyit.telemetrytieredstoragejava.migration;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Persistent journal of migration batches. A batch is journaled before it
 * starts and removed only after it is fully applied, so a crash
 * mid-migration is rolled forward exactly once on recovery.
 *
 * <p>The whole pending set is tiny (at most a few in-flight batches), so
 * every mutation rewrites the single journal file via tmp + atomic rename.
 */
public final class MigrationJournal implements Closeable {

    private final Path file;
    private final Map<Long, List<String>> pending = new LinkedHashMap<>();
    private long nextId = 1;

    public MigrationJournal(Path directory) {
        try {
            Files.createDirectories(directory);
            this.file = directory.resolve("migrations.journal");
            load();
        } catch (IOException e) {
            throw new UncheckedIOException("failed to open migration journal in " + directory, e);
        }
    }

    private void load() throws IOException {
        if (!Files.exists(file)) {
            return;
        }
        for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
            line = line.trim();
            if (line.isEmpty()) {
                continue;
            }
            int sep = line.indexOf(':');
            if (sep <= 0) {
                continue; // tolerate corrupt lines
            }
            try {
                long id = Long.parseLong(line.substring(0, sep));
                List<String> ids = new ArrayList<>();
                for (String s : line.substring(sep + 1).split(",")) {
                    if (!s.isEmpty()) {
                        ids.add(s);
                    }
                }
                pending.put(id, ids);
                nextId = Math.max(nextId, id + 1);
            } catch (NumberFormatException ignored) {
                // skip corrupt line
            }
        }
    }

    /** Records a pending batch of sample ids, returns the batch id. */
    public synchronized long beginBatch(List<String> sampleIds) {
        long id = nextId++;
        pending.put(id, List.copyOf(sampleIds));
        persist();
        return id;
    }

    public synchronized void markDone(long batchId) {
        if (pending.remove(batchId) != null) {
            persist();
        }
    }

    /** Ids of batches that were started but never marked done. */
    public synchronized Map<Long, List<String>> pendingBatches() {
        return new LinkedHashMap<>(pending);
    }

    private void persist() {
        try {
            Path tmp = file.resolveSibling(file.getFileName() + ".tmp");
            List<String> lines = new ArrayList<>();
            for (Map.Entry<Long, List<String>> e : pending.entrySet()) {
                lines.add(e.getKey() + ":" + String.join(",", e.getValue()));
            }
            Files.write(tmp, lines, StandardCharsets.UTF_8);
            Files.move(tmp, file, StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to persist migration journal", e);
        }
    }

    @Override
    public synchronized void close() {
        // nothing to release: every mutation is persisted eagerly
    }
}
