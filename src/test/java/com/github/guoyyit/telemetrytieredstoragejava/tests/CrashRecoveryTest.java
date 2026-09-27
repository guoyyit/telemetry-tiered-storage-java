package com.github.guoyyit.telemetrytieredstoragejava.tests;

import com.github.guoyyit.telemetrytieredstoragejava.config.StorageProperties;
import com.github.guoyyit.telemetrytieredstoragejava.migration.MigrationJournal;
import com.github.guoyyit.telemetrytieredstoragejava.model.TelemetrySample;
import com.github.guoyyit.telemetrytieredstoragejava.storage.StorageEngine;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A crash mid-migration must be rolled forward on restart: no sample is
 * migrated twice, none is lost, and post-recovery queries return exactly
 * what was visible before the crash.
 */
class CrashRecoveryTest {

    @TempDir
    Path dataDir;

    private StorageProperties properties() {
        StorageProperties p = new StorageProperties();
        p.setDataDir(dataDir.toString());
        p.setHotMaxAge(Duration.ofMillis(1));
        p.setCoolAfterIdle(Duration.ZERO);
        p.setMigrationBatchSize(100);
        p.setMaxHotSamples(10_000);
        return p;
    }

    private StorageEngine openEngineWithJournal(StorageProperties props) {
        StorageEngine engine = new StorageEngine(props);
        engine.recover();
        engine.setMigrationJournal(new MigrationJournal(Path.of(props.getDataDir()).resolve("journal")));
        return engine;
    }

    @Test
    void crashBeforeColdWriteIsRolledForward() {
        StorageProperties props = properties();
        long base = System.currentTimeMillis() - 100_000;

        StorageEngine engine = openEngineWithJournal(props);
        List<TelemetrySample> samples = new java.util.ArrayList<>();
        for (int i = 0; i < 20; i++) {
            samples.add(engine.write("cpu", base + i, Map.of(), i));
        }
        // simulate crash: batch journaled but nothing applied yet
        MigrationJournal journal = new MigrationJournal(Path.of(props.getDataDir()).resolve("journal"));
        journal.beginBatch(samples.stream().map(TelemetrySample::getId).toList());
        journal.close();
        engine.close();

        StorageEngine recovered = openEngineWithJournal(props);
        recovered.resumePendingMigrations();

        List<TelemetrySample> all = recovered.query("cpu", base - 1, base + 100);
        assertEquals(20, all.size());
        assertEquals(samples.stream().map(TelemetrySample::getId).collect(java.util.stream.Collectors.toSet()),
                all.stream().map(TelemetrySample::getId).collect(java.util.stream.Collectors.toSet()));
        assertEquals(0, recovered.hotStore().size(), "pending batch should have been rolled forward");
        assertTrue(new MigrationJournal(Path.of(props.getDataDir()).resolve("journal"))
                .pendingBatches().isEmpty());
        recovered.close();
    }

    @Test
    void crashAfterColdWriteBeforeHotDeleteDoesNotDuplicate() {
        StorageProperties props = properties();
        long base = System.currentTimeMillis() - 100_000;

        StorageEngine engine = openEngineWithJournal(props);
        List<TelemetrySample> samples = new java.util.ArrayList<>();
        for (int i = 0; i < 20; i++) {
            samples.add(engine.write("cpu", base + i, Map.of(), i));
        }
        // simulate crash: cold copy written, journal pending, hot copy still present
        engine.coldStore().appendBatch(samples);
        MigrationJournal journal = new MigrationJournal(Path.of(props.getDataDir()).resolve("journal"));
        journal.beginBatch(samples.stream().map(TelemetrySample::getId).toList());
        journal.close();
        engine.close();

        StorageEngine recovered = openEngineWithJournal(props);
        recovered.resumePendingMigrations();

        List<TelemetrySample> all = recovered.query("cpu", base - 1, base + 100);
        assertEquals(20, all.size(), "no duplicates after roll-forward");
        Set<String> ids = new HashSet<>();
        for (TelemetrySample s : all) {
            assertTrue(ids.add(s.getId()));
        }
        recovered.close();
    }

    @Test
    void crashAfterHotDeleteBeforeJournalCommitLosesNothing() {
        StorageProperties props = properties();
        long base = System.currentTimeMillis() - 100_000;

        StorageEngine engine = openEngineWithJournal(props);
        List<TelemetrySample> samples = new java.util.ArrayList<>();
        for (int i = 0; i < 20; i++) {
            samples.add(engine.write("cpu", base + i, Map.of(), i));
        }
        // fully migrate, then simulate a stale pending journal entry left by
        // a crash just before markDone
        engine.migrateToCold(samples);
        MigrationJournal journal = new MigrationJournal(Path.of(props.getDataDir()).resolve("journal"));
        journal.beginBatch(samples.stream().map(TelemetrySample::getId).toList());
        journal.close();
        engine.close();

        StorageEngine recovered = openEngineWithJournal(props);
        recovered.resumePendingMigrations();

        List<TelemetrySample> all = recovered.query("cpu", base - 1, base + 100);
        assertEquals(20, all.size());
        assertTrue(new MigrationJournal(Path.of(props.getDataDir()).resolve("journal"))
                .pendingBatches().isEmpty());
        recovered.close();
    }

    @Test
    void queryResultsAreIdenticalBeforeAndAfterCrash() {
        StorageProperties props = properties();
        long base = System.currentTimeMillis() - 100_000;

        StorageEngine engine = openEngineWithJournal(props);
        for (int i = 0; i < 50; i++) {
            engine.write("cpu", base + i, Map.of("k", "v" + i), i);
        }
        // migrate half, leave a pending batch, then "crash"
        List<TelemetrySample> half = engine.findCoolingCandidates(25);
        engine.migrateToCold(half);
        List<String> before = engine.query("cpu", base - 1, base + 1000)
                .stream().map(TelemetrySample::getId).toList();
        engine.close();

        StorageEngine recovered = openEngineWithJournal(props);
        recovered.resumePendingMigrations();
        List<String> after = recovered.query("cpu", base - 1, base + 1000)
                .stream().map(TelemetrySample::getId).toList();
        assertEquals(before, after);
        recovered.close();
    }
}
