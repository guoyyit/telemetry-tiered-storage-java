package com.github.guoyyit.telemetrytieredstoragejava.config;

import com.github.guoyyit.telemetrytieredstoragejava.migration.MigrationJournal;
import com.github.guoyyit.telemetrytieredstoragejava.migration.TieringManager;
import com.github.guoyyit.telemetrytieredstoragejava.query.QueryService;
import com.github.guoyyit.telemetrytieredstoragejava.retention.RetentionManager;
import com.github.guoyyit.telemetrytieredstoragejava.storage.StorageEngine;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.nio.file.Path;

/**
 * Wires the storage engine and its background services.
 */
@Configuration
@EnableConfigurationProperties(StorageProperties.class)
public class StorageConfiguration {

    @Bean(destroyMethod = "close")
    public StorageEngine storageEngine(StorageProperties properties) {
        StorageEngine engine = new StorageEngine(properties);
        engine.recover();
        return engine;
    }

    @Bean(destroyMethod = "close")
    public MigrationJournal migrationJournal(StorageProperties properties,
                                             StorageEngine engine) {
        MigrationJournal journal =
                new MigrationJournal(Path.of(properties.getDataDir()).resolve("journal"));
        engine.setMigrationJournal(journal);
        return journal;
    }

    @Bean(destroyMethod = "stop")
    public TieringManager tieringManager(StorageEngine engine, StorageProperties properties) {
        TieringManager manager = new TieringManager(engine, properties);
        manager.start();
        return manager;
    }

    @Bean(destroyMethod = "stop")
    public RetentionManager retentionManager(StorageEngine engine, StorageProperties properties) {
        RetentionManager manager = new RetentionManager(engine, properties);
        manager.start();
        return manager;
    }

    @Bean(destroyMethod = "close")
    public QueryService queryService(StorageEngine engine, StorageProperties properties) {
        return new QueryService(engine, properties);
    }
}
