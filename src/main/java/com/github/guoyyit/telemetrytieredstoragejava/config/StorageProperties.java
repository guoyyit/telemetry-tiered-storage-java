package com.github.guoyyit.telemetrytieredstoragejava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * Configuration for the tiered telemetry storage engine.
 */
@ConfigurationProperties(prefix = "telemetry.storage")
public class StorageProperties {

    /** Root directory of all persisted state. */
    private String dataDir = "data";

    /** Samples younger than this stay hot regardless of access frequency. */
    private Duration hotMaxAge = Duration.ofHours(1);

    /** A hot sample not accessed for this long becomes eligible for cooling. */
    private Duration coolAfterIdle = Duration.ofMinutes(30);

    /** Raw samples are deleted once older than this. */
    private Duration rawRetention = Duration.ofDays(7);

    /** Downsampled (cold archive) data is deleted once older than this. */
    private Duration downsampledRetention = Duration.ofDays(30);

    /** Max number of samples moved in a single migration batch. */
    private int migrationBatchSize = 500;

    /** Interval between tiering / retention scans. */
    private Duration scanInterval = Duration.ofSeconds(5);

    /** Compression codec for cold archives: gzip or none. */
    private String compression = "gzip";

    /** Default query timeout. */
    private Duration queryTimeout = Duration.ofSeconds(30);

    /** Maximum number of samples held in the hot in-memory index. */
    private int maxHotSamples = 100_000;

    public String getDataDir() {
        return dataDir;
    }

    public void setDataDir(String dataDir) {
        this.dataDir = dataDir;
    }

    public Duration getHotMaxAge() {
        return hotMaxAge;
    }

    public void setHotMaxAge(Duration hotMaxAge) {
        this.hotMaxAge = hotMaxAge;
    }

    public Duration getCoolAfterIdle() {
        return coolAfterIdle;
    }

    public void setCoolAfterIdle(Duration coolAfterIdle) {
        this.coolAfterIdle = coolAfterIdle;
    }

    public Duration getRawRetention() {
        return rawRetention;
    }

    public void setRawRetention(Duration rawRetention) {
        this.rawRetention = rawRetention;
    }

    public Duration getDownsampledRetention() {
        return downsampledRetention;
    }

    public void setDownsampledRetention(Duration downsampledRetention) {
        this.downsampledRetention = downsampledRetention;
    }

    public int getMigrationBatchSize() {
        return migrationBatchSize;
    }

    public void setMigrationBatchSize(int migrationBatchSize) {
        this.migrationBatchSize = migrationBatchSize;
    }

    public Duration getScanInterval() {
        return scanInterval;
    }

    public void setScanInterval(Duration scanInterval) {
        this.scanInterval = scanInterval;
    }

    public String getCompression() {
        return compression;
    }

    public void setCompression(String compression) {
        this.compression = compression;
    }

    public Duration getQueryTimeout() {
        return queryTimeout;
    }

    public void setQueryTimeout(Duration queryTimeout) {
        this.queryTimeout = queryTimeout;
    }

    public int getMaxHotSamples() {
        return maxHotSamples;
    }

    public void setMaxHotSamples(int maxHotSamples) {
        this.maxHotSamples = maxHotSamples;
    }
}
