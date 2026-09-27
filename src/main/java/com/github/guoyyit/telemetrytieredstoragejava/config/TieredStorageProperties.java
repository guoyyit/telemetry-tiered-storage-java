package com.github.guoyyit.telemetrytieredstoragejava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.nio.file.Path;
import java.time.Duration;

/**
 * All tunables of the tiered storage engine. Every knob mentioned by the
 * requirements (batch size, compression, scan interval, retention, memory
 * bounds, query timeout) lives here.
 */
@ConfigurationProperties(prefix = "telemetry")
public class TieredStorageProperties {

    public enum Compression { NONE, GZIP }

    /** Root directory for WAL, snapshot and cold archive blocks. */
    private Path dataDir = Path.of("./data");
    /** Samples older than this become demotion candidates. */
    private Duration hotMaxAge = Duration.ofHours(1);
    /** Sliding window used to measure access heat. */
    private Duration accessWindow = Duration.ofMinutes(10);
    /** Accesses inside the window at or above which a cold stream is promoted back to hot. */
    private int promoteHeatThreshold = 3;
    /** Number of samples moved per migration batch (bounds lock hold time and memory). */
    private int migrationBatchSize = 500;
    /** Interval between automatic migration scans. */
    private Duration scanInterval = Duration.ofSeconds(5);
    /** Compression used for cold archive blocks. */
    private Compression compression = Compression.GZIP;
    /** How long raw (full resolution) samples are kept. */
    private Duration rawRetention = Duration.ofDays(1);
    /** Extra time downsampled data is kept after raw retention expires. */
    private Duration downsampledRetention = Duration.ofDays(7);
    /** Bucket resolution used when downsampling. */
    private Duration downsampleResolution = Duration.ofMinutes(1);
    /** Hard bound on samples kept in the hot tier; excess is force-demoted. */
    private int maxHotSamples = 100_000;
    /** Hard bound on samples returned by a single query. */
    private int maxResultSamples = 100_000;
    /** Default query timeout in milliseconds. */
    private long defaultQueryTimeoutMs = 30_000;
    /** Whether background migration/retention scheduling is active. */
    private boolean schedulingEnabled = true;

    public Path getDataDir() { return dataDir; }
    public void setDataDir(Path dataDir) { this.dataDir = dataDir; }
    public Duration getHotMaxAge() { return hotMaxAge; }
    public void setHotMaxAge(Duration hotMaxAge) { this.hotMaxAge = hotMaxAge; }
    public Duration getAccessWindow() { return accessWindow; }
    public void setAccessWindow(Duration accessWindow) { this.accessWindow = accessWindow; }
    public int getPromoteHeatThreshold() { return promoteHeatThreshold; }
    public void setPromoteHeatThreshold(int promoteHeatThreshold) { this.promoteHeatThreshold = promoteHeatThreshold; }
    public int getMigrationBatchSize() { return migrationBatchSize; }
    public void setMigrationBatchSize(int migrationBatchSize) { this.migrationBatchSize = migrationBatchSize; }
    public Duration getScanInterval() { return scanInterval; }
    public void setScanInterval(Duration scanInterval) { this.scanInterval = scanInterval; }
    public Compression getCompression() { return compression; }
    public void setCompression(Compression compression) { this.compression = compression; }
    public Duration getRawRetention() { return rawRetention; }
    public void setRawRetention(Duration rawRetention) { this.rawRetention = rawRetention; }
    public Duration getDownsampledRetention() { return downsampledRetention; }
    public void setDownsampledRetention(Duration downsampledRetention) { this.downsampledRetention = downsampledRetention; }
    public Duration getDownsampleResolution() { return downsampleResolution; }
    public void setDownsampleResolution(Duration downsampleResolution) { this.downsampleResolution = downsampleResolution; }
    public int getMaxHotSamples() { return maxHotSamples; }
    public void setMaxHotSamples(int maxHotSamples) { this.maxHotSamples = maxHotSamples; }
    public int getMaxResultSamples() { return maxResultSamples; }
    public void setMaxResultSamples(int maxResultSamples) { this.maxResultSamples = maxResultSamples; }
    public long getDefaultQueryTimeoutMs() { return defaultQueryTimeoutMs; }
    public void setDefaultQueryTimeoutMs(long defaultQueryTimeoutMs) { this.defaultQueryTimeoutMs = defaultQueryTimeoutMs; }
    public boolean isSchedulingEnabled() { return schedulingEnabled; }
    public void setSchedulingEnabled(boolean schedulingEnabled) { this.schedulingEnabled = schedulingEnabled; }
}
