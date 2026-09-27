package com.github.guoyyit.telemetrytieredstoragejava.engine;

import com.github.guoyyit.telemetrytieredstoragejava.config.TieredStorageProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

/** Periodically runs one migration pass on the engine. */
@Service
public class MigrationService {

    private static final Logger log = LoggerFactory.getLogger(MigrationService.class);

    private final TieredStorageEngine engine;
    private final TieredStorageProperties props;

    public MigrationService(TieredStorageEngine engine, TieredStorageProperties props) {
        this.engine = engine;
        this.props = props;
    }

    @Scheduled(fixedDelayString = "${telemetry.scan-interval:5s}",
               initialDelayString = "${telemetry.scan-interval:5s}")
    public void scan() {
        if (!props.isSchedulingEnabled()) {
            return;
        }
        try {
            int moved = engine.migrateOnce();
            if (moved > 0) {
                log.debug("migration pass moved {} samples", moved);
            }
        } catch (RuntimeException e) {
            log.warn("migration pass failed: {}", e.getMessage(), e);
        }
    }
}
