package com.github.guoyyit.telemetrytieredstoragejava.engine;

import com.github.guoyyit.telemetrytieredstoragejava.config.TieredStorageProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

/** Periodically enforces the retention policy (downsample + delete). */
@Service
public class RetentionService {

    private static final Logger log = LoggerFactory.getLogger(RetentionService.class);

    private final TieredStorageEngine engine;
    private final TieredStorageProperties props;

    public RetentionService(TieredStorageEngine engine, TieredStorageProperties props) {
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
            int affected = engine.enforceRetentionOnce();
            if (affected > 0) {
                log.debug("retention pass affected {} samples", affected);
            }
        } catch (RuntimeException e) {
            log.warn("retention pass failed: {}", e.getMessage(), e);
        }
    }
}
