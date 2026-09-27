package com.github.guoyyit.telemetrytieredstoragejava.engine;

import com.github.guoyyit.telemetrytieredstoragejava.config.TieredStorageProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

@Configuration
@EnableScheduling
@EnableConfigurationProperties(TieredStorageProperties.class)
public class EngineConfig {

    @Bean(destroyMethod = "close")
    public TieredStorageEngine tieredStorageEngine(TieredStorageProperties props) {
        return new TieredStorageEngine(props);
    }
}
