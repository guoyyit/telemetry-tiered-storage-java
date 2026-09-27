package com.github.guoyyit.telemetrytieredstoragejava.api;

import com.github.guoyyit.telemetrytieredstoragejava.model.TelemetrySample;
import com.github.guoyyit.telemetrytieredstoragejava.query.QueryService;
import com.github.guoyyit.telemetrytieredstoragejava.storage.StorageEngine;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * REST endpoint for writing and querying telemetry samples.
 */
@RestController
@RequestMapping("/api/telemetry")
public class TelemetryController {

    private final StorageEngine engine;
    private final QueryService queryService;

    public TelemetryController(StorageEngine engine, QueryService queryService) {
        this.engine = engine;
        this.queryService = queryService;
    }

    @PostMapping("/{stream}")
    public TelemetrySample write(@PathVariable String stream,
                                 @RequestParam long timestamp,
                                 @RequestParam(defaultValue = "0") double value,
                                 @RequestBody(required = false) Map<String, String> dimensions) {
        return engine.write(stream, timestamp, dimensions, value);
    }

    @GetMapping("/{stream}")
    public List<TelemetrySample> query(@PathVariable String stream,
                                       @RequestParam long from,
                                       @RequestParam long to) {
        return queryService.query(stream, from, to);
    }
}
