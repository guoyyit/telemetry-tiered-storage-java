package com.github.guoyyit.telemetrytieredstoragejava.api;

import com.github.guoyyit.telemetrytieredstoragejava.engine.TieredStorageEngine;
import com.github.guoyyit.telemetrytieredstoragejava.model.TelemetrySample;
import com.github.guoyyit.telemetrytieredstoragejava.query.QueryCancelledException;
import com.github.guoyyit.telemetrytieredstoragejava.query.QueryOptions;
import com.github.guoyyit.telemetrytieredstoragejava.query.QueryResultTooLargeException;
import com.github.guoyyit.telemetrytieredstoragejava.query.QueryTimeoutException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** REST API for writes, cross-tier queries and admin triggers. */
@RestController
@RequestMapping("/api/telemetry")
public class TelemetryController {

    private final TieredStorageEngine engine;

    public TelemetryController(TieredStorageEngine engine) {
        this.engine = engine;
    }

    public record SampleDto(Long timestamp, Map<String, Double> values) {
    }

    public record WriteRequest(List<SampleDto> samples) {
    }

    public record ErrorResponse(String error) {
    }

    @PostMapping("/{stream}")
    public List<String> write(@PathVariable String stream, @RequestBody WriteRequest request) {
        List<TelemetrySample> samples = new ArrayList<>();
        if (request.samples() != null) {
            for (SampleDto dto : request.samples()) {
                if (dto.timestamp() == null) {
                    throw new IllegalArgumentException("every sample needs a timestamp");
                }
                samples.add(TelemetrySample.of(stream, dto.timestamp(),
                        dto.values() != null ? dto.values() : Map.of()));
            }
        }
        return engine.write(stream, samples);
    }

    @GetMapping("/{stream}")
    public List<TelemetrySample> query(@PathVariable String stream,
                                       @RequestParam long from,
                                       @RequestParam long to,
                                       @RequestParam(required = false) Long timeoutMs) {
        return engine.query(stream, from, to, QueryOptions.of(timeoutMs, null));
    }

    @GetMapping("/stats")
    public TieredStorageEngine.Stats stats() {
        return engine.stats();
    }

    @PostMapping("/admin/migrate")
    public int migrate() {
        return engine.migrateOnce();
    }

    @PostMapping("/admin/retention")
    public int retention() {
        return engine.enforceRetentionOnce();
    }

    @ExceptionHandler(QueryTimeoutException.class)
    public ResponseEntity<ErrorResponse> timeout(QueryTimeoutException e) {
        return ResponseEntity.status(HttpStatus.REQUEST_TIMEOUT).body(new ErrorResponse(e.getMessage()));
    }

    @ExceptionHandler(QueryCancelledException.class)
    public ResponseEntity<ErrorResponse> cancelled(QueryCancelledException e) {
        return ResponseEntity.status(HttpStatus.GONE).body(new ErrorResponse(e.getMessage()));
    }

    @ExceptionHandler(QueryResultTooLargeException.class)
    public ResponseEntity<ErrorResponse> tooLarge(QueryResultTooLargeException e) {
        return ResponseEntity.status(HttpStatus.PAYLOAD_TOO_LARGE).body(new ErrorResponse(e.getMessage()));
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<ErrorResponse> badRequest(IllegalArgumentException e) {
        return ResponseEntity.badRequest().body(new ErrorResponse(e.getMessage()));
    }
}
