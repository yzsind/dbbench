package com.ninedata.dbbench.web;

import com.ninedata.dbbench.database.DatabaseAdapter;
import com.ninedata.dbbench.engine.BenchmarkEngine;
import com.ninedata.dbbench.metrics.MetricsRegistry;
import com.ninedata.dbbench.metrics.MetricsSnapshot;
import com.ninedata.dbbench.metrics.OSMetricsCollector;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/metrics")
@RequiredArgsConstructor
public class MetricsController {
    private final MetricsRegistry metricsRegistry;
    private final OSMetricsCollector osMetricsCollector;
    private final BenchmarkEngine engine;

    @GetMapping("/current")
    public ResponseEntity<Map<String, Object>> current() {
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("transaction", metricsRegistry.getCurrentMetrics());
        response.put("os", osMetricsCollector.collect());
        response.put("status", engine.getStatus());
        response.put("running", engine.isRunning());
        response.put("loading", engine.isLoading());

        // Include load progress if loading
        if (engine.isLoading()) {
            response.put("loadProgress", engine.getLoadProgress());
            response.put("loadMessage", engine.getLoadMessage());
        }

        return ResponseEntity.ok(response);
    }

    @GetMapping("/history")
    public ResponseEntity<?> history(@RequestParam(defaultValue = "60") int limit) {
        var history = metricsRegistry.getHistory();
        int start = Math.max(0, history.size() - limit);
        return ResponseEntity.ok(history.subList(start, history.size()));
    }

    /**
     * Get TPS history for chart restoration after page refresh
     */
    @GetMapping("/tps-history")
    public ResponseEntity<?> tpsHistory(@RequestParam(defaultValue = "60") int limit) {
        List<MetricsSnapshot> history = metricsRegistry.getHistory();
        int start = Math.max(0, history.size() - limit);

        List<Map<String, Object>> tpsData = history.subList(start, history.size()).stream()
                .map(snapshot -> {
                    Map<String, Object> point = new LinkedHashMap<>();
                    point.put("timestamp", snapshot.getTimestamp());
                    Map<String, Object> txMetrics = snapshot.getTransactionMetrics();
                    point.put("tps", txMetrics != null ? txMetrics.getOrDefault("tps", 0) : 0);
                    return point;
                })
                .collect(Collectors.toList());

        return ResponseEntity.ok(tpsData);
    }
}
