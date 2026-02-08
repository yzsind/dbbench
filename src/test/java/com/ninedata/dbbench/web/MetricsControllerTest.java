package com.ninedata.dbbench.web;

import com.ninedata.dbbench.config.BenchmarkConfig;
import com.ninedata.dbbench.config.DatabaseConfig;
import com.ninedata.dbbench.engine.BenchmarkEngine;
import com.ninedata.dbbench.metrics.MetricsRegistry;
import com.ninedata.dbbench.metrics.MetricsSnapshot;
import com.ninedata.dbbench.metrics.OSMetricsCollector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("MetricsController Tests")
class MetricsControllerTest {

    private MetricsController controller;
    private MetricsRegistry metricsRegistry;
    private OSMetricsCollector osMetricsCollector;
    private BenchmarkEngine engine;

    @BeforeEach
    void setUp() {
        DatabaseConfig dbConfig = new DatabaseConfig();
        BenchmarkConfig benchConfig = new BenchmarkConfig();
        metricsRegistry = new MetricsRegistry();
        osMetricsCollector = new OSMetricsCollector();
        osMetricsCollector.init();
        engine = new BenchmarkEngine(dbConfig, benchConfig, metricsRegistry, osMetricsCollector);
        controller = new MetricsController(metricsRegistry, osMetricsCollector, engine);
    }

    @Test
    @DisplayName("Should return current metrics")
    void testCurrent() {
        ResponseEntity<Map<String, Object>> response = controller.current();

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody());
        assertTrue(response.getBody().containsKey("transaction"));
        assertTrue(response.getBody().containsKey("os"));
        assertTrue(response.getBody().containsKey("status"));
        assertTrue(response.getBody().containsKey("running"));
        assertTrue(response.getBody().containsKey("loading"));
    }

    @Test
    @DisplayName("Should return empty history initially")
    void testHistoryEmpty() {
        ResponseEntity<?> response = controller.history(60);

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody());
        assertTrue(response.getBody() instanceof List);
        assertTrue(((List<?>) response.getBody()).isEmpty());
    }

    @Test
    @DisplayName("Should return history with snapshots")
    void testHistoryWithSnapshots() {
        // Add some snapshots
        metricsRegistry.reset();
        metricsRegistry.recordTransaction("NEW_ORDER", true, 1_000_000);
        metricsRegistry.takeSnapshot(new HashMap<>(), new HashMap<>());
        metricsRegistry.takeSnapshot(new HashMap<>(), new HashMap<>());

        ResponseEntity<?> response = controller.history(60);

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody());
        assertTrue(response.getBody() instanceof List);
        assertEquals(2, ((List<?>) response.getBody()).size());
    }

    @Test
    @DisplayName("Should limit history results")
    void testHistoryLimit() {
        // Add more snapshots than limit
        metricsRegistry.reset();
        for (int i = 0; i < 10; i++) {
            metricsRegistry.takeSnapshot(new HashMap<>(), new HashMap<>());
        }

        ResponseEntity<?> response = controller.history(5);

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody());
        assertTrue(response.getBody() instanceof List);
        assertEquals(5, ((List<?>) response.getBody()).size());
    }

    @Test
    @DisplayName("Should return TPS history")
    void testTpsHistory() {
        // Add some snapshots with TPS data
        metricsRegistry.reset();
        metricsRegistry.recordTransaction("NEW_ORDER", true, 1_000_000);
        metricsRegistry.takeSnapshot(new HashMap<>(), new HashMap<>());

        ResponseEntity<?> response = controller.tpsHistory(60);

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody());
        assertTrue(response.getBody() instanceof List);

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> tpsData = (List<Map<String, Object>>) response.getBody();
        assertFalse(tpsData.isEmpty());
        assertTrue(tpsData.get(0).containsKey("timestamp"));
        assertTrue(tpsData.get(0).containsKey("tps"));
    }

    @Test
    @DisplayName("Should return empty TPS history when no snapshots")
    void testTpsHistoryEmpty() {
        ResponseEntity<?> response = controller.tpsHistory(60);

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody());
        assertTrue(response.getBody() instanceof List);
        assertTrue(((List<?>) response.getBody()).isEmpty());
    }

    @Test
    @DisplayName("Current metrics should include OS metrics")
    void testCurrentIncludesOsMetrics() {
        ResponseEntity<Map<String, Object>> response = controller.current();

        @SuppressWarnings("unchecked")
        Map<String, Object> osMetrics = (Map<String, Object>) response.getBody().get("os");

        assertNotNull(osMetrics);
        assertTrue(osMetrics.containsKey("cpuUsage"));
        assertTrue(osMetrics.containsKey("memoryUsage"));
    }

    @Test
    @DisplayName("Current metrics should include transaction metrics")
    void testCurrentIncludesTransactionMetrics() {
        metricsRegistry.reset();
        metricsRegistry.recordTransaction("NEW_ORDER", true, 1_000_000);

        ResponseEntity<Map<String, Object>> response = controller.current();

        @SuppressWarnings("unchecked")
        Map<String, Object> txMetrics = (Map<String, Object>) response.getBody().get("transaction");

        assertNotNull(txMetrics);
        assertTrue(txMetrics.containsKey("totalTransactions"));
        assertEquals(1L, txMetrics.get("totalTransactions"));
    }
}
