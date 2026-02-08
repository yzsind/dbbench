package com.ninedata.dbbench.web;

import com.ninedata.dbbench.config.BenchmarkConfig;
import com.ninedata.dbbench.config.DatabaseConfig;
import com.ninedata.dbbench.engine.BenchmarkEngine;
import com.ninedata.dbbench.metrics.MetricsRegistry;
import com.ninedata.dbbench.metrics.OSMetricsCollector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("BenchmarkController Tests")
class BenchmarkControllerTest {

    private BenchmarkController controller;
    private BenchmarkEngine engine;
    private DatabaseConfig dbConfig;

    @BeforeEach
    void setUp() {
        dbConfig = new DatabaseConfig();
        BenchmarkConfig benchConfig = new BenchmarkConfig();
        MetricsRegistry metricsRegistry = new MetricsRegistry();
        OSMetricsCollector osMetricsCollector = new OSMetricsCollector();
        engine = new BenchmarkEngine(dbConfig, benchConfig, metricsRegistry, osMetricsCollector);
        controller = new BenchmarkController(engine, dbConfig);
    }

    @Test
    @DisplayName("Should return status")
    void testStatus() {
        ResponseEntity<Map<String, Object>> response = controller.status();

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody());
        assertTrue(response.getBody().containsKey("status"));
        assertTrue(response.getBody().containsKey("running"));
        assertTrue(response.getBody().containsKey("loading"));
        assertEquals("IDLE", response.getBody().get("status"));
        assertEquals(false, response.getBody().get("running"));
        assertEquals(false, response.getBody().get("loading"));
    }

    @Test
    @DisplayName("Should return results")
    void testResults() {
        ResponseEntity<Map<String, Object>> response = controller.results();

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody());
        assertTrue(response.getBody().containsKey("status"));
        assertTrue(response.getBody().containsKey("metrics"));
    }

    @Test
    @DisplayName("Should return config")
    void testConfig() {
        ResponseEntity<Map<String, Object>> response = controller.config();

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody());
        assertTrue(response.getBody().containsKey("database"));
        assertTrue(response.getBody().containsKey("benchmark"));
        assertTrue(response.getBody().containsKey("transactionMix"));
    }

    @Test
    @DisplayName("Should return load progress")
    void testLoadProgress() {
        ResponseEntity<Map<String, Object>> response = controller.loadProgress();

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody());
        assertTrue(response.getBody().containsKey("loading"));
        assertTrue(response.getBody().containsKey("progress"));
        assertTrue(response.getBody().containsKey("message"));
        assertTrue(response.getBody().containsKey("status"));
    }

    @Test
    @DisplayName("Should return empty logs initially")
    void testLogs() {
        ResponseEntity<?> response = controller.logs(100);

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody());
    }

    @Test
    @DisplayName("Should clear logs")
    void testClearLogs() {
        ResponseEntity<Map<String, Object>> response = controller.clearLogs();

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody());
        assertEquals(true, response.getBody().get("success"));
        assertEquals("Logs cleared", response.getBody().get("message"));
    }

    @Test
    @DisplayName("Should stop benchmark gracefully when not running")
    void testStopWhenNotRunning() {
        ResponseEntity<Map<String, Object>> response = controller.stop();

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody());
        assertEquals(true, response.getBody().get("success"));
    }

    @Test
    @DisplayName("Should fail to cancel load when not loading")
    void testCancelLoadWhenNotLoading() {
        ResponseEntity<Map<String, Object>> response = controller.cancelLoad();

        assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
        assertNotNull(response.getBody());
        assertEquals(false, response.getBody().get("success"));
        assertTrue(response.getBody().get("error").toString().contains("No data loading"));
    }
}
