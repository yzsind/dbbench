package com.ninedata.dbbench.engine;

import com.ninedata.dbbench.config.BenchmarkConfig;
import com.ninedata.dbbench.config.DatabaseConfig;
import com.ninedata.dbbench.metrics.MetricsRegistry;
import com.ninedata.dbbench.metrics.OSMetricsCollector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("BenchmarkEngine Tests")
class BenchmarkEngineTest {

    private BenchmarkEngine engine;
    private DatabaseConfig dbConfig;
    private BenchmarkConfig benchConfig;
    private MetricsRegistry metricsRegistry;
    private OSMetricsCollector osMetricsCollector;

    @BeforeEach
    void setUp() {
        dbConfig = new DatabaseConfig();
        benchConfig = new BenchmarkConfig();
        metricsRegistry = new MetricsRegistry();
        osMetricsCollector = new OSMetricsCollector();
        engine = new BenchmarkEngine(dbConfig, benchConfig, metricsRegistry, osMetricsCollector);
    }

    @Test
    @DisplayName("Should have IDLE status initially")
    void testInitialStatus() {
        assertEquals("IDLE", engine.getStatus());
        assertFalse(engine.isRunning());
        assertFalse(engine.isLoading());
    }

    @Test
    @DisplayName("Should return correct config")
    void testGetConfig() {
        Map<String, Object> config = engine.getConfig();

        assertNotNull(config);
        assertTrue(config.containsKey("database"));
        assertTrue(config.containsKey("benchmark"));
        assertTrue(config.containsKey("transactionMix"));

        @SuppressWarnings("unchecked")
        Map<String, Object> db = (Map<String, Object>) config.get("database");
        assertEquals("mysql", db.get("type"));

        @SuppressWarnings("unchecked")
        Map<String, Object> bench = (Map<String, Object>) config.get("benchmark");
        assertEquals(10, bench.get("warehouses"));
        assertEquals(50, bench.get("terminals"));
        assertEquals(60, bench.get("duration"));
    }

    @Test
    @DisplayName("Should update config correctly")
    void testUpdateConfig() {
        Map<String, Object> newConfig = new HashMap<>();

        Map<String, Object> db = new HashMap<>();
        db.put("type", "postgresql");
        db.put("jdbcUrl", "jdbc:postgresql://localhost:5432/test");
        newConfig.put("database", db);

        Map<String, Object> bench = new HashMap<>();
        bench.put("warehouses", 20);
        bench.put("terminals", 100);
        newConfig.put("benchmark", bench);

        engine.updateConfig(newConfig);

        Map<String, Object> config = engine.getConfig();

        @SuppressWarnings("unchecked")
        Map<String, Object> updatedDb = (Map<String, Object>) config.get("database");
        assertEquals("postgresql", updatedDb.get("type"));

        @SuppressWarnings("unchecked")
        Map<String, Object> updatedBench = (Map<String, Object>) config.get("benchmark");
        assertEquals(20, updatedBench.get("warehouses"));
        assertEquals(100, updatedBench.get("terminals"));
    }

    @Test
    @DisplayName("Should update transaction mix correctly")
    void testUpdateTransactionMix() {
        Map<String, Object> newConfig = new HashMap<>();

        Map<String, Object> mix = new HashMap<>();
        mix.put("newOrder", 50);
        mix.put("payment", 40);
        mix.put("orderStatus", 3);
        mix.put("delivery", 3);
        mix.put("stockLevel", 4);
        newConfig.put("transactionMix", mix);

        engine.updateConfig(newConfig);

        Map<String, Object> config = engine.getConfig();

        @SuppressWarnings("unchecked")
        Map<String, Object> updatedMix = (Map<String, Object>) config.get("transactionMix");
        assertEquals(50, updatedMix.get("newOrder"));
        assertEquals(40, updatedMix.get("payment"));
    }

    @Test
    @DisplayName("Should return results with status and metrics")
    void testGetResults() {
        Map<String, Object> results = engine.getResults();

        assertNotNull(results);
        assertTrue(results.containsKey("status"));
        assertTrue(results.containsKey("metrics"));
        assertEquals("IDLE", results.get("status"));
    }

    @Test
    @DisplayName("Should manage log history")
    void testLogHistory() {
        // Initially empty
        assertTrue(engine.getLogHistory().isEmpty());

        // Clear logs should work even when empty
        engine.clearLogs();
        assertTrue(engine.getLogHistory().isEmpty());
    }

    @Test
    @DisplayName("Should have correct initial load progress")
    void testInitialLoadProgress() {
        assertEquals(0, engine.getLoadProgress());
        assertEquals("", engine.getLoadMessage());
    }

    @Test
    @DisplayName("Should set metrics callback")
    void testSetMetricsCallback() {
        // Should not throw
        engine.setMetricsCallback(metrics -> {});
    }

    @Test
    @DisplayName("Should set log callback")
    void testSetLogCallback() {
        // Should not throw
        engine.setLogCallback(log -> {});
    }

    @Test
    @DisplayName("Should stop gracefully when not running")
    void testStopWhenNotRunning() {
        // Should not throw
        engine.stop();
        assertEquals("IDLE", engine.getStatus());
    }

    @Test
    @DisplayName("Should shutdown gracefully")
    void testShutdown() {
        engine.shutdown();
        assertEquals("SHUTDOWN", engine.getStatus());
    }

    @Test
    @DisplayName("Should fail to cancel load when not loading")
    void testCancelLoadWhenNotLoading() {
        assertThrows(IllegalStateException.class, () -> engine.cancelLoad());
    }
}
