package com.ninedata.dbbench.metrics;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("MetricsRegistry Tests")
class MetricsRegistryTest {

    private MetricsRegistry registry;

    @BeforeEach
    void setUp() {
        registry = new MetricsRegistry();
        registry.reset();
    }

    @Test
    @DisplayName("Should create new metrics on first access")
    void testGetOrCreate() {
        TransactionMetrics metrics = registry.getOrCreate("NEW_ORDER");

        assertNotNull(metrics);
        assertEquals("NEW_ORDER", metrics.getName());
        assertEquals(0, metrics.getCount());
    }

    @Test
    @DisplayName("Should return same metrics instance for same name")
    void testGetOrCreateSameInstance() {
        TransactionMetrics metrics1 = registry.getOrCreate("PAYMENT");
        TransactionMetrics metrics2 = registry.getOrCreate("PAYMENT");

        assertSame(metrics1, metrics2);
    }

    @Test
    @DisplayName("Should record successful transaction")
    void testRecordSuccessfulTransaction() {
        registry.recordTransaction("NEW_ORDER", true, 1_000_000);

        TransactionMetrics metrics = registry.getOrCreate("NEW_ORDER");
        assertEquals(1, metrics.getCount());
        assertEquals(1, metrics.getSuccessCount());
        assertEquals(0, metrics.getFailureCount());
    }

    @Test
    @DisplayName("Should record failed transaction")
    void testRecordFailedTransaction() {
        registry.recordTransaction("NEW_ORDER", false, 1_000_000);

        TransactionMetrics metrics = registry.getOrCreate("NEW_ORDER");
        assertEquals(1, metrics.getCount());
        assertEquals(0, metrics.getSuccessCount());
        assertEquals(1, metrics.getFailureCount());
    }

    @Test
    @DisplayName("Should reset all metrics")
    void testReset() {
        registry.recordTransaction("NEW_ORDER", true, 1_000_000);
        registry.recordTransaction("PAYMENT", true, 2_000_000);

        registry.reset();

        // After reset, getOrCreate should return fresh metrics
        TransactionMetrics metrics = registry.getOrCreate("NEW_ORDER");
        assertEquals(0, metrics.getCount());
        assertTrue(registry.getHistory().isEmpty());
    }

    @Test
    @DisplayName("Should calculate current metrics correctly")
    void testGetCurrentMetrics() {
        registry.recordTransaction("NEW_ORDER", true, 1_000_000);
        registry.recordTransaction("NEW_ORDER", true, 2_000_000);
        registry.recordTransaction("PAYMENT", true, 3_000_000);
        registry.recordTransaction("PAYMENT", false, 4_000_000);

        Map<String, Object> metrics = registry.getCurrentMetrics();

        assertEquals(4L, metrics.get("totalTransactions"));
        assertEquals(3L, metrics.get("totalSuccess"));
        assertEquals(1L, metrics.get("totalFailure"));
        assertEquals(75.0, (Double) metrics.get("overallSuccessRate"), 0.01);

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> txMetrics = (List<Map<String, Object>>) metrics.get("transactions");
        assertEquals(2, txMetrics.size());
    }

    @Test
    @DisplayName("Should take snapshot with all metrics")
    void testTakeSnapshot() {
        registry.recordTransaction("NEW_ORDER", true, 1_000_000);

        Map<String, Object> dbMetrics = new HashMap<>();
        dbMetrics.put("connections", 10);

        Map<String, Object> osMetrics = new HashMap<>();
        osMetrics.put("cpuUsage", 50.0);

        registry.takeSnapshot(dbMetrics, osMetrics);

        List<MetricsSnapshot> history = registry.getHistory();
        assertEquals(1, history.size());

        MetricsSnapshot snapshot = history.get(0);
        assertNotNull(snapshot.getTimestamp());
        assertNotNull(snapshot.getTransactionMetrics());
        assertEquals(10, snapshot.getDatabaseMetrics().get("connections"));
        assertEquals(50.0, snapshot.getOsMetrics().get("cpuUsage"));
    }

    @Test
    @DisplayName("Should handle null metrics in snapshot")
    void testTakeSnapshotWithNullMetrics() {
        registry.recordTransaction("NEW_ORDER", true, 1_000_000);

        registry.takeSnapshot(null, null);

        List<MetricsSnapshot> history = registry.getHistory();
        assertEquals(1, history.size());

        MetricsSnapshot snapshot = history.get(0);
        assertNotNull(snapshot.getDatabaseMetrics());
        assertNotNull(snapshot.getOsMetrics());
        assertTrue(snapshot.getDatabaseMetrics().isEmpty());
        assertTrue(snapshot.getOsMetrics().isEmpty());
    }

    @Test
    @DisplayName("Should limit history to 3600 entries")
    void testHistoryLimit() {
        for (int i = 0; i < 3700; i++) {
            registry.takeSnapshot(new HashMap<>(), new HashMap<>());
        }

        assertEquals(3600, registry.getHistory().size());
    }

    @Test
    @DisplayName("Should calculate TPS correctly")
    void testTpsCalculation() throws InterruptedException {
        registry.reset();

        // Record some transactions
        for (int i = 0; i < 100; i++) {
            registry.recordTransaction("NEW_ORDER", true, 1_000_000);
        }

        // Wait a bit to have elapsed time
        Thread.sleep(100);

        Map<String, Object> metrics = registry.getCurrentMetrics();

        // TPS should be positive
        double tps = (Double) metrics.get("tps");
        assertTrue(tps > 0, "TPS should be positive");
    }

    @Test
    @DisplayName("Should mark end time correctly")
    void testMarkEnd() throws InterruptedException {
        registry.reset();
        registry.recordTransaction("NEW_ORDER", true, 1_000_000);

        Thread.sleep(50);
        registry.markEnd();

        // Get metrics before and after markEnd
        Map<String, Object> metrics1 = registry.getCurrentMetrics();
        Thread.sleep(100);
        Map<String, Object> metrics2 = registry.getCurrentMetrics();

        // Elapsed time should be the same after markEnd
        assertEquals(metrics1.get("elapsedSeconds"), metrics2.get("elapsedSeconds"));
    }

    @Test
    @DisplayName("Should return zero metrics when empty")
    void testEmptyMetrics() {
        Map<String, Object> metrics = registry.getCurrentMetrics();

        assertEquals(0L, metrics.get("totalTransactions"));
        assertEquals(0L, metrics.get("totalSuccess"));
        assertEquals(0L, metrics.get("totalFailure"));
        assertEquals(0.0, metrics.get("overallSuccessRate"));
        assertEquals(0.0, metrics.get("avgLatencyMs"));
    }

    @Test
    @DisplayName("Should handle multiple transaction types")
    void testMultipleTransactionTypes() {
        registry.recordTransaction("NEW_ORDER", true, 1_000_000);
        registry.recordTransaction("PAYMENT", true, 2_000_000);
        registry.recordTransaction("ORDER_STATUS", true, 3_000_000);
        registry.recordTransaction("DELIVERY", true, 4_000_000);
        registry.recordTransaction("STOCK_LEVEL", true, 5_000_000);

        Map<String, Object> metrics = registry.getCurrentMetrics();

        assertEquals(5L, metrics.get("totalTransactions"));
        assertEquals(5L, metrics.get("totalSuccess"));

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> txMetrics = (List<Map<String, Object>>) metrics.get("transactions");
        assertEquals(5, txMetrics.size());
    }
}
