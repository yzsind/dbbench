package com.ninedata.dbbench.metrics;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("MetricsSnapshot Tests")
class MetricsSnapshotTest {

    @Test
    @DisplayName("Should set and get timestamp")
    void testTimestamp() {
        MetricsSnapshot snapshot = new MetricsSnapshot();
        long timestamp = System.currentTimeMillis();

        snapshot.setTimestamp(timestamp);

        assertEquals(timestamp, snapshot.getTimestamp());
    }

    @Test
    @DisplayName("Should set and get transaction metrics")
    void testTransactionMetrics() {
        MetricsSnapshot snapshot = new MetricsSnapshot();
        Map<String, Object> txMetrics = new HashMap<>();
        txMetrics.put("tps", 100.5);
        txMetrics.put("totalTransactions", 1000L);

        snapshot.setTransactionMetrics(txMetrics);

        assertEquals(100.5, snapshot.getTransactionMetrics().get("tps"));
        assertEquals(1000L, snapshot.getTransactionMetrics().get("totalTransactions"));
    }

    @Test
    @DisplayName("Should set and get database metrics")
    void testDatabaseMetrics() {
        MetricsSnapshot snapshot = new MetricsSnapshot();
        Map<String, Object> dbMetrics = new HashMap<>();
        dbMetrics.put("connections", 50);
        dbMetrics.put("activeConnections", 10);

        snapshot.setDatabaseMetrics(dbMetrics);

        assertEquals(50, snapshot.getDatabaseMetrics().get("connections"));
        assertEquals(10, snapshot.getDatabaseMetrics().get("activeConnections"));
    }

    @Test
    @DisplayName("Should set and get OS metrics")
    void testOsMetrics() {
        MetricsSnapshot snapshot = new MetricsSnapshot();
        Map<String, Object> osMetrics = new HashMap<>();
        osMetrics.put("cpuUsage", 75.5);
        osMetrics.put("memoryUsage", 60.0);

        snapshot.setOsMetrics(osMetrics);

        assertEquals(75.5, snapshot.getOsMetrics().get("cpuUsage"));
        assertEquals(60.0, snapshot.getOsMetrics().get("memoryUsage"));
    }

    @Test
    @DisplayName("Should handle null metrics")
    void testNullMetrics() {
        MetricsSnapshot snapshot = new MetricsSnapshot();

        snapshot.setTransactionMetrics(null);
        snapshot.setDatabaseMetrics(null);
        snapshot.setOsMetrics(null);

        assertNull(snapshot.getTransactionMetrics());
        assertNull(snapshot.getDatabaseMetrics());
        assertNull(snapshot.getOsMetrics());
    }

    @Test
    @DisplayName("Should handle empty metrics")
    void testEmptyMetrics() {
        MetricsSnapshot snapshot = new MetricsSnapshot();

        snapshot.setTransactionMetrics(new HashMap<>());
        snapshot.setDatabaseMetrics(new HashMap<>());
        snapshot.setOsMetrics(new HashMap<>());

        assertTrue(snapshot.getTransactionMetrics().isEmpty());
        assertTrue(snapshot.getDatabaseMetrics().isEmpty());
        assertTrue(snapshot.getOsMetrics().isEmpty());
    }

    @Test
    @DisplayName("Should support complex nested metrics")
    void testNestedMetrics() {
        MetricsSnapshot snapshot = new MetricsSnapshot();
        Map<String, Object> txMetrics = new HashMap<>();

        Map<String, Object> newOrderMetrics = new HashMap<>();
        newOrderMetrics.put("count", 100);
        newOrderMetrics.put("avgLatency", 5.5);

        txMetrics.put("NEW_ORDER", newOrderMetrics);
        snapshot.setTransactionMetrics(txMetrics);

        @SuppressWarnings("unchecked")
        Map<String, Object> retrieved = (Map<String, Object>) snapshot.getTransactionMetrics().get("NEW_ORDER");
        assertEquals(100, retrieved.get("count"));
        assertEquals(5.5, retrieved.get("avgLatency"));
    }
}
