package com.ninedata.dbbench.metrics;

import lombok.Getter;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

@Component
public class MetricsRegistry {
    private final Map<String, TransactionMetrics> transactionMetrics = new ConcurrentHashMap<>();
    @Getter
    private final List<MetricsSnapshot> history = new CopyOnWriteArrayList<>();
    private volatile long startTime;
    private volatile long endTime;

    public void reset() {
        transactionMetrics.clear();
        history.clear();
        startTime = System.currentTimeMillis();
        endTime = 0;
    }

    public void markEnd() {
        endTime = System.currentTimeMillis();
    }

    public TransactionMetrics getOrCreate(String name) {
        return transactionMetrics.computeIfAbsent(name, TransactionMetrics::new);
    }

    public void recordTransaction(String name, boolean success, long latencyNanos) {
        TransactionMetrics metrics = getOrCreate(name);
        if (success) {
            metrics.recordSuccess(latencyNanos);
        } else {
            metrics.recordFailure(latencyNanos);
        }
    }

    public Map<String, Object> getCurrentMetrics() {
        Map<String, Object> result = new LinkedHashMap<>();

        long totalCount = 0;
        long totalSuccess = 0;
        double totalLatency = 0;

        List<Map<String, Object>> txMetrics = new ArrayList<>();
        for (TransactionMetrics m : transactionMetrics.values()) {
            Map<String, Object> tx = new LinkedHashMap<>();
            tx.put("name", m.getName());
            tx.put("count", m.getCount());
            tx.put("success", m.getSuccessCount());
            tx.put("failure", m.getFailureCount());
            tx.put("successRate", Math.round(m.getSuccessRate() * 100.0) / 100.0);
            tx.put("avgLatencyMs", Math.round(m.getAverageLatencyMs() * 100.0) / 100.0);
            tx.put("minLatencyMs", Math.round(m.getMinLatencyMs() * 100.0) / 100.0);
            tx.put("maxLatencyMs", Math.round(m.getMaxLatencyMs() * 100.0) / 100.0);
            txMetrics.add(tx);

            totalCount += m.getCount();
            totalSuccess += m.getSuccessCount();
            totalLatency += m.getAverageLatencyMs() * m.getCount();
        }

        result.put("transactions", txMetrics);
        result.put("totalTransactions", totalCount);
        result.put("totalSuccess", totalSuccess);
        result.put("totalFailure", totalCount - totalSuccess);
        result.put("overallSuccessRate", totalCount > 0 ? Math.round((totalSuccess * 100.0 / totalCount) * 100.0) / 100.0 : 0);
        result.put("avgLatencyMs", totalCount > 0 ? Math.round((totalLatency / totalCount) * 100.0) / 100.0 : 0);

        long elapsed = (endTime > 0 ? endTime : System.currentTimeMillis()) - startTime;
        result.put("elapsedSeconds", elapsed / 1000);
        result.put("tps", elapsed > 0 ? Math.round((totalCount * 1000.0 / elapsed) * 100.0) / 100.0 : 0);

        return result;
    }

    public void takeSnapshot(Map<String, Object> dbMetrics, Map<String, Object> osMetrics) {
        MetricsSnapshot snapshot = new MetricsSnapshot();
        snapshot.setTimestamp(System.currentTimeMillis());
        snapshot.setTransactionMetrics(new HashMap<>(getCurrentMetrics()));
        snapshot.setDatabaseMetrics(dbMetrics != null ? new HashMap<>(dbMetrics) : new HashMap<>());
        snapshot.setOsMetrics(osMetrics != null ? new HashMap<>(osMetrics) : new HashMap<>());
        history.add(snapshot);

        // Keep only last hour of data (3600 snapshots at 1/sec)
        while (history.size() > 3600) {
            history.remove(0);
        }
    }
}
