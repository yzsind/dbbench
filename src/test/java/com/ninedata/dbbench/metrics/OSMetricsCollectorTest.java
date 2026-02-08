package com.ninedata.dbbench.metrics;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("OSMetricsCollector Tests")
class OSMetricsCollectorTest {

    private OSMetricsCollector collector;

    @BeforeEach
    void setUp() {
        collector = new OSMetricsCollector();
        collector.init();
    }

    @Test
    @DisplayName("Should collect CPU metrics")
    void testCollectCpuMetrics() {
        Map<String, Object> metrics = collector.collect();

        assertTrue(metrics.containsKey("cpuUsage"));
        assertTrue(metrics.containsKey("cpuCores"));

        double cpuUsage = (Double) metrics.get("cpuUsage");
        assertTrue(cpuUsage >= 0 && cpuUsage <= 100,
            "CPU usage should be between 0 and 100, got: " + cpuUsage);

        int cpuCores = (Integer) metrics.get("cpuCores");
        assertTrue(cpuCores > 0, "CPU cores should be positive");
    }

    @Test
    @DisplayName("Should collect memory metrics")
    void testCollectMemoryMetrics() {
        Map<String, Object> metrics = collector.collect();

        assertTrue(metrics.containsKey("memoryTotal"));
        assertTrue(metrics.containsKey("memoryUsed"));
        assertTrue(metrics.containsKey("memoryFree"));
        assertTrue(metrics.containsKey("memoryUsage"));

        long memoryTotal = (Long) metrics.get("memoryTotal");
        long memoryUsed = (Long) metrics.get("memoryUsed");
        long memoryFree = (Long) metrics.get("memoryFree");

        assertTrue(memoryTotal > 0, "Total memory should be positive");
        assertTrue(memoryUsed >= 0, "Used memory should be non-negative");
        assertTrue(memoryFree >= 0, "Free memory should be non-negative");
        assertTrue(memoryUsed + memoryFree <= memoryTotal + 100,
            "Used + Free should approximately equal Total");
    }

    @Test
    @DisplayName("Should collect swap metrics")
    void testCollectSwapMetrics() {
        Map<String, Object> metrics = collector.collect();

        assertTrue(metrics.containsKey("swapTotal"));
        assertTrue(metrics.containsKey("swapUsed"));

        long swapTotal = (Long) metrics.get("swapTotal");
        long swapUsed = (Long) metrics.get("swapUsed");

        assertTrue(swapTotal >= 0, "Swap total should be non-negative");
        assertTrue(swapUsed >= 0, "Swap used should be non-negative");
    }

    @Test
    @DisplayName("Should collect process metrics")
    void testCollectProcessMetrics() {
        Map<String, Object> metrics = collector.collect();

        assertTrue(metrics.containsKey("processCount"));
        assertTrue(metrics.containsKey("threadCount"));

        int processCount = (Integer) metrics.get("processCount");
        int threadCount = (Integer) metrics.get("threadCount");

        assertTrue(processCount > 0, "Process count should be positive");
        assertTrue(threadCount > 0, "Thread count should be positive");
    }

    @Test
    @DisplayName("Should collect disk I/O metrics")
    void testCollectDiskMetrics() {
        Map<String, Object> metrics = collector.collect();

        assertTrue(metrics.containsKey("diskReadBytes"));
        assertTrue(metrics.containsKey("diskWriteBytes"));
        assertTrue(metrics.containsKey("diskReadBytesPerSec"));
        assertTrue(metrics.containsKey("diskWriteBytesPerSec"));

        long diskReadBytes = (Long) metrics.get("diskReadBytes");
        long diskWriteBytes = (Long) metrics.get("diskWriteBytes");

        assertTrue(diskReadBytes >= 0, "Disk read bytes should be non-negative");
        assertTrue(diskWriteBytes >= 0, "Disk write bytes should be non-negative");
    }

    @Test
    @DisplayName("Should collect network I/O metrics")
    void testCollectNetworkMetrics() {
        Map<String, Object> metrics = collector.collect();

        assertTrue(metrics.containsKey("networkRecvBytes"));
        assertTrue(metrics.containsKey("networkSentBytes"));
        assertTrue(metrics.containsKey("networkRecvBytesPerSec"));
        assertTrue(metrics.containsKey("networkSentBytesPerSec"));

        long networkRecvBytes = (Long) metrics.get("networkRecvBytes");
        long networkSentBytes = (Long) metrics.get("networkSentBytes");

        assertTrue(networkRecvBytes >= 0, "Network recv bytes should be non-negative");
        assertTrue(networkSentBytes >= 0, "Network sent bytes should be non-negative");
    }

    @Test
    @DisplayName("Should return non-empty metrics map")
    void testCollectReturnsNonEmptyMap() {
        Map<String, Object> metrics = collector.collect();

        assertNotNull(metrics);
        assertFalse(metrics.isEmpty());
    }

    @Test
    @DisplayName("Should handle multiple consecutive collections")
    void testMultipleCollections() throws InterruptedException {
        Map<String, Object> metrics1 = collector.collect();
        Thread.sleep(100);
        Map<String, Object> metrics2 = collector.collect();

        assertNotNull(metrics1);
        assertNotNull(metrics2);

        // Both should have the same keys
        assertEquals(metrics1.keySet(), metrics2.keySet());
    }

    @Test
    @DisplayName("Memory usage should be between 0 and 100")
    void testMemoryUsageRange() {
        Map<String, Object> metrics = collector.collect();

        double memoryUsage = (Double) metrics.get("memoryUsage");
        assertTrue(memoryUsage >= 0 && memoryUsage <= 100,
            "Memory usage should be between 0 and 100, got: " + memoryUsage);
    }
}
