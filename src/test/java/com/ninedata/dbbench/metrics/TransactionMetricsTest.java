package com.ninedata.dbbench.metrics;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("TransactionMetrics Tests")
class TransactionMetricsTest {

    private TransactionMetrics metrics;

    @BeforeEach
    void setUp() {
        metrics = new TransactionMetrics("TEST_TX");
    }

    @Test
    @DisplayName("Should initialize with correct name")
    void testInitialization() {
        assertEquals("TEST_TX", metrics.getName());
        assertEquals(0, metrics.getCount());
        assertEquals(0, metrics.getSuccessCount());
        assertEquals(0, metrics.getFailureCount());
    }

    @Test
    @DisplayName("Should record success correctly")
    void testRecordSuccess() {
        metrics.recordSuccess(1_000_000); // 1ms in nanos

        assertEquals(1, metrics.getCount());
        assertEquals(1, metrics.getSuccessCount());
        assertEquals(0, metrics.getFailureCount());
        assertEquals(100.0, metrics.getSuccessRate(), 0.01);
    }

    @Test
    @DisplayName("Should record failure correctly")
    void testRecordFailure() {
        metrics.recordFailure(2_000_000); // 2ms in nanos

        assertEquals(1, metrics.getCount());
        assertEquals(0, metrics.getSuccessCount());
        assertEquals(1, metrics.getFailureCount());
        assertEquals(0.0, metrics.getSuccessRate(), 0.01);
    }

    @Test
    @DisplayName("Should calculate average latency correctly")
    void testAverageLatency() {
        metrics.recordSuccess(1_000_000); // 1ms
        metrics.recordSuccess(3_000_000); // 3ms
        metrics.recordSuccess(2_000_000); // 2ms

        assertEquals(2.0, metrics.getAverageLatencyMs(), 0.01);
    }

    @Test
    @DisplayName("Should track min latency correctly")
    void testMinLatency() {
        metrics.recordSuccess(5_000_000); // 5ms
        metrics.recordSuccess(2_000_000); // 2ms
        metrics.recordSuccess(8_000_000); // 8ms

        assertEquals(2.0, metrics.getMinLatencyMs(), 0.01);
    }

    @Test
    @DisplayName("Should track max latency correctly")
    void testMaxLatency() {
        metrics.recordSuccess(5_000_000); // 5ms
        metrics.recordSuccess(2_000_000); // 2ms
        metrics.recordSuccess(8_000_000); // 8ms

        assertEquals(8.0, metrics.getMaxLatencyMs(), 0.01);
    }

    @Test
    @DisplayName("Should return 0 for min latency when no records")
    void testMinLatencyNoRecords() {
        assertEquals(0.0, metrics.getMinLatencyMs(), 0.01);
    }

    @Test
    @DisplayName("Should return 0 for average latency when no records")
    void testAverageLatencyNoRecords() {
        assertEquals(0.0, metrics.getAverageLatencyMs(), 0.01);
    }

    @Test
    @DisplayName("Should calculate success rate correctly with mixed results")
    void testSuccessRateMixed() {
        metrics.recordSuccess(1_000_000);
        metrics.recordSuccess(1_000_000);
        metrics.recordSuccess(1_000_000);
        metrics.recordFailure(1_000_000);

        assertEquals(4, metrics.getCount());
        assertEquals(3, metrics.getSuccessCount());
        assertEquals(1, metrics.getFailureCount());
        assertEquals(75.0, metrics.getSuccessRate(), 0.01);
    }

    @Test
    @DisplayName("Should be thread-safe for concurrent updates")
    void testConcurrentUpdates() throws InterruptedException {
        int threadCount = 10;
        int operationsPerThread = 1000;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);

        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            executor.submit(() -> {
                try {
                    for (int j = 0; j < operationsPerThread; j++) {
                        if (threadId % 2 == 0) {
                            metrics.recordSuccess(1_000_000);
                        } else {
                            metrics.recordFailure(1_000_000);
                        }
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await(10, TimeUnit.SECONDS);
        executor.shutdown();

        assertEquals(threadCount * operationsPerThread, metrics.getCount());
        assertEquals(5 * operationsPerThread, metrics.getSuccessCount());
        assertEquals(5 * operationsPerThread, metrics.getFailureCount());
    }

    @Test
    @DisplayName("Should handle very small latencies")
    void testSmallLatencies() {
        metrics.recordSuccess(100); // 0.0001ms

        assertEquals(0.0001, metrics.getAverageLatencyMs(), 0.00001);
        assertEquals(0.0001, metrics.getMinLatencyMs(), 0.00001);
        assertEquals(0.0001, metrics.getMaxLatencyMs(), 0.00001);
    }

    @Test
    @DisplayName("Should handle very large latencies")
    void testLargeLatencies() {
        long largeLatency = 60_000_000_000L; // 60 seconds in nanos
        metrics.recordSuccess(largeLatency);

        assertEquals(60000.0, metrics.getAverageLatencyMs(), 0.01);
        assertEquals(60000.0, metrics.getMaxLatencyMs(), 0.01);
    }

    @Test
    @DisplayName("Should update min correctly when new value is smaller")
    void testMinLatencyUpdate() {
        metrics.recordSuccess(10_000_000); // 10ms
        assertEquals(10.0, metrics.getMinLatencyMs(), 0.01);

        metrics.recordSuccess(5_000_000); // 5ms
        assertEquals(5.0, metrics.getMinLatencyMs(), 0.01);

        metrics.recordSuccess(15_000_000); // 15ms - should not change min
        assertEquals(5.0, metrics.getMinLatencyMs(), 0.01);
    }

    @Test
    @DisplayName("Should update max correctly when new value is larger")
    void testMaxLatencyUpdate() {
        metrics.recordSuccess(10_000_000); // 10ms
        assertEquals(10.0, metrics.getMaxLatencyMs(), 0.01);

        metrics.recordSuccess(15_000_000); // 15ms
        assertEquals(15.0, metrics.getMaxLatencyMs(), 0.01);

        metrics.recordSuccess(5_000_000); // 5ms - should not change max
        assertEquals(15.0, metrics.getMaxLatencyMs(), 0.01);
    }
}
