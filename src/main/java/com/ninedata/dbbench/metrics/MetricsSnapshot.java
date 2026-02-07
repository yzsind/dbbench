package com.ninedata.dbbench.metrics;

import lombok.Data;
import java.util.Map;

@Data
public class MetricsSnapshot {
    private long timestamp;
    private Map<String, Object> transactionMetrics;
    private Map<String, Object> databaseMetrics;
    private Map<String, Object> osMetrics;
}
