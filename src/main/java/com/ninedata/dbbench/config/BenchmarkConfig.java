package com.ninedata.dbbench.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@Component
@ConfigurationProperties(prefix = "benchmark")
public class BenchmarkConfig {
    private int warehouses = 10;
    private int terminals = 50;
    private int duration = 60;
    private int rampup = 10;
    private boolean thinkTime = true;
    private int loadConcurrency = 4;
    private MixConfig mix = new MixConfig();

    @Data
    public static class MixConfig {
        private int newOrder = 45;
        private int payment = 43;
        private int orderStatus = 4;
        private int delivery = 4;
        private int stockLevel = 4;
    }
}
