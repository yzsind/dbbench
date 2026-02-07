package com.ninedata.dbbench.cli;

import com.ninedata.dbbench.config.BenchmarkConfig;
import com.ninedata.dbbench.config.DatabaseConfig;
import com.ninedata.dbbench.database.DatabaseAdapter;
import com.ninedata.dbbench.database.DatabaseFactory;
import com.ninedata.dbbench.metrics.MetricsRegistry;
import com.ninedata.dbbench.metrics.OSMetricsCollector;
import com.ninedata.dbbench.engine.BenchmarkEngine;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Command(name = "dbbench", mixinStandardHelpOptions = true, version = "1.0.0",
        description = "TPC-C Database Benchmark Tool")
public class CLIRunner implements Callable<Integer> {

    @Option(names = {"-t", "--type"}, description = "Database type (mysql, postgresql, oracle, sqlserver, db2, dameng, oceanbase, tidb)", defaultValue = "mysql")
    private String dbType;

    @Option(names = {"--url"}, description = "JDBC URL", defaultValue = "jdbc:mysql://127.0.0.1:3306/tpcc?useSSL=false&allowPublicKeyRetrieval=true&rewriteBatchedStatements=true")
    private String jdbcUrl;

    @Option(names = {"-u", "--user"}, description = "Database username", defaultValue = "sysbench")
    private String username;

    @Option(names = {"-p", "--password"}, description = "Database password", defaultValue = "sysbench")
    private String password;

    @Option(names = {"-w", "--warehouses"}, description = "Number of warehouses", defaultValue = "1")
    private int warehouses;

    @Option(names = {"-c", "--terminals"}, description = "Number of terminals (concurrent connections)", defaultValue = "5")
    private int terminals;

    @Option(names = {"-r", "--duration"}, description = "Test duration in seconds", defaultValue = "60")
    private int duration;

    @Option(names = {"--load-only"}, description = "Only load data, don't run benchmark")
    private boolean loadOnly;

    @Option(names = {"--skip-load"}, description = "Skip data loading, run benchmark only")
    private boolean skipLoad;

    @Option(names = {"--pool-size"}, description = "Connection pool size", defaultValue = "50")
    private int poolSize;

    @Option(names = {"--load-threads"}, description = "Number of parallel threads for data loading", defaultValue = "4")
    private int loadConcurrency;

    public static void run(String[] args) {
        int exitCode = new CommandLine(new CLIRunner()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public Integer call() throws Exception {
        printBanner();

        // Configure database
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setType(dbType);
        dbConfig.setJdbcUrl(jdbcUrl);
        dbConfig.setUsername(username);
        dbConfig.setPassword(password);
        dbConfig.getPool().setSize(poolSize);

        // Configure benchmark
        BenchmarkConfig benchConfig = new BenchmarkConfig();
        benchConfig.setWarehouses(warehouses);
        benchConfig.setTerminals(terminals);
        benchConfig.setDuration(duration);
        benchConfig.setLoadConcurrency(loadConcurrency);

        System.out.println("Configuration:");
        System.out.printf("  Database: %s%n", dbType);
        System.out.printf("  JDBC URL: %s%n", jdbcUrl);
        System.out.printf("  Warehouses: %d, Terminals: %d, Duration: %ds%n", warehouses, terminals, duration);
        System.out.printf("  Load Threads: %d, Pool Size: %d%n", loadConcurrency, poolSize);
        System.out.println();

        MetricsRegistry metricsRegistry = new MetricsRegistry();
        OSMetricsCollector osMetricsCollector = new OSMetricsCollector();
        BenchmarkEngine engine = new BenchmarkEngine(dbConfig, benchConfig, metricsRegistry, osMetricsCollector);

        try {
            // Initialize
            System.out.println("Initializing database connection...");
            engine.initialize();
            System.out.println("Database connection established.");
            System.out.println();

            // Load data
            if (!skipLoad) {
                System.out.println("Loading TPC-C data...");
                long loadStart = System.currentTimeMillis();
                engine.loadData(System.out::println);
                long loadTime = (System.currentTimeMillis() - loadStart) / 1000;
                System.out.printf("Data loading completed in %d seconds.%n", loadTime);
                System.out.println();
            }

            if (loadOnly) {
                System.out.println("Load-only mode, skipping benchmark.");
                return 0;
            }

            // Run benchmark
            System.out.println("Starting benchmark...");
            System.out.println("Press Ctrl+C to stop early.");
            System.out.println();

            // Start metrics display
            ScheduledExecutorService displayScheduler = Executors.newSingleThreadScheduledExecutor();
            displayScheduler.scheduleAtFixedRate(() -> {
                Map<String, Object> metrics = metricsRegistry.getCurrentMetrics();
                System.out.printf("\rTPS: %.2f | Total: %d | Success: %.1f%% | Avg Latency: %.2fms | Elapsed: %ds",
                        metrics.get("tps"),
                        metrics.get("totalTransactions"),
                        metrics.get("overallSuccessRate"),
                        metrics.get("avgLatencyMs"),
                        metrics.get("elapsedSeconds"));
            }, 1, 1, TimeUnit.SECONDS);

            engine.start();

            // Wait for completion
            Thread.sleep(duration * 1000L + 2000);

            displayScheduler.shutdown();
            System.out.println();
            System.out.println();

            // Print final results
            printResults(metricsRegistry.getCurrentMetrics());

            return 0;
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
            return 1;
        } finally {
            engine.shutdown();
        }
    }

    private void printBanner() {
        System.out.println("╔═══════════════════════════════════════════════════════════╗");
        System.out.println("║           DBBench - TPC-C Database Benchmark              ║");
        System.out.println("║                     Version 1.0.0                         ║");
        System.out.println("╚═══════════════════════════════════════════════════════════╝");
        System.out.println();
    }

    private void printResults(Map<String, Object> metrics) {
        System.out.println("╔═══════════════════════════════════════════════════════════╗");
        System.out.println("║                    BENCHMARK RESULTS                      ║");
        System.out.println("╠═══════════════════════════════════════════════════════════╣");
        System.out.printf("║  Throughput (TPS):        %10.2f                      ║%n", metrics.get("tps"));
        System.out.printf("║  Total Transactions:      %10d                      ║%n", metrics.get("totalTransactions"));
        System.out.printf("║  Successful:              %10d                      ║%n", metrics.get("totalSuccess"));
        System.out.printf("║  Failed:                  %10d                      ║%n", metrics.get("totalFailure"));
        System.out.printf("║  Success Rate:            %10.2f%%                     ║%n", metrics.get("overallSuccessRate"));
        System.out.printf("║  Average Latency:         %10.2f ms                   ║%n", metrics.get("avgLatencyMs"));
        System.out.printf("║  Duration:                %10d seconds               ║%n", metrics.get("elapsedSeconds"));
        System.out.println("╚═══════════════════════════════════════════════════════════╝");
    }
}
