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
        description = "NineData DBBench - TPC-C Database Benchmark Tool")
public class CLIRunner implements Callable<Integer> {

    // Database connection options
    @Option(names = {"--jdbcurl"}, required = true,
            description = "JDBC URL (e.g., jdbc:mysql://host:3306/db, jdbc:postgresql://host:5432/db)")
    private String jdbcUrl;

    @Option(names = {"-u", "--user"}, description = "Database username", defaultValue = "root")
    private String username;

    @Option(names = {"-p", "--password"}, description = "Database password", defaultValue = "")
    private String password;

    @Option(names = {"--pool-size"}, description = "Connection pool size", defaultValue = "50")
    private int poolSize;

    // Benchmark options
    @Option(names = {"-w", "--warehouses"}, description = "Number of warehouses", defaultValue = "1")
    private int warehouses;

    @Option(names = {"-c", "--terminals"}, description = "Number of terminals (concurrent threads)", defaultValue = "10")
    private int terminals;

    @Option(names = {"-d", "--duration"}, description = "Test duration in seconds", defaultValue = "60")
    private int duration;

    @Option(names = {"--load-threads"}, description = "Number of parallel threads for data loading", defaultValue = "4")
    private int loadConcurrency;

    // Run mode options
    @Option(names = {"--load-only"}, description = "Only load data, don't run benchmark")
    private boolean loadOnly;

    @Option(names = {"--clean"}, description = "Clean existing data before loading")
    private boolean clean;

    public static void run(String[] args) {
        int exitCode = new CommandLine(new CLIRunner()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public Integer call() throws Exception {
        printBanner();

        // Auto-detect database type from JDBC URL
        String dbType = detectDatabaseType(jdbcUrl);
        if (dbType == null) {
            System.err.println("Error: Unable to detect database type from JDBC URL: " + jdbcUrl);
            System.err.println("Supported formats: jdbc:mysql://, jdbc:postgresql://, jdbc:oracle:, jdbc:sqlserver://, jdbc:db2://, jdbc:dm://, jdbc:oceanbase://, jdbc:tidb://");
            return 1;
        }

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
        System.out.printf("  Database Type: %s%n", dbType.toUpperCase());
        System.out.printf("  JDBC URL:      %s%n", jdbcUrl);
        System.out.printf("  Username:      %s%n", username);
        System.out.printf("  Pool Size:     %d%n", poolSize);
        System.out.println();
        System.out.printf("  Warehouses:    %d%n", warehouses);
        System.out.printf("  Terminals:     %d%n", terminals);
        System.out.printf("  Duration:      %ds%n", duration);
        System.out.printf("  Load Threads:  %d%n", loadConcurrency);
        System.out.println();

        MetricsRegistry metricsRegistry = new MetricsRegistry();
        OSMetricsCollector osMetricsCollector = new OSMetricsCollector();
        osMetricsCollector.init();
        BenchmarkEngine engine = new BenchmarkEngine(dbConfig, benchConfig, metricsRegistry, osMetricsCollector);

        try {
            // Initialize
            System.out.println("Initializing database connection...");
            engine.initialize();
            System.out.println("Database connection established.");
            System.out.println();

            // Clean data if requested
            if (clean) {
                System.out.println("Cleaning existing data...");
                engine.cleanData();
                System.out.println("Data cleaned.");
                System.out.println();
            }

            // Load data only if --load-only or --clean is specified
            if (loadOnly || clean) {
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

    /**
     * Auto-detect database type from JDBC URL
     */
    private String detectDatabaseType(String url) {
        if (url == null) return null;
        String lowerUrl = url.toLowerCase();

        if (lowerUrl.startsWith("jdbc:mysql://")) {
            return "mysql";
        } else if (lowerUrl.startsWith("jdbc:postgresql://")) {
            return "postgresql";
        } else if (lowerUrl.startsWith("jdbc:oracle:")) {
            return "oracle";
        } else if (lowerUrl.startsWith("jdbc:sqlserver://")) {
            return "sqlserver";
        } else if (lowerUrl.startsWith("jdbc:db2://")) {
            return "db2";
        } else if (lowerUrl.startsWith("jdbc:dm://")) {
            return "dameng";
        } else if (lowerUrl.startsWith("jdbc:oceanbase://")) {
            return "oceanbase";
        } else if (lowerUrl.startsWith("jdbc:tidb://")) {
            return "tidb";
        }
        // TiDB and OceanBase often use MySQL protocol
        if (lowerUrl.contains("tidb") || lowerUrl.contains(":4000/")) {
            return "tidb";
        }
        if (lowerUrl.contains("oceanbase") || lowerUrl.contains(":2881/")) {
            return "oceanbase";
        }
        return null;
    }

    private void printBanner() {
        System.out.println();
        System.out.println("  _   _ _            ____        _        ");
        System.out.println(" | \\ | (_)_ __   ___|  _ \\  __ _| |_ __ _ ");
        System.out.println(" |  \\| | | '_ \\ / _ \\ | | |/ _` | __/ _` |");
        System.out.println(" | |\\  | | | | |  __/ |_| | (_| | || (_| |");
        System.out.println(" |_| \\_|_|_| |_|\\___|____/ \\__,_|\\__\\__,_|");
        System.out.println();
        System.out.println("  ____  ____  ____                  _     ");
        System.out.println(" |  _ \\| __ )| __ )  ___ _ __   ___| |__  ");
        System.out.println(" | | | |  _ \\|  _ \\ / _ \\ '_ \\ / __| '_ \\ ");
        System.out.println(" | |_| | |_) | |_) |  __/ | | | (__| | | |");
        System.out.println(" |____/|____/|____/ \\___|_| |_|\\___|_| |_|");
        System.out.println();
        System.out.println("  TPC-C Database Benchmark Tool  v1.0.0");
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
