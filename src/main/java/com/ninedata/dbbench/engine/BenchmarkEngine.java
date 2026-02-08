package com.ninedata.dbbench.engine;

import com.ninedata.dbbench.config.BenchmarkConfig;
import com.ninedata.dbbench.config.DatabaseConfig;
import com.ninedata.dbbench.database.DatabaseAdapter;
import com.ninedata.dbbench.database.DatabaseFactory;
import com.ninedata.dbbench.metrics.MetricsRegistry;
import com.ninedata.dbbench.metrics.OSMetricsCollector;
import com.ninedata.dbbench.tpcc.TPCCUtil;
import com.ninedata.dbbench.tpcc.loader.TPCCLoader;
import com.ninedata.dbbench.tpcc.transaction.*;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

@Slf4j
@Component
public class BenchmarkEngine {
    private final DatabaseConfig dbConfig;
    private final BenchmarkConfig benchConfig;
    private final MetricsRegistry metricsRegistry;
    private final OSMetricsCollector osMetricsCollector;

    private DatabaseAdapter adapter;
    private ExecutorService executorService;
    private ScheduledExecutorService metricsScheduler;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean loading = new AtomicBoolean(false);
    @Getter
    private volatile String status = "IDLE";
    private Consumer<Map<String, Object>> metricsCallback;
    private Consumer<Map<String, Object>> logCallback;

    // Log history
    private final List<Map<String, Object>> logHistory = Collections.synchronizedList(new ArrayList<>());
    private static final int MAX_LOG_HISTORY = 1000;

    // Loading progress
    @Getter
    private volatile int loadProgress = 0;
    @Getter
    private volatile String loadMessage = "";
    private volatile TPCCLoader currentLoader = null;

    public BenchmarkEngine(DatabaseConfig dbConfig, BenchmarkConfig benchConfig,
                           MetricsRegistry metricsRegistry, OSMetricsCollector osMetricsCollector) {
        this.dbConfig = dbConfig;
        this.benchConfig = benchConfig;
        this.metricsRegistry = metricsRegistry;
        this.osMetricsCollector = osMetricsCollector;
    }

    public void setMetricsCallback(Consumer<Map<String, Object>> callback) {
        this.metricsCallback = callback;
    }

    public void setLogCallback(Consumer<Map<String, Object>> callback) {
        this.logCallback = callback;
    }

    private void addLog(String level, String message) {
        Map<String, Object> logEntry = new LinkedHashMap<>();
        logEntry.put("timestamp", LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")));
        logEntry.put("level", level);
        logEntry.put("message", message);

        logHistory.add(logEntry);
        while (logHistory.size() > MAX_LOG_HISTORY) {
            logHistory.remove(0);
        }

        if (logCallback != null) {
            Map<String, Object> data = new LinkedHashMap<>();
            data.put("type", "log");
            data.put("log", logEntry);
            logCallback.accept(data);
        }

        // Also log to slf4j
        switch (level) {
            case "ERROR" -> log.error(message);
            case "WARN" -> log.warn(message);
            default -> log.info(message);
        }
    }

    public List<Map<String, Object>> getLogHistory() {
        return new ArrayList<>(logHistory);
    }

    public void clearLogs() {
        logHistory.clear();
    }

    public void updateConfig(Map<String, Object> newConfig) {
        if (running.get() || loading.get()) {
            throw new IllegalStateException("Cannot update config while running or loading");
        }

        // Update database config
        if (newConfig.containsKey("database")) {
            @SuppressWarnings("unchecked")
            Map<String, Object> db = (Map<String, Object>) newConfig.get("database");
            if (db.containsKey("type")) dbConfig.setType((String) db.get("type"));
            if (db.containsKey("jdbcUrl")) dbConfig.setJdbcUrl((String) db.get("jdbcUrl"));
            if (db.containsKey("username")) dbConfig.setUsername((String) db.get("username"));
            if (db.containsKey("password")) dbConfig.setPassword((String) db.get("password"));
            if (db.containsKey("poolSize")) dbConfig.getPool().setSize(((Number) db.get("poolSize")).intValue());
        }

        // Update benchmark config
        if (newConfig.containsKey("benchmark")) {
            @SuppressWarnings("unchecked")
            Map<String, Object> bench = (Map<String, Object>) newConfig.get("benchmark");
            if (bench.containsKey("warehouses")) benchConfig.setWarehouses(((Number) bench.get("warehouses")).intValue());
            if (bench.containsKey("terminals")) benchConfig.setTerminals(((Number) bench.get("terminals")).intValue());
            if (bench.containsKey("duration")) benchConfig.setDuration(((Number) bench.get("duration")).intValue());
            if (bench.containsKey("thinkTime")) benchConfig.setThinkTime((Boolean) bench.get("thinkTime"));
            if (bench.containsKey("loadConcurrency")) benchConfig.setLoadConcurrency(((Number) bench.get("loadConcurrency")).intValue());
        }

        // Update transaction mix
        if (newConfig.containsKey("transactionMix")) {
            @SuppressWarnings("unchecked")
            Map<String, Object> mix = (Map<String, Object>) newConfig.get("transactionMix");
            if (mix.containsKey("newOrder")) benchConfig.getMix().setNewOrder(((Number) mix.get("newOrder")).intValue());
            if (mix.containsKey("payment")) benchConfig.getMix().setPayment(((Number) mix.get("payment")).intValue());
            if (mix.containsKey("orderStatus")) benchConfig.getMix().setOrderStatus(((Number) mix.get("orderStatus")).intValue());
            if (mix.containsKey("delivery")) benchConfig.getMix().setDelivery(((Number) mix.get("delivery")).intValue());
            if (mix.containsKey("stockLevel")) benchConfig.getMix().setStockLevel(((Number) mix.get("stockLevel")).intValue());
        }

        // Close existing adapter if config changed
        if (adapter != null) {
            adapter.close();
            adapter = null;
            status = "IDLE";
        }

        addLog("INFO", "Configuration updated");
    }

    public void initialize() throws SQLException {
        status = "INITIALIZING";
        addLog("INFO", "Initializing database connection...");
        addLog("INFO", String.format("Database: %s, URL: %s", dbConfig.getType(), dbConfig.getJdbcUrl()));

        if (adapter != null) {
            adapter.close();
            adapter = null;
        }

        try {
            adapter = DatabaseFactory.create(dbConfig);
            adapter.initialize();
            status = "INITIALIZED";
            addLog("INFO", "Database connection initialized successfully");
        } catch (Exception e) {
            adapter = null;
            status = "ERROR";
            throw new SQLException("Failed to initialize database connection: " + e.getMessage(), e);
        }
    }

    /**
     * Ensure database connection is initialized
     */
    private void ensureInitialized() throws SQLException {
        if (adapter == null || !isAdapterReady()) {
            initialize();
        }
    }

    /**
     * Check if adapter is ready (has valid connection pool)
     */
    private boolean isAdapterReady() {
        if (adapter == null) return false;
        try {
            // Try to get a connection to verify the pool is working
            adapter.getConnection().close();
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Check if TPC-C data has been loaded
     */
    private boolean isDataLoaded() {
        try (var conn = adapter.getConnection();
             var stmt = conn.createStatement();
             var rs = stmt.executeQuery("SELECT COUNT(*) FROM warehouse")) {
            if (rs.next()) {
                return rs.getInt(1) > 0;
            }
        } catch (Exception e) {
            // Table doesn't exist or other error
            return false;
        }
        return false;
    }

    /**
     * Synchronous data loading for CLI usage
     */
    public void loadData(Consumer<String> progressCallback) throws SQLException {
        if (loading.get()) {
            throw new IllegalStateException("Data loading already in progress");
        }

        ensureInitialized();

        loading.set(true);
        status = "LOADING";

        try {
            progressCallback.accept("Dropping existing schema...");
            adapter.dropSchema();

            progressCallback.accept("Creating schema...");
            adapter.createSchema();

            TPCCLoader loader = new TPCCLoader(adapter, benchConfig.getWarehouses(), benchConfig.getLoadConcurrency());
            currentLoader = loader;
            loader.setProgressCallback(progressCallback);
            loader.load();
            currentLoader = null;

            status = "LOADED";
            progressCallback.accept("Data load completed successfully");
        } catch (Exception e) {
            status = "ERROR";
            throw new SQLException("Data load failed: " + e.getMessage(), e);
        } finally {
            loading.set(false);
        }
    }

    /**
     * Asynchronous data loading for Web UI usage
     */
    public void loadDataAsync() throws SQLException {
        if (loading.get()) {
            throw new IllegalStateException("Data loading already in progress");
        }

        ensureInitialized();

        loading.set(true);
        loadProgress = 0;
        loadMessage = "Starting data load...";

        CompletableFuture.runAsync(() -> {
            try {
                status = "LOADING";
                addLog("INFO", String.format("Starting TPC-C data load for %d warehouse(s) with %d threads",
                        benchConfig.getWarehouses(), benchConfig.getLoadConcurrency()));

                broadcastLoadProgress(0, "Dropping existing schema...");
                adapter.dropSchema();

                broadcastLoadProgress(5, "Creating schema...");
                adapter.createSchema();
                addLog("INFO", "Schema created successfully");

                TPCCLoader loader = new TPCCLoader(adapter, benchConfig.getWarehouses(), benchConfig.getLoadConcurrency());
                currentLoader = loader;
                loader.setProgressCallback(msg -> {
                    addLog("INFO", msg);
                    // Parse progress from message
                    if (msg.contains("Items loaded")) {
                        broadcastLoadProgress(15, msg);
                    } else if (msg.contains("Warehouse") && msg.contains("completed")) {
                        // Extract progress from "Warehouse X completed (Y/Z)"
                        try {
                            int start = msg.indexOf("(") + 1;
                            int mid = msg.indexOf("/");
                            int end = msg.indexOf(")");
                            int completed = Integer.parseInt(msg.substring(start, mid));
                            int total = Integer.parseInt(msg.substring(mid + 1, end));
                            int progress = 15 + (int) ((completed * 80.0) / total);
                            broadcastLoadProgress(progress, msg);
                        } catch (Exception e) {
                            broadcastLoadProgress(loadProgress, msg);
                        }
                    } else {
                        broadcastLoadProgress(loadProgress, msg);
                    }
                });
                loader.load();
                currentLoader = null;

                broadcastLoadProgress(100, "Data load completed");
                status = "LOADED";
                addLog("INFO", "Data load completed successfully");

                // Broadcast final status change
                broadcastStatusChange("LOADED");
            } catch (Exception e) {
                currentLoader = null;
                // Check if it was cancelled
                if (e.getMessage() != null && e.getMessage().contains("cancelled")) {
                    status = "CANCELLED";
                    addLog("WARN", "Data load cancelled by user");
                    broadcastLoadProgress(-1, "Cancelled by user");
                    broadcastStatusChange("CANCELLED");
                } else {
                    status = "ERROR";
                    addLog("ERROR", "Data load failed: " + e.getMessage());
                    broadcastLoadProgress(-1, "Error: " + e.getMessage());
                    broadcastStatusChange("ERROR");
                }
            } finally {
                loading.set(false);
                currentLoader = null;
            }
        });
    }

    /**
     * Cancel the current data loading process
     */
    public void cancelLoad() {
        if (!loading.get()) {
            throw new IllegalStateException("No data loading in progress");
        }

        addLog("INFO", "Cancelling data load...");
        if (currentLoader != null) {
            currentLoader.cancel();
        }
    }

    private void broadcastStatusChange(String newStatus) {
        if (logCallback != null) {
            Map<String, Object> data = new LinkedHashMap<>();
            data.put("type", "status");
            data.put("status", newStatus);
            data.put("loading", loading.get());
            data.put("running", running.get());
            logCallback.accept(data);
        }
    }

    private void broadcastLoadProgress(int progress, String message) {
        loadProgress = progress;
        loadMessage = message;

        if (logCallback != null) {
            Map<String, Object> data = new LinkedHashMap<>();
            data.put("type", "progress");
            data.put("progress", progress);
            data.put("message", message);
            data.put("status", status);
            logCallback.accept(data);
        }
    }

    public void cleanData() throws SQLException {
        if (running.get() || loading.get()) {
            throw new IllegalStateException("Cannot clean data while running or loading");
        }

        ensureInitialized();

        addLog("INFO", "Cleaning TPC-C data...");
        adapter.dropSchema();
        status = "INITIALIZED";
        addLog("INFO", "Data cleaned successfully");
    }

    public void start() throws SQLException {
        if (running.get()) {
            addLog("WARN", "Benchmark already running");
            return;
        }

        ensureInitialized();

        // Check if data is loaded
        if (!isDataLoaded()) {
            throw new IllegalStateException("No TPC-C data found. Please load data first.");
        }

        running.set(true);
        status = "RUNNING";
        metricsRegistry.reset();

        // Set error callback for transactions
        AbstractTransaction.setErrorCallback(this::addLog);

        int terminals = benchConfig.getTerminals();
        executorService = Executors.newFixedThreadPool(terminals);
        metricsScheduler = Executors.newSingleThreadScheduledExecutor();

        addLog("INFO", String.format("Starting benchmark with %d terminals for %d seconds", terminals, benchConfig.getDuration()));
        addLog("INFO", String.format("Transaction mix: NewOrder=%d%%, Payment=%d%%, OrderStatus=%d%%, Delivery=%d%%, StockLevel=%d%%",
                benchConfig.getMix().getNewOrder(), benchConfig.getMix().getPayment(),
                benchConfig.getMix().getOrderStatus(), benchConfig.getMix().getDelivery(),
                benchConfig.getMix().getStockLevel()));

        // Broadcast status change
        broadcastStatusChange("RUNNING");

        // Start metrics collection
        metricsScheduler.scheduleAtFixedRate(this::collectAndBroadcastMetrics, 1, 1, TimeUnit.SECONDS);

        // Start terminal workers
        for (int i = 0; i < terminals; i++) {
            int terminalId = i + 1;
            int warehouseId = (i % benchConfig.getWarehouses()) + 1;
            int districtId = (i % TPCCUtil.DISTRICTS_PER_WAREHOUSE) + 1;
            executorService.submit(() -> runTerminal(terminalId, warehouseId, districtId));
        }

        // Schedule stop
        metricsScheduler.schedule(this::stop, benchConfig.getDuration(), TimeUnit.SECONDS);
    }

    private void runTerminal(int terminalId, int warehouseId, int districtId) {
        Random random = new Random();
        int[] weights = {
            benchConfig.getMix().getNewOrder(),
            benchConfig.getMix().getPayment(),
            benchConfig.getMix().getOrderStatus(),
            benchConfig.getMix().getDelivery(),
            benchConfig.getMix().getStockLevel()
        };
        int totalWeight = Arrays.stream(weights).sum();

        while (running.get()) {
            // Select transaction based on mix
            int r = random.nextInt(totalWeight);
            int cumulative = 0;
            int txType = 0;
            for (int i = 0; i < weights.length; i++) {
                cumulative += weights[i];
                if (r < cumulative) {
                    txType = i;
                    break;
                }
            }

            AbstractTransaction tx = switch (txType) {
                case 0 -> new NewOrderTransaction(adapter, warehouseId, districtId);
                case 1 -> new PaymentTransaction(adapter, warehouseId, districtId);
                case 2 -> new OrderStatusTransaction(adapter, warehouseId, districtId);
                case 3 -> new DeliveryTransaction(adapter, warehouseId, districtId);
                case 4 -> new StockLevelTransaction(adapter, warehouseId, districtId);
                default -> new NewOrderTransaction(adapter, warehouseId, districtId);
            };

            long startTime = System.nanoTime();
            boolean success = tx.execute();
            long latency = System.nanoTime() - startTime;

            metricsRegistry.recordTransaction(tx.getName(), success, latency);

            // Think time
            if (benchConfig.isThinkTime()) {
                try {
                    Thread.sleep(random.nextInt(100) + 50);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }

    private void collectAndBroadcastMetrics() {
        try {
            Map<String, Object> dbMetrics = adapter.collectMetrics();
            Map<String, Object> osMetrics = osMetricsCollector.collect();
            Map<String, Object> hostMetrics = adapter.collectHostMetrics();
            metricsRegistry.takeSnapshot(dbMetrics, osMetrics);

            if (metricsCallback != null) {
                Map<String, Object> allMetrics = new LinkedHashMap<>();
                allMetrics.put("transaction", metricsRegistry.getCurrentMetrics());
                allMetrics.put("database", dbMetrics);
                allMetrics.put("os", osMetrics);
                allMetrics.put("dbHost", hostMetrics);
                allMetrics.put("status", status);
                metricsCallback.accept(allMetrics);
            }
        } catch (Exception e) {
            log.warn("Error collecting metrics: {}", e.getMessage());
        }
    }

    public void stop() {
        if (!running.get()) {
            return;
        }
        running.set(false);
        status = "STOPPING";
        metricsRegistry.markEnd();

        addLog("INFO", "Stopping benchmark...");

        if (executorService != null) {
            executorService.shutdownNow();
            try {
                executorService.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        if (metricsScheduler != null) {
            metricsScheduler.shutdownNow();
        }

        status = "STOPPED";
        addLog("INFO", "Benchmark stopped");

        // Log final results
        Map<String, Object> metrics = metricsRegistry.getCurrentMetrics();
        addLog("INFO", String.format("Final Results: TPS=%.2f, Total=%d, Success=%.2f%%, AvgLatency=%.2fms",
                metrics.get("tps"), metrics.get("totalTransactions"),
                metrics.get("overallSuccessRate"), metrics.get("avgLatencyMs")));

        // Final metrics broadcast
        collectAndBroadcastMetrics();

        // Broadcast status change
        broadcastStatusChange("STOPPED");
    }

    public void shutdown() {
        stop();
        if (adapter != null) {
            adapter.close();
        }
        status = "SHUTDOWN";
    }

    public boolean isRunning() {
        return running.get();
    }

    public boolean isLoading() {
        return loading.get();
    }

    public Map<String, Object> getResults() {
        Map<String, Object> results = new LinkedHashMap<>();
        results.put("status", status);
        results.put("metrics", metricsRegistry.getCurrentMetrics());
        return results;
    }

    public Map<String, Object> getConfig() {
        Map<String, Object> config = new LinkedHashMap<>();

        // Database config
        Map<String, Object> db = new LinkedHashMap<>();
        db.put("type", dbConfig.getType());
        db.put("jdbcUrl", dbConfig.getJdbcUrl());
        db.put("username", dbConfig.getUsername());
        db.put("poolSize", dbConfig.getPool().getSize());
        config.put("database", db);

        // Benchmark config
        Map<String, Object> bench = new LinkedHashMap<>();
        bench.put("warehouses", benchConfig.getWarehouses());
        bench.put("terminals", benchConfig.getTerminals());
        bench.put("duration", benchConfig.getDuration());
        bench.put("rampup", benchConfig.getRampup());
        bench.put("thinkTime", benchConfig.isThinkTime());
        bench.put("loadConcurrency", benchConfig.getLoadConcurrency());
        config.put("benchmark", bench);

        // Transaction mix
        Map<String, Object> mix = new LinkedHashMap<>();
        mix.put("newOrder", benchConfig.getMix().getNewOrder());
        mix.put("payment", benchConfig.getMix().getPayment());
        mix.put("orderStatus", benchConfig.getMix().getOrderStatus());
        mix.put("delivery", benchConfig.getMix().getDelivery());
        mix.put("stockLevel", benchConfig.getMix().getStockLevel());
        config.put("transactionMix", mix);

        return config;
    }
}
