/**
 * DBBench Dashboard Application
 * TPC-C Database Benchmark Tool - Web Interface
 */

// Global state
let ws = null;
let tpsChart = null;
let cpuChart = null;
let networkChart = null;
let dbCpuChart = null;
let dbDiskChart = null;
let dbConnChart = null;
const maxDataPoints = 60;
let currentConfig = null;
let allLogs = [];
let statusPollInterval = null;
let lastStatus = null;
// For calculating disk I/O rate
let lastDiskReadBytes = 0;
let lastDiskWriteBytes = 0;
let lastDiskTime = Date.now();

// DOM Ready
document.addEventListener('DOMContentLoaded', () => {
    initCharts();
    connectWebSocket();
    loadInitialState();
    setupEventListeners();
    startStatusPolling();
});

// ==================== Initial State Loading ====================

async function loadInitialState() {
    try {
        // Load config, status, metrics, and logs in parallel
        const [configRes, metricsRes, logsRes] = await Promise.all([
            fetch('/api/benchmark/config'),
            fetch('/api/metrics/current'),
            fetch('/api/benchmark/logs?limit=100')
        ]);

        const config = await configRes.json();
        const metrics = await metricsRes.json();
        const logs = await logsRes.json();

        // Apply config
        currentConfig = config;
        displayConfig(config);

        // Apply status
        updateStatus(metrics.status);
        lastStatus = metrics.status;

        // Apply metrics
        updateMetrics(metrics);

        // If running or has data, restore chart from history
        if (metrics.status === 'RUNNING' || metrics.status === 'STOPPED') {
            await restoreChartHistory();
        }

        // If loading, show progress
        if (metrics.loading) {
            updateLoadProgress(metrics.loadProgress || 0, metrics.loadMessage || 'Loading...');
        }

        // Apply logs
        allLogs = logs;
        const logEl = document.getElementById('log');
        logEl.innerHTML = '';
        logs.slice(-50).forEach(log => appendLogEntry(log));

        addLog('Dashboard initialized', 'success');
    } catch (e) {
        console.error('Failed to load initial state:', e);
        addLog('Failed to load initial state: ' + e.message, 'error');
    }
}

async function restoreChartHistory() {
    try {
        const res = await fetch('/api/metrics/tps-history?limit=60');
        const history = await res.json();

        if (history && history.length > 0) {
            tpsChart.data.labels = [];
            tpsChart.data.datasets[0].data = [];

            history.forEach(point => {
                const time = new Date(point.timestamp).toLocaleTimeString();
                tpsChart.data.labels.push(time);
                tpsChart.data.datasets[0].data.push(point.tps || 0);
            });

            tpsChart.update();
            addLog(`Restored ${history.length} chart data points`, 'info');
        }
    } catch (e) {
        console.error('Failed to restore chart history:', e);
    }
}

// ==================== Status Polling ====================

function startStatusPolling() {
    // Poll status every 2 seconds as a fallback when WebSocket is disconnected
    statusPollInterval = setInterval(async () => {
        // Only poll if WebSocket is not connected
        if (ws && ws.readyState === WebSocket.OPEN) {
            return;
        }

        try {
            const res = await fetch('/api/metrics/current');
            const data = await res.json();
            updateMetrics(data);

            // Check for status changes
            if (data.status !== lastStatus) {
                lastStatus = data.status;
                addLog(`Status changed to: ${data.status}`, 'info');
            }
        } catch (e) {
            console.error('Status poll failed:', e);
        }
    }, 2000);
}

// ==================== Charts ====================

function initCharts() {
    // TPS Chart
    const tpsCtx = document.getElementById('tpsChart').getContext('2d');
    tpsChart = new Chart(tpsCtx, {
        type: 'line',
        data: {
            labels: [],
            datasets: [{
                label: 'TPS',
                data: [],
                borderColor: '#00d9ff',
                backgroundColor: 'rgba(0, 217, 255, 0.1)',
                fill: true,
                tension: 0.4,
                pointRadius: 2,
                pointHoverRadius: 4
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            animation: { duration: 0 },
            plugins: { legend: { display: false } },
            scales: {
                x: { grid: { color: '#333' }, ticks: { color: '#888', maxTicksLimit: 10 } },
                y: { grid: { color: '#333' }, ticks: { color: '#888' }, beginAtZero: true, suggestedMax: 10 }
            }
        }
    });

    // CPU Chart
    const cpuCtx = document.getElementById('cpuChart').getContext('2d');
    cpuChart = new Chart(cpuCtx, {
        type: 'line',
        data: {
            labels: [],
            datasets: [{
                label: 'CPU %',
                data: [],
                borderColor: '#ff6b6b',
                backgroundColor: 'rgba(255, 107, 107, 0.1)',
                fill: true,
                tension: 0.4,
                pointRadius: 2,
                pointHoverRadius: 4
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            animation: { duration: 0 },
            plugins: { legend: { display: false } },
            scales: {
                x: { grid: { color: '#333' }, ticks: { color: '#888', maxTicksLimit: 10 } },
                y: { grid: { color: '#333' }, ticks: { color: '#888' }, beginAtZero: true, max: 100 }
            }
        }
    });

    // Network Chart
    const netCtx = document.getElementById('networkChart').getContext('2d');
    networkChart = new Chart(netCtx, {
        type: 'line',
        data: {
            labels: [],
            datasets: [
                {
                    label: 'Recv',
                    data: [],
                    borderColor: '#00ff88',
                    backgroundColor: 'rgba(0, 255, 136, 0.1)',
                    fill: false,
                    tension: 0.4,
                    pointRadius: 2,
                    pointHoverRadius: 4
                },
                {
                    label: 'Sent',
                    data: [],
                    borderColor: '#ffa500',
                    backgroundColor: 'rgba(255, 165, 0, 0.1)',
                    fill: false,
                    tension: 0.4,
                    pointRadius: 2,
                    pointHoverRadius: 4
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            animation: { duration: 0 },
            plugins: {
                legend: {
                    display: true,
                    position: 'top',
                    labels: { color: '#888', boxWidth: 12, padding: 10 }
                }
            },
            scales: {
                x: { grid: { color: '#333' }, ticks: { color: '#888', maxTicksLimit: 10 } },
                y: {
                    grid: { color: '#333' },
                    ticks: {
                        color: '#888',
                        callback: function(value) { return formatBytesShort(value) + '/s'; }
                    },
                    beginAtZero: true
                }
            }
        }
    });

    // Database CPU Chart
    const dbCpuCtx = document.getElementById('dbCpuChart').getContext('2d');
    dbCpuChart = new Chart(dbCpuCtx, {
        type: 'line',
        data: {
            labels: [],
            datasets: [{
                label: 'DB CPU %',
                data: [],
                borderColor: '#ff6b6b',
                backgroundColor: 'rgba(255, 107, 107, 0.1)',
                fill: true,
                tension: 0.4,
                pointRadius: 2,
                pointHoverRadius: 4
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            animation: { duration: 0 },
            plugins: { legend: { display: false } },
            scales: {
                x: { grid: { color: '#333' }, ticks: { color: '#888', maxTicksLimit: 10 } },
                y: { grid: { color: '#333' }, ticks: { color: '#888' }, beginAtZero: true, max: 100 }
            }
        }
    });

    // Database Disk I/O Chart
    const dbDiskCtx = document.getElementById('dbDiskChart').getContext('2d');
    dbDiskChart = new Chart(dbDiskCtx, {
        type: 'line',
        data: {
            labels: [],
            datasets: [
                {
                    label: 'Read',
                    data: [],
                    borderColor: '#3498db',
                    backgroundColor: 'rgba(52, 152, 219, 0.1)',
                    fill: false,
                    tension: 0.4,
                    pointRadius: 2,
                    pointHoverRadius: 4
                },
                {
                    label: 'Write',
                    data: [],
                    borderColor: '#e74c3c',
                    backgroundColor: 'rgba(231, 76, 60, 0.1)',
                    fill: false,
                    tension: 0.4,
                    pointRadius: 2,
                    pointHoverRadius: 4
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            animation: { duration: 0 },
            plugins: {
                legend: {
                    display: true,
                    position: 'top',
                    labels: { color: '#888', boxWidth: 12, padding: 10 }
                }
            },
            scales: {
                x: { grid: { color: '#333' }, ticks: { color: '#888', maxTicksLimit: 10 } },
                y: {
                    grid: { color: '#333' },
                    ticks: {
                        color: '#888',
                        callback: function(value) { return formatBytesShort(value) + '/s'; }
                    },
                    beginAtZero: true
                }
            }
        }
    });

    // Database Connections & Locks Chart
    const dbConnCtx = document.getElementById('dbConnChart').getContext('2d');
    dbConnChart = new Chart(dbConnCtx, {
        type: 'line',
        data: {
            labels: [],
            datasets: [
                {
                    label: 'Connections',
                    data: [],
                    borderColor: '#00d9ff',
                    backgroundColor: 'rgba(0, 217, 255, 0.1)',
                    fill: false,
                    tension: 0.4,
                    pointRadius: 2,
                    pointHoverRadius: 4,
                    yAxisID: 'y'
                },
                {
                    label: 'Lock Waits',
                    data: [],
                    borderColor: '#ff4757',
                    backgroundColor: 'rgba(255, 71, 87, 0.1)',
                    fill: false,
                    tension: 0.4,
                    pointRadius: 2,
                    pointHoverRadius: 4,
                    yAxisID: 'y1'
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            animation: { duration: 0 },
            plugins: {
                legend: {
                    display: true,
                    position: 'top',
                    labels: { color: '#888', boxWidth: 12, padding: 10 }
                }
            },
            scales: {
                x: { grid: { color: '#333' }, ticks: { color: '#888', maxTicksLimit: 10 } },
                y: {
                    type: 'linear',
                    display: true,
                    position: 'left',
                    grid: { color: '#333' },
                    ticks: { color: '#00d9ff' },
                    beginAtZero: true,
                    title: { display: true, text: 'Connections', color: '#00d9ff' }
                },
                y1: {
                    type: 'linear',
                    display: true,
                    position: 'right',
                    grid: { drawOnChartArea: false },
                    ticks: { color: '#ff4757' },
                    beginAtZero: true,
                    title: { display: true, text: 'Lock Waits', color: '#ff4757' }
                }
            }
        }
    });
}

// ==================== WebSocket ====================

function connectWebSocket() {
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    ws = new WebSocket(`${protocol}//${window.location.host}/ws/metrics`);

    ws.onopen = () => {
        document.getElementById('wsStatus').className = 'ws-status ws-connected';
        addLog('WebSocket connected', 'success');
    };

    ws.onclose = () => {
        document.getElementById('wsStatus').className = 'ws-status ws-disconnected';
        addLog('WebSocket disconnected, reconnecting...', 'warn');
        setTimeout(connectWebSocket, 3000);
    };

    ws.onerror = () => {
        console.error('WebSocket error');
    };

    ws.onmessage = (event) => {
        try {
            const data = JSON.parse(event.data);
            handleWebSocketMessage(data);
        } catch (e) {
            console.error('Failed to parse WebSocket message:', e);
        }
    };
}

function handleWebSocketMessage(data) {
    // Handle log messages
    if (data.type === 'log' && data.log) {
        const log = data.log;
        allLogs.push(log);
        if (allLogs.length > 1000) allLogs.shift();
        appendLogEntry(log);
        return;
    }

    // Handle progress updates
    if (data.type === 'progress') {
        updateLoadProgress(data.progress, data.message);
        if (data.status) {
            updateStatus(data.status);
            lastStatus = data.status;
        }
        return;
    }

    // Handle status change notifications
    if (data.type === 'status') {
        console.log('Status change received:', data.status);
        updateStatus(data.status);
        lastStatus = data.status;
        return;
    }

    // Handle metrics updates
    updateMetrics(data);
}

// ==================== Metrics Update ====================

function updateMetrics(data) {
    if (data.transaction) {
        const tx = data.transaction;
        document.getElementById('tps').textContent = tx.tps?.toFixed(2) || '0.00';
        document.getElementById('totalTx').textContent = tx.totalTransactions || 0;
        document.getElementById('successRate').textContent = (tx.overallSuccessRate || 0).toFixed(1) + '%';
        document.getElementById('avgLatency').textContent = (tx.avgLatencyMs?.toFixed(2) || '0.00') + ' ms';
        document.getElementById('elapsed').textContent = (tx.elapsedSeconds || 0) + 's';

        // Update chart - only when benchmark is running
        if (data.status === 'RUNNING' && tx.tps !== undefined) {
            const now = new Date().toLocaleTimeString();
            tpsChart.data.labels.push(now);
            tpsChart.data.datasets[0].data.push(tx.tps || 0);

            // Keep max 60 data points
            if (tpsChart.data.labels.length > maxDataPoints) {
                tpsChart.data.labels.shift();
                tpsChart.data.datasets[0].data.shift();
            }

            tpsChart.update();
        }

        // Update transaction table
        if (tx.transactions && tx.transactions.length > 0) {
            const tbody = document.getElementById('txTable');
            tbody.innerHTML = tx.transactions.map(t => `
                <tr>
                    <td>${t.name}</td>
                    <td>${t.count}</td>
                    <td style="color: #00ff88">${t.success}</td>
                    <td style="color: #ff4757">${t.failure}</td>
                    <td>${t.successRate?.toFixed(1) || 0}%</td>
                    <td>${t.avgLatencyMs?.toFixed(2) || 0} ms</td>
                </tr>
            `).join('');
        }
    }

    if (data.os) {
        const os = data.os;
        document.getElementById('cpuUsage').textContent = (os.cpuUsage || 0).toFixed(1) + '%';
        document.getElementById('cpuBar').style.width = Math.min(os.cpuUsage || 0, 100) + '%';
        document.getElementById('memUsage').textContent = (os.memoryUsage || 0).toFixed(1) + '%';
        document.getElementById('memBar').style.width = Math.min(os.memoryUsage || 0, 100) + '%';
        document.getElementById('loadAvg').textContent = os.loadAvg1?.toFixed(2) || '0.00';

        // Always update CPU and Network charts for continuous monitoring
        const now = new Date().toLocaleTimeString();

        // CPU chart
        cpuChart.data.labels.push(now);
        cpuChart.data.datasets[0].data.push(os.cpuUsage || 0);
        if (cpuChart.data.labels.length > maxDataPoints) {
            cpuChart.data.labels.shift();
            cpuChart.data.datasets[0].data.shift();
        }
        cpuChart.update();

        // Network chart
        networkChart.data.labels.push(now);
        networkChart.data.datasets[0].data.push(os.networkRecvBytesPerSec || 0);
        networkChart.data.datasets[1].data.push(os.networkSentBytesPerSec || 0);
        if (networkChart.data.labels.length > maxDataPoints) {
            networkChart.data.labels.shift();
            networkChart.data.datasets[0].data.shift();
            networkChart.data.datasets[1].data.shift();
        }
        networkChart.update();
    }

    if (data.database) {
        const db = data.database;
        document.getElementById('dbConnections').textContent = db.active_connections || db.activeConnections || 0;
        document.getElementById('bufferHit').textContent = (db.buffer_pool_hit_ratio || db.cache_hit_ratio || 0).toFixed(1) + '%';
        document.getElementById('lockWaits').textContent = db.row_lock_waits || db.waiting_locks || db.lock_waits || 0;
        document.getElementById('slowQueries').textContent = db.slow_queries || 0;

        // Update database connections & locks chart
        const now = new Date().toLocaleTimeString();
        const connections = db.active_connections || db.activeConnections || 0;
        const lockWaits = db.row_lock_waits || db.waiting_locks || db.lock_waits || 0;

        dbConnChart.data.labels.push(now);
        dbConnChart.data.datasets[0].data.push(connections);
        dbConnChart.data.datasets[1].data.push(lockWaits);

        if (dbConnChart.data.labels.length > maxDataPoints) {
            dbConnChart.data.labels.shift();
            dbConnChart.data.datasets[0].data.shift();
            dbConnChart.data.datasets[1].data.shift();
        }
        dbConnChart.update();
    }

    // Database Host Metrics - Update Charts
    if (data.dbHost) {
        const host = data.dbHost;
        const now = new Date().toLocaleTimeString();

        // CPU Chart
        if (host.cpuUsage !== undefined) {
            dbCpuChart.data.labels.push(now);
            dbCpuChart.data.datasets[0].data.push(host.cpuUsage);
            if (dbCpuChart.data.labels.length > maxDataPoints) {
                dbCpuChart.data.labels.shift();
                dbCpuChart.data.datasets[0].data.shift();
            }
            dbCpuChart.update();
        }

        // Disk I/O Chart - Calculate rate (bytes per second)
        if (host.diskReadBytes !== undefined && host.diskWriteBytes !== undefined) {
            const currentTime = Date.now();
            const timeDiff = (currentTime - lastDiskTime) / 1000; // seconds

            if (lastDiskReadBytes > 0 && timeDiff > 0) {
                const readRate = Math.max(0, (host.diskReadBytes - lastDiskReadBytes) / timeDiff);
                const writeRate = Math.max(0, (host.diskWriteBytes - lastDiskWriteBytes) / timeDiff);

                dbDiskChart.data.labels.push(now);
                dbDiskChart.data.datasets[0].data.push(readRate);
                dbDiskChart.data.datasets[1].data.push(writeRate);

                if (dbDiskChart.data.labels.length > maxDataPoints) {
                    dbDiskChart.data.labels.shift();
                    dbDiskChart.data.datasets[0].data.shift();
                    dbDiskChart.data.datasets[1].data.shift();
                }
                dbDiskChart.update();
            }

            lastDiskReadBytes = host.diskReadBytes;
            lastDiskWriteBytes = host.diskWriteBytes;
            lastDiskTime = currentTime;
        }
    }

    if (data.status) {
        updateStatus(data.status);
        lastStatus = data.status;
    }

    // Handle loading progress from metrics endpoint
    if (data.loading && data.loadProgress !== undefined) {
        updateLoadProgress(data.loadProgress, data.loadMessage || 'Loading...');
    }
}

// ==================== Status ====================

function updateStatus(status) {
    const el = document.getElementById('status');
    const oldStatus = el.textContent;
    el.textContent = status;
    el.className = 'status status-' + status.toLowerCase();

    // Detect status changes and show notifications
    if (oldStatus && oldStatus !== status && oldStatus !== 'IDLE') {
        handleStatusChange(oldStatus, status);
    }

    const isRunning = status === 'RUNNING';
    const isLoading = status === 'LOADING';
    const isStopping = status === 'STOPPING';
    const canConfig = !isRunning && !isLoading && !isStopping;

    document.getElementById('btnStart').disabled = isRunning || isLoading;
    document.getElementById('btnStop').disabled = !isRunning;
    document.getElementById('btnLoad').disabled = isRunning || isLoading;
    document.getElementById('btnClean').disabled = isRunning || isLoading;
    document.getElementById('btnConfig').disabled = !canConfig;

    // Show/hide progress container
    const progressContainer = document.getElementById('loadProgressContainer');
    if (isLoading) {
        progressContainer.classList.add('active');
        document.getElementById('btnCancelLoad').disabled = false;
    } else if (status === 'LOADED' || status === 'ERROR' || status === 'INITIALIZED' || status === 'CANCELLED') {
        // Hide after a delay
        setTimeout(() => {
            const currentStatus = document.getElementById('status').textContent;
            if (currentStatus !== 'LOADING') {
                progressContainer.classList.remove('active');
            }
        }, 3000);
    }
}

// ==================== Configuration ====================

function displayConfig(cfg) {
    if (cfg.database) {
        document.getElementById('cfgDbType').textContent = cfg.database.type?.toUpperCase() || '-';
        document.getElementById('cfgJdbcUrl').textContent = cfg.database.jdbcUrl || '-';
        document.getElementById('cfgPoolSize').textContent = cfg.database.poolSize || '-';
    }

    if (cfg.benchmark) {
        document.getElementById('cfgWarehouses').textContent = cfg.benchmark.warehouses || '-';
        document.getElementById('cfgTerminals').textContent = cfg.benchmark.terminals || '-';
        document.getElementById('cfgDuration').textContent = (cfg.benchmark.duration || '-') + 's';
        document.getElementById('cfgLoadConcurrency').textContent = cfg.benchmark.loadConcurrency || '-';
    }

    if (cfg.transactionMix) {
        document.getElementById('cfgMixNewOrder').textContent = cfg.transactionMix.newOrder + '%';
        document.getElementById('cfgMixPayment').textContent = cfg.transactionMix.payment + '%';
        document.getElementById('cfgMixOrderStatus').textContent = cfg.transactionMix.orderStatus + '%';
        document.getElementById('cfgMixDelivery').textContent = cfg.transactionMix.delivery + '%';
        document.getElementById('cfgMixStockLevel').textContent = cfg.transactionMix.stockLevel + '%';
    }
}

function openConfigModal() {
    if (!currentConfig) {
        addLog('Configuration not loaded yet', 'error');
        return;
    }

    // Populate form with current config
    const cfg = currentConfig;

    // Database config
    document.getElementById('cfgFormDbType').value = cfg.database?.type || 'mysql';
    document.getElementById('cfgFormJdbcUrl').value = cfg.database?.jdbcUrl || '';
    document.getElementById('cfgFormDbUser').value = cfg.database?.username || '';
    document.getElementById('cfgFormDbPass').value = '';  // Don't show password
    document.getElementById('cfgFormPoolSize').value = cfg.database?.poolSize || 50;

    // Benchmark config
    document.getElementById('cfgFormWarehouses').value = cfg.benchmark?.warehouses || 10;
    document.getElementById('cfgFormTerminals').value = cfg.benchmark?.terminals || 50;
    document.getElementById('cfgFormDuration').value = cfg.benchmark?.duration || 60;
    document.getElementById('cfgFormLoadConcurrency').value = cfg.benchmark?.loadConcurrency || 4;
    document.getElementById('cfgFormThinkTime').checked = cfg.benchmark?.thinkTime || false;

    // Transaction mix
    document.getElementById('cfgFormMixNewOrder').value = cfg.transactionMix?.newOrder || 45;
    document.getElementById('cfgFormMixPayment').value = cfg.transactionMix?.payment || 43;
    document.getElementById('cfgFormMixOrderStatus').value = cfg.transactionMix?.orderStatus || 4;
    document.getElementById('cfgFormMixDelivery').value = cfg.transactionMix?.delivery || 4;
    document.getElementById('cfgFormMixStockLevel').value = cfg.transactionMix?.stockLevel || 4;

    // Clear connection test result
    document.getElementById('connectionTestResult').innerHTML = '';

    openModal('configModal');
}

async function saveConfig() {
    const newConfig = {
        database: {
            type: document.getElementById('cfgFormDbType').value,
            jdbcUrl: document.getElementById('cfgFormJdbcUrl').value,
            username: document.getElementById('cfgFormDbUser').value,
            poolSize: parseInt(document.getElementById('cfgFormPoolSize').value)
        },
        benchmark: {
            warehouses: parseInt(document.getElementById('cfgFormWarehouses').value),
            terminals: parseInt(document.getElementById('cfgFormTerminals').value),
            duration: parseInt(document.getElementById('cfgFormDuration').value),
            loadConcurrency: parseInt(document.getElementById('cfgFormLoadConcurrency').value),
            thinkTime: document.getElementById('cfgFormThinkTime').checked
        },
        transactionMix: {
            newOrder: parseInt(document.getElementById('cfgFormMixNewOrder').value),
            payment: parseInt(document.getElementById('cfgFormMixPayment').value),
            orderStatus: parseInt(document.getElementById('cfgFormMixOrderStatus').value),
            delivery: parseInt(document.getElementById('cfgFormMixDelivery').value),
            stockLevel: parseInt(document.getElementById('cfgFormMixStockLevel').value)
        }
    };

    // Add password only if provided
    const password = document.getElementById('cfgFormDbPass').value;
    if (password) {
        newConfig.database.password = password;
    }

    // Validate transaction mix
    const mixTotal = newConfig.transactionMix.newOrder +
                     newConfig.transactionMix.payment +
                     newConfig.transactionMix.orderStatus +
                     newConfig.transactionMix.delivery +
                     newConfig.transactionMix.stockLevel;

    if (mixTotal !== 100) {
        showToast('error', 'Invalid Configuration', `Transaction mix must total 100% (currently ${mixTotal}%)`);
        return;
    }

    try {
        const res = await fetch('/api/benchmark/config', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(newConfig)
        });
        const data = await res.json();

        if (data.success) {
            currentConfig = data.config;
            displayConfig(currentConfig);
            closeModal('configModal');
            addLog('Configuration saved successfully', 'success');
            showToast('success', 'Configuration Saved', 'Benchmark configuration has been updated');

            // Clear connection test result
            document.getElementById('connectionTestResult').innerHTML = '';
        } else {
            addLog('Failed to save config: ' + data.error, 'error');
            showToast('error', 'Save Failed', data.error);
        }
    } catch (e) {
        addLog('Failed to save config: ' + e.message, 'error');
        showToast('error', 'Save Failed', e.message);
    }
}

// ==================== Load Progress ====================

function updateLoadProgress(progress, message) {
    const container = document.getElementById('loadProgressContainer');
    const bar = document.getElementById('loadProgressBar');
    const percent = document.getElementById('loadProgressPercent');
    const msg = document.getElementById('loadProgressMessage');

    container.classList.add('active');

    if (progress >= 0) {
        bar.style.width = progress + '%';
        percent.textContent = progress + '%';
    } else {
        bar.style.width = '0%';
        percent.textContent = 'Error';
    }

    msg.textContent = message || '';
}

// ==================== Logs ====================

function addLog(message, type = 'info') {
    const logEl = document.getElementById('log');
    const entry = document.createElement('div');
    entry.className = 'log-entry ' + type;
    const timestamp = new Date().toLocaleTimeString();
    entry.innerHTML = `<span class="log-timestamp">[${timestamp}]</span>${message}`;
    logEl.appendChild(entry);
    logEl.scrollTop = logEl.scrollHeight;

    // Keep log size manageable
    while (logEl.children.length > 100) {
        logEl.removeChild(logEl.firstChild);
    }
}

function appendLogEntry(log) {
    const logEl = document.getElementById('log');
    const entry = document.createElement('div');
    entry.className = 'log-entry ' + (log.level || 'info').toLowerCase();
    entry.innerHTML = `<span class="log-timestamp">[${log.timestamp || new Date().toLocaleTimeString()}]</span>${escapeHtml(log.message)}`;
    logEl.appendChild(entry);
    logEl.scrollTop = logEl.scrollHeight;

    while (logEl.children.length > 100) {
        logEl.removeChild(logEl.firstChild);
    }
}

async function openLogViewer() {
    // Load full log history
    try {
        const res = await fetch('/api/benchmark/logs?limit=1000');
        allLogs = await res.json();
    } catch (e) {
        console.error('Failed to load logs:', e);
    }

    renderLogViewer();
    openModal('logModal');
}

function renderLogViewer(filter = '', level = 'all') {
    const viewer = document.getElementById('logViewerContent');
    const filterLower = filter.toLowerCase();

    const filteredLogs = allLogs.filter(log => {
        const matchesText = !filter || log.message.toLowerCase().includes(filterLower);
        const matchesLevel = level === 'all' || log.level?.toLowerCase() === level;
        return matchesText && matchesLevel;
    });

    if (filteredLogs.length === 0) {
        viewer.innerHTML = '<div class="log-entry text-muted text-center" style="padding: 20px;">No logs found</div>';
        return;
    }

    viewer.innerHTML = filteredLogs.map(log => `
        <div class="log-entry ${(log.level || 'info').toLowerCase()}">
            <span class="log-timestamp">[${log.timestamp || '-'}]</span>
            <span class="log-level">[${log.level || 'INFO'}]</span>
            ${escapeHtml(log.message)}
        </div>
    `).join('');

    viewer.scrollTop = viewer.scrollHeight;
}

function filterLogs() {
    const filter = document.getElementById('logFilter').value;
    const level = document.querySelector('.log-filter-btn.active')?.dataset.level || 'all';
    renderLogViewer(filter, level);
}

function setLogLevel(btn, level) {
    document.querySelectorAll('.log-filter-btn').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
    filterLogs();
}

async function clearLogs() {
    if (!confirm('Are you sure you want to clear all logs?')) return;

    try {
        await fetch('/api/benchmark/logs', { method: 'DELETE' });
        allLogs = [];
        document.getElementById('log').innerHTML = '<div class="log-entry">Logs cleared</div>';
        renderLogViewer();
        addLog('Logs cleared', 'info');
    } catch (e) {
        addLog('Failed to clear logs: ' + e.message, 'error');
    }
}

// ==================== API Calls ====================

async function apiCall(endpoint, method = 'POST', body = null) {
    try {
        const options = { method };
        if (body) {
            options.headers = { 'Content-Type': 'application/json' };
            options.body = JSON.stringify(body);
        }

        const res = await fetch(`/api/benchmark/${endpoint}`, options);
        const data = await res.json();

        if (data.success) {
            addLog(data.message, 'success');
            if (data.status) {
                updateStatus(data.status);
                lastStatus = data.status;
            }
        } else {
            addLog('Error: ' + data.error, 'error');
            showToast('error', 'Operation Failed', data.error);
        }
        return data;
    } catch (e) {
        addLog('Request failed: ' + e.message, 'error');
        showToast('error', 'Request Failed', e.message);
        return { success: false, error: e.message };
    }
}

async function loadData() {
    addLog('Starting data load... this may take several minutes', 'info');
    showToast('info', 'Data Loading', 'Starting data load, this may take several minutes');
    document.getElementById('loadProgressContainer').classList.add('active');
    document.getElementById('btnCancelLoad').disabled = false;
    updateLoadProgress(0, 'Starting...');
    await apiCall('load');
}

async function cancelLoad() {
    if (!confirm('Are you sure you want to cancel data loading? Partial data may remain in the database.')) {
        return;
    }

    addLog('Cancelling data load...', 'warn');
    document.getElementById('btnCancelLoad').disabled = true;
    const result = await apiCall('load/cancel');
    if (result.success) {
        showToast('warning', 'Cancelling', 'Data loading is being cancelled...');
    }
}

async function cleanData() {
    if (!confirm('Are you sure you want to clean all test data? This will drop all TPC-C tables.')) {
        return;
    }

    addLog('Cleaning test data...', 'info');
    const result = await apiCall('clean');
    if (result.success) {
        showToast('success', 'Data Cleaned', 'All TPC-C tables have been dropped');
    }
}

async function startBenchmark() {
    // Clear TPS chart data for new run (keep CPU/Network for continuous monitoring)
    tpsChart.data.labels = [];
    tpsChart.data.datasets[0].data = [];
    tpsChart.update();

    // Clear database charts for new run
    dbCpuChart.data.labels = [];
    dbCpuChart.data.datasets[0].data = [];
    dbCpuChart.update();

    dbDiskChart.data.labels = [];
    dbDiskChart.data.datasets[0].data = [];
    dbDiskChart.data.datasets[1].data = [];
    dbDiskChart.update();

    dbConnChart.data.labels = [];
    dbConnChart.data.datasets[0].data = [];
    dbConnChart.data.datasets[1].data = [];
    dbConnChart.update();

    // Reset disk I/O tracking
    lastDiskReadBytes = 0;
    lastDiskWriteBytes = 0;
    lastDiskTime = Date.now();

    addLog('Starting benchmark...', 'info');
    const result = await apiCall('start');
    if (result.success) {
        showToast('success', 'Benchmark Started', 'Benchmark is now running');
    }
}

async function stopBenchmark() {
    addLog('Stopping benchmark...', 'info');
    const result = await apiCall('stop');
    if (result.success) {
        showToast('info', 'Benchmark Stopped', 'Benchmark has been stopped');
    }
}

// ==================== Modal ====================

function openModal(modalId) {
    document.getElementById(modalId).classList.add('active');
}

function closeModal(modalId) {
    document.getElementById(modalId).classList.remove('active');
}

// ==================== Event Listeners ====================

function setupEventListeners() {
    // Close modal on overlay click (except configModal)
    document.querySelectorAll('.modal-overlay').forEach(overlay => {
        overlay.addEventListener('click', (e) => {
            // Don't close configModal when clicking outside
            if (e.target === overlay && overlay.id !== 'configModal') {
                overlay.classList.remove('active');
            }
        });
    });

    // Close modal on Escape key (except configModal)
    document.addEventListener('keydown', (e) => {
        if (e.key === 'Escape') {
            document.querySelectorAll('.modal-overlay.active').forEach(modal => {
                // Don't close configModal with Escape
                if (modal.id !== 'configModal') {
                    modal.classList.remove('active');
                }
            });
        }
    });

    // Log filter input
    const logFilter = document.getElementById('logFilter');
    if (logFilter) {
        logFilter.addEventListener('input', filterLogs);
    }

    // Database type change - update JDBC URL template
    const dbTypeSelect = document.getElementById('cfgFormDbType');
    if (dbTypeSelect) {
        dbTypeSelect.addEventListener('change', onDbTypeChange);
    }

    // Handle page visibility change - refresh data when page becomes visible
    document.addEventListener('visibilitychange', () => {
        if (document.visibilityState === 'visible') {
            loadInitialState();
        }
    });
}

// ==================== JDBC URL Templates ====================

const jdbcUrlTemplates = {
    mysql: 'jdbc:mysql://127.0.0.1:3306/tpcc?useSSL=false&allowPublicKeyRetrieval=true&rewriteBatchedStatements=true',
    postgresql: 'jdbc:postgresql://127.0.0.1:5432/tpcc',
    oracle: 'jdbc:oracle:thin:@127.0.0.1:1521:orcl',
    sqlserver: 'jdbc:sqlserver://127.0.0.1:1433;databaseName=tpcc;encrypt=false',
    db2: 'jdbc:db2://127.0.0.1:50000/tpcc',
    dameng: 'jdbc:dm://127.0.0.1:5236/tpcc',
    oceanbase: 'jdbc:oceanbase://127.0.0.1:2881/tpcc',
    tidb: 'jdbc:mysql://127.0.0.1:4000/tpcc?useSSL=false&rewriteBatchedStatements=true'
};

function onDbTypeChange() {
    const dbType = document.getElementById('cfgFormDbType').value;
    const jdbcUrlInput = document.getElementById('cfgFormJdbcUrl');

    if (jdbcUrlTemplates[dbType]) {
        jdbcUrlInput.value = jdbcUrlTemplates[dbType];
    }

    // Clear connection test result when type changes
    document.getElementById('connectionTestResult').innerHTML = '';
}

// ==================== Utilities ====================

function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

function formatBytes(bytes) {
    if (bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}

function formatBytesShort(bytes) {
    if (bytes === 0) return '0';
    const k = 1024;
    const sizes = ['B', 'K', 'M', 'G', 'T'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + sizes[i];
}

function formatDuration(seconds) {
    if (seconds < 60) return seconds + 's';
    const mins = Math.floor(seconds / 60);
    const secs = seconds % 60;
    return mins + 'm ' + secs + 's';
}

// ==================== Toast Notifications ====================

function showToast(type, title, message, duration = 5000) {
    const container = document.getElementById('toastContainer');
    const toast = document.createElement('div');
    toast.className = `toast toast-${type}`;

    const icons = {
        success: '&#10004;',
        error: '&#10006;',
        warning: '&#9888;',
        info: '&#8505;'
    };

    toast.innerHTML = `
        <span class="toast-icon">${icons[type] || icons.info}</span>
        <div class="toast-content">
            <div class="toast-title">${escapeHtml(title)}</div>
            <div class="toast-message">${escapeHtml(message)}</div>
        </div>
        <button class="toast-close" onclick="this.parentElement.remove()">&times;</button>
    `;

    container.appendChild(toast);

    // Auto remove after duration
    if (duration > 0) {
        setTimeout(() => {
            toast.classList.add('hiding');
            setTimeout(() => toast.remove(), 300);
        }, duration);
    }

    return toast;
}

// ==================== Connection Test ====================

async function testConnection() {
    const btn = document.getElementById('btnTestConnection');
    const result = document.getElementById('connectionTestResult');

    // Get current form values
    const config = {
        database: {
            type: document.getElementById('cfgFormDbType').value,
            jdbcUrl: document.getElementById('cfgFormJdbcUrl').value,
            username: document.getElementById('cfgFormDbUser').value,
            password: document.getElementById('cfgFormDbPass').value
        }
    };

    // Show testing state
    btn.disabled = true;
    result.className = 'connection-test-result testing';
    result.innerHTML = '<span class="spinner"></span> Testing...';

    try {
        const res = await fetch('/api/benchmark/test-connection', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(config)
        });
        const data = await res.json();

        if (data.success) {
            result.className = 'connection-test-result success';
            result.innerHTML = `&#10004; ${data.message}`;
            showToast('success', 'Connection Successful', `Connected to ${data.database}`);
        } else {
            result.className = 'connection-test-result error';
            result.innerHTML = `&#10006; ${data.error}`;

            let suggestion = data.suggestion || 'Check your connection settings';
            showToast('error', 'Connection Failed', `${data.error}\n\nSuggestion: ${suggestion}`, 8000);
        }
    } catch (e) {
        result.className = 'connection-test-result error';
        result.innerHTML = `&#10006; ${e.message}`;
        showToast('error', 'Connection Test Failed', e.message);
    } finally {
        btn.disabled = false;
    }
}

// ==================== Status Change Detection ====================

function handleStatusChange(oldStatus, newStatus) {
    // Handle specific status transitions
    if (oldStatus === 'LOADING' && newStatus === 'LOADED') {
        showToast('success', 'Data Loaded', 'TPC-C data has been loaded successfully');
        document.getElementById('loadProgressContainer').classList.remove('active');
    } else if (oldStatus === 'LOADING' && newStatus === 'CANCELLED') {
        showToast('warning', 'Load Cancelled', 'Data loading was cancelled by user');
        document.getElementById('loadProgressContainer').classList.remove('active');
    } else if (oldStatus === 'LOADING' && newStatus === 'ERROR') {
        showToast('error', 'Load Failed', 'Data loading failed, check logs for details');
    } else if (oldStatus === 'RUNNING' && newStatus === 'STOPPED') {
        showToast('info', 'Benchmark Complete', 'Benchmark has finished running');
    } else if (newStatus === 'ERROR') {
        showToast('error', 'Error', 'An error occurred, check logs for details');
    }
}
