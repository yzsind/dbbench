package com.ninedata.dbbench.metrics;

import com.jcraft.jsch.*;
import com.ninedata.dbbench.config.DatabaseConfig;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.*;

@Slf4j
public class SshMetricsCollector {

    private static final String COLLECT_CMD =
            "cat /proc/stat /proc/meminfo /proc/diskstats /proc/net/dev 2>/dev/null";

    private final DatabaseConfig.SshConfig sshConfig;
    private final String effectiveHost;
    private Session session;

    // Previous values for delta-based rate calculation
    private long prevTimestamp = 0;
    private long[] prevCpuTimes = null;
    private final Map<String, long[]> prevDiskStats = new HashMap<>();
    private final Map<String, long[]> prevNetStats = new HashMap<>();

    public SshMetricsCollector(DatabaseConfig.SshConfig sshConfig, String effectiveHost) {
        this.sshConfig = sshConfig;
        this.effectiveHost = effectiveHost;
    }

    public synchronized void connect() throws JSchException {
        if (session != null && session.isConnected()) {
            return;
        }

        JSch jsch = new JSch();

        // Add private key if configured
        String privateKey = sshConfig.getPrivateKey();
        if (privateKey != null && !privateKey.isBlank()) {
            String passphrase = sshConfig.getPassphrase();
            if (passphrase != null && !passphrase.isBlank()) {
                jsch.addIdentity(privateKey, passphrase);
            } else {
                jsch.addIdentity(privateKey);
            }
        }
        String host = effectiveHost;
        int port = sshConfig.getPort();
        String username = sshConfig.getUsername();

        log.info("Connecting SSH to {}@{}:{}", username, host, port);

        session = jsch.getSession(username, host, port);

        // Password auth
        String password = sshConfig.getPassword();
        if (password != null && !password.isBlank()) {
            session.setPassword(password);
        }

        // Disable strict host key checking for benchmark tool
        Properties config = new Properties();
        config.put("StrictHostKeyChecking", "no");
        session.setConfig(config);

        session.setServerAliveInterval(30000);
        session.setServerAliveCountMax(3);
        session.setTimeout(10000);
        session.connect(10000);

        log.info("SSH connected to {}@{}:{}", username, host, port);
    }

    public synchronized void disconnect() {
        if (session != null) {
            try {
                session.disconnect();
            } catch (Exception e) {
                log.debug("Error disconnecting SSH: {}", e.getMessage());
            }
            session = null;
        }
        prevTimestamp = 0;
        prevCpuTimes = null;
        prevDiskStats.clear();
        prevNetStats.clear();
    }

    public synchronized boolean isConnected() {
        return session != null && session.isConnected();
    }

    /**
     * Collect metrics via SSH. Returns a map with CPU, memory, disk I/O, and network I/O metrics.
     * First call stores baseline and returns zeros for rate-based metrics.
     */
    public synchronized Map<String, Object> collect() {
        Map<String, Object> metrics = new LinkedHashMap<>();
        if (!isConnected()) {
            // Try to reconnect
            try {
                connect();
            } catch (Exception e) {
                log.warn("SSH reconnect failed: {}", e.getMessage());
                return metrics;
            }
        }

        try {
            String output = executeCommand(COLLECT_CMD);
            if (output == null || output.isEmpty()) {
                return metrics;
            }

            long now = System.currentTimeMillis();
            parseProcStat(output, metrics, now);
            parseProcMeminfo(output, metrics);
            parseProcDiskstats(output, metrics, now);
            parseProcNetDev(output, metrics, now);
            prevTimestamp = now;

            return metrics;
        } catch (Exception e) {
            log.warn("SSH metrics collection failed: {}", e.getMessage());
            // Session may be broken, disconnect so next call reconnects
            disconnect();
            return metrics;
        }
    }

    private String executeCommand(String command) throws JSchException, java.io.IOException {
        ChannelExec channel = (ChannelExec) session.openChannel("exec");
        channel.setCommand(command);
        channel.setInputStream(null);
        channel.setErrStream(System.err);

        StringBuilder sb = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(channel.getInputStream(), StandardCharsets.UTF_8))) {
            channel.connect(5000);
            String line;
            while ((line = reader.readLine()) != null) {
                sb.append(line).append('\n');
            }
        } finally {
            channel.disconnect();
        }
        return sb.toString();
    }

    // ==================== /proc/stat parser ====================

    private void parseProcStat(String output, Map<String, Object> metrics, long now) {
        // Find "cpu " line (aggregate)
        for (String line : output.split("\n")) {
            if (line.startsWith("cpu ")) {
                String[] parts = line.trim().split("\\s+");
                if (parts.length >= 8) {
                    // user, nice, system, idle, iowait, irq, softirq
                    long user = Long.parseLong(parts[1]);
                    long nice = Long.parseLong(parts[2]);
                    long system = Long.parseLong(parts[3]);
                    long idle = Long.parseLong(parts[4]);
                    long iowait = parts.length > 5 ? Long.parseLong(parts[5]) : 0;
                    long irq = parts.length > 6 ? Long.parseLong(parts[6]) : 0;
                    long softirq = parts.length > 7 ? Long.parseLong(parts[7]) : 0;

                    long[] current = {user, nice, system, idle, iowait, irq, softirq};

                    if (prevCpuTimes != null) {
                        long totalDelta = 0;
                        long idleDelta = 0;
                        for (int i = 0; i < current.length; i++) {
                            totalDelta += current[i] - prevCpuTimes[i];
                        }
                        idleDelta = (current[3] - prevCpuTimes[3]) + (current[4] - prevCpuTimes[4]);

                        if (totalDelta > 0) {
                            double cpuUsage = 100.0 * (totalDelta - idleDelta) / totalDelta;
                            metrics.put("cpuUsage", Math.round(cpuUsage * 100.0) / 100.0);
                        }
                    }
                    prevCpuTimes = current;
                }
                break;
            }
        }
    }

    // ==================== /proc/meminfo parser ====================

    private void parseProcMeminfo(String output, Map<String, Object> metrics) {
        long memTotal = 0, memAvailable = 0, memFree = 0, buffers = 0, cached = 0;

        for (String line : output.split("\n")) {
            if (line.startsWith("MemTotal:")) {
                memTotal = parseKbValue(line);
            } else if (line.startsWith("MemAvailable:")) {
                memAvailable = parseKbValue(line);
            } else if (line.startsWith("MemFree:")) {
                memFree = parseKbValue(line);
            } else if (line.startsWith("Buffers:")) {
                buffers = parseKbValue(line);
            } else if (line.startsWith("Cached:")) {
                cached = parseKbValue(line);
            }
        }

        if (memTotal > 0) {
            // Use MemAvailable if present (Linux 3.14+), otherwise estimate
            long available = memAvailable > 0 ? memAvailable : (memFree + buffers + cached);
            long used = memTotal - available;
            double usagePercent = 100.0 * used / memTotal;

            metrics.put("memoryUsage", Math.round(usagePercent * 100.0) / 100.0);
            metrics.put("memoryUsedMB", used / 1024);
            metrics.put("memoryTotalMB", memTotal / 1024);
        }
    }

    private long parseKbValue(String line) {
        // Format: "MemTotal:       16384000 kB"
        String[] parts = line.split("\\s+");
        if (parts.length >= 2) {
            try {
                return Long.parseLong(parts[1]);
            } catch (NumberFormatException e) {
                return 0;
            }
        }
        return 0;
    }

    // ==================== /proc/diskstats parser ====================

    private static final Set<String> DISK_PREFIXES = Set.of(
            "sd", "vd", "xvd", "nvme", "hd", "dm-"
    );

    private boolean isPhysicalDisk(String name) {
        // Filter to physical devices only (no partitions like sda1)
        for (String prefix : DISK_PREFIXES) {
            if (name.startsWith(prefix)) {
                // For sd/vd/hd: accept "sda" but not "sda1"
                if (prefix.equals("dm-")) return true;
                String suffix = name.substring(prefix.length());
                if (prefix.equals("nvme")) {
                    // nvme0n1 is a device, nvme0n1p1 is a partition
                    return !suffix.contains("p");
                }
                // sda, vda, etc. - no trailing digits means whole disk
                return !suffix.isEmpty() && suffix.chars().allMatch(Character::isLetter);
            }
        }
        return false;
    }

    private void parseProcDiskstats(String output, Map<String, Object> metrics, long now) {
        long totalReadBytes = 0;
        long totalWriteBytes = 0;

        for (String line : output.split("\n")) {
            String trimmed = line.trim();
            // diskstats format: major minor name reads_completed ... sectors_read ... writes_completed ... sectors_written
            String[] parts = trimmed.split("\\s+");
            if (parts.length >= 10) {
                String name = parts[2];
                if (isPhysicalDisk(name)) {
                    try {
                        long sectorsRead = Long.parseLong(parts[5]);
                        long sectorsWritten = Long.parseLong(parts[9]);
                        // Sector size is typically 512 bytes
                        totalReadBytes += sectorsRead * 512;
                        totalWriteBytes += sectorsWritten * 512;
                    } catch (NumberFormatException ignored) {}
                }
            }
        }

        if (totalReadBytes > 0 || totalWriteBytes > 0) {
            long[] current = {totalReadBytes, totalWriteBytes};
            long[] prev = prevDiskStats.get("_total");

            if (prev != null && prevTimestamp > 0) {
                double elapsed = (now - prevTimestamp) / 1000.0;
                if (elapsed > 0) {
                    metrics.put("diskReadBytesPerSec", Math.max(0, (current[0] - prev[0]) / elapsed));
                    metrics.put("diskWriteBytesPerSec", Math.max(0, (current[1] - prev[1]) / elapsed));
                }
            }
            prevDiskStats.put("_total", current);
        }
    }

    // ==================== /proc/net/dev parser ====================

    private static final Set<String> IGNORED_INTERFACES = Set.of("lo", "docker0", "br-", "veth");

    private boolean isRealInterface(String name) {
        for (String ignored : IGNORED_INTERFACES) {
            if (name.startsWith(ignored)) return false;
        }
        return true;
    }

    private void parseProcNetDev(String output, Map<String, Object> metrics, long now) {
        long totalRecv = 0;
        long totalSent = 0;

        for (String line : output.split("\n")) {
            String trimmed = line.trim();
            // Format: "eth0: recv_bytes packets ... sent_bytes packets ..."
            int colonIdx = trimmed.indexOf(':');
            if (colonIdx > 0) {
                String iface = trimmed.substring(0, colonIdx).trim();
                if (isRealInterface(iface)) {
                    String[] parts = trimmed.substring(colonIdx + 1).trim().split("\\s+");
                    if (parts.length >= 9) {
                        try {
                            totalRecv += Long.parseLong(parts[0]);
                            totalSent += Long.parseLong(parts[8]);
                        } catch (NumberFormatException ignored) {}
                    }
                }
            }
        }

        if (totalRecv > 0 || totalSent > 0) {
            long[] current = {totalRecv, totalSent};
            long[] prev = prevNetStats.get("_total");

            if (prev != null && prevTimestamp > 0) {
                double elapsed = (now - prevTimestamp) / 1000.0;
                if (elapsed > 0) {
                    metrics.put("networkRecvBytesPerSec", Math.max(0, (current[0] - prev[0]) / elapsed));
                    metrics.put("networkSentBytesPerSec", Math.max(0, (current[1] - prev[1]) / elapsed));
                }
            }
            prevNetStats.put("_total", current);
        }
    }
}
