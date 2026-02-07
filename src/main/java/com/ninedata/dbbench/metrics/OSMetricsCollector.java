package com.ninedata.dbbench.metrics;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import oshi.SystemInfo;
import oshi.hardware.*;
import oshi.software.os.OperatingSystem;

import jakarta.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Component
public class OSMetricsCollector {
    private final SystemInfo systemInfo;
    private final CentralProcessor processor;
    private final GlobalMemory memory;
    private final OperatingSystem os;
    private long[] prevTicks;

    // For calculating rates
    private long prevNetRecv = 0;
    private long prevNetSent = 0;
    private long prevDiskRead = 0;
    private long prevDiskWrite = 0;
    private long prevTimestamp = 0;

    // Cache the list of valid network interface names
    private List<String> validNetworkInterfaces = new ArrayList<>();

    public OSMetricsCollector() {
        this.systemInfo = new SystemInfo();
        this.processor = systemInfo.getHardware().getProcessor();
        this.memory = systemInfo.getHardware().getMemory();
        this.os = systemInfo.getOperatingSystem();
        this.prevTicks = processor.getSystemCpuLoadTicks();
        this.prevTimestamp = System.currentTimeMillis();
    }

    @PostConstruct
    public void init() {
        // Detect and cache valid network interfaces
        detectNetworkInterfaces();
        // Initialize previous values
        initPreviousValues();
    }

    private void detectNetworkInterfaces() {
        log.info("Detecting network interfaces...");
        for (NetworkIF net : systemInfo.getHardware().getNetworkIFs()) {
            String name = net.getName();
            String displayName = net.getDisplayName();
            String mac = net.getMacaddr();

            // Skip loopback
            if (name.equalsIgnoreCase("lo") || name.equalsIgnoreCase("lo0") ||
                displayName.toLowerCase().contains("loopback")) {
                log.debug("Skipping loopback interface: {}", name);
                continue;
            }

            // Skip interfaces with no MAC or all-zero MAC
            if (mac == null || mac.isEmpty() || mac.equals("00:00:00:00:00:00")) {
                log.debug("Skipping interface with no MAC: {}", name);
                continue;
            }

            // Skip common virtual interface prefixes
            String nameLower = name.toLowerCase();
            if (nameLower.startsWith("veth") || nameLower.startsWith("docker") ||
                nameLower.startsWith("br-") || nameLower.startsWith("virbr") ||
                nameLower.startsWith("vbox") || nameLower.startsWith("vmnet")) {
                log.debug("Skipping virtual interface: {}", name);
                continue;
            }

            // This is a valid interface
            validNetworkInterfaces.add(name);
            log.info("Using network interface: {} ({}), MAC: {}", name, displayName, mac);
        }

        // If no interfaces found, use all non-loopback interfaces
        if (validNetworkInterfaces.isEmpty()) {
            log.warn("No physical network interfaces detected, using all non-loopback interfaces");
            for (NetworkIF net : systemInfo.getHardware().getNetworkIFs()) {
                String name = net.getName();
                if (!name.equalsIgnoreCase("lo") && !name.equalsIgnoreCase("lo0")) {
                    validNetworkInterfaces.add(name);
                    log.info("Fallback: using interface {}", name);
                }
            }
        }
    }

    private void initPreviousValues() {
        try {
            for (NetworkIF net : systemInfo.getHardware().getNetworkIFs()) {
                if (validNetworkInterfaces.contains(net.getName())) {
                    net.updateAttributes();
                    prevNetRecv += net.getBytesRecv();
                    prevNetSent += net.getBytesSent();
                }
            }
            for (HWDiskStore disk : systemInfo.getHardware().getDiskStores()) {
                prevDiskRead += disk.getReadBytes();
                prevDiskWrite += disk.getWriteBytes();
            }
            log.info("Initial network stats - Recv: {} bytes, Sent: {} bytes", prevNetRecv, prevNetSent);
        } catch (Exception e) {
            log.warn("Error initializing previous values: {}", e.getMessage());
        }
    }

    public Map<String, Object> collect() {
        Map<String, Object> metrics = new LinkedHashMap<>();
        long currentTime = System.currentTimeMillis();
        double timeDeltaSec = (currentTime - prevTimestamp) / 1000.0;
        if (timeDeltaSec <= 0) timeDeltaSec = 1.0;

        try {
            // CPU
            double cpuLoad = processor.getSystemCpuLoadBetweenTicks(prevTicks) * 100;
            prevTicks = processor.getSystemCpuLoadTicks();
            metrics.put("cpuUsage", Math.round(cpuLoad * 100.0) / 100.0);
            metrics.put("cpuCores", processor.getLogicalProcessorCount());

            // Memory
            long totalMemory = memory.getTotal();
            long availableMemory = memory.getAvailable();
            long usedMemory = totalMemory - availableMemory;
            metrics.put("memoryTotal", totalMemory / (1024 * 1024));
            metrics.put("memoryUsed", usedMemory / (1024 * 1024));
            metrics.put("memoryFree", availableMemory / (1024 * 1024));
            metrics.put("memoryUsage", Math.round((usedMemory * 100.0 / totalMemory) * 100.0) / 100.0);

            // Swap
            VirtualMemory vm = memory.getVirtualMemory();
            metrics.put("swapTotal", vm.getSwapTotal() / (1024 * 1024));
            metrics.put("swapUsed", vm.getSwapUsed() / (1024 * 1024));

            // Load average
            double[] loadAverage = processor.getSystemLoadAverage(3);
            if (loadAverage[0] >= 0) {
                metrics.put("loadAvg1", Math.round(loadAverage[0] * 100.0) / 100.0);
                metrics.put("loadAvg5", Math.round(loadAverage[1] * 100.0) / 100.0);
                metrics.put("loadAvg15", Math.round(loadAverage[2] * 100.0) / 100.0);
            }

            // Process count
            metrics.put("processCount", os.getProcessCount());
            metrics.put("threadCount", os.getThreadCount());

            // Disk I/O
            long diskReads = 0, diskWrites = 0;
            for (HWDiskStore disk : systemInfo.getHardware().getDiskStores()) {
                diskReads += disk.getReadBytes();
                diskWrites += disk.getWriteBytes();
            }
            metrics.put("diskReadBytes", diskReads);
            metrics.put("diskWriteBytes", diskWrites);

            // Disk I/O rate (bytes per second)
            double diskReadRate = (diskReads - prevDiskRead) / timeDeltaSec;
            double diskWriteRate = (diskWrites - prevDiskWrite) / timeDeltaSec;
            metrics.put("diskReadBytesPerSec", Math.max(0, Math.round(diskReadRate)));
            metrics.put("diskWriteBytesPerSec", Math.max(0, Math.round(diskWriteRate)));
            prevDiskRead = diskReads;
            prevDiskWrite = diskWrites;

            // Network I/O - use cached valid interfaces
            long netRecv = 0, netSent = 0;
            for (NetworkIF net : systemInfo.getHardware().getNetworkIFs()) {
                //if (validNetworkInterfaces.contains(net.getName())) {
                    net.updateAttributes();
                    netRecv += net.getBytesRecv();
                    netSent += net.getBytesSent();
                //}
            }
            metrics.put("networkRecvBytes", netRecv);
            metrics.put("networkSentBytes", netSent);

            // Network I/O rate (bytes per second)
            long netRecvDelta = netRecv - prevNetRecv;
            long netSentDelta = netSent - prevNetSent;

            // Handle counter reset or first collection
            if (netRecvDelta < 0) netRecvDelta = 0;
            if (netSentDelta < 0) netSentDelta = 0;

            double netRecvRate = netRecvDelta / timeDeltaSec;
            double netSentRate = netSentDelta / timeDeltaSec;

            metrics.put("networkRecvBytesPerSec", Math.round(netRecvRate));
            metrics.put("networkSentBytesPerSec", Math.round(netSentRate));

            prevNetRecv = netRecv;
            prevNetSent = netSent;
            prevTimestamp = currentTime;

        } catch (Exception e) {
            log.warn("Error collecting OS metrics: {}", e.getMessage());
        }

        return metrics;
    }
}
