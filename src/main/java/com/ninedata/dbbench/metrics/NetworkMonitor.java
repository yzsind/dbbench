package com.ninedata.dbbench.metrics;

/**
 * @author zhengsheng.ye
 * @date 2026/2/7
 */
import oshi.SystemInfo;
import oshi.hardware.HardwareAbstractionLayer;
import oshi.hardware.NetworkIF;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class NetworkMonitor {

    public static void main(String[] args) throws InterruptedException {
        SystemInfo si = new SystemInfo();
        HardwareAbstractionLayer hal = si.getHardware();

        // 1. 获取初始数据
        List<NetworkIF> networkIFs = hal.getNetworkIFs();

        System.out.println("开始监控网络流量 (每秒刷新)...");

        while (true) {
            // 记录当前时间
            long timeStamp = System.currentTimeMillis();

            for (NetworkIF net : networkIFs) {
                // 排除没有 MAC 地址的虚拟网卡或未启动的网卡
                if (net.getMacaddr().equals("00:00:00:00:00:00") || net.getBytesRecv() == 0) {
                    continue;
                }

                // 保存当前总量
                long oldRecv = net.getBytesRecv();
                long oldSent = net.getBytesSent();

                // 等待 1 秒进行采样
                TimeUnit.SECONDS.sleep(1);

                // 更新网卡统计信息（必须 update 才能拿到新数据）
                net.updateAttributes();

                // 计算差值
                long bytesRecv = net.getBytesRecv() - oldRecv;
                long bytesSent = net.getBytesSent() - oldSent;

                // 打印结果 (转换为 KB/s)
                System.out.printf("网卡: %-10s | 下行: %8.2f KB/s | 上行: %8.2f KB/s%n",
                        net.getName(),
                        bytesRecv / 1024.0,
                        bytesSent / 1024.0);
            }
            System.out.println("---------------------------------------------------------");
        }
    }
}