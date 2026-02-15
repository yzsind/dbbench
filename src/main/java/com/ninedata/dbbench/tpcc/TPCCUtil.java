package com.ninedata.dbbench.tpcc;

import java.util.concurrent.ThreadLocalRandom;

public class TPCCUtil {
    public static final int ITEMS = 100000;
    public static final int DISTRICTS_PER_WAREHOUSE = 10;
    public static final int CUSTOMERS_PER_DISTRICT = 3000;
    public static final int ORDERS_PER_DISTRICT = 3000;

    private static final String[] SYLLABLES = {
        "BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING"
    };

    public static String generateLastName(int num) {
        return SYLLABLES[num / 100] + SYLLABLES[(num / 10) % 10] + SYLLABLES[num % 10];
    }

    public static int NURand(int A, int x, int y) {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        int C;
        switch (A) {
            case 255 -> C = rnd.nextInt(256);
            case 1023 -> C = rnd.nextInt(1024);
            case 8191 -> C = rnd.nextInt(8192);
            default -> C = 0;
        }
        return (((rnd.nextInt(A + 1) | (rnd.nextInt(y - x + 1) + x)) + C) % (y - x + 1)) + x;
    }

    public static String randomString(int minLen, int maxLen) {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        int len = minLen + rnd.nextInt(maxLen - minLen + 1);
        byte[] bytes = new byte[len];
        rnd.nextBytes(bytes);
        for (int i = 0; i < len; i++) {
            bytes[i] = (byte) ('a' + (bytes[i] & 0x7F) % 26);
        }
        return new String(bytes);
    }

    public static String randomNumericString(int len) {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        byte[] bytes = new byte[len];
        rnd.nextBytes(bytes);
        for (int i = 0; i < len; i++) {
            bytes[i] = (byte) ('0' + (bytes[i] & 0x7F) % 10);
        }
        return new String(bytes);
    }

    public static double randomDouble(double min, double max) {
        return min + (max - min) * ThreadLocalRandom.current().nextDouble();
    }

    public static int randomInt(int min, int max) {
        return min + ThreadLocalRandom.current().nextInt(max - min + 1);
    }

    public static String randomZip() {
        return randomNumericString(4) + "11111";
    }
}
