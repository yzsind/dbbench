package com.ninedata.dbbench.tpcc;

import java.util.Random;

public class TPCCUtil {
    public static final int ITEMS = 100000;
    public static final int DISTRICTS_PER_WAREHOUSE = 10;
    public static final int CUSTOMERS_PER_DISTRICT = 3000;
    public static final int ORDERS_PER_DISTRICT = 3000;

    private static final String[] SYLLABLES = {
        "BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING"
    };

    private static final Random random = new Random();

    public static String generateLastName(int num) {
        return SYLLABLES[num / 100] + SYLLABLES[(num / 10) % 10] + SYLLABLES[num % 10];
    }

    public static int NURand(int A, int x, int y) {
        int C;
        switch (A) {
            case 255 -> C = random.nextInt(256);
            case 1023 -> C = random.nextInt(1024);
            case 8191 -> C = random.nextInt(8192);
            default -> C = 0;
        }
        return (((random.nextInt(A + 1) | (random.nextInt(y - x + 1) + x)) + C) % (y - x + 1)) + x;
    }

    public static String randomString(int minLen, int maxLen) {
        int len = minLen + random.nextInt(maxLen - minLen + 1);
        StringBuilder sb = new StringBuilder(len);
        for (int i = 0; i < len; i++) {
            sb.append((char) ('a' + random.nextInt(26)));
        }
        return sb.toString();
    }

    public static String randomNumericString(int len) {
        StringBuilder sb = new StringBuilder(len);
        for (int i = 0; i < len; i++) {
            sb.append((char) ('0' + random.nextInt(10)));
        }
        return sb.toString();
    }

    public static double randomDouble(double min, double max) {
        return min + (max - min) * random.nextDouble();
    }

    public static int randomInt(int min, int max) {
        return min + random.nextInt(max - min + 1);
    }

    public static String randomZip() {
        return randomNumericString(4) + "11111";
    }
}
