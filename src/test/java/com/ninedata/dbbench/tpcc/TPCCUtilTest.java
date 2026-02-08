package com.ninedata.dbbench.tpcc;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.RepeatedTest;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("TPCCUtil Tests")
class TPCCUtilTest {

    @Test
    @DisplayName("Should have correct TPC-C constants")
    void testConstants() {
        assertEquals(100000, TPCCUtil.ITEMS);
        assertEquals(10, TPCCUtil.DISTRICTS_PER_WAREHOUSE);
        assertEquals(3000, TPCCUtil.CUSTOMERS_PER_DISTRICT);
        assertEquals(3000, TPCCUtil.ORDERS_PER_DISTRICT);
    }

    @Test
    @DisplayName("Should generate valid last name for 0")
    void testGenerateLastNameZero() {
        String lastName = TPCCUtil.generateLastName(0);
        assertEquals("BARBARBAR", lastName);
    }

    @Test
    @DisplayName("Should generate valid last name for 999")
    void testGenerateLastNameMax() {
        String lastName = TPCCUtil.generateLastName(999);
        // 999 = 9/9/9 -> EING + EING + EING
        assertEquals("EINGEINGEING", lastName);
    }

    @Test
    @DisplayName("Should generate valid last name for middle value")
    void testGenerateLastNameMiddle() {
        String lastName = TPCCUtil.generateLastName(123);
        // 123 = 1/2/3 -> OUGHT + ABLE + PRI
        assertEquals("OUGHTABLEPRI", lastName);
    }

    @Test
    @DisplayName("Should generate last name with correct syllables")
    void testGenerateLastNameSyllables() {
        // Test specific combinations
        assertEquals("BARBARBAR", TPCCUtil.generateLastName(0));   // 0/0/0
        assertEquals("BAROUGHTOUGHT", TPCCUtil.generateLastName(11)); // 0/1/1
        assertEquals("OUGHTBARBAR", TPCCUtil.generateLastName(100)); // 1/0/0
    }

    @RepeatedTest(100)
    @DisplayName("Should generate NURand within valid range for A=255")
    void testNURand255() {
        int result = TPCCUtil.NURand(255, 1, 3000);
        assertTrue(result >= 1 && result <= 3000,
            "NURand(255, 1, 3000) should be between 1 and 3000, got: " + result);
    }

    @RepeatedTest(100)
    @DisplayName("Should generate NURand within valid range for A=1023")
    void testNURand1023() {
        int result = TPCCUtil.NURand(1023, 1, 100000);
        assertTrue(result >= 1 && result <= 100000,
            "NURand(1023, 1, 100000) should be between 1 and 100000, got: " + result);
    }

    @RepeatedTest(100)
    @DisplayName("Should generate NURand within valid range for A=8191")
    void testNURand8191() {
        int result = TPCCUtil.NURand(8191, 1, 3000);
        assertTrue(result >= 1 && result <= 3000,
            "NURand(8191, 1, 3000) should be between 1 and 3000, got: " + result);
    }

    @Test
    @DisplayName("Should generate random string with correct length range")
    void testRandomString() {
        for (int i = 0; i < 100; i++) {
            String result = TPCCUtil.randomString(10, 20);
            assertTrue(result.length() >= 10 && result.length() <= 20,
                "Random string length should be between 10 and 20, got: " + result.length());
            assertTrue(result.matches("[a-z]+"),
                "Random string should only contain lowercase letters");
        }
    }

    @Test
    @DisplayName("Should generate random string with exact length when min equals max")
    void testRandomStringExactLength() {
        String result = TPCCUtil.randomString(15, 15);
        assertEquals(15, result.length());
    }

    @Test
    @DisplayName("Should generate random numeric string with correct length")
    void testRandomNumericString() {
        for (int i = 0; i < 100; i++) {
            String result = TPCCUtil.randomNumericString(16);
            assertEquals(16, result.length());
            assertTrue(result.matches("[0-9]+"),
                "Random numeric string should only contain digits");
        }
    }

    @RepeatedTest(100)
    @DisplayName("Should generate random double within range")
    void testRandomDouble() {
        double result = TPCCUtil.randomDouble(1.0, 100.0);
        assertTrue(result >= 1.0 && result <= 100.0,
            "Random double should be between 1.0 and 100.0, got: " + result);
    }

    @Test
    @DisplayName("Should generate random double with exact value when min equals max")
    void testRandomDoubleExact() {
        double result = TPCCUtil.randomDouble(50.0, 50.0);
        assertEquals(50.0, result, 0.0001);
    }

    @RepeatedTest(100)
    @DisplayName("Should generate random int within range")
    void testRandomInt() {
        int result = TPCCUtil.randomInt(1, 10);
        assertTrue(result >= 1 && result <= 10,
            "Random int should be between 1 and 10, got: " + result);
    }

    @Test
    @DisplayName("Should generate random int with exact value when min equals max")
    void testRandomIntExact() {
        int result = TPCCUtil.randomInt(5, 5);
        assertEquals(5, result);
    }

    @Test
    @DisplayName("Should generate valid zip code format")
    void testRandomZip() {
        for (int i = 0; i < 100; i++) {
            String zip = TPCCUtil.randomZip();
            assertEquals(9, zip.length(), "Zip code should be 9 characters");
            assertTrue(zip.endsWith("11111"), "Zip code should end with 11111");
            assertTrue(zip.substring(0, 4).matches("[0-9]+"),
                "First 4 characters should be digits");
        }
    }

    @Test
    @DisplayName("Should generate different random strings")
    void testRandomStringVariety() {
        String first = TPCCUtil.randomString(10, 20);
        boolean foundDifferent = false;
        for (int i = 0; i < 100; i++) {
            if (!TPCCUtil.randomString(10, 20).equals(first)) {
                foundDifferent = true;
                break;
            }
        }
        assertTrue(foundDifferent, "Should generate different random strings");
    }

    @Test
    @DisplayName("Should generate different NURand values")
    void testNURandVariety() {
        int first = TPCCUtil.NURand(255, 1, 3000);
        boolean foundDifferent = false;
        for (int i = 0; i < 100; i++) {
            if (TPCCUtil.NURand(255, 1, 3000) != first) {
                foundDifferent = true;
                break;
            }
        }
        assertTrue(foundDifferent, "Should generate different NURand values");
    }

    @Test
    @DisplayName("Should handle edge case for randomInt with negative range")
    void testRandomIntNegativeRange() {
        int result = TPCCUtil.randomInt(-10, -1);
        assertTrue(result >= -10 && result <= -1,
            "Random int should be between -10 and -1, got: " + result);
    }

    @Test
    @DisplayName("Should handle edge case for randomDouble with negative range")
    void testRandomDoubleNegativeRange() {
        double result = TPCCUtil.randomDouble(-100.0, -1.0);
        assertTrue(result >= -100.0 && result <= -1.0,
            "Random double should be between -100.0 and -1.0, got: " + result);
    }
}
