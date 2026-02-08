package com.ninedata.dbbench.config;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("BenchmarkConfig Tests")
class BenchmarkConfigTest {

    private BenchmarkConfig config;

    @BeforeEach
    void setUp() {
        config = new BenchmarkConfig();
    }

    @Test
    @DisplayName("Should have correct default values")
    void testDefaultValues() {
        assertEquals(10, config.getWarehouses());
        assertEquals(50, config.getTerminals());
        assertEquals(60, config.getDuration());
        assertEquals(10, config.getRampup());
        assertTrue(config.isThinkTime());
        assertEquals(4, config.getLoadConcurrency());
    }

    @Test
    @DisplayName("Should have correct default transaction mix")
    void testDefaultTransactionMix() {
        BenchmarkConfig.MixConfig mix = config.getMix();

        assertNotNull(mix);
        assertEquals(45, mix.getNewOrder());
        assertEquals(43, mix.getPayment());
        assertEquals(4, mix.getOrderStatus());
        assertEquals(4, mix.getDelivery());
        assertEquals(4, mix.getStockLevel());
    }

    @Test
    @DisplayName("Transaction mix should sum to 100")
    void testTransactionMixSum() {
        BenchmarkConfig.MixConfig mix = config.getMix();

        int total = mix.getNewOrder() + mix.getPayment() + mix.getOrderStatus()
                + mix.getDelivery() + mix.getStockLevel();

        assertEquals(100, total);
    }

    @Test
    @DisplayName("Should allow setting all properties")
    void testSetProperties() {
        config.setWarehouses(20);
        config.setTerminals(100);
        config.setDuration(120);
        config.setRampup(20);
        config.setThinkTime(false);
        config.setLoadConcurrency(8);

        assertEquals(20, config.getWarehouses());
        assertEquals(100, config.getTerminals());
        assertEquals(120, config.getDuration());
        assertEquals(20, config.getRampup());
        assertFalse(config.isThinkTime());
        assertEquals(8, config.getLoadConcurrency());
    }

    @Test
    @DisplayName("Should allow setting transaction mix")
    void testSetTransactionMix() {
        BenchmarkConfig.MixConfig mix = config.getMix();

        mix.setNewOrder(50);
        mix.setPayment(40);
        mix.setOrderStatus(3);
        mix.setDelivery(3);
        mix.setStockLevel(4);

        assertEquals(50, mix.getNewOrder());
        assertEquals(40, mix.getPayment());
        assertEquals(3, mix.getOrderStatus());
        assertEquals(3, mix.getDelivery());
        assertEquals(4, mix.getStockLevel());
    }

    @Test
    @DisplayName("Should have non-null mix config by default")
    void testMixConfigNotNull() {
        assertNotNull(config.getMix());
    }

    @Test
    @DisplayName("Should allow zero values for mix")
    void testZeroMixValues() {
        BenchmarkConfig.MixConfig mix = config.getMix();

        mix.setNewOrder(100);
        mix.setPayment(0);
        mix.setOrderStatus(0);
        mix.setDelivery(0);
        mix.setStockLevel(0);

        assertEquals(100, mix.getNewOrder());
        assertEquals(0, mix.getPayment());
    }

    @Test
    @DisplayName("Should allow minimum warehouse count")
    void testMinimumWarehouses() {
        config.setWarehouses(1);
        assertEquals(1, config.getWarehouses());
    }

    @Test
    @DisplayName("Should allow minimum terminal count")
    void testMinimumTerminals() {
        config.setTerminals(1);
        assertEquals(1, config.getTerminals());
    }

    @Test
    @DisplayName("Should allow large warehouse count")
    void testLargeWarehouses() {
        config.setWarehouses(1000);
        assertEquals(1000, config.getWarehouses());
    }

    @Test
    @DisplayName("Should allow large terminal count")
    void testLargeTerminals() {
        config.setTerminals(500);
        assertEquals(500, config.getTerminals());
    }
}
