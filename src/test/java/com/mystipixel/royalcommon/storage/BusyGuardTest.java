package com.mystipixel.royalcommon.storage;

import org.junit.jupiter.api.Test;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BusyGuardTest {

    @Test
    void oneActionPerKey() {
        BusyGuard<UUID> busy = new BusyGuard<>();
        UUID alice = UUID.randomUUID();
        UUID bob = UUID.randomUUID();
        assertTrue(busy.tryEnter(alice));
        assertFalse(busy.tryEnter(alice));
        assertTrue(busy.tryEnter(bob));
        assertTrue(busy.isBusy(alice));
        busy.exit(alice);
        assertFalse(busy.isBusy(alice));
        assertTrue(busy.tryEnter(alice));
    }

    @Test
    void onlyOneOfManyThreadsGetsIn() throws Exception {
        BusyGuard<UUID> busy = new BusyGuard<>();
        UUID key = UUID.randomUUID();
        AtomicInteger winners = new AtomicInteger();
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(16);
        for (int i = 0; i < 16; i++) {
            pool.execute(() -> {
                try {
                    start.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                if (busy.tryEnter(key)) {
                    winners.incrementAndGet();
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS));
        assertEquals(1, winners.get());
    }
}
