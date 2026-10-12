package com.mystipixel.royalcommon.storage;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.SQLException;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LeaseTest {

    @TempDir
    Path dir;

    private HikariDataSource pool;
    private Lease lobby;
    private Lease survival;

    @BeforeEach
    void setUp() throws SQLException {
        pool = TestDb.sqlite(dir.resolve("test.db"));
        TxRunner db = new TxRunner(pool);
        lobby = new Lease(db, "royaltest", "lobby");
        survival = new Lease(db, "royaltest", "survival");
        lobby.createTable();
    }

    @AfterEach
    void tearDown() {
        pool.close();
    }

    @Test
    void oneHolderUntilItExpires() throws Exception {
        Duration ttl = Duration.ofMillis(300);
        assertTrue(lobby.tryAcquireOrRenew("interest", ttl));
        assertFalse(survival.tryAcquireOrRenew("interest", ttl));
        assertTrue(lobby.tryAcquireOrRenew("interest", ttl), "holder renews");
        assertTrue(lobby.isHolder("interest"));
        assertFalse(survival.isHolder("interest"));

        Thread.sleep(400);
        assertFalse(lobby.isHolder("interest"));
        assertTrue(survival.tryAcquireOrRenew("interest", ttl), "taken over after expiry");
        assertFalse(lobby.tryAcquireOrRenew("interest", ttl));
    }

    @Test
    void leasesAreIndependentByName() throws SQLException {
        assertTrue(lobby.tryAcquireOrRenew("interest", Duration.ofMinutes(1)));
        assertTrue(survival.tryAcquireOrRenew("auctions", Duration.ofMinutes(1)));
    }

    @Test
    void releaseFreesItAtOnce() throws SQLException {
        assertTrue(lobby.tryAcquireOrRenew("interest", Duration.ofMinutes(1)));
        survival.release("interest");
        assertFalse(survival.tryAcquireOrRenew("interest", Duration.ofMinutes(1)), "only the holder can release");
        lobby.release("interest");
        assertTrue(survival.tryAcquireOrRenew("interest", Duration.ofMinutes(1)));
    }

    @Test
    void keeperRunsTheJobOnlyWhileHeld() throws SQLException {
        AtomicInteger lobbyRuns = new AtomicInteger();
        AtomicInteger survivalRuns = new AtomicInteger();
        Logger logger = Logger.getLogger("LeaseTest");
        Lease.Keeper lobbyKeeper = lobby.keep("interest", Duration.ofMinutes(1), lobbyRuns::incrementAndGet, logger);
        Lease.Keeper survivalKeeper = survival.keep("interest", Duration.ofMinutes(1), survivalRuns::incrementAndGet, logger);

        lobbyKeeper.tick();
        survivalKeeper.tick();
        lobbyKeeper.tick();

        assertEquals(2, lobbyRuns.get());
        assertEquals(0, survivalRuns.get());
        assertTrue(lobbyKeeper.isHeld());
        assertFalse(survivalKeeper.isHeld());

        lobby.release("interest");
        survivalKeeper.tick();
        assertEquals(1, survivalRuns.get());
    }
}
