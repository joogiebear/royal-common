package com.mystipixel.royalcommon.storage;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.SQLException;
import java.time.Duration;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

// Two PlayerHandoff objects with different server ids on one SQLite file stand in for two servers.
class PlayerHandoffTest {

    @TempDir
    Path dir;

    private HikariDataSource pool;
    private TxRunner db;
    private PlayerHandoff lobby;
    private PlayerHandoff survival;
    private final UUID alice = UUID.randomUUID();

    @BeforeEach
    void setUp() throws SQLException {
        pool = TestDb.sqlite(dir.resolve("test.db"));
        db = new TxRunner(pool);
        lobby = new PlayerHandoff(db, "royaltest", "lobby", Duration.ofMinutes(1));
        survival = new PlayerHandoff(db, "royaltest", "survival", Duration.ofMinutes(1));
        lobby.createTable();
        survival.createTable();
    }

    @AfterEach
    void tearDown() {
        pool.close();
    }

    @Test
    void tableNameUsesThePrefix() {
        assertEquals("royaltest_player_session", lobby.table());
    }

    @Test
    void followsAPlayerFromServerToServer() throws SQLException {
        assertEquals(PlayerHandoff.Status.NO_RECORD, survival.check(alice).status());

        lobby.markActive(alice);
        assertEquals(PlayerHandoff.Status.THIS_SERVER, lobby.check(alice).status());
        PlayerHandoff.Result busy = survival.check(alice);
        assertEquals(PlayerHandoff.Status.ACTIVE_ELSEWHERE, busy.status());
        assertEquals("lobby", busy.holder());
        assertFalse(busy.ready());

        assertTrue(lobby.markSaved(alice));
        PlayerHandoff.Result saved = survival.check(alice);
        assertEquals(PlayerHandoff.Status.SAVED, saved.status());
        assertTrue(saved.ready());

        survival.markActive(alice);
        assertEquals(PlayerHandoff.Status.ACTIVE_ELSEWHERE, lobby.check(alice).status());
        assertFalse(lobby.markSaved(alice), "a late save from the old server must not mark the new one's row");
        assertEquals(PlayerHandoff.Status.ACTIVE_ELSEWHERE, lobby.check(alice).status());
    }

    @Test
    void rowsOfAServerThatStoppedHeartbeatingGoStale() throws Exception {
        PlayerHandoff crashed = new PlayerHandoff(db, "royaltest", "crashed", Duration.ofMillis(300));
        PlayerHandoff waiting = new PlayerHandoff(db, "royaltest", "waiting", Duration.ofMillis(300));
        crashed.markActive(alice);
        assertEquals(PlayerHandoff.Status.ACTIVE_ELSEWHERE, waiting.check(alice).status());

        Thread.sleep(200);
        assertEquals(1, crashed.heartbeat());
        Thread.sleep(200);
        assertEquals(PlayerHandoff.Status.ACTIVE_ELSEWHERE, waiting.check(alice).status(), "heartbeat kept it fresh");

        Thread.sleep(250);
        assertEquals(PlayerHandoff.Status.STALE, waiting.check(alice).status());
    }

    @Test
    void sqliteNeverWaits() throws SQLException {
        lobby.markActive(alice);
        long start = System.nanoTime();
        PlayerHandoff.Result result = survival.awaitSaved(alice, Duration.ofSeconds(10));
        assertEquals(PlayerHandoff.Status.SINGLE_SERVER, result.status());
        assertTrue(result.ready());
        assertTrue(System.nanoTime() - start < Duration.ofSeconds(1).toNanos());
    }
}
