package com.mystipixel.royalcommon.storage;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TxRunnerTest {

    @TempDir
    Path dir;

    private final List<HikariDataSource> pools = new ArrayList<>();
    private TxRunner db;

    @BeforeEach
    void setUp() throws SQLException {
        db = new TxRunner(open(dir.resolve("test.db"), 5000));
        TestDb.exec(db, "CREATE TABLE counter (id TEXT PRIMARY KEY, n INTEGER NOT NULL)");
        TestDb.exec(db, "INSERT INTO counter VALUES ('a', 0), ('b', 0), ('c', 0)");
    }

    @AfterEach
    void tearDown() {
        pools.forEach(HikariDataSource::close);
    }

    private HikariDataSource open(Path file, int busyTimeoutMs) {
        HikariDataSource pool = TestDb.sqlite(file, busyTimeoutMs);
        pools.add(pool);
        return pool;
    }

    @Test
    void detectsSqlite() {
        assertEquals(Dialect.SQLITE, db.dialect());
    }

    @Test
    void databaseClockIsEpochMillis() throws SQLException {
        long now = TestDb.queryLong(db, "SELECT " + Dialect.SQLITE.nowMillis());
        assertTrue(Math.abs(now - System.currentTimeMillis()) < 5000, "db now " + now);
    }

    @Test
    void commitsWork() throws SQLException {
        db.inTransaction(tx -> increment(tx, "a"));
        assertEquals(1, TestDb.queryLong(db, "SELECT n FROM counter WHERE id = 'a'"));
    }

    @Test
    void rollsBackWhenWorkThrows() throws SQLException {
        assertThrows(IllegalStateException.class, () -> db.inTransaction(tx -> {
            increment(tx, "a");
            throw new IllegalStateException("boom");
        }));
        assertEquals(0, TestDb.queryLong(db, "SELECT n FROM counter WHERE id = 'a'"));
        // the connection is usable again afterwards
        db.inTransaction(tx -> increment(tx, "a"));
        assertEquals(1, TestDb.queryLong(db, "SELECT n FROM counter WHERE id = 'a'"));
    }

    @Test
    void retriesBusyAndGivesUpOnOtherErrors() throws SQLException {
        AtomicInteger calls = new AtomicInteger();
        String result = db.inTransaction(tx -> {
            if (calls.incrementAndGet() < 3) {
                throw new SQLException("[SQLITE_BUSY] The database file is locked", null, 5);
            }
            return "done";
        });
        assertEquals("done", result);
        assertEquals(3, calls.get());
        assertEquals(2, db.retries());

        calls.set(0);
        assertThrows(SQLException.class, () -> db.inTransaction(tx -> {
            calls.incrementAndGet();
            throw new SQLException("syntax error", null, 1);
        }));
        assertEquals(1, calls.get());
    }

    @Test
    void stopsAfterMaxAttempts() throws SQLException {
        TxRunner twice = new TxRunner(db.dataSource(), 2);
        AtomicInteger calls = new AtomicInteger();
        assertThrows(SQLException.class, () -> twice.inTransaction(tx -> {
            calls.incrementAndGet();
            throw new SQLException("busy", null, 5);
        }));
        assertEquals(2, calls.get());
    }

    @Test
    void recognisesMysqlDeadlockAndLockWaitTimeout() {
        assertTrue(TxRunner.isRetryable(Dialect.MYSQL, new SQLException("deadlock", "40001", 1213)));
        assertTrue(TxRunner.isRetryable(Dialect.MYSQL, new SQLException("lock wait", "HY000", 1205)));
        assertTrue(TxRunner.isRetryable(Dialect.MYSQL, new SQLException("serialization", "40001", 0)));
        assertTrue(TxRunner.isRetryable(Dialect.MYSQL,
                new SQLException("wrapped", "HY000", 0, new SQLException("deadlock", "40001", 1213))));
        assertFalse(TxRunner.isRetryable(Dialect.MYSQL, new SQLException("duplicate", "23000", 1062)));
        assertFalse(TxRunner.isRetryable(Dialect.SQLITE, new SQLException("deadlock", "40001", 1213)));
        assertTrue(TxRunner.isRetryable(Dialect.SQLITE, new SQLException("busy snapshot", null, 517)));
    }

    @Test
    void lockAllLocksInSortedOrderAndReportsExistingRows() throws SQLException {
        Set<String> found = db.inTransaction(tx -> tx.lockAll("counter", "id", List.of("c", "missing", "a")));
        assertEquals(List.of("a", "c"), new ArrayList<>(found));
    }

    @Test
    void rejectsIdentifiersThatAreNotPlain() {
        assertThrows(IllegalArgumentException.class,
                () -> db.inTransaction(tx -> tx.lock("counter; DROP TABLE counter", "id", "a")));
    }

    // Two pools on one file with no busy timeout behave like two processes: BEGIN IMMEDIATE collides
    // constantly and only the retry loop gets every increment through.
    @Test
    void concurrentWritersFromTwoPoolsLoseNothing() throws Exception {
        TxRunner one = new TxRunner(open(dir.resolve("test.db"), 0), 200);
        TxRunner two = new TxRunner(open(dir.resolve("test.db"), 0), 200);
        int perThread = 200;
        List<Throwable> failures = new ArrayList<>();
        Thread[] threads = {incrementer(one, perThread, failures), incrementer(two, perThread, failures)};
        for (Thread thread : threads) {
            thread.start();
        }
        for (Thread thread : threads) {
            thread.join();
        }
        assertTrue(failures.isEmpty(), "failures: " + failures);
        assertEquals(2L * perThread, TestDb.queryLong(db, "SELECT n FROM counter WHERE id = 'a'"));
        assertTrue(one.retries() + two.retries() > 0, "expected SQLITE_BUSY retries with busy_timeout=0");
    }

    private static Thread incrementer(TxRunner runner, int times, List<Throwable> failures) {
        return new Thread(() -> {
            try {
                for (int i = 0; i < times; i++) {
                    runner.inTransaction(tx -> increment(tx, "a"));
                }
            } catch (Throwable t) {
                synchronized (failures) {
                    failures.add(t);
                }
            }
        });
    }

    // read, then write the read value + 1: loses updates unless the row is locked for the whole transaction
    static Void increment(Tx tx, String id) throws SQLException {
        tx.lock("counter", "id", id);
        long n;
        try (PreparedStatement read = tx.connection().prepareStatement("SELECT n FROM counter WHERE id = ?")) {
            read.setString(1, id);
            try (ResultSet row = read.executeQuery()) {
                row.next();
                n = row.getLong(1);
            }
        }
        try (PreparedStatement write = tx.connection().prepareStatement("UPDATE counter SET n = ? WHERE id = ?")) {
            write.setLong(1, n + 1);
            write.setString(2, id);
            write.executeUpdate();
        }
        return null;
    }
}
