package com.mystipixel.royalcommon.storage;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Two independent "servers" (separate pools, runners and server ids) against one MySQL or MariaDB database.
 * Runs only when ROYALCOMMON_TEST_MYSQL_URL is set, e.g. jdbc:mysql://127.0.0.1:3306/royaltest, with
 * ROYALCOMMON_TEST_MYSQL_USER and ROYALCOMMON_TEST_MYSQL_PASSWORD. Tables get a random prefix and are dropped.
 */
@Tag("mysql")
@EnabledIfEnvironmentVariable(named = "ROYALCOMMON_TEST_MYSQL_URL", matches = ".+")
class MySqlTwoServersTest {

    private final String prefix = "rct" + UUID.randomUUID().toString().substring(0, 8);
    private final List<String> tables = new ArrayList<>();
    private HikariDataSource poolA;
    private HikariDataSource poolB;
    private TxRunner a;
    private TxRunner b;
    private ExecutorService threads;

    @BeforeEach
    void setUp() throws SQLException {
        String url = System.getenv("ROYALCOMMON_TEST_MYSQL_URL");
        String user = System.getenv().getOrDefault("ROYALCOMMON_TEST_MYSQL_USER", "root");
        String password = System.getenv().getOrDefault("ROYALCOMMON_TEST_MYSQL_PASSWORD", "");
        poolA = TestDb.mysql(url, user, password, 4);
        poolB = TestDb.mysql(url, user, password, 4);
        a = new TxRunner(poolA, 50);
        b = new TxRunner(poolB, 50);
        threads = Executors.newFixedThreadPool(4);
    }

    @AfterEach
    void tearDown() throws SQLException {
        threads.shutdownNow();
        for (String table : tables) {
            TestDb.exec(a, "DROP TABLE IF EXISTS " + table);
        }
        poolA.close();
        poolB.close();
    }

    private String table(String suffix) {
        String name = prefix + "_" + suffix;
        tables.add(name);
        return name;
    }

    // both callables start together, so the two servers really overlap
    private <T> List<T> together(Callable<T> first, Callable<T> second) throws Exception {
        CountDownLatch go = new CountDownLatch(1);
        Future<T> one = threads.submit(() -> {
            go.await();
            return first.call();
        });
        Future<T> two = threads.submit(() -> {
            go.await();
            return second.call();
        });
        go.countDown();
        return Arrays.asList(one.get(120, TimeUnit.SECONDS), two.get(120, TimeUnit.SECONDS));
    }

    @Test
    void detectsMysqlAndReadsTheDatabaseClock() throws SQLException {
        assertEquals(Dialect.MYSQL, a.dialect());
        long now = TestDb.queryLong(a, "SELECT " + Dialect.MYSQL.nowMillis());
        assertTrue(Math.abs(now - System.currentTimeMillis()) < 5000, "db now " + now);
    }

    @Test
    void lockedIncrementsFromTwoServersNeverLoseAnUpdate() throws Exception {
        String counter = table("counter");
        TestDb.exec(a, "CREATE TABLE " + counter + " (id VARCHAR(16) PRIMARY KEY, n BIGINT NOT NULL)");
        TestDb.exec(a, "INSERT INTO " + counter + " VALUES ('a', 0)");

        together(() -> increments(a, counter, 500), () -> increments(b, counter, 500));

        assertEquals(1000, TestDb.queryLong(a, "SELECT n FROM " + counter + " WHERE id = 'a'"));
    }

    private Void increments(TxRunner runner, String counter, int times) throws SQLException {
        for (int i = 0; i < times; i++) {
            runner.inTransaction(tx -> {
                tx.lock(counter, "id", "a");
                long n = readLong(tx, "SELECT n FROM " + counter + " WHERE id = 'a'");
                try (PreparedStatement write = tx.connection().prepareStatement(
                        "UPDATE " + counter + " SET n = ? WHERE id = 'a'")) {
                    write.setLong(1, n + 1);
                    write.executeUpdate();
                }
                return null;
            });
        }
        return null;
    }

    @Test
    void oppositeTransfersWithSortedLocksAllCommit() throws Exception {
        String accounts = accounts();
        together(() -> transfers(a, accounts, "x", "y", 300, true),
                () -> transfers(b, accounts, "y", "x", 300, true));
        assertBalances(accounts, 1000, 1000);
    }

    // Locking in caller order (x then y against y then x) deadlocks for real; the retry loop must recover
    // every transfer and the totals must still add up.
    @Test
    void deadlocksFromUnsortedLocksAreRetried() throws Exception {
        String accounts = accounts();
        together(() -> transfers(a, accounts, "x", "y", 150, false),
                () -> transfers(b, accounts, "y", "x", 150, false));
        assertBalances(accounts, 1000, 1000);
        assertTrue(a.retries() + b.retries() > 0, "expected at least one deadlock retry");
        System.out.println("deadlock retries: " + (a.retries() + b.retries()));
    }

    private String accounts() throws SQLException {
        String accounts = table("accounts");
        TestDb.exec(a, "CREATE TABLE " + accounts + " (id VARCHAR(16) PRIMARY KEY, balance BIGINT NOT NULL)");
        TestDb.exec(a, "INSERT INTO " + accounts + " VALUES ('x', 1000), ('y', 1000)");
        return accounts;
    }

    private Void transfers(TxRunner runner, String accounts, String from, String to, int times, boolean sorted)
            throws SQLException {
        for (int i = 0; i < times; i++) {
            runner.inTransaction(tx -> {
                if (sorted) {
                    tx.lockAll(accounts, "id", List.of(from, to));
                } else {
                    tx.lock(accounts, "id", from);
                    pause();
                    tx.lock(accounts, "id", to);
                }
                move(tx, accounts, from, -1);
                move(tx, accounts, to, 1);
                return null;
            });
        }
        return null;
    }

    private static void pause() {
        try {
            Thread.sleep(2);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void move(Tx tx, String accounts, String id, long delta) throws SQLException {
        try (PreparedStatement statement = tx.connection().prepareStatement(
                "UPDATE " + accounts + " SET balance = balance + ? WHERE id = ?")) {
            statement.setLong(1, delta);
            statement.setString(2, id);
            statement.executeUpdate();
        }
    }

    private void assertBalances(String accounts, long x, long y) throws SQLException {
        assertEquals(x, TestDb.queryLong(a, "SELECT balance FROM " + accounts + " WHERE id = 'x'"));
        assertEquals(y, TestDb.queryLong(a, "SELECT balance FROM " + accounts + " WHERE id = 'y'"));
    }

    @Test
    void exactlyOneServerWinsAFreeLease() throws Exception {
        Lease leaseA = new Lease(a, prefix, "server-a");
        Lease leaseB = new Lease(b, prefix, "server-b");
        tables.add(leaseA.table());
        leaseA.createTable();
        for (int round = 0; round < 50; round++) {
            String name = "job-" + round;
            List<Boolean> won = together(() -> leaseA.tryAcquireOrRenew(name, Duration.ofMinutes(1)),
                    () -> leaseB.tryAcquireOrRenew(name, Duration.ofMinutes(1)));
            assertTrue(won.get(0) ^ won.get(1), "round " + round + ": " + won);
        }
    }

    @Test
    void leaseStaysWithItsHolderAndFailsOverAfterExpiry() throws Exception {
        Lease leaseA = new Lease(a, prefix, "server-a");
        Lease leaseB = new Lease(b, prefix, "server-b");
        tables.add(leaseA.table());
        leaseA.createTable();
        Duration ttl = Duration.ofMillis(1000);
        assertTrue(leaseA.tryAcquireOrRenew("interest", ttl));

        // both keep trying for 2.5 s; A renews every 100 ms, so B must never get in
        AtomicInteger bWins = new AtomicInteger();
        AtomicInteger aLosses = new AtomicInteger();
        long end = System.nanoTime() + Duration.ofMillis(2500).toNanos();
        together(() -> {
            while (System.nanoTime() < end) {
                if (!leaseA.tryAcquireOrRenew("interest", ttl)) {
                    aLosses.incrementAndGet();
                }
                Thread.sleep(100);
            }
            return null;
        }, () -> {
            while (System.nanoTime() < end) {
                if (leaseB.tryAcquireOrRenew("interest", ttl)) {
                    bWins.incrementAndGet();
                }
                Thread.sleep(20);
            }
            return null;
        });
        assertEquals(0, bWins.get());
        assertEquals(0, aLosses.get());
        assertTrue(leaseA.isHolder("interest"));

        // A stops renewing (crash): B takes over once the ttl has run out, not before
        long stoppedAt = System.nanoTime();
        while (!leaseB.tryAcquireOrRenew("interest", ttl)) {
            Thread.sleep(20);
            assertTrue(System.nanoTime() - stoppedAt < Duration.ofSeconds(5).toNanos(), "no failover");
        }
        long waitedMs = (System.nanoTime() - stoppedAt) / 1_000_000;
        System.out.println("lease failover after " + waitedMs + " ms (ttl 1000 ms)");
        assertTrue(waitedMs >= 700, "took over too early: " + waitedMs + " ms");
        assertTrue(leaseB.isHolder("interest"));
        assertFalse(leaseA.isHolder("interest"));
        assertFalse(leaseA.tryAcquireOrRenew("interest", ttl));
    }

    @Test
    void awaitSavedWaitsForTheOtherServersSave() throws Exception {
        PlayerHandoff lobby = new PlayerHandoff(a, prefix, "lobby", Duration.ofMinutes(1));
        PlayerHandoff survival = new PlayerHandoff(b, prefix, "survival", Duration.ofMinutes(1));
        tables.add(lobby.table());
        lobby.createTable();
        UUID alice = UUID.randomUUID();
        lobby.markActive(alice);

        long start = System.nanoTime();
        Future<PlayerHandoff.Result> waiting = threads.submit(() -> survival.awaitSaved(alice, Duration.ofSeconds(10)));
        Thread.sleep(600);
        assertFalse(waiting.isDone(), "must wait while lobby still has alice ACTIVE");
        assertTrue(lobby.markSaved(alice));
        PlayerHandoff.Result result = waiting.get(5, TimeUnit.SECONDS);
        long waitedMs = (System.nanoTime() - start) / 1_000_000;

        assertEquals(PlayerHandoff.Status.SAVED, result.status());
        assertEquals("lobby", result.holder());
        assertTrue(waitedMs >= 600 && waitedMs < 2000, "waited " + waitedMs + " ms");

        survival.markActive(alice);
        assertEquals(PlayerHandoff.Status.THIS_SERVER, survival.awaitSaved(alice, Duration.ofSeconds(1)).status());
    }

    @Test
    void awaitSavedTimesOutOrSkipsADeadServer() throws Exception {
        PlayerHandoff lobby = new PlayerHandoff(a, prefix, "lobby", Duration.ofMillis(1500));
        PlayerHandoff survival = new PlayerHandoff(b, prefix, "survival", Duration.ofMillis(1500));
        tables.add(lobby.table());
        lobby.createTable();
        UUID alice = UUID.randomUUID();
        lobby.markActive(alice);

        PlayerHandoff.Result timedOut = survival.awaitSaved(alice, Duration.ofMillis(300));
        assertEquals(PlayerHandoff.Status.TIMED_OUT, timedOut.status());
        assertEquals("lobby", timedOut.holder());

        // lobby never heartbeats or saves, as if it crashed
        PlayerHandoff.Result stale = survival.awaitSaved(alice, Duration.ofSeconds(5));
        assertEquals(PlayerHandoff.Status.STALE, stale.status());
    }

    @Test
    void twoServersMigratingTogetherRunTheMigrationOnce() throws Exception {
        SchemaVersion schemaA = new SchemaVersion(a, prefix);
        SchemaVersion schemaB = new SchemaVersion(b, prefix);
        tables.add(schemaA.table());
        String items = table("items");
        List<String> applied = new CopyOnWriteArrayList<>();
        SchemaVersion.Migration migration = (connection, dialect, from, to) -> {
            applied.add(from + "->" + to);
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("CREATE TABLE IF NOT EXISTS " + items + " (id INT PRIMARY KEY)");
            }
            pauseLong();
        };
        List<Integer> before = together(() -> schemaA.ensure(3, migration), () -> schemaB.ensure(3, migration));

        assertEquals(List.of("0->3"), applied);
        assertEquals(Set.of(0, 3), Set.copyOf(before));
        assertEquals(3, schemaB.current());
    }

    private static void pauseLong() {
        try {
            Thread.sleep(300);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static long readLong(Tx tx, String sql) throws SQLException {
        try (Statement statement = tx.connection().createStatement();
             ResultSet row = statement.executeQuery(sql)) {
            row.next();
            return row.getLong(1);
        }
    }
}
