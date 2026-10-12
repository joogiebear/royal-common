package com.mystipixel.royalcommon.storage;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class VersionPollerTest {

    @TempDir
    Path dir;

    private HikariDataSource pool;
    private TxRunner db;
    private DbExecutor executor;
    private final List<Set<UUID>> callbacks = new CopyOnWriteArrayList<>();
    private VersionPoller<UUID> poller;

    @BeforeEach
    void setUp() throws SQLException {
        pool = TestDb.sqlite(dir.resolve("test.db"));
        db = new TxRunner(pool);
        TestDb.exec(db, "CREATE TABLE accounts (uuid TEXT PRIMARY KEY, version INTEGER NOT NULL)");
        executor = new DbExecutor("Test", MainThread.direct(), Logger.getLogger("VersionPollerTest"));
        poller = new VersionPoller<>(pool, executor,
                VersionPoller.byColumn("accounts", "uuid", "version", UUID::fromString), callbacks::add);
    }

    @AfterEach
    void tearDown() {
        executor.shutdown(Duration.ofSeconds(5));
        pool.close();
    }

    private void write(UUID id, long version) throws SQLException {
        db.inTransaction(tx -> {
            try (PreparedStatement statement = tx.connection().prepareStatement(
                    "INSERT INTO accounts (uuid, version) VALUES (?, ?) ON CONFLICT(uuid) DO UPDATE SET version = excluded.version")) {
                statement.setString(1, id.toString());
                statement.setLong(2, version);
                statement.executeUpdate();
            }
            return null;
        });
    }

    private Set<UUID> poll(UUID... keys) throws Exception {
        return poller.poll(List.of(keys)).get(5, TimeUnit.SECONDS);
    }

    // the first query waits for release after counting down started; later ones run straight through
    private static VersionPoller.Query<UUID> heldOnce(CountDownLatch started, CountDownLatch release) {
        VersionPoller.Query<UUID> columns = VersionPoller.byColumn("accounts", "uuid", "version", UUID::fromString);
        return (connection, keys) -> {
            if (started.getCount() > 0) {
                started.countDown();
                try {
                    assertTrue(release.await(5, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                    throw new IllegalStateException(e);
                }
            }
            return columns.versions(connection, keys);
        };
    }

    @Test
    void reportsKeysAnotherServerChanged() throws Exception {
        UUID alice = UUID.randomUUID();
        UUID bob = UUID.randomUUID();
        write(alice, 1);
        write(bob, 1);
        poller.seen(alice, 1);
        poller.seen(bob, 1);
        assertEquals(Set.of(), poll(alice, bob));

        write(bob, 2); // another server
        assertEquals(Set.of(bob), poll(alice, bob));
        assertEquals(List.of(Set.of(bob)), callbacks);

        assertEquals(Set.of(), poll(alice, bob), "reported once, then it is the new baseline");
    }

    @Test
    void ownWritesAreNotReported() throws Exception {
        UUID alice = UUID.randomUUID();
        write(alice, 1);
        poller.seen(alice, 1);
        write(alice, 2);
        poller.seen(alice, 2);
        assertEquals(Set.of(), poll(alice));
        assertTrue(callbacks.isEmpty());
    }

    @Test
    void firstSightOnlySetsTheBaseline() throws Exception {
        UUID alice = UUID.randomUUID();
        write(alice, 7);
        assertEquals(Set.of(), poll(alice));
        write(alice, 8);
        assertEquals(Set.of(alice), poll(alice));
    }

    @Test
    void forgottenKeysStartOver() throws Exception {
        UUID alice = UUID.randomUUID();
        write(alice, 1);
        poller.seen(alice, 1);
        poller.forget(alice);
        write(alice, 2);
        assertEquals(Set.of(), poll(alice));
    }

    @Test
    void handlesMoreKeysThanOneStatementBinds() throws Exception {
        List<UUID> keys = new ArrayList<>();
        for (int i = 0; i < 1203; i++) {
            UUID id = UUID.randomUUID();
            keys.add(id);
            write(id, 1);
            poller.seen(id, 1);
        }
        write(keys.get(0), 2);
        write(keys.get(1202), 2);
        Set<UUID> changed = poller.poll(keys).get(10, TimeUnit.SECONDS);
        assertEquals(Set.of(keys.get(0), keys.get(1202)), changed);
    }

    @Test
    void keyForgottenDuringAPollIsNotTrackedAgain() throws Exception {
        CountDownLatch queryStarted = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        VersionPoller<UUID> slow = new VersionPoller<>(pool, executor,
                heldOnce(queryStarted, release), callbacks::add);

        UUID alice = UUID.randomUUID();
        write(alice, 1);
        slow.seen(alice, 1);
        CompletableFuture<Set<UUID>> inFlight = slow.poll(List.of(alice));
        assertTrue(queryStarted.await(5, TimeUnit.SECONDS));
        slow.forget(alice); // player left while the query ran
        release.countDown();
        assertEquals(Set.of(), inFlight.get(5, TimeUnit.SECONDS));

        // back on this server later: a fresh first sight, not a change against the old baseline
        write(alice, 2);
        assertEquals(Set.of(), slow.poll(List.of(alice)).get(5, TimeUnit.SECONDS));
        assertTrue(callbacks.isEmpty());
    }

    @Test
    void lowerBaselineSetDuringAPollIsKept() throws Exception {
        CountDownLatch queryStarted = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        VersionPoller<UUID> slow = new VersionPoller<>(pool, executor,
                heldOnce(queryStarted, release), callbacks::add);

        UUID alice = UUID.randomUUID();
        write(alice, 2);
        slow.seen(alice, 1);
        CompletableFuture<Set<UUID>> inFlight = slow.poll(List.of(alice));
        assertTrue(queryStarted.await(5, TimeUnit.SECONDS));
        // a failed reload puts the cached version back so the next poll reports the change again
        slow.forget(alice);
        slow.seen(alice, 1);
        release.countDown();
        inFlight.get(5, TimeUnit.SECONDS);

        assertEquals(Set.of(alice), slow.poll(List.of(alice)).get(5, TimeUnit.SECONDS));
    }
}
