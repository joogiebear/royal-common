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
import java.util.concurrent.CopyOnWriteArrayList;
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
}
