package com.mystipixel.royalcommon.storage;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SchemaVersionTest {

    @TempDir
    Path dir;

    private final List<HikariDataSource> pools = new ArrayList<>();
    private final List<String> applied = new CopyOnWriteArrayList<>();

    @AfterEach
    void tearDown() {
        pools.forEach(HikariDataSource::close);
    }

    private TxRunner open(int busyTimeoutMs) throws SQLException {
        HikariDataSource pool = TestDb.sqlite(dir.resolve("test.db"), busyTimeoutMs);
        pools.add(pool);
        return new TxRunner(pool, 200);
    }

    private SchemaVersion.Migration recording() {
        return (connection, dialect, from, to) -> {
            applied.add(from + "->" + to);
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("CREATE TABLE IF NOT EXISTS items (id INTEGER PRIMARY KEY)");
            }
        };
    }

    @Test
    void migratesOnceAndRecordsTheVersion() throws SQLException {
        SchemaVersion schema = new SchemaVersion(open(5000), "royaltest");
        assertEquals(0, schema.current());
        assertEquals(0, schema.ensure(2, recording()));
        assertEquals(2, schema.current());
        assertEquals(2, schema.ensure(2, recording()));
        assertEquals(List.of("0->2"), applied);
        assertEquals(2, schema.ensure(3, recording()));
        assertEquals(List.of("0->2", "2->3"), applied);
    }

    @Test
    void refusesADatabaseFromANewerBuild() throws SQLException {
        SchemaVersion schema = new SchemaVersion(open(5000), "royaltest");
        schema.ensure(5, recording());
        SchemaTooNewException e = assertThrows(SchemaTooNewException.class, () -> schema.ensure(4, recording()));
        assertEquals(5, e.databaseVersion());
        assertEquals(4, e.supportedVersion());
        assertTrue(e.getMessage().contains("royaltest_schema_version"));
    }

    @Test
    void aFailedMigrationLeavesNothingBehind() throws SQLException {
        TxRunner db = open(5000);
        SchemaVersion schema = new SchemaVersion(db, "royaltest");
        assertThrows(SQLException.class, () -> schema.ensure(1, (connection, dialect, from, to) -> {
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("CREATE TABLE half_done (id INTEGER)");
                statement.executeUpdate("THIS IS NOT SQL");
            }
        }));
        assertEquals(0, schema.current());
        assertEquals(0, TestDb.queryLong(db, "SELECT COUNT(*) FROM sqlite_master WHERE name = 'half_done'"));
    }

    @Test
    void twoServersStartingTogetherMigrateOnce() throws Exception {
        SchemaVersion one = new SchemaVersion(open(0), "royaltest");
        SchemaVersion two = new SchemaVersion(open(0), "royaltest");
        List<Throwable> failures = new CopyOnWriteArrayList<>();
        Thread a = new Thread(() -> ensure(one, failures));
        Thread b = new Thread(() -> ensure(two, failures));
        a.start();
        b.start();
        a.join();
        b.join();
        assertTrue(failures.isEmpty(), "failures: " + failures);
        assertEquals(List.of("0->3"), applied);
        assertEquals(3, one.current());
    }

    private void ensure(SchemaVersion schema, List<Throwable> failures) {
        try {
            schema.ensure(3, (connection, dialect, from, to) -> {
                recording().migrate(connection, dialect, from, to);
                try {
                    Thread.sleep(100); // hold the lock long enough for the other server to collide
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        } catch (Throwable t) {
            failures.add(t);
        }
    }
}
