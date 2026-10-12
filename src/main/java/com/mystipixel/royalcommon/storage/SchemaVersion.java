package com.mystipixel.royalcommon.storage;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * The plugin's database schema version, in table {@code <prefix>_schema_version} (one row). {@link #ensure}
 * brings the database up to the version the code expects, safely when several servers start at once, and
 * refuses to run against a database a newer plugin build has already migrated.
 */
public final class SchemaVersion {

    /** Moves the schema from one version to a later one. */
    @FunctionalInterface
    public interface Migration {
        /**
         * Applies every step after from up to and including to.
         *
         * <p>On SQLite this runs inside one transaction. On MySQL it runs in autocommit mode, because DDL
         * commits implicitly there anyway; other servers are kept out by a named lock instead, and each step
         * should be safe to repeat in case a server dies half way.
         */
        void migrate(Connection connection, Dialect dialect, int from, int to) throws SQLException;
    }

    private static final int LOCK_WAIT_SECONDS = 120;

    private final TxRunner db;
    private final String prefix;
    private final String table;

    public SchemaVersion(TxRunner db, String tablePrefix) {
        this.db = db;
        this.prefix = Sql.prefix(tablePrefix);
        this.table = prefix + "_schema_version";
    }

    public String table() {
        return table;
    }

    /** The version recorded in the database; 0 for a database this plugin has never migrated. */
    public int current() throws SQLException {
        createTable();
        try (Connection connection = db.dataSource().getConnection()) {
            return read(connection);
        }
    }

    /**
     * Migrates the database to codeVersion if it is older. A second server starting at the same time waits
     * for the first to finish and then finds nothing to do.
     *
     * @return the version the database had before
     * @throws SchemaTooNewException when the database is already past codeVersion
     */
    public int ensure(int codeVersion, Migration migration) throws SQLException {
        createTable();
        return db.dialect() == Dialect.MYSQL
                ? ensureMysql(codeVersion, migration)
                : db.inTransaction(tx -> migrateLocked(tx.connection(), codeVersion, migration));
    }

    // GET_LOCK belongs to the session, so it survives the implicit commits of DDL in the migration
    private int ensureMysql(int codeVersion, Migration migration) throws SQLException {
        String lockName = prefix + "_schema";
        try (Connection connection = db.dataSource().getConnection()) {
            connection.setAutoCommit(true);
            try (PreparedStatement lock = connection.prepareStatement("SELECT GET_LOCK(?, ?)")) {
                lock.setString(1, lockName);
                lock.setInt(2, LOCK_WAIT_SECONDS);
                try (ResultSet row = lock.executeQuery()) {
                    if (!row.next() || row.getInt(1) != 1) {
                        throw new SQLException("Timed out waiting for another server to finish migrating " + table);
                    }
                }
            }
            try {
                return migrateLocked(connection, codeVersion, migration);
            } finally {
                try (PreparedStatement unlock = connection.prepareStatement("SELECT RELEASE_LOCK(?)")) {
                    unlock.setString(1, lockName);
                    unlock.executeQuery().close();
                }
            }
        }
    }

    private int migrateLocked(Connection connection, int codeVersion, Migration migration) throws SQLException {
        int before = read(connection);
        if (before > codeVersion) {
            throw new SchemaTooNewException(table, before, codeVersion);
        }
        if (before < codeVersion) {
            migration.migrate(connection, db.dialect(), before, codeVersion);
            try (PreparedStatement update = connection.prepareStatement(
                    "UPDATE " + table + " SET version = ? WHERE id = 1")) {
                update.setInt(1, codeVersion);
                update.executeUpdate();
            }
        }
        return before;
    }

    private int read(Connection connection) throws SQLException {
        String sql = "SELECT version FROM " + table + " WHERE id = 1";
        try (Statement statement = connection.createStatement();
             ResultSet row = statement.executeQuery(sql)) {
            return row.next() ? row.getInt(1) : 0;
        }
    }

    private void createTable() throws SQLException {
        String ddl = db.dialect() == Dialect.MYSQL
                ? "CREATE TABLE IF NOT EXISTS " + table + " (id INT NOT NULL PRIMARY KEY, version INT NOT NULL)"
                : "CREATE TABLE IF NOT EXISTS " + table + " (id INTEGER NOT NULL PRIMARY KEY, version INTEGER NOT NULL)";
        db.inTransaction(tx -> {
            try (Statement statement = tx.connection().createStatement()) {
                statement.executeUpdate(ddl);
                statement.executeUpdate(db.dialect().insertIgnore() + " INTO " + table + " (id, version) VALUES (1, 0)");
            }
            return null;
        });
    }
}
