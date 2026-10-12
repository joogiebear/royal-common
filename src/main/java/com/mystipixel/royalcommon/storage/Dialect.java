package com.mystipixel.royalcommon.storage;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Locale;
import javax.sql.DataSource;

/**
 * The two databases the Royal plugins support, told apart by the driver's metadata. MariaDB counts as
 * {@link #MYSQL}.
 */
public enum Dialect {
    SQLITE,
    MYSQL;

    public static Dialect of(Connection connection) throws SQLException {
        String product = connection.getMetaData().getDatabaseProductName();
        String name = product == null ? "" : product.toLowerCase(Locale.ROOT);
        if (name.contains("sqlite")) {
            return SQLITE;
        }
        if (name.contains("mysql") || name.contains("mariadb")) {
            return MYSQL;
        }
        throw new SQLException("Unsupported database: " + product + " (expected SQLite, MySQL or MariaDB)");
    }

    public static Dialect detect(DataSource dataSource) throws SQLException {
        try (Connection connection = dataSource.getConnection()) {
            return of(connection);
        }
    }

    /** {@code INSERT IGNORE} or {@code INSERT OR IGNORE}, to start an insert that skips existing keys. */
    public String insertIgnore() {
        return this == MYSQL ? "INSERT IGNORE" : "INSERT OR IGNORE";
    }

    /**
     * Suffix for a SELECT that locks the rows it reads: {@code " FOR UPDATE"} on MySQL, empty on SQLite,
     * where {@link TxRunner} transactions already hold the write lock for the whole database.
     */
    public String forUpdate() {
        return this == MYSQL ? " FOR UPDATE" : "";
    }

    /**
     * SQL expression for the database's current time in epoch milliseconds. Leases and hand-off rows use it
     * instead of each server's own clock, so servers with skewed clocks still agree on what has expired.
     */
    public String nowMillis() {
        return this == MYSQL
                ? "(TIMESTAMPDIFF(MICROSECOND, '1970-01-01 00:00:00', UTC_TIMESTAMP(6)) DIV 1000)"
                : "CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)";
    }
}
