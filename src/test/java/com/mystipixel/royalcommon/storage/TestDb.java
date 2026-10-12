package com.mystipixel.royalcommon.storage;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

final class TestDb {

    private TestDb() {
    }

    // same driver settings the plugins use (RoyalBank SqliteSettings), pool of one
    static HikariDataSource sqlite(Path file) {
        return sqlite(file, 5000);
    }

    static HikariDataSource sqlite(Path file, int busyTimeoutMs) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:sqlite:" + file.toAbsolutePath());
        config.setMaximumPoolSize(1);
        Properties props = new Properties();
        props.setProperty("journal_mode", "WAL");
        props.setProperty("synchronous", "FULL");
        props.setProperty("busy_timeout", String.valueOf(busyTimeoutMs));
        props.setProperty("foreign_keys", "true");
        config.setDataSourceProperties(props);
        return new HikariDataSource(config);
    }

    static HikariDataSource mysql(String url, String user, String password, int poolSize) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(url);
        config.setUsername(user);
        config.setPassword(password);
        config.setMaximumPoolSize(poolSize);
        return new HikariDataSource(config);
    }

    static void exec(TxRunner db, String sql) throws SQLException {
        try (Connection connection = db.dataSource().getConnection();
             Statement statement = connection.createStatement()) {
            statement.executeUpdate(sql);
        }
    }

    static long queryLong(TxRunner db, String sql) throws SQLException {
        try (Connection connection = db.dataSource().getConnection();
             Statement statement = connection.createStatement();
             ResultSet row = statement.executeQuery(sql)) {
            if (!row.next()) {
                throw new SQLException("no row: " + sql);
            }
            return row.getLong(1);
        }
    }
}
