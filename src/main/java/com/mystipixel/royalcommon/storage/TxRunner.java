package com.mystipixel.royalcommon.storage;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import javax.sql.DataSource;

/**
 * Runs units of work in one JDBC transaction each, retrying the whole unit when the database reports a
 * deadlock, a lock wait timeout or a busy database.
 *
 * <p>On MySQL the transaction is an ordinary one (autocommit off); rows are locked with
 * {@link Tx#lock}. On SQLite every transaction starts with {@code BEGIN IMMEDIATE}, which takes the database
 * write lock up front: a transaction that read first and wrote later could otherwise fail with SQLITE_BUSY
 * half way through. The plugins use a pool of one SQLite connection, so transactions from one plugin already
 * run one at a time; the immediate lock matters when another connection or process opens the same file.
 *
 * <p>Work can run more than once, so it must not have side effects outside the transaction (no Bukkit
 * calls, no cache updates) until it returns. Do not start a transaction from inside another one: with a pool
 * of one connection the inner call waits for a connection that never comes back.
 */
public final class TxRunner {

    /** A unit of work that runs inside one transaction. */
    @FunctionalInterface
    public interface Work<T> {
        T run(Tx tx) throws SQLException;
    }

    public static final int DEFAULT_ATTEMPTS = 5;

    private static final int MYSQL_DEADLOCK = 1213;
    private static final int MYSQL_LOCK_WAIT_TIMEOUT = 1205;
    private static final int SQLITE_BUSY = 5;
    private static final int SQLITE_LOCKED = 6;

    private final DataSource dataSource;
    private final Dialect dialect;
    private final int maxAttempts;
    private final AtomicLong retries = new AtomicLong();

    public TxRunner(DataSource dataSource) throws SQLException {
        this(dataSource, DEFAULT_ATTEMPTS);
    }

    public TxRunner(DataSource dataSource, int maxAttempts) throws SQLException {
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be at least 1");
        }
        this.dataSource = dataSource;
        this.dialect = Dialect.detect(dataSource);
        this.maxAttempts = maxAttempts;
    }

    public DataSource dataSource() {
        return dataSource;
    }

    public Dialect dialect() {
        return dialect;
    }

    /** How many times a unit of work has been retried since this runner was created. */
    public long retries() {
        return retries.get();
    }

    /**
     * Runs work in one transaction and commits it. Rolls back and rethrows when the work throws; retries up
     * to the configured number of attempts when the failure is a deadlock, lock wait timeout or busy database.
     */
    public <T> T inTransaction(Work<T> work) throws SQLException {
        for (int attempt = 1; ; attempt++) {
            try (Connection connection = dataSource.getConnection()) {
                return dialect == Dialect.MYSQL ? runMysql(connection, work) : runSqlite(connection, work);
            } catch (SQLException e) {
                if (attempt >= maxAttempts || !isRetryable(dialect, e)) {
                    throw e;
                }
                retries.incrementAndGet();
                if (!backOff(attempt)) {
                    throw e;
                }
            }
        }
    }

    private <T> T runMysql(Connection connection, Work<T> work) throws SQLException {
        connection.setAutoCommit(false);
        try {
            T result = work.run(new Tx(connection, dialect));
            connection.commit();
            return result;
        } catch (SQLException | RuntimeException | Error e) {
            rollbackQuietly(connection, e);
            throw e;
        } finally {
            restoreAutoCommit(connection);
        }
    }

    // a pool may hand the connection to code that expects autocommit; a failure here must not hide the real one
    private static void restoreAutoCommit(Connection connection) {
        try {
            connection.setAutoCommit(true);
        } catch (SQLException ignored) {
            // the connection is broken; the pool discards it when it is closed
        }
    }

    // sqlite-jdbc's own autocommit-off mode opens a deferred transaction, so BEGIN/COMMIT are issued by hand
    private <T> T runSqlite(Connection connection, Work<T> work) throws SQLException {
        connection.setAutoCommit(true);
        try (Statement statement = connection.createStatement()) {
            statement.execute("BEGIN IMMEDIATE");
            try {
                T result = work.run(new Tx(connection, dialect));
                statement.execute("COMMIT");
                return result;
            } catch (SQLException | RuntimeException | Error e) {
                try {
                    statement.execute("ROLLBACK");
                } catch (SQLException rollback) {
                    e.addSuppressed(rollback);
                }
                throw e;
            }
        }
    }

    private static void rollbackQuietly(Connection connection, Throwable failure) {
        try {
            connection.rollback();
        } catch (SQLException rollback) {
            failure.addSuppressed(rollback);
        }
    }

    static boolean isRetryable(Dialect dialect, SQLException failure) {
        for (Throwable t = failure; t != null; t = t.getCause()) {
            if (t instanceof SQLException sql) {
                for (SQLException e = sql; e != null; e = e.getNextException()) {
                    if (retryableCode(dialect, e)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    private static boolean retryableCode(Dialect dialect, SQLException e) {
        if (dialect == Dialect.MYSQL) {
            return "40001".equals(e.getSQLState())
                    || e.getErrorCode() == MYSQL_DEADLOCK
                    || e.getErrorCode() == MYSQL_LOCK_WAIT_TIMEOUT;
        }
        // extended result codes (SQLITE_BUSY_SNAPSHOT = 517 ...) keep the primary code in the low byte
        int primary = e.getErrorCode() & 0xff;
        if (primary == SQLITE_BUSY || primary == SQLITE_LOCKED) {
            return true;
        }
        String message = e.getMessage();
        return message != null && (message.contains("SQLITE_BUSY") || message.contains("SQLITE_LOCKED"));
    }

    // false when interrupted (plugin shutting down): give up instead of retrying
    private static boolean backOff(int attempt) {
        long millis = Math.min(200, 5L << Math.min(attempt, 6)) + ThreadLocalRandom.current().nextLong(10);
        try {
            Thread.sleep(millis);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }
}
