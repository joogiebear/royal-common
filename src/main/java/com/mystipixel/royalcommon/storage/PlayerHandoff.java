package com.mystipixel.royalcommon.storage;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.UUID;

/**
 * Hands a player's data from one server to the next on a shared database. The server a player is on marks
 * them ACTIVE; after the quit save commits it marks them SAVED. The next server waits for SAVED before it
 * loads, so it never reads data the previous server is still writing.
 *
 * <p>Table {@code <prefix>_player_session}: uuid, server_id, state (ACTIVE or SAVED), updated_at (database
 * time, epoch ms). A row whose server stopped calling {@link #heartbeat} for longer than the stale threshold
 * is treated as left behind by a crash and ignored.
 *
 * <p>On SQLite there is only one server, so {@link #awaitSaved} returns at once.
 *
 * <p>Proxy switches: Velocity and BungeeCord disconnect a player from the old server only after the new one
 * accepts the login. Waiting in {@code AsyncPlayerPreLoginEvent} therefore covers a player who quit and
 * rejoined, but on a direct server switch the old server's quit save cannot start until that wait ends; keep
 * the timeout short there, or wait again after join before loading.
 */
public final class PlayerHandoff {

    public enum Status {
        /** No server has recorded this player. */
        NO_RECORD,
        /** The last server finished saving. */
        SAVED,
        /** This server is the one that recorded the player. */
        THIS_SERVER,
        /** The recording server stopped heartbeating and is presumed dead. */
        STALE,
        /** SQLite: one server, nothing to wait for. */
        SINGLE_SERVER,
        /** Another server still has the player ACTIVE after the timeout. */
        TIMED_OUT,
        /** Another server has the player ACTIVE; only returned by {@link #check}. */
        ACTIVE_ELSEWHERE
    }

    /**
     * @param holder the server that has the row, or null when there is none
     */
    public record Result(Status status, String holder) {
        /** True when it is safe to load the player's data. */
        public boolean ready() {
            return status != Status.TIMED_OUT && status != Status.ACTIVE_ELSEWHERE;
        }
    }

    private static final long POLL_MILLIS = 100;

    private final TxRunner db;
    private final String table;
    private final String serverId;
    private final Duration staleAfter;

    /**
     * @param staleAfter how long an ACTIVE row may go without a heartbeat before it counts as abandoned;
     *                   call {@link #heartbeat} at least three times in that span
     */
    public PlayerHandoff(TxRunner db, String tablePrefix, String serverId, Duration staleAfter) {
        this.db = db;
        this.table = Sql.prefix(tablePrefix) + "_player_session";
        this.serverId = serverId;
        this.staleAfter = staleAfter;
    }

    public String table() {
        return table;
    }

    public void createTable() throws SQLException {
        String ddl = db.dialect() == Dialect.MYSQL ? """
                CREATE TABLE IF NOT EXISTS %s (
                    uuid CHAR(36) NOT NULL PRIMARY KEY,
                    server_id VARCHAR(64) NOT NULL,
                    state VARCHAR(8) NOT NULL,
                    updated_at BIGINT NOT NULL
                )""" : """
                CREATE TABLE IF NOT EXISTS %s (
                    uuid TEXT NOT NULL PRIMARY KEY,
                    server_id TEXT NOT NULL,
                    state TEXT NOT NULL,
                    updated_at INTEGER NOT NULL
                )""";
        db.inTransaction(tx -> {
            try (Statement statement = tx.connection().createStatement()) {
                statement.executeUpdate(ddl.formatted(table));
            }
            return null;
        });
    }

    /** Records that this server has loaded the player. Call after the load. */
    public void markActive(UUID player) throws SQLException {
        String now = db.dialect().nowMillis();
        String sql = db.dialect() == Dialect.MYSQL
                ? "INSERT INTO " + table + " (uuid, server_id, state, updated_at) VALUES (?, ?, 'ACTIVE', " + now
                + ") ON DUPLICATE KEY UPDATE server_id = ?, state = 'ACTIVE', updated_at = " + now
                : "INSERT INTO " + table + " (uuid, server_id, state, updated_at) VALUES (?, ?, 'ACTIVE', " + now
                + ") ON CONFLICT(uuid) DO UPDATE SET server_id = excluded.server_id, state = 'ACTIVE',"
                + " updated_at = excluded.updated_at";
        db.inTransaction(tx -> {
            try (PreparedStatement statement = tx.connection().prepareStatement(sql)) {
                statement.setString(1, player.toString());
                statement.setString(2, serverId);
                if (db.dialect() == Dialect.MYSQL) {
                    statement.setString(3, serverId);
                }
                statement.executeUpdate();
            }
            return null;
        });
    }

    /**
     * Records that the quit save has committed. Call only after that commit. Does nothing when another server
     * has taken the player over in the meantime.
     *
     * @return false when the row belongs to another server or is missing
     */
    public boolean markSaved(UUID player) throws SQLException {
        String sql = "UPDATE " + table + " SET state = 'SAVED', updated_at = " + db.dialect().nowMillis()
                + " WHERE uuid = ? AND server_id = ?";
        return db.inTransaction(tx -> {
            try (PreparedStatement statement = tx.connection().prepareStatement(sql)) {
                statement.setString(1, player.toString());
                statement.setString(2, serverId);
                return statement.executeUpdate() > 0;
            }
        });
    }

    /** Refreshes every ACTIVE row of this server so other servers don't treat them as stale. */
    public int heartbeat() throws SQLException {
        String sql = "UPDATE " + table + " SET updated_at = " + db.dialect().nowMillis()
                + " WHERE server_id = ? AND state = 'ACTIVE'";
        return db.inTransaction(tx -> {
            try (PreparedStatement statement = tx.connection().prepareStatement(sql)) {
                statement.setString(1, serverId);
                return statement.executeUpdate();
            }
        });
    }

    /** One look at the player's row, without waiting. */
    public Result check(UUID player) throws SQLException {
        String sql = "SELECT server_id, state, updated_at, " + db.dialect().nowMillis() + " FROM " + table
                + " WHERE uuid = ?";
        try (Connection connection = db.dataSource().getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, player.toString());
            try (ResultSet row = statement.executeQuery()) {
                if (!row.next()) {
                    return new Result(Status.NO_RECORD, null);
                }
                String holder = row.getString(1);
                if (holder.equals(serverId)) {
                    return new Result(Status.THIS_SERVER, holder);
                }
                if ("SAVED".equals(row.getString(2))) {
                    return new Result(Status.SAVED, holder);
                }
                if (row.getLong(4) - row.getLong(3) > staleAfter.toMillis()) {
                    return new Result(Status.STALE, holder);
                }
                return new Result(Status.ACTIVE_ELSEWHERE, holder);
            }
        }
    }

    /**
     * Blocks until the player's data is safe to load or the timeout passes. Call off the main thread, e.g.
     * from {@code AsyncPlayerPreLoginEvent}. On {@link Status#TIMED_OUT} the caller decides whether to load
     * anyway or refuse the login, and should log it.
     */
    public Result awaitSaved(UUID player, Duration timeout) throws SQLException {
        if (db.dialect() == Dialect.SQLITE) {
            return new Result(Status.SINGLE_SERVER, null);
        }
        long deadline = System.nanoTime() + timeout.toNanos();
        while (true) {
            Result result = check(player);
            if (result.ready()) {
                return result;
            }
            long left = deadline - System.nanoTime();
            if (left <= 0) {
                return new Result(Status.TIMED_OUT, result.holder());
            }
            try {
                Thread.sleep(Math.min(POLL_MILLIS, Math.max(1, left / 1_000_000)));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return new Result(Status.TIMED_OUT, result.holder());
            }
        }
    }
}
