package com.mystipixel.royalcommon.storage;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.bukkit.plugin.Plugin;
import org.bukkit.scheduler.BukkitTask;

/**
 * Named leases so only one server at a time does a network-wide job (paying interest, expiring auctions).
 * Table {@code <prefix>_lease}: name, holder (server id), expires_at (database time, epoch ms).
 *
 * <p>A lease is held until it expires unless its holder renews it. For a repeating job use {@link #keep},
 * which renews every third of the ttl and runs the job only while this server holds the lease.
 */
public final class Lease {

    private final TxRunner db;
    private final String table;
    private final String serverId;

    public Lease(TxRunner db, String tablePrefix, String serverId) {
        this.db = db;
        this.table = Sql.prefix(tablePrefix) + "_lease";
        this.serverId = serverId;
    }

    public String table() {
        return table;
    }

    public void createTable() throws SQLException {
        String ddl = db.dialect() == Dialect.MYSQL ? """
                CREATE TABLE IF NOT EXISTS %s (
                    name VARCHAR(64) NOT NULL PRIMARY KEY,
                    holder VARCHAR(64) NOT NULL,
                    expires_at BIGINT NOT NULL
                )""" : """
                CREATE TABLE IF NOT EXISTS %s (
                    name TEXT NOT NULL PRIMARY KEY,
                    holder TEXT NOT NULL,
                    expires_at INTEGER NOT NULL
                )""";
        db.inTransaction(tx -> {
            try (Statement statement = tx.connection().createStatement()) {
                statement.executeUpdate(ddl.formatted(table));
            }
            return null;
        });
    }

    /**
     * Takes the lease if it is free or expired, or extends it if this server already holds it.
     *
     * @return true if this server holds the lease for ttl from now
     */
    public boolean tryAcquireOrRenew(String name, Duration ttl) throws SQLException {
        checkName(name);
        String now = db.dialect().nowMillis();
        // the placeholder row is born expired, so the UPDATE below is the only place a lease is ever granted
        String insert = db.dialect().insertIgnore() + " INTO " + table + " (name, holder, expires_at) VALUES (?, '', 0)";
        String claim = "UPDATE " + table + " SET holder = ?, expires_at = " + now + " + ?"
                + " WHERE name = ? AND (holder = ? OR expires_at < " + now + ")";
        return db.inTransaction(tx -> {
            try (PreparedStatement statement = tx.connection().prepareStatement(insert)) {
                statement.setString(1, name);
                statement.executeUpdate();
            }
            try (PreparedStatement statement = tx.connection().prepareStatement(claim)) {
                statement.setString(1, serverId);
                statement.setLong(2, ttl.toMillis());
                statement.setString(3, name);
                statement.setString(4, serverId);
                return statement.executeUpdate() > 0;
            }
        });
    }

    /** Asks the database whether this server holds an unexpired lease. */
    public boolean isHolder(String name) throws SQLException {
        String sql = "SELECT 1 FROM " + table + " WHERE name = ? AND holder = ? AND expires_at >= "
                + db.dialect().nowMillis();
        try (Connection connection = db.dataSource().getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, name);
            statement.setString(2, serverId);
            try (ResultSet row = statement.executeQuery()) {
                return row.next();
            }
        }
    }

    /** Gives the lease up so another server can take it at once. Does nothing if this server isn't the holder. */
    public void release(String name) throws SQLException {
        String sql = "UPDATE " + table + " SET holder = '', expires_at = 0 WHERE name = ? AND holder = ?";
        db.inTransaction(tx -> {
            try (PreparedStatement statement = tx.connection().prepareStatement(sql)) {
                statement.setString(1, name);
                statement.setString(2, serverId);
                statement.executeUpdate();
            }
            return null;
        });
    }

    /**
     * Keeps a lease renewed and runs whileHeld after each successful renewal, on the database thread. Nothing
     * runs until {@link Keeper#start}; tests can call {@link Keeper#tick} directly.
     */
    public Keeper keep(String name, Duration ttl, Runnable whileHeld, Logger logger) {
        checkName(name);
        return new Keeper(name, ttl, whileHeld, logger);
    }

    private static void checkName(String name) {
        if (name == null || name.isEmpty() || name.length() > 64) {
            throw new IllegalArgumentException("Lease name must be 1 to 64 characters: " + name);
        }
    }

    /** A lease this server tries to hold for as long as it runs. */
    public final class Keeper {

        private final String name;
        private final Duration ttl;
        private final Runnable whileHeld;
        private final Logger logger;
        private final AtomicBoolean queued = new AtomicBoolean();
        private volatile long heldUntilNanos;
        private volatile boolean stopped;
        private BukkitTask task;
        private DbExecutor dbExecutor;

        private Keeper(String name, Duration ttl, Runnable whileHeld, Logger logger) {
            this.name = name;
            this.ttl = ttl;
            this.whileHeld = whileHeld;
            this.logger = logger;
        }

        /** Renews (or takes) the lease once and, if held, runs the job. Call on the database thread. */
        public void tick() {
            if (stopped) {
                return;
            }
            // measured before the statement runs, so the local view expires no later than the database's
            long start = System.nanoTime();
            boolean held;
            try {
                held = tryAcquireOrRenew(name, ttl);
            } catch (SQLException e) {
                logger.log(Level.WARNING, "Could not renew lease " + name, e);
                held = false;
            }
            heldUntilNanos = held ? start + ttl.toNanos() : 0;
            if (held) {
                try {
                    whileHeld.run();
                } catch (RuntimeException e) {
                    logger.log(Level.SEVERE, "Job under lease " + name + " failed", e);
                }
            }
        }

        /** True while the last renewal is still within its ttl. */
        public boolean isHeld() {
            long until = heldUntilNanos;
            return until != 0 && System.nanoTime() - until < 0;
        }

        /** Renews every ttl / 3 through the plugin's database thread. */
        public void start(Plugin plugin, DbExecutor dbExecutor) {
            if (task != null) {
                throw new IllegalStateException("Lease keeper for " + name + " is already running");
            }
            stopped = false;
            this.dbExecutor = dbExecutor;
            long period = Math.max(1, ttl.toMillis() / 3 / 50);
            task = plugin.getServer().getScheduler().runTaskTimer(plugin, () -> {
                if (queued.compareAndSet(false, true)) {
                    dbExecutor.submit("lease " + name, () -> {
                        try {
                            tick();
                        } finally {
                            queued.set(false);
                        }
                    });
                }
            }, 0L, period);
        }

        /**
         * Stops renewing and releases the lease on the database thread, after any renewal already queued there.
         * Call from onDisable before {@link DbExecutor#shutdown}.
         */
        public void stop() {
            stopped = true;
            heldUntilNanos = 0;
            if (task != null) {
                task.cancel();
                task = null;
            }
            if (dbExecutor != null) {
                dbExecutor.submit("release lease " + name, () -> {
                    try {
                        release(name);
                    } catch (SQLException e) {
                        logger.log(Level.WARNING, "Could not release lease " + name + "; it frees itself when it expires", e);
                    }
                });
                dbExecutor = null;
            }
        }
    }
}
