package com.mystipixel.royalcommon.storage;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.sql.DataSource;
import org.bukkit.plugin.Plugin;
import org.bukkit.scheduler.BukkitTask;

/**
 * Notices rows another server changed. Each watched row carries a version number that every write raises;
 * this keeps the last version this server saw per key and, every few ticks, asks the database for the
 * current ones. Keys whose version went up are handed to a callback on the main thread, which reloads them.
 *
 * <p>Versions must only ever go up. After this server writes a row itself, report the new version with
 * {@link #seen} so its own write is not mistaken for someone else's. A key seen for the first time by a
 * poll only sets the baseline and is not reported.
 */
public final class VersionPoller<K> {

    /** Reads the current version of each key that has a row. Keys without a row are left out of the map. */
    @FunctionalInterface
    public interface Query<K> {
        Map<K, Long> versions(Connection connection, Collection<K> keys) throws SQLException;
    }

    private static final int CHUNK = 500;

    /**
     * A query for the common case of one table with a key column and a version column. Keys are bound as
     * {@code toString()} and read back with parse, e.g. {@code UUID::fromString}.
     */
    public static <K> Query<K> byColumn(String table, String keyColumn, String versionColumn,
                                        Function<String, K> parse) {
        String select = "SELECT " + Sql.identifier(keyColumn) + ", " + Sql.identifier(versionColumn)
                + " FROM " + Sql.identifier(table) + " WHERE " + keyColumn + " IN (";
        return (connection, keys) -> {
            Map<K, Long> versions = new HashMap<>();
            List<K> all = new ArrayList<>(keys);
            for (int from = 0; from < all.size(); from += CHUNK) {
                List<K> chunk = all.subList(from, Math.min(all.size(), from + CHUNK));
                String sql = select + String.join(", ", Collections.nCopies(chunk.size(), "?")) + ")";
                try (PreparedStatement statement = connection.prepareStatement(sql)) {
                    for (int i = 0; i < chunk.size(); i++) {
                        statement.setString(i + 1, chunk.get(i).toString());
                    }
                    try (ResultSet rows = statement.executeQuery()) {
                        while (rows.next()) {
                            versions.put(parse.apply(rows.getString(1)), rows.getLong(2));
                        }
                    }
                }
            }
            return versions;
        };
    }

    private final DataSource dataSource;
    private final DbExecutor db;
    private final Query<K> query;
    private final Consumer<Set<K>> onChanged;
    private final Object lock = new Object();
    // guarded by lock; each running poll holds the keys forgotten since it took its snapshot
    private final Map<K, Long> lastSeen = new HashMap<>();
    private final List<Set<K>> forgottenDuringPolls = new ArrayList<>();
    private final AtomicBoolean polling = new AtomicBoolean();
    private BukkitTask task;

    /**
     * @param onChanged runs on the main thread with the keys whose version went up; never with an empty set
     */
    public VersionPoller(DataSource dataSource, DbExecutor db, Query<K> query, Consumer<Set<K>> onChanged) {
        this.dataSource = dataSource;
        this.db = db;
        this.query = query;
        this.onChanged = onChanged;
    }

    /** Records a version this server already has, after loading the row or writing it. */
    public void seen(K key, long version) {
        synchronized (lock) {
            lastSeen.merge(key, version, Math::max);
        }
    }

    /** Stops tracking a key, e.g. when its player leaves. A poll already running will not track it again. */
    public void forget(K key) {
        synchronized (lock) {
            lastSeen.remove(key);
            for (Set<K> forgotten : forgottenDuringPolls) {
                forgotten.add(key);
            }
        }
    }

    /**
     * Polls once on the database thread. The returned future completes with the changed keys after the
     * callback has been handed to the main thread.
     */
    public CompletableFuture<Set<K>> poll(Collection<K> keys) {
        List<K> snapshot = List.copyOf(keys);
        Set<K> forgotten = new HashSet<>();
        synchronized (lock) {
            forgottenDuringPolls.add(forgotten);
        }
        CompletableFuture<Set<K>> result = db.submit("version poll", () -> changedSince(snapshot, forgotten));
        result.whenComplete((changed, failure) -> {
            synchronized (lock) {
                // by identity: two polls' sets can be equal
                forgottenDuringPolls.removeIf(set -> set == forgotten);
            }
        });
        return result.thenApply(changed -> {
            if (!changed.isEmpty()) {
                db.mainThread().execute(() -> onChanged.accept(changed));
            }
            return changed;
        });
    }

    private Set<K> changedSince(List<K> keys, Set<K> forgotten) throws SQLException {
        if (keys.isEmpty()) {
            return Set.of();
        }
        Map<K, Long> current;
        try (Connection connection = dataSource.getConnection()) {
            current = query.versions(connection, keys);
        }
        Set<K> changed = new LinkedHashSet<>();
        synchronized (lock) {
            current.forEach((key, version) -> {
                if (forgotten.contains(key)) {
                    return;
                }
                Long before = lastSeen.get(key);
                if (before != null && version > before) {
                    changed.add(key);
                }
                seen(key, version);
            });
        }
        return Set.copyOf(changed);
    }

    /**
     * Polls every periodTicks. keys is read on the main thread each time (e.g. the online players' ids). A
     * poll still waiting on the database thread makes the next one skip.
     */
    public void start(Plugin plugin, long periodTicks, Supplier<? extends Collection<K>> keys) {
        stop();
        task = plugin.getServer().getScheduler().runTaskTimer(plugin, () -> {
            if (!polling.compareAndSet(false, true)) {
                return;
            }
            poll(keys.get()).whenComplete((changed, failure) -> polling.set(false));
        }, periodTicks, periodTicks);
    }

    public void stop() {
        if (task != null) {
            task.cancel();
            task = null;
        }
    }
}
