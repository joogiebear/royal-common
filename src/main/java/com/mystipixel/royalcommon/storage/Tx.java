package com.mystipixel.royalcommon.storage;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.TreeSet;

/**
 * One open transaction, handed to a {@link TxRunner.Work}. Valid only inside that work; do not keep it.
 */
public final class Tx {

    private final Connection connection;
    private final Dialect dialect;

    Tx(Connection connection, Dialect dialect) {
        this.connection = connection;
        this.dialect = dialect;
    }

    public Connection connection() {
        return connection;
    }

    public Dialect dialect() {
        return dialect;
    }

    /**
     * Locks one row until the transaction ends. On MySQL this is {@code SELECT ... FOR UPDATE}; on SQLite the
     * transaction already holds the database write lock, so it only checks the row exists. A row that does
     * not exist is not locked: insert it first (for example with {@link Dialect#insertIgnore()}).
     *
     * @return whether the row exists
     */
    public boolean lock(String table, String keyColumn, Object key) throws SQLException {
        String sql = "SELECT 1 FROM " + Sql.identifier(table) + " WHERE " + Sql.identifier(keyColumn) + " = ?"
                + dialect.forUpdate();
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            Sql.bind(statement, 1, key);
            try (ResultSet rows = statement.executeQuery()) {
                return rows.next();
            }
        }
    }

    /**
     * Locks several rows of one table in sorted key order, so two transactions locking the same rows never
     * wait on each other in a cycle. Every caller must pass the keys as the same type for the order to match.
     *
     * @return the keys whose rows exist, in the order they were locked
     */
    public <K extends Comparable<? super K>> Set<K> lockAll(String table, String keyColumn, Collection<K> keys)
            throws SQLException {
        Set<K> found = new LinkedHashSet<>();
        for (K key : new TreeSet<>(keys)) {
            if (lock(table, keyColumn, key)) {
                found.add(key);
            }
        }
        return found;
    }
}
