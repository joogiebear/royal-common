package com.mystipixel.royalcommon.storage;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.UUID;
import java.util.regex.Pattern;

final class Sql {

    // table and column names are spliced into SQL text, so only plain identifiers are accepted
    private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_]{0,63}");

    private Sql() {
    }

    static String identifier(String name) {
        if (name == null || !IDENTIFIER.matcher(name).matches()) {
            throw new IllegalArgumentException("Not a plain SQL identifier: " + name);
        }
        return name;
    }

    // table name prefix such as "royalbank"; the longest suffix added is "_schema_version"
    static String prefix(String prefix) {
        if (prefix == null || prefix.length() > 40) {
            throw new IllegalArgumentException("Table prefix must be 1 to 40 characters: " + prefix);
        }
        return identifier(prefix);
    }

    static void bind(PreparedStatement statement, int index, Object value) throws SQLException {
        if (value instanceof UUID) {
            statement.setString(index, value.toString());
        } else {
            statement.setObject(index, value);
        }
    }
}
