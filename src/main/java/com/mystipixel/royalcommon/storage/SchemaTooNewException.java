package com.mystipixel.royalcommon.storage;

/**
 * The database was migrated by a newer build of the plugin than the one running. Running on would risk
 * writing data the newer schema doesn't expect, so the plugin should log this and disable itself.
 */
public final class SchemaTooNewException extends IllegalStateException {

    private final int databaseVersion;
    private final int supportedVersion;

    public SchemaTooNewException(String table, int databaseVersion, int supportedVersion) {
        super("Database schema version " + databaseVersion + " (" + table + ") is newer than this build supports ("
                + supportedVersion + "). Update the plugin on this server to the version the other servers run;"
                + " do not point an older build at this database.");
        this.databaseVersion = databaseVersion;
        this.supportedVersion = supportedVersion;
    }

    public int databaseVersion() {
        return databaseVersion;
    }

    public int supportedVersion() {
        return supportedVersion;
    }
}
