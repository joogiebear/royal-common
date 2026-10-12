package com.mystipixel.royalcommon.storage;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * The name this server goes by in leases and hand-off rows. Uses the configured id when there is one,
 * otherwise generates one and keeps it in {@code server-id.txt} in the plugin's data folder.
 *
 * <p>A server folder copied to make a second server carries the same file, and both servers then claim to
 * be the same one. Set the id in the config on such servers, or delete the copied file.
 */
public final class ServerId {

    public static final String FILE_NAME = "server-id.txt";

    private static final Pattern VALID = Pattern.compile("[A-Za-z0-9._-]{1,64}");

    private ServerId() {
    }

    /**
     * @param configured the id from the plugin's config; blank or null to use the generated one
     */
    public static String resolve(String configured, Path dataFolder) throws IOException {
        if (configured != null && !configured.isBlank()) {
            return validate(configured.trim(), "configured server-id");
        }
        Path file = dataFolder.resolve(FILE_NAME);
        if (Files.exists(file)) {
            return validate(Files.readString(file, StandardCharsets.UTF_8).trim(), file.toString());
        }
        String id = "server-" + UUID.randomUUID().toString().substring(0, 8);
        Files.createDirectories(dataFolder);
        Path temp = Files.createTempFile(dataFolder, FILE_NAME, ".tmp");
        Files.writeString(temp, id + System.lineSeparator(), StandardCharsets.UTF_8);
        try {
            Files.move(temp, file, StandardCopyOption.ATOMIC_MOVE);
        } catch (AtomicMoveNotSupportedException e) {
            Files.move(temp, file, StandardCopyOption.REPLACE_EXISTING);
        }
        return id;
    }

    private static String validate(String id, String source) {
        if (!VALID.matcher(id).matches()) {
            throw new IllegalArgumentException("Invalid " + source + " '" + id
                    + "': use 1 to 64 letters, digits, dots, dashes or underscores");
        }
        return id;
    }
}
