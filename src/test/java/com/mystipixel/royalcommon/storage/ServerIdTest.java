package com.mystipixel.royalcommon.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ServerIdTest {

    @TempDir
    Path dir;

    @Test
    void configuredIdWins() throws Exception {
        assertEquals("lobby-1", ServerId.resolve("  lobby-1 ", dir));
        assertFalse(Files.exists(dir.resolve(ServerId.FILE_NAME)));
    }

    @Test
    void generatedIdIsKeptAcrossRestarts() throws Exception {
        Path folder = dir.resolve("plugins/RoyalBank");
        String first = ServerId.resolve("", folder);
        assertTrue(first.startsWith("server-"));
        assertEquals(first, ServerId.resolve(null, folder));
        assertEquals(first, Files.readString(folder.resolve(ServerId.FILE_NAME)).trim());
        try (var files = Files.list(folder)) {
            assertEquals(1, files.count(), "no temp file left behind");
        }
    }

    @Test
    void rejectsIdsThatCannotBeStored() {
        assertThrows(IllegalArgumentException.class, () -> ServerId.resolve("has space", dir));
        assertThrows(IllegalArgumentException.class, () -> ServerId.resolve("x".repeat(65), dir));
    }
}
