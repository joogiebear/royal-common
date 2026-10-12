package com.mystipixel.royalcommon.storage;

import java.util.concurrent.Executor;
import org.bukkit.plugin.Plugin;

/**
 * Where results go back to after database work: the server's main thread in a plugin, the calling thread
 * in tests. Use it as the executor of {@code thenAcceptAsync} and similar.
 */
@FunctionalInterface
public interface MainThread extends Executor {

    /**
     * Runs tasks on the next server tick. Once the plugin is disabled the scheduler refuses tasks, so they are
     * dropped with a warning; don't rely on a main-thread callback for anything that must happen at shutdown.
     */
    static MainThread bukkit(Plugin plugin) {
        return task -> {
            if (!plugin.isEnabled()) {
                plugin.getLogger().warning("Skipped a main-thread callback because the plugin is disabled.");
                return;
            }
            plugin.getServer().getScheduler().runTask(plugin, task);
        };
    }

    /** Runs tasks straight away on the calling thread. For tests. */
    static MainThread direct() {
        return Runnable::run;
    }
}
