package com.mystipixel.royalcommon.storage;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * At most one in-flight action per key, usually a player's UUID: a second click while a withdrawal is still
 * on the database thread is refused instead of queued. Safe to use from any thread.
 *
 * <pre>{@code
 * if (!busy.tryEnter(uuid)) return;          // main thread, on click
 * db.submit("withdraw", () -> ...)
 *   .whenComplete((r, e) -> busy.exit(uuid)); // always release, also on failure
 * }</pre>
 */
public final class BusyGuard<K> {

    private final Set<K> inFlight = ConcurrentHashMap.newKeySet();

    /** @return true if the key was free and is now taken by the caller */
    public boolean tryEnter(K key) {
        return inFlight.add(key);
    }

    public void exit(K key) {
        inFlight.remove(key);
    }

    public boolean isBusy(K key) {
        return inFlight.contains(key);
    }
}
