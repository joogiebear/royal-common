package com.mystipixel.royalcommon.storage;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * One database thread per plugin, named {@code <Plugin>-DB}. Work runs one item at a time in the order it was
 * submitted, so two writes for the same player can never overtake each other.
 *
 * <pre>{@code
 * db.submit("load " + uuid, () -> store.load(uuid))
 *   .thenAcceptAsync(data -> cache.put(uuid, data), db.mainThread());
 * }</pre>
 *
 * Failed work is logged here with its label, and the returned future completes exceptionally.
 */
public final class DbExecutor {

    /**
     * What {@link #shutdown} left behind.
     *
     * @param drained true when every queued item finished before the timeout
     * @param leftOver labels of the items that were still running or had not started
     */
    public record ShutdownReport(boolean drained, List<String> leftOver) {
    }

    private final class Task<T> implements Runnable {
        final String label;
        final Callable<T> work;
        final CompletableFuture<T> future = new CompletableFuture<>();

        Task(String label, Callable<T> work) {
            this.label = label;
            this.work = work;
        }

        @Override
        public void run() {
            running = label;
            try {
                future.complete(work.call());
            } catch (Exception | Error e) {
                logger.log(Level.SEVERE, "Database task failed: " + label, e);
                future.completeExceptionally(e);
            } finally {
                running = null;
            }
        }
    }

    private final String threadName;
    private final MainThread mainThread;
    private final Logger logger;
    private final ThreadPoolExecutor executor;
    private volatile Thread thread;
    private volatile String running;

    public DbExecutor(String pluginName, MainThread mainThread, Logger logger) {
        this.threadName = pluginName + "-DB";
        this.mainThread = mainThread;
        this.logger = logger;
        this.executor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(), r -> {
            Thread t = new Thread(r, threadName);
            t.setDaemon(true);
            thread = t;
            return t;
        });
    }

    public <T> CompletableFuture<T> submit(String label, Callable<T> work) {
        Task<T> task = new Task<>(label, work);
        try {
            executor.execute(task);
        } catch (RejectedExecutionException e) {
            logger.severe("Database task submitted after shutdown, not run: " + label);
            task.future.completeExceptionally(new IllegalStateException(threadName + " is shut down", e));
        }
        return task.future;
    }

    public CompletableFuture<Void> submit(String label, Runnable work) {
        return submit(label, () -> {
            work.run();
            return null;
        });
    }

    public MainThread mainThread() {
        return mainThread;
    }

    /** True on this executor's thread; blocking on one of its own futures there would wait forever. */
    public boolean isDbThread() {
        return Thread.currentThread() == thread;
    }

    /**
     * Stops taking work and waits up to timeout for the queue to drain. Whatever is still queued after that is
     * cancelled, the running item is interrupted, and both are logged and listed in the report. Call from
     * {@code onDisable} after the last save has been submitted.
     */
    public ShutdownReport shutdown(Duration timeout) {
        executor.shutdown();
        boolean drained;
        try {
            drained = executor.awaitTermination(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            drained = false;
        }
        if (drained) {
            return new ShutdownReport(true, List.of());
        }
        List<String> leftOver = new ArrayList<>();
        String stuck = running;
        if (stuck != null) {
            leftOver.add(stuck);
        }
        for (Runnable queued : executor.shutdownNow()) {
            Task<?> task = (Task<?>) queued;
            leftOver.add(task.label);
            task.future.completeExceptionally(new CancellationException(threadName + " shut down before this ran"));
        }
        logger.severe(threadName + " did not finish within " + timeout.toMillis() + " ms; unfinished: " + leftOver);
        return new ShutdownReport(false, List.copyOf(leftOver));
    }
}
