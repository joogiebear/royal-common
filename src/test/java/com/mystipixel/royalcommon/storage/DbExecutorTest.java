package com.mystipixel.royalcommon.storage;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DbExecutorTest {

    private final Logger logger = Logger.getLogger("DbExecutorTest");

    @Test
    void runsInSubmissionOrderOnANamedThread() throws Exception {
        DbExecutor db = new DbExecutor("Test", MainThread.direct(), logger);
        List<Integer> order = new CopyOnWriteArrayList<>();
        CompletableFuture<String> last = null;
        for (int i = 0; i < 100; i++) {
            int n = i;
            last = db.submit("task " + i, () -> {
                order.add(n);
                return Thread.currentThread().getName();
            });
        }
        assertEquals("Test-DB", last.get(5, TimeUnit.SECONDS));
        assertEquals(java.util.stream.IntStream.range(0, 100).boxed().toList(), order);
        assertTrue(db.shutdown(Duration.ofSeconds(5)).drained());
    }

    @Test
    void handsResultsToTheMainThreadExecutor() throws Exception {
        List<String> ranOn = new CopyOnWriteArrayList<>();
        MainThread recording = task -> {
            ranOn.add("main");
            task.run();
        };
        DbExecutor db = new DbExecutor("Test", recording, logger);
        CompletableFuture<Void> done = db.submit("load", () -> 42)
                .thenAcceptAsync(value -> ranOn.add("got " + value), db.mainThread());
        done.get(5, TimeUnit.SECONDS);
        assertEquals(List.of("main", "got 42"), ranOn);
        db.shutdown(Duration.ofSeconds(5));
    }

    @Test
    void failedWorkCompletesExceptionallyAndLaterWorkStillRuns() throws Exception {
        DbExecutor db = new DbExecutor("Test", MainThread.direct(), logger);
        CompletableFuture<Object> failed = db.submit("bad", () -> {
            throw new IllegalStateException("boom");
        });
        ExecutionException e = assertThrows(ExecutionException.class, () -> failed.get(5, TimeUnit.SECONDS));
        assertInstanceOf(IllegalStateException.class, e.getCause());
        assertEquals(1, db.submit("good", () -> 1).get(5, TimeUnit.SECONDS));
        db.shutdown(Duration.ofSeconds(5));
    }

    @Test
    void knowsItsOwnThread() throws Exception {
        DbExecutor db = new DbExecutor("Test", MainThread.direct(), logger);
        assertFalse(db.isDbThread());
        assertTrue(db.submit("check", db::isDbThread).get(5, TimeUnit.SECONDS));
        db.shutdown(Duration.ofSeconds(5));
    }

    @Test
    void shutdownDrainsQueuedWork() throws Exception {
        DbExecutor db = new DbExecutor("Test", MainThread.direct(), logger);
        List<Integer> done = new CopyOnWriteArrayList<>();
        for (int i = 0; i < 20; i++) {
            int n = i;
            db.submit("save " + i, () -> {
                Thread.sleep(2);
                done.add(n);
                return null;
            });
        }
        DbExecutor.ShutdownReport report = db.shutdown(Duration.ofSeconds(5));
        assertTrue(report.drained());
        assertEquals(20, done.size());
    }

    @Test
    void shutdownReportsWhatWasLeftAfterTheTimeout() throws Exception {
        DbExecutor db = new DbExecutor("Test", MainThread.direct(), logger);
        CountDownLatch release = new CountDownLatch(1);
        db.submit("stuck", () -> {
            release.await();
            return null;
        });
        CompletableFuture<Object> queued = db.submit("save alice", () -> null);
        db.submit("save bob", () -> null);

        DbExecutor.ShutdownReport report = db.shutdown(Duration.ofMillis(200));

        assertFalse(report.drained());
        assertEquals(List.of("stuck", "save alice", "save bob"), report.leftOver());
        assertTrue(queued.isCompletedExceptionally());
        CompletableFuture<Object> late = db.submit("too late", () -> null);
        assertTrue(late.isCompletedExceptionally());
    }
}
