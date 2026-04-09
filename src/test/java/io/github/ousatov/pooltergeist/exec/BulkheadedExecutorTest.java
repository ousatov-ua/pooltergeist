package io.github.ousatov.pooltergeist.exec;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link BulkheadedExecutor}.
 *
 * @author Oleksii Usatov
 */
@Slf4j
class BulkheadedExecutorTest {

  private BulkheadedExecutor executor;

  @BeforeEach
  void setUp() {
    executor = new BulkheadedExecutor(4);
  }

  @AfterEach
  void tearDown() {
    executor.shutdownNow();
  }

  @Test
  void testExecuteRunsTask() throws Exception {
    var latch = new CountDownLatch(1);
    executor.execute(latch::countDown);
    assertTrue(latch.await(5, TimeUnit.SECONDS), "Task should have run");
  }

  @Test
  void testSubmitRunnable() {
    var future = executor.submit(() -> log.info("running"));
    assertDoesNotThrow(() -> future.get(5, TimeUnit.SECONDS));
  }

  @Test
  void testSubmitRunnableWithResult() throws Exception {
    var future = executor.submit(() -> {}, "result");
    assertEquals("result", future.get(5, TimeUnit.SECONDS));
  }

  @Test
  void testSubmitCallable() throws Exception {
    var future = executor.submit(() -> "hello");
    assertEquals("hello", future.get(5, TimeUnit.SECONDS));
  }

  @Test
  void testInvokeAll() throws Exception {
    List<Callable<Integer>> tasks = List.of(() -> 1, () -> 2, () -> 3);
    var futures = executor.invokeAll(tasks);
    assertEquals(3, futures.size());
    for (var f : futures) {
      assertTrue(f.isDone());
    }
  }

  @Test
  void testInvokeAllWithTimeout() throws Exception {
    List<Callable<Integer>> tasks = List.of(() -> 1, () -> 2);
    var futures = executor.invokeAll(tasks, 5, TimeUnit.SECONDS);
    assertEquals(2, futures.size());
    for (var f : futures) {
      assertTrue(f.isDone());
    }
  }

  @Test
  void testInvokeAny() throws Exception {
    List<Callable<String>> tasks = List.of(() -> "a", () -> "b");
    String result = executor.invokeAny(tasks);
    assertNotNull(result);
    assertTrue(result.equals("a") || result.equals("b"));
  }

  @Test
  void testInvokeAnyWithTimeout() throws Exception {
    List<Callable<String>> tasks = List.of(() -> "x");
    String result = executor.invokeAny(tasks, 5, TimeUnit.SECONDS);
    assertEquals("x", result);
  }

  @Test
  void testShutdownRejectsExecute() {
    executor.shutdown();
    assertTrue(executor.isShutdown());
    assertThrows(RejectedExecutionException.class, () -> executor.execute(() -> {}));
  }

  @Test
  void testShutdownRejectsSubmitRunnable() {
    executor.shutdown();
    assertThrows(RejectedExecutionException.class, () -> executor.submit(() -> {}));
  }

  @Test
  void testShutdownRejectsSubmitCallable() {
    executor.shutdown();
    assertThrows(
        RejectedExecutionException.class, () -> executor.submit((Callable<Void>) () -> null));
  }

  @Test
  @SuppressWarnings("java:S5778")
  void testShutdownRejectsInvokeAll() {
    executor.shutdown();
    assertThrows(RejectedExecutionException.class, () -> executor.invokeAll(List.of(() -> "x")));
  }

  @Test
  @SuppressWarnings("java:S5778")
  void testShutdownRejectsInvokeAny() {
    executor.shutdown();
    assertThrows(RejectedExecutionException.class, () -> executor.invokeAny(List.of(() -> "x")));
  }

  @Test
  void testShutdownNow() {
    var remaining = executor.shutdownNow();
    assertTrue(executor.isShutdown());
    assertNotNull(remaining);
  }

  @Test
  void testIsTerminated() throws Exception {
    executor.shutdown();
    assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    assertTrue(executor.isTerminated());
  }

  @Test
  void testAwaitTerminationNotShutdown() throws Exception {
    assertFalse(executor.awaitTermination(1, TimeUnit.MILLISECONDS));
  }

  @Test
  void testMetrics() throws Exception {
    assertEquals(4, executor.getMaxInFlight());
    assertEquals(4, executor.getAvailablePermits());
    assertEquals(4, executor.getMaxAvailableInFlight());
    assertEquals(0, executor.getInFlight());
    assertEquals(0, executor.getWaiting());
    assertEquals(0, executor.getSubmitted());
    assertEquals(0, executor.getCompleted());

    // execute() is the path that increments submitted/completed counters
    var latch = new CountDownLatch(1);
    var done = new CountDownLatch(1);
    executor.execute(
        () -> {
          try {
            var _ = latch.await(5, TimeUnit.SECONDS);
          } catch (InterruptedException _) {
            Thread.currentThread().interrupt();
          } finally {
            done.countDown();
          }
        });
    await().atLeast(100, TimeUnit.MILLISECONDS).until(() -> true);
    assertEquals(1, executor.getSubmitted());
    latch.countDown();
    assertTrue(done.await(5, TimeUnit.SECONDS));
    executor.shutdown();
    var _ = executor.awaitTermination(5, TimeUnit.SECONDS);
    assertEquals(1, executor.getCompleted());
  }

  @Test
  void testBackpressure() throws Exception {
    // Create executor with 1 permit to make backpressure easy to observe
    try (var single =
        new BulkheadedExecutor(Executors.newVirtualThreadPerTaskExecutor(), 1, false)) {
      var holding = new CountDownLatch(1);
      var started = new CountDownLatch(1);

      // Occupy the single permit
      single.execute(
          () -> {
            started.countDown();
            try {
              var _ = holding.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException _) {
              Thread.currentThread().interrupt();
            }
          });
      assertTrue(started.await(5, TimeUnit.SECONDS));

      // Submit a second task in a background thread — it will block waiting for the permit
      var waitingCount = new AtomicInteger(0);
      Thread blocked =
          Thread.ofVirtual()
              .start(
                  () -> {
                    waitingCount.incrementAndGet();
                    single.execute(() -> {});
                    waitingCount.decrementAndGet();
                  });

      // Give the blocked thread time to arrive at the permit
      await().atMost(200, TimeUnit.MILLISECONDS).until(() -> true);
      assertTrue(
          single.getWaiting() > 0 || waitingCount.get() > 0,
          "At least one thread should be waiting for a permit");

      holding.countDown();
      blocked.join(5000);
    }
  }

  @Test
  void testDefaultConstructorUsesVirtualThreads() throws Exception {
    try (var bulkheaded = new BulkheadedExecutor(2)) {
      var counter = new AtomicInteger(0);
      var f1 = bulkheaded.submit(counter::incrementAndGet);
      var f2 = bulkheaded.submit(counter::incrementAndGet);
      f1.get(5, TimeUnit.SECONDS);
      f2.get(5, TimeUnit.SECONDS);
      assertEquals(2, counter.get());
    }
  }

  @Test
  void testFairConstructor() throws Exception {
    try (var fair = new BulkheadedExecutor(Executors.newVirtualThreadPerTaskExecutor(), 2, true)) {
      var future = fair.submit(() -> "fair");
      assertEquals("fair", future.get(5, TimeUnit.SECONDS));
    }
  }

  @Test
  void testInvalidMaxInFlight() {
    assertThrows(IllegalArgumentException.class, () -> new BulkheadedExecutor(0));
    assertThrows(IllegalArgumentException.class, () -> new BulkheadedExecutor(-1));
  }

  @Test
  void testNullDelegate() {
    assertThrows(NullPointerException.class, () -> new BulkheadedExecutor(null, 1, false));
  }

  @Test
  void testSubmitAfterShutdownNow() {
    executor.shutdownNow();
    assertThrows(RejectedExecutionException.class, () -> executor.submit(() -> {}));
  }

  @Test
  @SuppressWarnings("java:S5778")
  void testInvokeAllWithTimeoutAfterShutdown() {
    executor.shutdown();
    assertThrows(
        RejectedExecutionException.class,
        () -> executor.invokeAll(List.of(() -> "x"), 1, TimeUnit.SECONDS));
  }

  @Test
  @SuppressWarnings("java:S5778")
  void testInvokeAnyWithTimeoutAfterShutdown() {
    executor.shutdown();
    assertThrows(
        RejectedExecutionException.class,
        () -> executor.invokeAny(List.of(() -> "x"), 1, TimeUnit.SECONDS));
  }

  @Test
  void testMultipleTasksComplete() throws Exception {
    int count = 10;
    var counter = new AtomicInteger(0);
    var futures = new java.util.ArrayList<java.util.concurrent.Future<?>>(count);
    for (int i = 0; i < count; i++) {
      futures.add(executor.submit(counter::incrementAndGet));
    }
    for (var f : futures) {
      f.get(10, TimeUnit.SECONDS);
    }
    assertEquals(count, counter.get());
  }

  @Test
  void testCompletedNotIncrementedOnDelegateRejection() {
    // Use a delegate already shut down to trigger RejectedExecutionException from delegate
    var shutDelegate = Executors.newSingleThreadExecutor();
    shutDelegate.shutdown();
    try (var be = new BulkheadedExecutor(shutDelegate, 2, false)) {
      assertThrows(Exception.class, () -> be.execute(() -> {}));
      // completed should not have been incremented since task was never run
      assertEquals(0, be.getCompleted());
    }
  }
}
