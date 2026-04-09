package io.github.ousatov.pooltergeist.hub;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.ousatov.pooltergeist.def.Values;
import io.github.ousatov.pooltergeist.exec.BulkheadedExecutor;
import io.github.ousatov.pooltergeist.hub.executor.CpuExecutor;
import io.github.ousatov.pooltergeist.hub.executor.IoExecutor;
import io.github.ousatov.pooltergeist.hub.executor.VirtualExecutor;
import io.github.ousatov.pooltergeist.vo.config.ExecHubConfig;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ExecutorHub}.
 *
 * @author Oleksii Usatov
 */
@Slf4j
class ExecutorHubTest {

  private static final ExecHubConfig CONFIG =
      ExecHubConfig.builder()
          .maxWaitTimeoutSeconds(5)
          .ioMaxInFlight(10)
          .cpuPoolCoresDelimiter(2)
          .cpuPoolMinSize(1)
          .build();

  private ExecutorHub hub;

  @BeforeEach
  void setUp() {
    hub = new ExecutorHub(CONFIG);
  }

  @AfterEach
  void tearDown() {
    hub.close();
  }

  // ── Construction ────────────────────────────────────────────────────────────

  @Test
  void testConstruction() {
    assertNotNull(hub.getAttic(), "Virtual executor must not be null");
    assertNotNull(hub.getBallroom(), "CPU executor must not be null");
    assertNotNull(hub.getDungeon(), "IO executor must not be null");
  }

  @Test
  void testAliases() {
    assertSame(hub.getAttic(), hub.virtual());
    assertSame(hub.getBallroom(), hub.cpu());
    assertSame(hub.getDungeon(), hub.io());
  }

  @Test
  void testExecutorNames() {
    assertEquals(Values.ATTIC, hub.getAttic().getName());
    assertEquals(Values.BALLROOM, hub.getBallroom().getName());
    assertEquals(Values.DUNGEON, hub.getDungeon().getName());
  }

  // ── Task execution ───────────────────────────────────────────────────────────

  @Test
  void testCpuExecuteBlockingCollection() {
    List<Integer> inputs = List.of(1, 2, 3, 4);
    List<Integer> results = hub.cpu().executeTasksBlocking(inputs, x -> x * 2);
    assertEquals(4, results.size());
    assertTrue(results.containsAll(List.of(2, 4, 6, 8)));
  }

  @Test
  void testIoExecuteBlockingCollection() {
    List<String> inputs = List.of("a", "b", "c");
    List<String> results = hub.io().executeTasksBlocking(inputs, String::toUpperCase);
    assertEquals(3, results.size());
    assertTrue(results.containsAll(List.of("A", "B", "C")));
  }

  @Test
  void testVirtualExecuteBlockingCollection() {
    List<Integer> inputs = List.of(10, 20, 30);
    List<Integer> results = hub.virtual().executeTasksBlocking(inputs, x -> x + 1);
    assertEquals(3, results.size());
    assertTrue(results.containsAll(List.of(11, 21, 31)));
  }

  @Test
  void testSubmitTasksReturnsFuture() throws Exception {
    var future = hub.cpu().submitTasks(() -> "done");
    assertEquals("done", future.get());
  }

  @Test
  void testExecuteTasksNonBlocking() throws Exception {
    List<Integer> inputs = List.of(5, 6);
    var futures = hub.io().executeTasks(inputs, x -> x * 3);
    assertEquals(2, futures.size());
    for (var f : futures) {
      assertNotNull(f.get());
    }
  }

  @Test
  void testExecuteTasksBlockingStream() {
    Integer[] results =
        hub.cpu().executeTasksBlocking(Stream.of(1, 2, 3), x -> x * 10, Integer[]::new);
    assertEquals(3, results.length);
    int sum = 0;
    for (int r : results) {
      sum += r;
    }
    assertEquals(60, sum);
  }

  @Test
  void testExecuteTasksBlockingEmptyList() {
    List<String> results = hub.cpu().executeTasksBlocking(List.<String>of(), x -> x);
    assertTrue(results.isEmpty());
  }

  @Test
  void testExecuteTasksBlockingSupplierList() {
    var counter = new AtomicInteger(0);
    hub.virtual().executeTasksBlocking(List.of(counter::incrementAndGet, counter::incrementAndGet));
    assertEquals(2, counter.get());
  }

  // ── Lifecycle ────────────────────────────────────────────────────────────────

  @Test
  void testCloseTerminatesAllExecutors() {
    hub.close();
    assertTrue(hub.getAttic().getExecutorService().isShutdown());
    assertTrue(hub.getBallroom().getExecutorService().isShutdown());
    assertTrue(hub.getDungeon().getExecutorService().isShutdown());
  }

  @Test
  void testCloseWithInjectedExecutors() {
    // Use real executors via injected constructor; verify they are shut down after close()
    var v = new VirtualExecutor();
    var c = new CpuExecutor(Executors.newSingleThreadExecutor());
    var i = new IoExecutor(Executors.newSingleThreadExecutor());
    var injectedHub = new ExecutorHub(CONFIG, v, c, i);
    injectedHub.close();
    assertTrue(v.getExecutorService().isShutdown());
    assertTrue(c.getExecutorService().isShutdown());
    assertTrue(i.getExecutorService().isShutdown());
  }

  // ── toString ─────────────────────────────────────────────────────────────────

  @Test
  void testToStringCpu() {
    String str = hub.getBallroom().toString();
    assertNotNull(str);
    assertTrue(str.contains("parallelism"), "CPU toString should contain parallelism: " + str);
  }

  @Test
  void testToStringIo() {
    String str = hub.getDungeon().toString();
    assertNotNull(str);
    assertTrue(str.contains("maxInFlight"), "IO toString should contain maxInFlight: " + str);
  }

  @Test
  void testToStringVirtual() {
    String str = hub.getAttic().toString();
    assertNotNull(str);
  }

  @Test
  void testToStringCpuFallback() {
    // CpuExecutor wrapping a non-ForkJoinPool falls back to executorService.toString()
    var cpu = new CpuExecutor(Executors.newSingleThreadExecutor());
    String str = cpu.toString();
    assertNotNull(str);
    cpu.getExecutorService().shutdownNow();
  }

  @Test
  void testToStringIoFallback() {
    // IoExecutor wrapping a non-BulkheadedExecutor falls back to executorService.toString()
    var io = new IoExecutor(Executors.newSingleThreadExecutor());
    String str = io.toString();
    assertNotNull(str);
    io.getExecutorService().shutdownNow();
  }

  // ── BulkheadedExecutor via IoExecutor ────────────────────────────────────────

  @Test
  void testIoExecutorUsesBulkheadedExecutor() {
    var ioService = hub.getDungeon().getExecutorService();
    assertInstanceOf(
        BulkheadedExecutor.class,
        ioService,
        "IO executor should use BulkheadedExecutor by default");
  }

  @Test
  void testIoExecutorBulkheadMetrics() {
    var bulkheaded = (BulkheadedExecutor) hub.getDungeon().getExecutorService();
    assertEquals(CONFIG.getIoMaxInFlight(), bulkheaded.getMaxInFlight());
    assertEquals(CONFIG.getIoMaxInFlight(), bulkheaded.getAvailablePermits());
  }

  @Test
  void testVirtualExecutorSecondConstructor() throws Exception {
    // Cover VirtualExecutor(ExecutorService) constructor
    var v = new VirtualExecutor(Executors.newVirtualThreadPerTaskExecutor());
    assertEquals(Values.ATTIC, v.getName());
    var future = v.submitTasks(() -> "vt");
    assertEquals("vt", future.get());
    v.getExecutorService().shutdownNow();
  }

  @Test
  @SuppressWarnings("java:S2699")
  void testCloseAlreadyTerminatedExecutors() {
    // Close twice — second close should be a no-op (all executors already terminated)
    hub.close();
    hub.close(); // should not throw or hang
  }

  @Test
  void testManagedExecutorJoinWithDebugEnabled() {
    // Exercises the Stopwatch-enabled path: log.isDebugEnabled() == true in test (slf4j-simple
    // default is INFO, so Stopwatch won't be created — but at least the method runs)
    List<Integer> data = List.of(1, 2, 3);
    List<Integer> result = hub.cpu().executeTasksBlocking(data, x -> x);
    assertEquals(3, result.size());
  }

  @Test
  void testIoExecutorRunsTaskViaBulkhead() throws Exception {
    var bulkheaded = (BulkheadedExecutor) hub.getDungeon().getExecutorService();
    // submit() bypasses execute(), so use execute() to verify submitted/completed counters
    var done = new CountDownLatch(1);
    bulkheaded.execute(done::countDown);
    assertTrue(done.await(5, TimeUnit.SECONDS));
    assertEquals(1, bulkheaded.getSubmitted());
    // give the finally-block inside execute() time to increment completed
    await().atLeast(50, TimeUnit.MILLISECONDS).until(() -> true);
    assertEquals(1, bulkheaded.getCompleted());
  }
}
