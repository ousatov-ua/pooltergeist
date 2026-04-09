package io.github.ousatov.pooltergeist.manager;

import io.github.ousatov.pooltergeist.stat.StatTracker;
import io.github.ousatov.pooltergeist.vo.config.TaskManagerConfig;
import io.github.ousatov.pooltergeist.vo.manager.WorkUnit;
import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;

/**
 * Manages processing of all units of work. Built on the base of a limited queue.
 *
 * <p>Submit work items via {@link #submit}, then call {@link #waitForCompletion} with the sentinel
 * instance to block until all submitted work is processed.
 *
 * @param <T> work unit type — must implement {@link WorkUnit}
 * @param <R> result type returned by the processing function
 * @author Oleksii Usatov
 */
@Slf4j
public class TaskManager<T extends WorkUnit, R> implements Closeable {

  private final TaskManagerConfig config;
  private final LinkedBlockingDeque<T> workUnitsDeque;
  private final LimitedQueue<Runnable> tasksDeque;
  private final Function<T, R> function;
  private final ExecutorService processorsPool;
  private final ScheduledExecutorService statusExecutor;
  private final ExecutorService taskManagerExecutor;
  private final StatTracker stats;
  private final CountDownLatch dispatchDone = new CountDownLatch(1);
  private volatile boolean finished;

  /**
   * Creates and starts the task manager.
   *
   * @param config tuning parameters
   * @param function processing function applied to each work unit
   */
  public TaskManager(TaskManagerConfig config, Function<T, R> function) {
    this.config = config;
    this.function = function;
    final int unitsOfWorkDequeSize = config.getWorkUnitsDequeSize();
    final int tasksDequeSize = config.getTasksDequeSize();
    final var nThreads = config.getEventProcessingParallelism();
    this.workUnitsDeque = new LinkedBlockingDeque<>(unitsOfWorkDequeSize);
    this.tasksDeque = new LimitedQueue<>(tasksDequeSize);
    this.processorsPool =
        new ThreadPoolExecutor(nThreads, nThreads, 0L, TimeUnit.MILLISECONDS, tasksDeque);
    this.statusExecutor = Executors.newScheduledThreadPool(1);
    this.stats = new StatTracker(config.getLogForRecordCount());
    statusExecutor.scheduleAtFixedRate(
        () ->
            log.info(
                "Total values submitted={}, workUnitsDeque size={}, tasksDeque size={}",
                stats.getTotalSubmitted(),
                workUnitsDeque.size(),
                tasksDeque.size()),
        1,
        1,
        TimeUnit.MINUTES);
    this.taskManagerExecutor = Executors.newFixedThreadPool(1);
    this.taskManagerExecutor.execute(this::dispatch);
  }

  private static void shutdownAndAwait(
      ExecutorService executor, boolean now, long timeout, TimeUnit unit, String name)
      throws InterruptedException {
    if (now) {
      executor.shutdownNow();
    } else {
      executor.shutdown();
    }
    boolean terminated = executor.awaitTermination(timeout, unit);
    log.info("{} is terminated={}", name, terminated);
  }

  @SuppressWarnings("java:S135")
  private void dispatch() {
    try {
      while (true) {
        T workUnit = null;
        try {
          workUnit = workUnitsDeque.take();
          if (workUnit == workUnit.getLastWorkUnit()) {
            log.info("Last task is reached, workUnitsDeque is empty={}", workUnitsDeque.isEmpty());
            finished = true;
            break;
          }
          final var finalWorkUnit = workUnit;
          processorsPool.submit(
              () -> recordStatistics(finalWorkUnit, function.apply(finalWorkUnit)));
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          log.error("Dispatch interrupted", ie);
          break;
        } catch (Exception e) {
          log.error("Could not process workUnit={}", workUnit, e);
        }
      }
    } finally {
      dispatchDone.countDown();
    }
  }

  /**
   * @return true when the dispatch loop has exited and the processor pool has terminated
   */
  public boolean isFinished() {
    return finished && processorsPool.isTerminated();
  }

  /**
   * Submits a work unit for processing.
   *
   * @param workUnit T
   * @throws InterruptedException if the calling thread is interrupted while waiting for queue space
   */
  public void submit(T workUnit) throws InterruptedException {
    workUnitsDeque.put(workUnit);
  }

  /**
   * Enqueues the sentinel, waits for the dispatch loop to finish, then shuts down the processor
   * pool and waits for all in-flight tasks to complete.
   *
   * @param lastWorkUnit sentinel produced by {@link WorkUnit#getLastWorkUnit()}
   */
  public void waitForCompletion(T lastWorkUnit) {
    try {
      workUnitsDeque.put(lastWorkUnit);
      dispatchDone.await();
      log.info("Dispatch finished, draining processor pool...");
      processorsPool.shutdown();
      boolean terminated =
          processorsPool.awaitTermination(
              config.getWaitTimeForAllTasksFinishedMinute(), TimeUnit.MINUTES);
      log.info("All tasks are executed, terminated={}", terminated);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      log.error("Exception during finishing", e);
    }
  }

  /** Logs processing statistics collected so far. */
  public void logStatistics() {
    stats.logSummary();
  }

  /**
   * Override to mark a result as an error for statistics purposes.
   *
   * @param result R
   * @return true if the result represents a processing error
   */
  protected boolean isInError(R result) {
    return false;
  }

  private void recordStatistics(WorkUnit workUnit, R result) {
    stats.mark(workUnit, isInError(result));
  }

  @Override
  public void close() throws IOException {
    try {
      log.info("Closing task manager...");
      shutdownAndAwait(
          processorsPool,
          false,
          config.getWaitTimeForAllTasksFinishedMinute(),
          TimeUnit.MINUTES,
          "ProcessorsPool");
      shutdownAndAwait(statusExecutor, true, 1, TimeUnit.MINUTES, "StatusExecutor");
      shutdownAndAwait(taskManagerExecutor, true, 1, TimeUnit.MINUTES, "TaskManagerExecutor");
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Cannot close resources", e);
    }
  }
}
