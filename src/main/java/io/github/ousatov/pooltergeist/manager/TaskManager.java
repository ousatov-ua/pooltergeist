package io.github.ousatov.pooltergeist.manager;

import io.github.ousatov.pooltergeist.stat.StatTracker;
import io.github.ousatov.pooltergeist.vo.config.TaskManagerConfig;
import io.github.ousatov.pooltergeist.vo.manager.WorkUnit;
import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;

/**
 * TaskManager
 *
 * <p>Manages processing of all units of work. Built on the base of a limited queue.
 *
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
  private volatile boolean finished;

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

  @SuppressWarnings("java:S135")
  private void dispatch() {
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
        processorsPool.submit(() -> recordStatistics(finalWorkUnit, function.apply(finalWorkUnit)));
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        log.error("Dispatch interrupted", ie);
        break;
      } catch (Exception e) {
        log.error("Could not process workUnit={}", workUnit, e);
      }
    }
  }

  /**
   * @return true when the dispatch loop and all task queues are drained
   */
  public boolean isFinished() {
    return finished && workUnitsDeque.isEmpty() && tasksDeque.isEmpty();
  }

  /**
   * Submit a task
   *
   * @param workUnit T
   * @throws InterruptedException exception
   */
  public void submit(T workUnit) throws InterruptedException {
    workUnitsDeque.put(workUnit);
  }

  /**
   * Put the last value to the queue and wait for all submitted tasks to finish.
   *
   * @param lastWorkUnit last unit of work
   */
  public void waitForCompletion(T lastWorkUnit) {
    try {
      workUnitsDeque.put(lastWorkUnit);
      while (!isFinished()) {
        log.info("Waiting for all tasks left the queue");
        TimeUnit.SECONDS.sleep(config.getWaitTimeForCheckingFinishedSeconds());
      }
      log.info("All tasks are executed");
    } catch (Exception ine) {
      Thread.currentThread().interrupt();
      log.error("Exception during finishing", ine);
    }
  }

  /** Log statistics */
  public void logStatistics() {
    stats.logSummary();
  }

  /**
   * Check if processing was marked as failed
   *
   * @param result R
   * @return true if is in the error state
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
      log.info("Waiting for all submitted tasks finished");
      processorsPool.shutdown();
      var terminated =
          processorsPool.awaitTermination(
              config.getWaitTimeForAllTasksFinishedMinute(), TimeUnit.MINUTES);
      log.info("ProcessorsPool is terminated={}", terminated);
      log.info("All submitted tasks finished");
      statusExecutor.shutdownNow();
      terminated = statusExecutor.awaitTermination(1, TimeUnit.MINUTES);
      log.info("StatusExecutor is finished={}", terminated);
      log.info("All tasks left the queue");
      log.info("Shutdown taskManager...");
      taskManagerExecutor.shutdownNow();
      terminated = taskManagerExecutor.awaitTermination(1, TimeUnit.MINUTES);
      log.info("Task manager is terminated={}", terminated);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Cannot close resources", e);
    }
  }
}
