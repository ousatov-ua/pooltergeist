package io.github.ousatov.pooltergeist.hub;

import io.github.ousatov.pooltergeist.hub.executor.CpuExecutor;
import io.github.ousatov.pooltergeist.hub.executor.IoExecutor;
import io.github.ousatov.pooltergeist.hub.executor.ManagedExecutor;
import io.github.ousatov.pooltergeist.hub.executor.VirtualExecutor;
import io.github.ousatov.pooltergeist.vo.config.ExecHubConfig;
import java.util.List;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Manages executor pools for parallel task execution. Provides IO-bound and CPU-bound executors
 * with context propagation.
 *
 * @author Oleksii Usatov
 */
@Slf4j
public class ExecutorHub {

  private final ExecHubConfig config;

  /** Virtual threads (Attic). */
  @Getter private final VirtualExecutor attic;

  /** CPU-bound work (Ballroom). */
  @Getter private final CpuExecutor ballroom;

  /** IO-bound work (Dungeon). */
  @Getter private final IoExecutor dungeon;

  /**
   * Initializes executor pools based on configuration.
   *
   * @param config application configuration
   */
  public ExecutorHub(ExecHubConfig config) {
    this.config = config;
    this.attic = new VirtualExecutor();
    this.ballroom = new CpuExecutor(config);
    this.dungeon = new IoExecutor(config);
  }

  /**
   * Creates an ExecutorHub with pre-constructed executors (useful for testing).
   *
   * @param config application configuration
   * @param attic virtual executor
   * @param ballroom cpu executor
   * @param dungeon io executor
   */
  public ExecutorHub(
      ExecHubConfig config, VirtualExecutor attic, CpuExecutor ballroom, IoExecutor dungeon) {
    this.config = config;
    this.attic = attic;
    this.ballroom = ballroom;
    this.dungeon = dungeon;
  }

  private static void shutdownNow(ManagedExecutor chamber) {
    var executorService = chamber.getExecutorService();
    if (executorService != null) {
      executorService.shutdownNow();
    }
  }

  /**
   * Shuts down all executor pools. All pools receive {@code shutdownNow} first so they wind down
   * concurrently, then each is awaited in turn.
   */
  public void close() {
    log.info("Going to shutdown executors...");
    List<ManagedExecutor> executors = List.of(attic, ballroom, dungeon);
    for (ManagedExecutor e : executors) {
      if (e == null) {
        continue;
      }
      log.info("Shutdown issued for {}", e.getName());
      shutdownNow(e);
    }
    for (ManagedExecutor e : executors) {
      if (e == null) {
        continue;
      }
      log.info("Waiting for {} to be shutdown...", e.getName());
      awaitTermination(e);
      log.info("{} is shutdown.", e.getName());
    }
    log.info("Executors are shutdown.");
  }

  private void awaitTermination(ManagedExecutor executor) {
    var executorService = executor.getExecutorService();
    try {
      if (executorService != null && !executorService.isTerminated()) {
        var result =
            executorService.awaitTermination(config.getMaxWaitTimeoutSeconds(), TimeUnit.SECONDS);
        if (!result) {
          log.warn("Timeout during shutdown of the executor={}", executor.getName());
        }
      }
    } catch (InterruptedException e) {
      log.warn("Problem during shutdown of the executor={}", executor.getName(), e);
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Returns the virtual-thread executor.
   *
   * @return {@link VirtualExecutor}
   */
  public VirtualExecutor virtual() {
    return attic;
  }

  /**
   * Returns the CPU-bound executor.
   *
   * @return {@link CpuExecutor}
   */
  public CpuExecutor cpu() {
    return ballroom;
  }

  /**
   * Returns the IO-bound executor.
   *
   * @return {@link IoExecutor}
   */
  public IoExecutor io() {
    return dungeon;
  }
}
