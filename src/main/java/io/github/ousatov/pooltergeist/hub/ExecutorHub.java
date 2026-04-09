package io.github.ousatov.pooltergeist.hub;

import io.github.ousatov.pooltergeist.hub.executor.CpuExecutor;
import io.github.ousatov.pooltergeist.hub.executor.IoExecutor;
import io.github.ousatov.pooltergeist.hub.executor.ManagedExecutor;
import io.github.ousatov.pooltergeist.hub.executor.VirtualExecutor;
import io.github.ousatov.pooltergeist.vo.config.ExecHubConfig;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Manages executor pools for parallel task execution. Provides IO-bound, CPU-bound, Virtual threads
 * executors.
 *
 * @author Oleksii Usatov
 */
@Slf4j
public class ExecutorHub {

  /** Maximum timeout in seconds for pool termination. */
  private final ExecHubConfig config;

  /** Virtual threads */
  @Getter private final VirtualExecutor attic;

  /** Cpu bound work */
  @Getter private final CpuExecutor ballroom;

  /** IO bound work */
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

  public ExecutorHub(
      ExecHubConfig config, VirtualExecutor attic, CpuExecutor ballroom, IoExecutor dungeon) {
    this.config = config;
    this.attic = attic;
    this.ballroom = ballroom;
    this.dungeon = dungeon;
  }

  /**
   * ShutdownNow pool
   *
   * @param chamber {@link ManagedExecutor}
   */
  private static void shutdownNow(ManagedExecutor chamber) {
    var executorService = chamber.getExecutorService();
    if (executorService != null) {
      executorService.shutdownNow();
    }
  }

  /** Shuts down all executor pools gracefully on application context destruction. */
  public void close() {

    log.info("Going to shutdown executors...");
    close(attic);
    close(ballroom);
    close(dungeon);
    log.info("Executors are shutdown.");
  }

  private void close(ManagedExecutor managedExecutor) {
    log.info("Going to shutdown {}...", managedExecutor.getName());
    shutdownNow(managedExecutor);
    log.info("Waiting for {} to be shutdown...", managedExecutor.getName());
    awaitTermination(managedExecutor);
    log.info("{} is shutdown.", managedExecutor.getName());
  }

  /**
   * Wait for termination
   *
   * @param executor {@link ManagedExecutor}
   */
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
   * For regular naming virtual threads executor :)
   *
   * @return @{@link VirtualExecutor}
   */
  public VirtualExecutor virtual() {
    return attic;
  }

  /**
   * For regular naming cpu threads executor :)
   *
   * @return @{@link CpuExecutor}
   */
  public CpuExecutor cpu() {
    return ballroom;
  }

  /**
   * For regular naming io threads executor :)
   *
   * @return @{@link CpuExecutor}
   */
  public IoExecutor io() {
    return dungeon;
  }
}
