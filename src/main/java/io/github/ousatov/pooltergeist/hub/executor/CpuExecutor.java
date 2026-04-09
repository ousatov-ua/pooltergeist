package io.github.ousatov.pooltergeist.hub.executor;

import io.github.ousatov.pooltergeist.def.Values;
import io.github.ousatov.pooltergeist.vo.config.ExecHubConfig;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import lombok.extern.slf4j.Slf4j;

/**
 * Executor for CPU-bound operations, backed by a {@link ForkJoinPool}.
 *
 * @author Oleksii Usatov
 * @since 09.04.2026
 */
@Slf4j
public class CpuExecutor extends ManagedExecutor {

  /**
   * Creates a CPU executor sized relative to available processors.
   *
   * @param config executor hub configuration
   */
  public CpuExecutor(ExecHubConfig config) {
    super(createPool(config));
    log.info(
        "commonPoolParallelism={}, computePoolSize={}",
        Runtime.getRuntime().availableProcessors(),
        ((ForkJoinPool) executorService).getParallelism());
  }

  /**
   * Creates a CPU executor wrapping the given service (for testing).
   *
   * @param executorService backing executor
   */
  public CpuExecutor(ExecutorService executorService) {
    super(executorService);
  }

  private static ForkJoinPool createPool(ExecHubConfig config) {
    int commonPoolParallelism = Math.max(1, Runtime.getRuntime().availableProcessors());
    int computePoolSize =
        Math.max(
            commonPoolParallelism / config.getCpuPoolCoresDelimiter(), config.getCpuPoolMinSize());
    return new ForkJoinPool(computePoolSize);
  }

  @Override
  public String getName() {
    return Values.BALLROOM;
  }

  @Override
  public String toString() {
    if (executorService instanceof ForkJoinPool p) {
      return "parallelism=%d, size=%d, active=%d, running=%d, queuedTasks≈%d, queuedSubmissions=%d"
          .formatted(
              p.getParallelism(),
              p.getPoolSize(),
              p.getActiveThreadCount(),
              p.getRunningThreadCount(),
              p.getQueuedTaskCount(),
              p.getQueuedSubmissionCount());
    }
    return executorService.toString();
  }
}
