package io.github.ousatov.pooltergeist.hub.executor;

import io.github.ousatov.pooltergeist.def.Values;
import io.github.ousatov.pooltergeist.vo.config.ExecHubConfig;
import java.text.MessageFormat;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import lombok.extern.slf4j.Slf4j;

/**
 * Cpu Executor
 *
 * <p>CPU bound operations
 *
 * @author Oleksii Usatov
 * @since 09.04.2026
 */
@Slf4j
public class CpuExecutor extends ManagedExecutor {

  private static final String CPU_LOG =
      "parallelism={0}, size={1}, active={2}, running={3}, queuedTasks≈{4}, queuedSubmissions={5}";

  public CpuExecutor(ExecHubConfig config) {
    int commonPoolParallelism = Math.max(1, Runtime.getRuntime().availableProcessors());
    var computePoolSize =
        Math.max(
            commonPoolParallelism / config.getCpuPoolCoresDelimiter(), config.getCpuPoolMinSize());
    super(new ForkJoinPool(computePoolSize));
    log.info(
        "commonPoolParallelism={}, computePoolSize={}", commonPoolParallelism, computePoolSize);
  }

  public CpuExecutor(ExecutorService executorService) {
    super(executorService);
  }

  @Override
  public String getName() {
    return Values.BALLROOM;
  }

  @Override
  public String toString() {
    if (executorService instanceof ForkJoinPool forkJoinPool) {
      return MessageFormat.format(
          CPU_LOG,
          forkJoinPool.getParallelism(),
          forkJoinPool.getPoolSize(),
          forkJoinPool.getActiveThreadCount(),
          forkJoinPool.getRunningThreadCount(),
          forkJoinPool.getQueuedTaskCount(),
          forkJoinPool.getQueuedSubmissionCount());
    }
    return executorService.toString();
  }
}
