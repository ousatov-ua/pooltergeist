package io.github.ousatov.pooltergeist.hub.executor;

import io.github.ousatov.pooltergeist.def.Values;
import io.github.ousatov.pooltergeist.exec.BulkheadedExecutor;
import io.github.ousatov.pooltergeist.vo.config.ExecHubConfig;
import java.util.concurrent.ExecutorService;
import lombok.extern.slf4j.Slf4j;

/**
 * Executor for IO-bound operations, backed by a {@link BulkheadedExecutor}.
 *
 * @author Oleksii Usatov
 * @since 09.04.2026
 */
@Slf4j
public class IoExecutor extends ManagedExecutor {

  /**
   * Creates an IO executor with a bounded in-flight limit.
   *
   * @param config executor hub configuration
   */
  public IoExecutor(ExecHubConfig config) {
    super(new BulkheadedExecutor(config.getIoMaxInFlight()));
    log.info("IO max in flight: {}", config.getIoMaxInFlight());
  }

  /**
   * Creates an IO executor wrapping the given service (for testing).
   *
   * @param executorService backing executor
   */
  public IoExecutor(ExecutorService executorService) {
    super(executorService);
  }

  @Override
  public String getName() {
    return Values.DUNGEON;
  }

  @Override
  public String toString() {
    if (executorService instanceof BulkheadedExecutor b) {
      return "maxInFlight=%d, submitted=%d, available=%d, inFlight=%d, waiting=%d, completed=%d, maxAvailableInFlight=%d"
          .formatted(
              b.getMaxInFlight(),
              b.getSubmitted(),
              b.getAvailablePermits(),
              b.getInFlight(),
              b.getWaiting(),
              b.getCompleted(),
              b.getMaxAvailableInFlight());
    }
    return executorService.toString();
  }
}
