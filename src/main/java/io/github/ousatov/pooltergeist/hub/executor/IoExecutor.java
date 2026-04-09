package io.github.ousatov.pooltergeist.hub.executor;

import io.github.ousatov.pooltergeist.def.Values;
import io.github.ousatov.pooltergeist.exec.BulkheadedExecutor;
import io.github.ousatov.pooltergeist.vo.config.ExecHubConfig;
import java.text.MessageFormat;
import java.util.concurrent.ExecutorService;
import lombok.extern.slf4j.Slf4j;

/**
 * IO Executor
 *
 * <p>IO related operations
 *
 * @author Oleksii Usatov
 * @since 09.04.2026
 */
@Slf4j
public class IoExecutor extends ManagedExecutor {

  private static final String BULKHEADED_LOG =
      "maxInFlight={0}, submitted={1}, available={2}, inFlight={3}, waiting={4}, completed={5},"
          + " maxAvailableInFlight={6}";

  public IoExecutor(ExecHubConfig config) {
    super(new BulkheadedExecutor(config.getIoMaxInFlight()));
    log.info("IO max in flight: {}", config.getIoMaxInFlight());
  }

  public IoExecutor(ExecutorService executorService) {
    super(executorService);
  }

  @Override
  public String getName() {
    return Values.DUNGEON;
  }

  @Override
  public String toString() {
    if (executorService instanceof BulkheadedExecutor bulkheadedExecutor) {
      return MessageFormat.format(
          BULKHEADED_LOG,
          bulkheadedExecutor.getMaxInFlight(),
          bulkheadedExecutor.getSubmitted(),
          bulkheadedExecutor.getAvailablePermits(),
          bulkheadedExecutor.getInFlight(),
          bulkheadedExecutor.getWaiting(),
          bulkheadedExecutor.getCompleted(),
          bulkheadedExecutor.getMaxAvailableInFlight());
    }
    return executorService.toString();
  }
}
