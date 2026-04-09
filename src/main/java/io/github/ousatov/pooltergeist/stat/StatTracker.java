package io.github.ousatov.pooltergeist.stat;

import io.github.ousatov.pooltergeist.vo.manager.StatUnit;
import io.github.ousatov.pooltergeist.vo.manager.WorkUnit;
import java.util.LinkedHashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

/**
 * Tracks per-type processing statistics in a thread-safe manner.
 *
 * @author Oleksii Usatov
 */
@Slf4j
public final class StatTracker {

  private final int logForRecordCount;
  private final Map<String, StatUnit> recordsSubmitted = new LinkedHashMap<>();
  private long totalUnitsOfWorkSubmitted;

  /**
   * @param logForRecordCount interval at which progress is logged per type
   */
  public StatTracker(int logForRecordCount) {
    this.logForRecordCount = logForRecordCount;
  }

  /**
   * Records a processed work unit and logs progress when an interval is reached.
   *
   * @param workUnit the processed work unit
   * @param isError whether the result was an error
   */
  public synchronized void mark(WorkUnit workUnit, boolean isError) {
    totalUnitsOfWorkSubmitted++;
    var statValue = recordsSubmitted.getOrDefault(workUnit.getType(), StatUnit.EMPTY);
    long currentCount = statValue.currentCount() + 1;
    long totalCount = statValue.totalCount() + 1;
    long totalErrorCount = statValue.totalCount() + (isError ? 1 : 0);
    if (currentCount >= logForRecordCount) {
      log.info(
          "Processed total={}, in error={} records for type={}",
          totalCount,
          totalErrorCount,
          workUnit.getType());
      currentCount = 0;
    }
    recordsSubmitted.put(
        workUnit.getType(), new StatUnit(currentCount, totalCount, totalErrorCount));
  }

  /**
   * @return total number of units processed so far
   */
  public synchronized long getTotalSubmitted() {
    return totalUnitsOfWorkSubmitted;
  }

  /** Logs the final summary for all tracked types. */
  public synchronized void logSummary() {
    recordsSubmitted.forEach(
        (key, value) ->
            log.info(
                "Statistics: total={}, in error={} records for type={}",
                value.totalCount(),
                value.totalErrorCount(),
                key));
    log.info("Statistics: Total values submitted={}", totalUnitsOfWorkSubmitted);
  }
}
