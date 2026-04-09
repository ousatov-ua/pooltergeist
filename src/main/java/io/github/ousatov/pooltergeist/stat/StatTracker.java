package io.github.ousatov.pooltergeist.stat;

import io.github.ousatov.pooltergeist.vo.manager.StatUnit;
import io.github.ousatov.pooltergeist.vo.manager.WorkUnit;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import lombok.extern.slf4j.Slf4j;

/**
 * Tracks per-type processing statistics in a thread-safe manner.
 *
 * <p>Uses {@link ConcurrentHashMap#compute} for per-type atomic updates and a {@link LongAdder} for
 * the total counter, avoiding a global lock on the hot mark path.
 *
 * @author Oleksii Usatov
 */
@Slf4j
public final class StatTracker {

  private final int logForRecordCount;
  private final ConcurrentHashMap<String, StatUnit> recordsSubmitted = new ConcurrentHashMap<>();
  private final LongAdder totalUnitsOfWorkSubmitted = new LongAdder();

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
  public void mark(WorkUnit workUnit, boolean isError) {
    totalUnitsOfWorkSubmitted.increment();
    recordsSubmitted.compute(
        workUnit.getType(),
        (type, prev) -> {
          StatUnit statValue = (prev == null) ? StatUnit.EMPTY : prev;
          long currentCount = statValue.currentCount() + 1;
          long totalCount = statValue.totalCount() + 1;
          long totalErrorCount = statValue.totalErrorCount() + (isError ? 1 : 0);
          if (currentCount >= logForRecordCount) {
            log.info(
                "Processed total={}, in error={} records for type={}",
                totalCount,
                totalErrorCount,
                type);
            currentCount = 0;
          }
          return new StatUnit(currentCount, totalCount, totalErrorCount);
        });
  }

  /**
   * @return total number of units processed so far
   */
  public long getTotalSubmitted() {
    return totalUnitsOfWorkSubmitted.sum();
  }

  /** Logs the final summary for all tracked types. */
  public void logSummary() {
    recordsSubmitted.forEach(
        (key, value) ->
            log.info(
                "Statistics: total={}, in error={} records for type={}",
                value.totalCount(),
                value.totalErrorCount(),
                key));
    log.info("Statistics: Total values submitted={}", totalUnitsOfWorkSubmitted.sum());
  }
}
