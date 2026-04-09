package io.github.ousatov.pooltergeist.vo.manager;

/**
 * Unit of work dispatched by {@link io.github.ousatov.pooltergeist.manager.TaskManager}.
 *
 * <p><strong>Sentinel contract</strong>: {@link #getLastWorkUnit()} must always return the
 * <em>same</em> static singleton reference. {@code TaskManager} detects end-of-stream via identity
 * equality ({@code workUnit == workUnit.getLastWorkUnit()}), so returning a new instance will cause
 * the dispatch loop to never terminate.
 *
 * @author Oleksii Usatov
 */
public interface WorkUnit {

  /**
   * Returns the sentinel instance that signals end-of-stream to the dispatcher. Must be a stable
   * static reference — identity ({@code ==}) is used for comparison.
   *
   * @return the sentinel instance
   */
  WorkUnit getLastWorkUnit();

  /**
   * Returns the logical type label used to group per-type statistics.
   *
   * @return type label, non-null
   */
  String getType();
}
