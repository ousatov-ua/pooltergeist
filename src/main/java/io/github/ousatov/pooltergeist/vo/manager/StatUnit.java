package io.github.ousatov.pooltergeist.vo.manager;

/**
 * Contains statistics information
 *
 * @author Oleksii Usatov
 */
public record StatUnit(long currentCount, long totalCount, long totalErrorCount) {
  public static final StatUnit EMPTY = new StatUnit(0, 0, 0);
}
