package io.github.ousatov.pooltergeist.vo.manager;

/**
 * Unit of work
 *
 * @author Oleksii Usatov
 */
public interface WorkUnit {
  WorkUnit getLastWorkUnit();

  String getType();
}
