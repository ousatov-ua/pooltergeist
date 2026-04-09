package io.github.ousatov.pooltergeist.hub.executor;

import io.github.ousatov.pooltergeist.def.Values;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.extern.slf4j.Slf4j;

/**
 * Virtual Executor
 *
 * <p>Virtual threads
 *
 * @author Oleksii Usatov
 * @since 09.04.2026
 */
@Slf4j
public class VirtualExecutor extends ManagedExecutor {

  public VirtualExecutor() {
    super(Executors.newVirtualThreadPerTaskExecutor());
  }

  public VirtualExecutor(ExecutorService executorService) {
    super(executorService);
  }

  @Override
  public String getName() {
    return Values.ATTIC;
  }
}
