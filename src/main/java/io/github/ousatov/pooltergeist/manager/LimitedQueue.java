package io.github.ousatov.pooltergeist.manager;

import java.util.concurrent.LinkedBlockingQueue;
import lombok.NonNull;

/**
 * Needed for ThreadPoolExecutor Limited queue: executor will wait to submit runnable if the queue
 * is full
 *
 * @author Oleksii Usatov
 */
class LimitedQueue<E> extends LinkedBlockingQueue<E> {
  public LimitedQueue(int maxSize) {
    super(maxSize);
  }

  @Override
  public boolean offer(@NonNull E e) {

    // Turn offer() and add() into a blocking calls
    try {
      put(e);
      return true;
    } catch (InterruptedException _) {
      Thread.currentThread().interrupt();
    }
    return false;
  }
}
