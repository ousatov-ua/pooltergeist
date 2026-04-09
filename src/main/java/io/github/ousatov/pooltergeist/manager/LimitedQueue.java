package io.github.ousatov.pooltergeist.manager;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import lombok.NonNull;

/**
 * Blocking queue that converts {@link #offer} into a blocking {@link #put} call. This causes {@link
 * ThreadPoolExecutor} to apply backpressure when the queue is full rather than invoking the
 * rejection handler.
 *
 * <p><strong>Note</strong>: this overrides the standard {@code offer} contract (non-blocking,
 * returns {@code false} when full). Only use this queue with {@link ThreadPoolExecutor} where the
 * blocking-offer backpressure behavior is explicitly desired.
 *
 * @param <E> element type
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
