package io.github.ousatov.pooltergeist.exec;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import lombok.Getter;
import org.jspecify.annotations.NonNull;

/**
 * ExecutorService that runs tasks on virtual threads but limits in-flight concurrency using
 * semaphore.
 *
 * <ul>
 *   <li>Never rejects due to saturation — it waits for a permit.
 *   <li>Max in-flight is enforced across execute/submit/invokeAll/invokeAny.
 *   <li>{@link #getWaiting()} is an approximation; it may transiently over-count due to the
 *       inherent race between checking available permits and acquiring one.
 * </ul>
 *
 * @author Oleksii Usatov
 */
public final class BulkheadedExecutor extends AbstractExecutorService {

  private static final String TASKS_ARE_NULL = "tasks are null";
  private final AtomicInteger inFlight = new AtomicInteger(0);
  private final AtomicInteger waiting = new AtomicInteger(0);
  private final LongAdder submitted = new LongAdder();
  private final LongAdder completed = new LongAdder();
  @Getter private final int maxInFlight;
  private final ExecutorService delegate;
  private final Semaphore permits;
  private volatile boolean shutdown;

  /**
   * Creates a bulkheaded executor backed by a virtual-thread-per-task executor.
   *
   * @param maxInFlight maximum number of concurrently running tasks
   */
  public BulkheadedExecutor(int maxInFlight) {
    this(Executors.newVirtualThreadPerTaskExecutor(), maxInFlight, false);
  }

  /**
   * Creates a bulkheaded executor with an explicit delegate and fairness setting.
   *
   * @param delegate backing executor
   * @param maxInFlight maximum number of concurrently running tasks
   * @param fair whether the semaphore uses a fair ordering policy
   */
  public BulkheadedExecutor(ExecutorService delegate, int maxInFlight, boolean fair) {
    if (maxInFlight <= 0) {
      throw new IllegalArgumentException("maxInFlight must be > 0");
    }
    this.delegate = Objects.requireNonNull(delegate, "delegate");
    this.permits = new Semaphore(maxInFlight, fair);
    this.maxInFlight = maxInFlight;
  }

  /**
   * @return current number of tasks actively running
   */
  public int getInFlight() {
    return inFlight.get();
  }

  /**
   * @return approximate number of threads blocked waiting for a permit; may transiently over-count
   */
  public int getWaiting() {
    return waiting.get();
  }

  /**
   * @return sum of in-flight count and available permits (should equal {@code maxInFlight})
   */
  public int getMaxAvailableInFlight() {
    return inFlight.get() + permits.availablePermits();
  }

  /**
   * @return number of currently available permits
   */
  public int getAvailablePermits() {
    return permits.availablePermits();
  }

  /**
   * @return total number of tasks submitted since creation
   */
  public long getSubmitted() {
    return submitted.sum();
  }

  /**
   * @return total number of tasks that completed (successfully or with exception)
   */
  public long getCompleted() {
    return completed.sum();
  }

  @Override
  public void execute(@NonNull Runnable command) {
    ensureNotShutdown();
    submitted.increment();

    // Approximate waiting counter — may transiently over-count (documented in class Javadoc)
    boolean countedWaiting = false;
    if (permits.availablePermits() == 0) {
      waiting.incrementAndGet();
      countedWaiting = true;
    }

    permits.acquireUninterruptibly();
    if (countedWaiting) {
      waiting.decrementAndGet();
    }
    inFlight.incrementAndGet();

    try {
      delegate.execute(
          () -> {
            try {
              command.run();
            } finally {
              inFlight.decrementAndGet();
              completed.increment();
              permits.release();
            }
          });
    } catch (Exception e) {
      // Delegate rejected the task — release resources but do NOT count as completed
      inFlight.decrementAndGet();
      permits.release();
      throw e;
    }
  }

  private <T> Callable<T> wrap(Callable<T> task) {
    return () -> {
      permits.acquireUninterruptibly();
      try {
        return task.call();
      } finally {
        permits.release();
      }
    };
  }

  private Runnable wrap(Runnable task) {
    return () -> {
      permits.acquireUninterruptibly();
      try {
        task.run();
      } finally {
        permits.release();
      }
    };
  }

  private <T> List<Callable<T>> wrapAll(Collection<? extends Callable<T>> tasks) {
    List<Callable<T>> wrapped = new ArrayList<>(tasks.size());
    for (Callable<T> task : tasks) {
      wrapped.add(wrap(task));
    }
    return wrapped;
  }

  @Override
  public @NonNull Future<?> submit(@NonNull Runnable task) {
    Objects.requireNonNull(task, "task");
    ensureNotShutdown();
    return delegate.submit(wrap(task));
  }

  @Override
  public @NonNull <T> Future<T> submit(@NonNull Runnable task, T result) {
    Objects.requireNonNull(task, "task");
    ensureNotShutdown();
    return delegate.submit(wrap(task), result);
  }

  @Override
  public @NonNull <T> Future<T> submit(@NonNull Callable<T> task) {
    Objects.requireNonNull(task, "task");
    ensureNotShutdown();
    return delegate.submit(wrap(task));
  }

  @Override
  public @NonNull <T> List<Future<T>> invokeAll(@NonNull Collection<? extends Callable<T>> tasks)
      throws InterruptedException {
    Objects.requireNonNull(tasks, TASKS_ARE_NULL);
    ensureNotShutdown();
    return delegate.invokeAll(wrapAll(tasks));
  }

  @Override
  public @NonNull <T> List<Future<T>> invokeAll(
      @NonNull Collection<? extends Callable<T>> tasks, long timeout, @NonNull TimeUnit unit)
      throws InterruptedException {
    Objects.requireNonNull(tasks, TASKS_ARE_NULL);
    ensureNotShutdown();
    return delegate.invokeAll(wrapAll(tasks), timeout, unit);
  }

  @Override
  public @NonNull <T> T invokeAny(@NonNull Collection<? extends Callable<T>> tasks)
      throws InterruptedException, ExecutionException {
    Objects.requireNonNull(tasks, "tasks");
    ensureNotShutdown();
    return delegate.invokeAny(wrapAll(tasks));
  }

  @Override
  public @NonNull <T> T invokeAny(
      @NonNull Collection<? extends Callable<T>> tasks, long timeout, @NonNull TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
    Objects.requireNonNull(tasks, "tasks");
    ensureNotShutdown();
    return delegate.invokeAny(wrapAll(tasks), timeout, unit);
  }

  @Override
  public void shutdown() {
    shutdown = true;
    delegate.shutdown();
  }

  @Override
  public @NonNull List<Runnable> shutdownNow() {
    shutdown = true;
    return delegate.shutdownNow();
  }

  @Override
  public boolean isShutdown() {
    return shutdown || delegate.isShutdown();
  }

  @Override
  public boolean isTerminated() {
    return delegate.isTerminated();
  }

  @Override
  public boolean awaitTermination(long timeout, @NonNull TimeUnit unit)
      throws InterruptedException {
    return delegate.awaitTermination(timeout, unit);
  }

  private void ensureNotShutdown() {
    if (isShutdown()) {
      throw new RejectedExecutionException("ExecutorService is shutdown");
    }
  }
}
