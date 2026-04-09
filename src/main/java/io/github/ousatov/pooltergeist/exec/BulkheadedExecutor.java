package io.github.ousatov.pooltergeist.exec;

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
 * <p>- Never rejects due to saturation (it waits for a permit).
 *
 * <p>- Max in-flight is enforced across execute/submit/invokeAll/invokeAny.
 *
 * <p>
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

  public BulkheadedExecutor(int maxInFlight) {
    this(Executors.newVirtualThreadPerTaskExecutor(), maxInFlight, false);
  }

  public BulkheadedExecutor(ExecutorService delegate, int maxInFlight, boolean fair) {
    if (maxInFlight <= 0) {
      throw new IllegalArgumentException("maxInFlight must be > 0");
    }
    this.delegate = Objects.requireNonNull(delegate, "delegate");
    this.permits = new Semaphore(maxInFlight, fair);
    this.maxInFlight = maxInFlight;
  }

  public int getInFlight() {
    return inFlight.get();
  }

  public int getWaiting() {
    return waiting.get();
  }

  public int getMaxAvailableInFlight() {
    return inFlight.get() + permits.availablePermits();
  }

  public int getAvailablePermits() {
    return permits.availablePermits();
  }

  public long getSubmitted() {
    return submitted.sum();
  }

  public long getCompleted() {
    return completed.sum();
  }

  @Override
  public void execute(@NonNull Runnable command) {
    if (isShutdown()) {
      throw new RejectedExecutionException("ExecutorService is shutdown");
    }
    submitted.increment();

    // Backpressure: if no permits, we are waiting
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
      inFlight.decrementAndGet();
      completed.increment();
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
    var wrapped = tasks.stream().map(this::wrap).toList();
    return delegate.invokeAll(wrapped);
  }

  @Override
  public @NonNull <T> List<Future<T>> invokeAll(
      @NonNull Collection<? extends Callable<T>> tasks, long timeout, @NonNull TimeUnit unit)
      throws InterruptedException {
    Objects.requireNonNull(tasks, TASKS_ARE_NULL);
    ensureNotShutdown();
    var wrapped = tasks.stream().map(this::wrap).toList();
    return delegate.invokeAll(wrapped, timeout, unit);
  }

  @Override
  public @NonNull <T> T invokeAny(@NonNull Collection<? extends Callable<T>> tasks)
      throws InterruptedException, ExecutionException {
    Objects.requireNonNull(tasks, "tasks");
    ensureNotShutdown();
    var wrapped = tasks.stream().map(this::wrap).toList();
    return delegate.invokeAny(wrapped);
  }

  @Override
  public @NonNull <T> T invokeAny(
      @NonNull Collection<? extends Callable<T>> tasks, long timeout, @NonNull TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
    Objects.requireNonNull(tasks, "tasks");
    ensureNotShutdown();
    var wrapped = tasks.stream().map(this::wrap).toList();
    return delegate.invokeAny(wrapped, timeout, unit);
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
