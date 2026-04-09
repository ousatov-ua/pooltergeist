package io.github.ousatov.pooltergeist.hub.executor;

import com.google.common.base.Stopwatch;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Supplier;
import java.util.stream.Stream;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Common functionality for all executor rooms.
 *
 * @author Oleksii Usatov
 * @since 09.04.2026
 */
@Slf4j
@RequiredArgsConstructor
public abstract class ManagedExecutor {

  @Getter protected final ExecutorService executorService;

  /**
   * Waits for all futures and returns their results in the original order.
   *
   * @param futures list of futures to join
   * @param <T> result type
   * @return ordered list of results
   */
  public static <T> List<T> join(List<CompletableFuture<T>> futures) {
    Stopwatch stopwatch = log.isDebugEnabled() ? Stopwatch.createStarted() : null;
    try {
      CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
      List<T> result = new ArrayList<>(futures.size());
      for (CompletableFuture<T> f : futures) {
        result.add(f.join()); // non-blocking — future is already done
      }
      return result;
    } finally {
      if (stopwatch != null) {
        log.debug(
            "Join for size={} in {} ms",
            futures.size(),
            stopwatch.stop().elapsed(TimeUnit.MILLISECONDS));
      }
    }
  }

  /**
   * Waits for all futures and returns their results as an array.
   *
   * @param futures array of futures
   * @param arrayFactory array constructor (e.g. {@code String[]::new})
   * @param <T> result type
   * @return ordered array of results
   */
  public static <T> T[] join(CompletableFuture<T>[] futures, IntFunction<T[]> arrayFactory) {
    Stopwatch stopwatch = log.isDebugEnabled() ? Stopwatch.createStarted() : null;
    try {
      CompletableFuture.allOf(futures).join();
      T[] result = arrayFactory.apply(futures.length);
      for (int i = 0; i < futures.length; i++) {
        result[i] = futures[i].join();
      }
      return result;
    } finally {
      if (stopwatch != null) {
        log.debug(
            "Join for size={} in {} ms",
            futures.length,
            stopwatch.stop().elapsed(TimeUnit.MILLISECONDS));
      }
    }
  }

  /**
   * Executes all suppliers in parallel and blocks until all complete.
   *
   * @param functions tasks to execute
   * @param <T> result type
   */
  public <T> void executeTasksBlocking(List<Supplier<T>> functions) {
    if (functions.isEmpty()) {
      return;
    }
    List<CompletableFuture<T>> futures = new ArrayList<>(functions.size());
    for (Supplier<T> s : functions) {
      futures.add(submitTasks(s));
    }
    join(futures);
  }

  /**
   * Maps each element in {@code stream} to a task, runs all in parallel, and returns results as an
   * array.
   *
   * @param stream input stream
   * @param fn mapping function
   * @param arrayFactory array constructor
   * @param <T> return type
   * @param <W> input type
   * @return ordered array of results
   */
  @SuppressWarnings({"unchecked", "unused"})
  public <T, W> T[] executeTasksBlocking(
      Stream<W> stream, Function<W, T> fn, IntFunction<T[]> arrayFactory) {
    CompletableFuture<T>[] futures =
        stream
            .sequential()
            .map(d -> submitTasks(() -> fn.apply(d)))
            .toArray((int n) -> (CompletableFuture<T>[]) new CompletableFuture<?>[n]);
    return join(futures, arrayFactory);
  }

  /**
   * Maps each element in {@code data} to a task, runs all in parallel, and blocks until all
   * complete.
   *
   * @param data input collection
   * @param fn mapping function
   * @param <T> return type
   * @param <W> input type
   * @return list of results in encounter order
   */
  public <T, W> List<T> executeTasksBlocking(Collection<W> data, Function<W, T> fn) {
    List<CompletableFuture<T>> futures = new ArrayList<>(data.size());
    for (W w : data) {
      futures.add(submitTasks(() -> fn.apply(w)));
    }
    return join(futures);
  }

  /**
   * Maps each element in {@code data} to a task and submits all without waiting for completion.
   *
   * @param data input collection
   * @param function mapping function
   * @param <T> return type
   * @param <W> input type
   * @return list of futures (not yet joined)
   */
  @SuppressWarnings("unused")
  public <T, W> List<CompletableFuture<T>> executeTasks(
      Collection<W> data, Function<W, T> function) {
    List<CompletableFuture<T>> futures = new ArrayList<>(data.size());
    for (W d : data) {
      futures.add(submitTasks(() -> function.apply(d)));
    }
    return futures;
  }

  /**
   * Submits a single task asynchronously.
   *
   * @param supplier task
   * @param <T> result type
   * @return future representing the pending result
   */
  public <T> CompletableFuture<T> submitTasks(Supplier<T> supplier) {
    return CompletableFuture.supplyAsync(supplier, executorService);
  }

  /**
   * Returns a status string describing the current executor state.
   *
   * @return formatted status string with pool metrics
   */
  public String toString() {
    return executorService.toString();
  }

  /**
   * @return name of this executor (used for logging)
   */
  public abstract String getName();
}
