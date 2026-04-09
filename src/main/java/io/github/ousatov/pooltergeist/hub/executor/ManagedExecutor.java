package io.github.ousatov.pooltergeist.hub.executor;

import com.google.common.base.Stopwatch;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Supplier;
import java.util.stream.Stream;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Common functionality for all rooms
 *
 * @author Oleksii Usatov
 * @since 09.04.2026
 */
@Slf4j
@RequiredArgsConstructor
public abstract class ManagedExecutor {

  @Getter protected final ExecutorService executorService;

  /**
   * Wait for all futures
   *
   * @param futures list of {@link Future}
   * @param <T> type returned
   * @return result
   */
  public static <T> List<T> join(List<CompletableFuture<T>> futures) {
    Stopwatch stopwatch = Stopwatch.createStarted();
    try {
      CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).join();
      var result = new ArrayList<T>(futures.size());
      for (var f : futures) {
        result.add(f.join()); // non-blocking — future is already done
      }
      return result;
    } finally {
      log.debug(
          "Join for size={} in {} ms",
          futures.size(),
          stopwatch.stop().elapsed(TimeUnit.MILLISECONDS));
    }
  }

  /**
   * Wait for all futures
   *
   * @param futures list of {@link Future}
   * @param <T> type returned
   * @return result
   */
  public static <T> T[] join(CompletableFuture<T>[] futures, IntFunction<T[]> arrayFactory) {
    Stopwatch stopwatch = Stopwatch.createStarted();
    try {
      CompletableFuture.allOf(futures).join(); // wait for all to complete
      T[] result = arrayFactory.apply(futures.length);
      for (int i = 0; i < futures.length; i++) { // preserve original order
        result[i] = futures[i].join();
      }
      return result;
    } finally {
      log.debug(
          "Join for size={} in {} ms",
          futures.length,
          stopwatch.stop().elapsed(TimeUnit.MILLISECONDS));
    }
  }

  /**
   * Execute in parallel mode
   *
   * @param functions functions to apply
   * @param <T> Input type
   */
  public <T> void executeTasksBlocking(List<Supplier<T>> functions) {
    if (functions.isEmpty()) {
      return;
    }
    join(functions.stream().map(this::submitTasks).toList());
  }

  /**
   * Execute in parallel mode and wait for all tasks finished
   *
   * @param arrayFactory array function
   * @param fn function to apply
   * @param <T> Return type
   * @param <W> Provided type
   * @return result
   */
  @SuppressWarnings({"unchecked", "unused"})
  public <T, W> T[] executeTasksBlocking(
      Stream<W> stream, Function<W, T> fn, IntFunction<T[]> arrayFactory) {
    CompletableFuture<T>[] futures =
        stream
            .sequential()
            .map(d -> submitTasks(() -> fn.apply(d)))
            .toArray((int n) -> (CompletableFuture<T>[]) new CompletableFuture<?>[n]); // safe-ish
    return join(futures, arrayFactory);
  }

  /**
   * Return List for stream
   *
   * @param data stream
   * @param fn function
   * @param <T> Return type
   * @param <W> Provided type
   * @return list of results
   */
  public <T, W> List<T> executeTasksBlocking(Collection<W> data, Function<W, T> fn) {
    List<CompletableFuture<T>> futures =
        data.stream().map(w -> submitTasks(() -> fn.apply(w))).toList();
    return join(futures);
  }

  /**
   * Execute in parallel mode WITHOUT JOIN
   *
   * @param data data
   * @param function function to apply
   * @param <T> Return type
   * @param <W> Provided type
   * @return List of futures
   */
  @SuppressWarnings("unused")
  public <T, W> List<CompletableFuture<T>> executeTasks(
      Collection<W> data, Function<W, T> function) {
    return data.stream().map(d -> submitTasks(() -> function.apply(d))).toList();
  }

  /**
   * Submit a task
   *
   * @param supplier task
   * @param <T> type
   * @return Future
   */
  public <T> CompletableFuture<T> submitTasks(Supplier<T> supplier) {
    return CompletableFuture.supplyAsync(supplier, executorService);
  }

  /**
   * Generates status information string for the given executor.
   *
   * @return formatted status string with pool metrics
   */
  public String toString() {
    return executorService.toString();
  }

  public abstract String getName();
}
