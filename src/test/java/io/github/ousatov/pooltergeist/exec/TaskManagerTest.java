package io.github.ousatov.pooltergeist.exec;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.github.ousatov.pooltergeist.hub.executor.IoExecutor;
import io.github.ousatov.pooltergeist.manager.TaskManager;
import io.github.ousatov.pooltergeist.vo.config.ExecHubConfig;
import io.github.ousatov.pooltergeist.vo.config.TaskManagerConfig;
import io.github.ousatov.pooltergeist.vo.manager.WorkUnit;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

/**
 * Unit test for {@link TaskManager}
 *
 * @author Oleksii Usatov
 */
@Slf4j
class TaskManagerTest {

  @Test
  void testManaging() throws IOException {

    new IoExecutor(new ExecHubConfig(10, 10, 10, 10));

    // Contain results
    final var proceededUnits = new ConcurrentHashMap<Result, Integer>();
    final var config =
        TaskManagerConfig.builder()
            .eventProcessingParallelism(2)
            .workUnitsDequeSize(10)
            .tasksDequeSize(10)
            .waitTimeForAllTasksFinishedMinute(1)
            .build();

    // Our test function
    Function<CustomWorkOfUnit, Result> function =
        t -> {
          log.info("Proceeded {}", t);
          if (t.data() == 3) {
            await().pollDelay(2, TimeUnit.SECONDS).until(() -> true);
          } else {
            await().pollDelay(1, TimeUnit.SECONDS).until(() -> true);
          }
          var result = Result.builder().ok(t.data() != 3).sourceData(t.data()).build();
          proceededUnits.put(result, t.data());
          return result;
        };

    try (final var taskManager = new TestTaskManager(config, function)) {

      // Total number of tasks
      final int tasks = 20;
      for (int i = 0; i < tasks; i++) {
        var unit = CustomWorkOfUnit.builder().data(i).build();
        try {
          log.info("Submit next task = {}", unit);
          taskManager.submit(unit);
        } catch (InterruptedException ine) {
          Thread.currentThread().interrupt();
          throw new RuntimeException("Could not put value to queue, unit=" + unit, ine);
        }
      }

      // Notify taskManager that we'll not have more tasks and wait for having all submitted tasks
      // to be proceeded
      taskManager.waitForCompletion(CustomWorkOfUnit.LAST_VALUE);

      // Log final statistics
      taskManager.logStatistics();

      // Assertions
      assertEquals(tasks, proceededUnits.size());
      var okResults = proceededUnits.keySet().stream().filter(Result::ok).toList();
      assertEquals(tasks - 1, okResults.size());
      var failedResult = proceededUnits.keySet().stream().filter(r -> !r.ok()).toList();
      assertEquals(1, failedResult.size());
      assertEquals(3, failedResult.getFirst().sourceData());
    }
  }

  static class TestTaskManager extends TaskManager<CustomWorkOfUnit, Result> {

    public TestTaskManager(TaskManagerConfig config, Function<CustomWorkOfUnit, Result> function) {
      super(config, function);
    }

    @Override
    protected boolean isInError(Result result) {
      return !result.ok;
    }
  }

  @Builder
  record CustomWorkOfUnit(int data) implements WorkUnit {
    public static final CustomWorkOfUnit LAST_VALUE = new CustomWorkOfUnit(-1);

    @Override
    public WorkUnit getLastWorkUnit() {
      return LAST_VALUE;
    }

    @Override
    public String getType() {
      return "SomeType";
    }
  }

  @Builder
  record Result(int sourceData, boolean ok) {}
}
