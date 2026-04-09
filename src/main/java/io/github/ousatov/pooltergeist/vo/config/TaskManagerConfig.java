package io.github.ousatov.pooltergeist.vo.config;

import io.github.ousatov.pooltergeist.manager.TaskManager;
import lombok.Builder;
import lombok.Value;

/**
 * Config for {@link TaskManager}
 *
 * @author Oleksii Usatov
 */
@Value
@Builder
public class TaskManagerConfig {
  @Builder.Default int workUnitsDequeSize = 200;
  @Builder.Default int tasksDequeSize = 200;
  @Builder.Default int eventProcessingParallelism = 20;
  @Builder.Default int logForRecordCount = 100;
  @Builder.Default int waitTimeForAllTasksFinishedMinute = 30;
  @Builder.Default int waitTimeForCheckingFinishedSeconds = 10;
}
