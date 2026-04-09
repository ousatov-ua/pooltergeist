package io.github.ousatov.pooltergeist.vo.config;

import io.github.ousatov.pooltergeist.hub.ExecutorHub;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;

/**
 * Config for {@link ExecutorHub}
 *
 * @author Oleksii Usatov
 * @since 09.04.2026
 */
@Value
@Builder
@AllArgsConstructor
public class ExecHubConfig {
  @Builder.Default int maxWaitTimeoutSeconds = 300;
  @Builder.Default int ioMaxInFlight = 64;
  @Builder.Default int cpuPoolCoresDelimiter = 1;
  @Builder.Default int cpuPoolMinSize = 2;
}
