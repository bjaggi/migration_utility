package io.confluent.migrationutility.util;


import io.confluent.migrationutility.config.KafkaClusterConfig;
import io.confluent.migrationutility.exception.InvalidClusterIdException;

import java.util.Map;
import java.util.Optional;

public final class AppUtils {
  private AppUtils() {}

  public static Map<String, String> getClusterConfig(final KafkaClusterConfig clusterConfig, final String clusterId) {
    return Optional.ofNullable(clusterConfig.getClusters().get(clusterId))
            .orElseThrow(() -> new InvalidClusterIdException("Invalid cluster id: " + clusterId));
  }
}
