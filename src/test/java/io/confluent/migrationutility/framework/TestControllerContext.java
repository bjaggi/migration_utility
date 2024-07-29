package io.confluent.migrationutility.framework;

import io.confluent.migrationutility.config.KafkaClusterConfig;

import java.util.HashMap;
import java.util.Map;

public abstract class TestControllerContext {

  protected static final KafkaClusterConfig clusterConfig = new KafkaClusterConfig();
  static {
    final Map<String, Map<String, String>> wrapperConfig = new HashMap<>();
    final Map<String, String> mockConfig = new HashMap<>();
    mockConfig.put("bootstrap.servers", "mock:9092");
    wrapperConfig.put("src", mockConfig);
    wrapperConfig.put("dest", mockConfig);
    clusterConfig.setClusters(wrapperConfig);
  }

}
