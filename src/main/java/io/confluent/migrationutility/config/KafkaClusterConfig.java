package io.confluent.migrationutility.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.Map;

@Configuration
@ConfigurationProperties(prefix = "kafka")
public class KafkaClusterConfig {

  @Setter
  @Getter
  private Map<String, Map<String, String>> clusters;



}
