package io.confluent.migrationutility.model.topic;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TopicEntry implements Comparable<TopicEntry> {
  private String topic;
  private Integer partitions;
  private Short replicationFactor;
  private Map<String, String> configs;

  @Override
  public int compareTo(final TopicEntry o) {
    return topic.compareTo(o.getTopic());
  }
}
