package io.confluent.migrationutility.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ConsumerGroupMetadata {
  private String consumerGroupName;
  private List<TopicPartitionMetadata> topicPartitionMetadata;
}
