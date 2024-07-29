package io.confluent.migrationutility.model.group;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TopicPartitionMetadata implements Comparable<TopicPartitionMetadata> {
  private String topicName;
  private Integer partitionNum;
  private Long currentOffsetNumber;
  private Long logEndOffsetNumber;
  private Long consumerLag;
  private Long timestamp ;

  @Override
  public int compareTo(final TopicPartitionMetadata o) {
    final int i = this.topicName.compareTo(o.getTopicName());
    if (i == 0) {
      return this.timestamp.compareTo(o.getTimestamp());
    }
    return i;
  }
}
