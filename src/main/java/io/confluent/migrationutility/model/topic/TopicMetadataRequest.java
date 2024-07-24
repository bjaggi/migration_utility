package io.confluent.migrationutility.model.topic;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TopicMetadataRequest {
  private String sourceCluster;
  private List<String> topics;
  private String destinationCluster;

}
