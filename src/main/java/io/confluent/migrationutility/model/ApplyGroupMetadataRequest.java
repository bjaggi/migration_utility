package io.confluent.migrationutility.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ApplyGroupMetadataRequest {
  private String clusterId;
  private List<ConsumerGroupMetadata> groupMetadataList;
}
