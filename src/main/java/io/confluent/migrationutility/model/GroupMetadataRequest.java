package io.confluent.migrationutility.model;

import lombok.Data;

import java.util.List;

@Data
public class GroupMetadataRequest {
  private String clusterId;
  private List<String> groups;
  private List<String> topics;
}
