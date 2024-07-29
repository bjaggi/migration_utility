package io.confluent.migrationutility.model.group;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class GroupMetadataRequest {
  private String clusterId;
  private List<String> groups;
  private List<String> topics;
}
