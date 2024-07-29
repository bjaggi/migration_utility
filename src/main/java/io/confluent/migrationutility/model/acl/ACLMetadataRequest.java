package io.confluent.migrationutility.model.acl;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;


@Data
@AllArgsConstructor
@NoArgsConstructor
public class ACLMetadataRequest {
  private String sourceClusterId;
  private String destClusterId;
}
