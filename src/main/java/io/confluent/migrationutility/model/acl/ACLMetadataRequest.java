package io.confluent.migrationutility.model.acl;

import lombok.Data;


@Data
public class ACLMetadataRequest {
  private String sourceClusterId;
  private String destClusterId;
}
