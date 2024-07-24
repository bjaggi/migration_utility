package io.confluent.migrationutility.model;

import lombok.Data;


@Data
public class ApplyQuotaMetadataRequest {
  private String sourceClusterId;
  private String destClusterId;
}
