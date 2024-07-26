package io.confluent.migrationutility.model.quota;

import lombok.Data;


@Data
public class ApplyQuotaMetadataRequest {
  private String sourceClusterId;
  private String destClusterId;
}
