package io.confluent.migrationutility.model.quota;

import lombok.Data;


@Data
public class QuotaMetadataRequest {
  private String sourceClusterId;
  private String destClusterId;
}
