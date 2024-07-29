package io.confluent.migrationutility.model.quota;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class QuotaMetadataRequest {
  private String sourceClusterId;
  private String destClusterId;
}
