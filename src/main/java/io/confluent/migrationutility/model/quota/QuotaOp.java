package io.confluent.migrationutility.model.quota;

import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.kafka.common.quota.ClientQuotaAlteration;

@NoArgsConstructor
@Data
public class QuotaOp {
  private String key;
  private Double value;

  public QuotaOp(final ClientQuotaAlteration.Op op) {
    this.key = op.key();
    this.value = op.value();
  }

}
