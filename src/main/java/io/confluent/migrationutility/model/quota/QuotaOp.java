package io.confluent.migrationutility.model.quota;

import lombok.Data;
import org.apache.kafka.common.quota.ClientQuotaAlteration;

@Data
public class QuotaOp {
  private final String key;
  private final Double value;

public QuotaOp(final ClientQuotaAlteration.Op op) {
  this.key = op.key();
  this.value = op.value();
}
}
