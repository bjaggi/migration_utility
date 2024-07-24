package io.confluent.migrationutility.model.quota;

import lombok.Data;
import org.apache.kafka.common.quota.ClientQuotaAlteration;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Data
public class QuotaEntry {
  final Map<String, String> entities;
  final List<QuotaOp> quotaScope;

  public QuotaEntry(final ClientQuotaAlteration quota) {
    this.entities = quota.entity().entries();
    this.quotaScope = quota.ops().stream().map(QuotaOp::new).collect(Collectors.toList());
  }
}
