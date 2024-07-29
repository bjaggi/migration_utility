package io.confluent.migrationutility.model.quota;

import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.kafka.common.quota.ClientQuotaAlteration;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@NoArgsConstructor
@Data
public class QuotaEntry {
  Map<String, String> entities;
  List<QuotaOp> quotaScope;

  public QuotaEntry(final ClientQuotaAlteration quota) {
    this.entities = quota.entity().entries();
    this.quotaScope = quota.ops().stream().map(QuotaOp::new).collect(Collectors.toList());
  }
}
