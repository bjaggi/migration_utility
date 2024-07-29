package io.confluent.migrationutility.service;

import io.confluent.migrationutility.model.quota.QuotaEntry;
import io.confluent.migrationutility.model.quota.QuotaResponse;
import io.confluent.migrationutility.util.AdminClientUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.quota.ClientQuotaAlteration;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


@Slf4j
@Service
public class QuotaService {

  /**
   * Query and return all existing quotas
   * @param config clusterConfig
   * @return quotaResponse
   */
  public QuotaResponse listQuotas(final Map<String, String> config) {
    final List<QuotaEntry> quotaList = AdminClientUtils.listQuotas(config)
            .stream()
            .map(QuotaEntry::new)
            .collect(Collectors.toList());
    return new QuotaResponse(quotaList);

  }

  /**
   * Query all quotas from source cluster and apply them into destination cluster
   * @param srcConfig clusterConfig
   * @param destConfig clusterConfig
   * @return quotaResponse
   */
  public QuotaResponse applyQuotasRequest(final Map<String, String> srcConfig, final Map<String, String> destConfig) {
    final List<ClientQuotaAlteration> srcQuotas = AdminClientUtils.listQuotas(srcConfig);
    log.debug("Query source cluster quotas list result : {}", srcQuotas);

    AdminClientUtils.createQuotas(destConfig, srcQuotas);
    final List<ClientQuotaAlteration> destQuotas = AdminClientUtils.listQuotas(destConfig);
    log.debug("Query destination cluster quotas list result (post-apply) : {}", destQuotas);

    return new QuotaResponse(
            srcQuotas.stream().map(QuotaEntry::new).collect(Collectors.toList()),
            destQuotas.stream().map(QuotaEntry::new).collect(Collectors.toList())
    );
  }

  /**
   * Apply user-defined quotas into destination cluster
   * @param clusterConfig clusterConfig
   * @param entriesToApply user-defined quotas
   * @return quotaResponse
   */
  public QuotaResponse applyQuotasRequest(final Map<String, String> clusterConfig, final List<QuotaEntry> entriesToApply) {
    final List<ClientQuotaAlteration> targetEntriesToApply = mapEntriesToClientQuotaAlteration(entriesToApply);
    AdminClientUtils.createQuotas(clusterConfig, targetEntriesToApply);
    final List<ClientQuotaAlteration> destQuotas = AdminClientUtils.listQuotas(clusterConfig);
    log.debug("Query destination cluster quotas list (post-apply) result : {}", destQuotas);
    return new QuotaResponse(
            entriesToApply,
            destQuotas.stream().map(QuotaEntry::new).collect(Collectors.toList())
    );
  }

  private static List<ClientQuotaAlteration> mapEntriesToClientQuotaAlteration(final List<QuotaEntry> entriesToMap) {
    return entriesToMap
            .stream()
            .map(entry -> new ClientQuotaAlteration(
                    new ClientQuotaEntity(entry.getEntities()),
                    entry.getQuotaScope().stream().map(op -> new ClientQuotaAlteration.Op(op.getKey(), op.getValue())).collect(Collectors.toList())
            ))
            .collect(Collectors.toList());
  }

}
