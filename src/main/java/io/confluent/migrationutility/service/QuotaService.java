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

  public QuotaResponse listQuotas(final Map<String, String> config) {
    final List<QuotaEntry> quotaList = AdminClientUtils.listQuotas(config)
            .stream()
            .map(QuotaEntry::new)
            .collect(Collectors.toList());
    return new QuotaResponse(quotaList);

  }

  public QuotaResponse applyQuotasRequest(final Map<String, String> srcConfig, final Map<String, String> destConfig) {
    final List<ClientQuotaAlteration> srcQuotas = AdminClientUtils.listQuotas(srcConfig);
    log.info("Query source cluster quotas list result : {}", srcQuotas);
    log.info("Applying source cluster quotas to destination cluster");
    AdminClientUtils.createQuotas(destConfig, srcQuotas);
    final List<ClientQuotaAlteration> destQuotas = AdminClientUtils.listQuotas(destConfig);
    log.info("Query destination cluster quotas list result : {}", destQuotas);
    return new QuotaResponse(
            srcQuotas.stream().map(QuotaEntry::new).collect(Collectors.toList()),
            destQuotas.stream().map(QuotaEntry::new).collect(Collectors.toList())
    );
  }

  public QuotaResponse applyQuotasRequest(final Map<String, String> clusterConfig, final List<QuotaEntry> entriesToApply) {

    List<ClientQuotaAlteration> targetEntriesToApply = entriesToApply.stream().map(entry -> {
      return new ClientQuotaAlteration(
              new ClientQuotaEntity(entry.getEntities()),
              entry.getQuotaScope().stream().map(op -> new ClientQuotaAlteration.Op(op.getKey(), op.getValue())).collect(Collectors.toList())
      );
    }).collect(Collectors.toList());

    log.info("Applying source cluster quotas to destination cluster");
    AdminClientUtils.createQuotas(clusterConfig, targetEntriesToApply);
    final List<ClientQuotaAlteration> destQuotas = AdminClientUtils.listQuotas(clusterConfig);
    log.info("Query destination cluster quotas list result : {}", destQuotas);
    return new QuotaResponse(
            entriesToApply,
            destQuotas.stream().map(QuotaEntry::new).collect(Collectors.toList())
    );
  }


}
