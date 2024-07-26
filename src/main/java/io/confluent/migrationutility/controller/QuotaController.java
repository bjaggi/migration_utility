package io.confluent.migrationutility.controller;

import io.confluent.migrationutility.config.KafkaClusterConfig;
import io.confluent.migrationutility.model.quota.QuotaMetadataRequest;
import io.confluent.migrationutility.model.quota.QuotaMetadataExtendedRequest;
import io.confluent.migrationutility.model.quota.QuotaResponse;
import io.confluent.migrationutility.service.QuotaService;
import io.confluent.migrationutility.util.AppUtils;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@Tag(name = "Quota")
@Slf4j
@RestController
@RequestMapping("/v1/api/quota")
@RequiredArgsConstructor
public class QuotaController {

  private final KafkaClusterConfig clusterConfig;
  private final QuotaService quotaService;

  @Operation(
    description = "Export Quota metadata from Kafka Cluster.",
    summary = "Retrieves respective Quota from given kafka cluster "
)
@PostMapping("/export")
public QuotaResponse exportQuotas(@RequestBody final QuotaMetadataRequest request) {
  log.info("Received exportQuotas request : {}", request);
  final Map<String, String> config = AppUtils.getClusterConfig(clusterConfig, request.getSourceClusterId());
  return quotaService.listQuotas(config);
}

  @Operation(
          description = "Given a source and destination kafka cluster IDs, export all quotas from source kafka cluster and apply to destination kafka cluster",
          summary = "Export source cluster quotas and apply/create quotas against destination kafka cluster"
  )
  @PostMapping("/apply")
  public QuotaResponse applyQuotas(@RequestBody final QuotaMetadataRequest request) {
    log.info("Received applyQuotas request : {}", request);
    final Map<String, String> srcClusterConfig = AppUtils.getClusterConfig(clusterConfig, request.getSourceClusterId());
    final Map<String, String> destClusterConfig = AppUtils.getClusterConfig(clusterConfig, request.getDestClusterId());
    return quotaService.applyQuotasRequest(srcClusterConfig, destClusterConfig);
  }

  @Operation(
          description = "Given destination kafka cluster ID and target Quota list, apply/create specified quotas to destination kafka cluster",
          summary = "Apply/create provided quotas against destination kafka cluster"
  )
  @PostMapping("/applyV2")
  public QuotaResponse applyDefinedQuotas(@RequestBody final QuotaMetadataExtendedRequest request) {
    log.info("Received applyDefinedQuotas request : {}", request);
    final Map<String, String> destClusterConfig = AppUtils.getClusterConfig(clusterConfig, request.getClusterId());
    return quotaService.applyQuotasRequest(destClusterConfig, request.getQuotaEntries());
  }
}
