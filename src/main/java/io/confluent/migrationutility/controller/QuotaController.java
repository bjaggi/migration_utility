package io.confluent.migrationutility.controller;

import io.confluent.migrationutility.config.KafkaClusterConfig;
import io.confluent.migrationutility.exception.InvalidClusterIdException;
import io.confluent.migrationutility.model.quota.QuotaMetadataRequest;
import io.confluent.migrationutility.model.quota.QuotaMetadataExtendedRequest;
import io.confluent.migrationutility.model.quota.QuotaResponse;
import io.confluent.migrationutility.service.QuotaService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import java.util.Optional;

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
  log.info("Received request : {}", request);
  final Map<String, String> config = Optional.ofNullable(
          clusterConfig.getClusters().get(request.getSourceClusterId()))
          .orElseThrow(() -> new InvalidClusterIdException(request.getSourceClusterId()));
  return quotaService.listQuotas(config);
}


  @Operation(
          description = "Given a source and destination kafka cluster IDs, export all quotas from source kafka cluster and apply to destination kafka cluster",
          summary = "Export source cluster quotas and apply/create quotas against destination kafka cluster"
  )
  @PostMapping("/apply")
  public QuotaResponse applyQuotas(@RequestBody final QuotaMetadataRequest request) {
    log.info("Received request : {}", request);

    final Map<String, String> srcClusterConfig = Optional.ofNullable(
            clusterConfig.getClusters().get(request.getSourceClusterId())
    ).orElseThrow(() -> new InvalidClusterIdException(request.getSourceClusterId()));

    final Map<String, String> destClusterConfig = Optional.ofNullable(
            clusterConfig.getClusters().get(request.getDestClusterId())
    ).orElseThrow(() -> new InvalidClusterIdException(request.getDestClusterId()));

    return quotaService.applyQuotasRequest(srcClusterConfig, destClusterConfig);
  }

  @Operation(
          description = "Given a source and destination kafka cluster IDs, export all quotas from source kafka cluster and apply to destination kafka cluster",
          summary = "Export source cluster quotas and apply/create quotas against destination kafka cluster"
  )
  @PostMapping("/applyV2")
  public QuotaResponse applyQuotas(@RequestBody final QuotaMetadataExtendedRequest request) {
    log.info("Received request : {}", request);


    final Map<String, String> destClusterConfig = Optional.ofNullable(
            clusterConfig.getClusters().get(request.getClusterId())
    ).orElseThrow(() -> new InvalidClusterIdException(request.getClusterId()));

    return quotaService.applyQuotasRequest(destClusterConfig, request.getQuotaEntries());
  }
}
