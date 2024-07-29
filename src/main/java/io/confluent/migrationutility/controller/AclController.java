package io.confluent.migrationutility.controller;

import io.confluent.migrationutility.config.KafkaClusterConfig;
import io.confluent.migrationutility.model.acl.ACLMetadataRequest;
import io.confluent.migrationutility.model.acl.ACLMetadataRequestV2;
import io.confluent.migrationutility.model.acl.AclResponse;
import io.confluent.migrationutility.service.AclService;
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

@Tag(name = "Acl")
@Slf4j
@RestController
@RequestMapping("/v1/api/acl")
@RequiredArgsConstructor
public class AclController {

  private final KafkaClusterConfig clusterConfig;
  private final AclService aclService;

  @Operation(
          description = "Export ACL metadata from Kafka Cluster.",
          summary = "Retrieves all respective ACL from given kafka source cluster"
  )
  @PostMapping("/export")
  public AclResponse exportAllAcl(@RequestBody final ACLMetadataRequest request) {
    log.info("Received exportAllAcl request : {}", request);
    final Map<String, String> config = AppUtils.getClusterConfig(clusterConfig, request.getSourceClusterId());
    return aclService.listAcl(config);
  }

  @Operation(
          description = "Fetch all ACLs from source Kafka cluster and apply into destination Kafka cluster",
          summary = "Provided a source and destination cluster ID, synchronize all ACLs from source to destination Kafka cluster"
  )
  @PostMapping("/apply")
  public AclResponse applyAllAcl(@RequestBody final ACLMetadataRequest request) {
    log.info("Received applyAcl request : {}", request);
    final Map<String, String> sourceConfig = AppUtils.getClusterConfig(clusterConfig, request.getSourceClusterId());
    final Map<String, String> destConfig = AppUtils.getClusterConfig(clusterConfig, request.getDestClusterId());
    return aclService.applyAclMetadataRequest(sourceConfig, destConfig);
  }


  @Operation(
          description = "Apply provided ACL entries into specified Kafka cluster",
          summary = "Provided a destination cluster ID and ACL list, apply/create in destination Kafka cluster"
  )
  @PostMapping("/applyV2")
  public AclResponse applyDefinedAcl(@RequestBody final ACLMetadataRequestV2 request) {
    log.info("Received applyDefinedAcl  request : {}", request);
    final Map<String, String> destConfig = AppUtils.getClusterConfig(clusterConfig, request.getClusterId());
    return aclService.applyAclMetadataRequest(destConfig, request.getAclEntries());
  }

}
