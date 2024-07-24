package io.confluent.migrationutility.controller;

import io.confluent.migrationutility.config.KafkaClusterConfig;
import io.confluent.migrationutility.exception.InvalidClusterId;
import io.confluent.migrationutility.model.acl.ACLMetadataRequest;
import io.confluent.migrationutility.model.acl.ACLMetadataRequestV2;
import io.confluent.migrationutility.model.acl.AclEntry;
import io.confluent.migrationutility.model.acl.AclResponse;
import io.confluent.migrationutility.service.AclService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.apache.kafka.common.acl.AclBinding;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

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
public AclResponse exportAcl(@RequestBody final ACLMetadataRequest request) {
  log.info("Received request : {}", request);
  final Map<String, String> config = Optional.ofNullable(
          clusterConfig.getClusters().get(request.getSourceClusterId()))
          .orElseThrow(() -> new InvalidClusterId(request.getSourceClusterId()));
   final List<AclBinding> aclList = aclService.listAcl(config);
   return new AclResponse(
           aclList.stream().map(AclEntry::new).collect(Collectors.toList())
   );
}

  @Operation(
          description = "Fetch all ACLs from source Kafka cluster and apply into destination Kafka cluster",
          summary = "Provided a source and destination cluster ID, syncrhonize all ACLs from source to destination Kafka cluster"
  )
  @PostMapping("/apply")
  public AclResponse applyAcls(@RequestBody final ACLMetadataRequest request) {
    log.info("Received request : {}", request);
    final Map<String, String> sourceConfig = Optional.ofNullable(
            clusterConfig.getClusters().get(request.getSourceClusterId())
    ).orElseThrow(() -> new InvalidClusterId(request.getSourceClusterId()));

    final Map<String, String> destConfig = Optional.ofNullable(
            clusterConfig.getClusters().get(request.getDestClusterId())
    ).orElseThrow(() -> new InvalidClusterId(request.getDestClusterId()));

    return aclService.applyAclMetadataRequest(sourceConfig, destConfig);
  }

  @PostMapping("/applyv2")
  public AclResponse applyAclsV2(@RequestBody final ACLMetadataRequestV2 request) {
    log.info("Received request : {}", request);

    final Map<String, String> destConfig = Optional.ofNullable(
            clusterConfig.getClusters().get(request.getClusterId())
    ).orElseThrow(() -> new InvalidClusterId(request.getClusterId()));

    return aclService.applyAclMetadataRequest(destConfig, request.getAclEntries());

  }

}
