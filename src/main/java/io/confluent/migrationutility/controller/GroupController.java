package io.confluent.migrationutility.controller;

import io.confluent.migrationutility.config.KafkaClusterConfig;
import io.confluent.migrationutility.exception.InvalidClusterId;
import io.confluent.migrationutility.model.ApplyGroupMetadataRequest;
import io.confluent.migrationutility.model.ConsumerGroupMetadata;
import io.confluent.migrationutility.model.GroupMetadataRequest;
import io.confluent.migrationutility.model.PostApplyGroupOffsets;
import io.confluent.migrationutility.service.GroupService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.CollectionUtils;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@Tag(name = "ConsumerGroups")
@Slf4j
@RestController
@RequestMapping("/v1/api/cg")
@RequiredArgsConstructor
public class GroupController {

  private final KafkaClusterConfig clusterConfig;
  private final GroupService service;

  @Operation(
          description = "Export consumer group offsets/topic-partition metadata from Kafka Cluster.",
          summary = "Retrieves respective consumer group topic-partition metadata scraped from given kafka cluster identifier for each consumer group ID passed in request body"
  )
  @PostMapping("/export")
  public List<ConsumerGroupMetadata> exportConsumerGroupOffsets(@RequestBody final GroupMetadataRequest request) {
    log.info("Received request : {}", request);
    final Map<String, String> config = Optional.ofNullable(
            clusterConfig.getClusters().get(request.getClusterId())
    ).orElseThrow(() -> new InvalidClusterId(request.getClusterId()));

    final List<ConsumerGroupMetadata> consumerGroupMetadataList = service.consumerGroupMetadataList(config, request.getGroups());
    for (ConsumerGroupMetadata cgm : consumerGroupMetadataList ) {
      if (CollectionUtils.isEmpty(request.getTopics())) {
        cgm.setTopicPartitionMetadata(
                cgm.getTopicPartitionMetadata().stream()
                        .sorted()
                        .collect(Collectors.toList())
        );
      } else {
        cgm.setTopicPartitionMetadata(
                cgm.getTopicPartitionMetadata().stream()
                        .filter(tpm -> request.getTopics().contains(tpm.getTopicName()))
                        .sorted()
                        .collect(Collectors.toList())
        );
      }

    }
    return consumerGroupMetadataList;
  }

  @Operation(
          description = "Apply consumer group offset reset based on exported consumer group TP offset metadata.",
          summary = "For each consumer group, fetch offsets against referenced clusterId based on TP timestamps in request payload. For any TP that does not have an event with timestamp >= timestamp provided in payload, CG pointer will point at earliest non-expired offset post-apply execution. Otherwise, pick up from where CG left off."
  )
  @PostMapping("/apply")
  public List<PostApplyGroupOffsets> applyConsumerGroupOffsets(@RequestBody final ApplyGroupMetadataRequest request) {
    log.info("Received request : {}", request);
    final Map<String, String> config = Optional.ofNullable(
            clusterConfig.getClusters().get(request.getClusterId())
    ).orElseThrow(() -> new InvalidClusterId(request.getClusterId()));
    return service.applyConsumerGroupMetadata(config, request.getGroupMetadataList());
  }
}
