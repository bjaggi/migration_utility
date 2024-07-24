package io.confluent.migrationutility.controller;

import io.confluent.migrationutility.config.KafkaClusterConfig;
import io.confluent.migrationutility.exception.InvalidClusterId;
import io.confluent.migrationutility.model.topic.TopicMetadataExtendedRequest;
import io.confluent.migrationutility.model.topic.TopicMetadataRequest;
import io.confluent.migrationutility.model.topic.TopicMetadataResponse;
import io.confluent.migrationutility.service.TopicService;
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

@Tag(name = "Topics")
@Slf4j
@RestController
@RequestMapping("/v1/api/topics")
@RequiredArgsConstructor
public class TopicController {

  private final KafkaClusterConfig clusterConfig;
  private final TopicService topicService;

  @Operation(
    summary = "Export Kafka topic metadata from specified Kafka Cluster.",
    description = "Export Kafka topic metadata (partitions, RF, configs) from specified Kafka Cluster."
)
@PostMapping("/export")
public TopicMetadataResponse exportTopics(@RequestBody final TopicMetadataRequest request) {
  log.info("Received request : {}", request);
  final Map<String, String> config = Optional.ofNullable(
          clusterConfig.getClusters().get(request.getSourceCluster())
  ).orElseThrow(() -> new InvalidClusterId(request.getSourceCluster()));

  return topicService.listTopics(config, request.getTopics());

}

  @Operation(
          description = "Given source cluster, destination cluster, and (optionally) a target subset of topics to migrate, export all or subset of topics from source cluster and apply/create into destination cluster",
          summary = "Export all/subset of kafka topics from source cluster and apply into destination cluster."
  )
  @PostMapping("/apply")
  public TopicMetadataResponse applyTopics(@RequestBody final TopicMetadataRequest request) {
    log.info("Received request : {}", request);
    final Map<String, String> srcClusterConfig = Optional.ofNullable(
            clusterConfig.getClusters().get(request.getSourceCluster())
    ).orElseThrow(() -> new InvalidClusterId(request.getSourceCluster()));

    final Map<String, String> destClusterConfig = Optional.ofNullable(
            clusterConfig.getClusters().get(request.getDestinationCluster())
    ).orElseThrow(() -> new InvalidClusterId(request.getDestinationCluster()));

    return topicService.applyTopicMetadataRequest(srcClusterConfig, destClusterConfig, request.getTopics());
  }

  @Operation(
          description = "Given source cluster, destination cluster, and (optionally) a target subset of topics to migrate, export all or subset of topics from source cluster and apply/create into destination cluster",
          summary = "Export all/subset of kafka topics from source cluster and apply into destination cluster."
  )
  @PostMapping("/applyV2")
  public TopicMetadataResponse applyTopics(@RequestBody final TopicMetadataExtendedRequest request) {
    log.info("Received request : {}", request);

    final Map<String, String> destClusterConfig = Optional.ofNullable(
            clusterConfig.getClusters().get(request.getClusterId())
    ).orElseThrow(() -> new InvalidClusterId(request.getClusterId()));

    return topicService.applyTopicMetadataRequest(destClusterConfig, request.getTopicEntries());
  }
}
