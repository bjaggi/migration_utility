package io.confluent.migrationutility.controller;

import io.confluent.migrationutility.config.KafkaClusterConfig;
import io.confluent.migrationutility.model.topic.TopicMetadataExtendedRequest;
import io.confluent.migrationutility.model.topic.TopicMetadataRequest;
import io.confluent.migrationutility.model.topic.TopicMetadataResponse;
import io.confluent.migrationutility.service.TopicService;
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
    log.info("Received exportTopics request : {}", request);
    final Map<String, String> config = AppUtils.getClusterConfig(clusterConfig, request.getSourceCluster());
    return topicService.listTopics(config, request.getTopics());
  }

  @Operation(
          description = "Given source cluster, destination cluster, and (optionally) a target subset of topics to migrate, export all or subset of topics from source cluster and apply/create into destination cluster",
          summary = "Export all/subset of kafka topics from source cluster and apply into destination cluster."
  )
  @PostMapping("/apply")
  public TopicMetadataResponse applyTopics(@RequestBody final TopicMetadataRequest request) {
    log.info("Received applyTopics request : {}", request);
    final Map<String, String> srcClusterConfig = AppUtils.getClusterConfig(clusterConfig, request.getSourceCluster());
    final Map<String, String> destClusterConfig = AppUtils.getClusterConfig(clusterConfig, request.getDestinationCluster());
    return topicService.applyTopicMetadataRequest(srcClusterConfig, destClusterConfig, request.getTopics());
  }

  @Operation(
          description = "Given destination cluster and a defined set of topics to migrate, apply/create provided target topics into destination cluster",
          summary = "Apply/Create defined set of topic entries into destination cluster."
  )
  @PostMapping("/applyV2")
  public TopicMetadataResponse applyDefinedTopics(@RequestBody final TopicMetadataExtendedRequest request) {
    log.info("Received applyDefinedTopics request : {}", request);
    final Map<String, String> destClusterConfig = AppUtils.getClusterConfig(clusterConfig, request.getClusterId());
    return topicService.applyTopicMetadataRequest(destClusterConfig, request.getTopicEntries());
  }
}
