package io.confluent.migrationutility.service;

import io.confluent.migrationutility.model.topic.TopicEntry;
import io.confluent.migrationutility.model.topic.TopicMetadataResponse;
import io.confluent.migrationutility.util.AdminClientUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
@Service
public class TopicService {


  /**
   * Query topic metadata within kafka cluster
   * @param config cluster config
   * @param topics target topics of interest
   * @return all or target subset topic metadata
   */
  public TopicMetadataResponse listTopics(final Map<String, String> config, final List<String> topics) {
    final List<TopicEntry> srcTopicEntries = AdminClientUtils.listTopics2(config, topics);
    return new TopicMetadataResponse(srcTopicEntries);
  }

  /**
   * Export all/subset topics from source and apply all/subset topics in destination
   * @param srcConfig source cluster config
   * @param destConfig destination cluster config
   * @param targetTopics optional list of target topic names (topics of interest)
   * @return TopicMetadataResponse
   */
  public TopicMetadataResponse applyTopicMetadataRequest(final Map<String, String> srcConfig, final Map<String, String> destConfig, final List<String> targetTopics) {
    final List<TopicEntry> srcTopics = AdminClientUtils.listTopics2(srcConfig, targetTopics);
    final Set<String> destExistingTopics = listAndExtractTopicNames(destConfig, targetTopics);
    log.debug("Query destination topic list (pre-apply) result - {}", destExistingTopics);

    final List<TopicEntry> srcTopicsFiltered = srcTopics.stream()
            .filter(entry -> !destExistingTopics.contains(entry.getTopic()))
            .collect(Collectors.toList());
    log.debug("Topics to be created in destination cluster : {}", srcTopicsFiltered);
    AdminClientUtils.createTopics2(destConfig, srcTopicsFiltered);

    final List<TopicEntry> destTopics = AdminClientUtils.listTopics2(destConfig, targetTopics);
    log.debug("Query destination topic list (post-apply) result : {}", destTopics);
    return new TopicMetadataResponse(srcTopics, destTopics);
  }


  /**
   * Apply defined subset of topics into destination cluster
   * @param destConfig destination cluster config
   * @param targetTopics defined subset of topics to create
   * @return TopicMetadataResponse
   */
  public TopicMetadataResponse applyTopicMetadataRequest(final Map<String, String> destConfig, final List<TopicEntry> targetTopics) {
    List<String> targetTopicNames = targetTopics.stream().map(TopicEntry::getTopic).collect(Collectors.toList());
    final Set<String> destExistingTopics = listAndExtractTopicNames(destConfig, targetTopicNames);

    final List<TopicEntry> destTopicsFiltered = targetTopics.stream()
            .filter(entry -> !destExistingTopics.contains(entry.getTopic()))
            .collect(Collectors.toList());
    log.info("Defined topics to be created in destination cluster : {}", destTopicsFiltered);
    AdminClientUtils.createTopics2(destConfig, destTopicsFiltered);

    final List<TopicEntry> destTopics = AdminClientUtils.listTopics2(destConfig, targetTopicNames);
    log.info("Query destination topic list (post-apply) result : {}", destTopics);
    return new TopicMetadataResponse(targetTopics, destTopics);
  }

  private static Set<String> listAndExtractTopicNames(final Map<String, String> config, final List<String> topics) {
    return AdminClientUtils.listTopics2(config, topics)
            .stream()
            .map(TopicEntry::getTopic)
            .collect(Collectors.toSet());
  }


}
