package io.confluent.migrationutility.util;

import io.confluent.migrationutility.exception.AdminClientException;
import io.confluent.migrationutility.model.topic.TopicEntry;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterClientQuotasResult;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.CreateAclsResult;
import org.apache.kafka.clients.admin.DescribeClientQuotasOptions;
import org.apache.kafka.clients.admin.DescribeClientQuotasResult;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsResult;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.quota.ClientQuotaAlteration;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.kafka.common.quota.ClientQuotaFilter;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Slf4j
public final class AdminClientUtils {
  private AdminClientUtils() {}

  public static boolean consumerGroupIsEmpty(final Map<String, String> clusterConfigs, final String groupId) {
    final Map<String, Object> config = new HashMap<>(clusterConfigs);
    try (final AdminClient client = AdminClient.create(config)) {
      final DescribeConsumerGroupsResult cgDescribeResult = client.describeConsumerGroups(Collections.singleton(groupId));
      final Map<String, ConsumerGroupDescription> cgDescription = cgDescribeResult.all().get();
      return cgDescription.get(groupId).members().isEmpty();
    } catch (ExecutionException | InterruptedException e) {
      log.error("Unexpected threading error during consumerGroupIsActive query: {}", e.getMessage(), e);
      Thread.currentThread().interrupt();
      throw new AdminClientException(e);
    } catch (final Exception e) {
      log.error("Unexpected error during consumerGroupIsActive query: {}", e.getMessage(), e);
      throw new AdminClientException(e);
    }
  }

  // key == group.id
  public static Map<String, Map<TopicPartition, OffsetAndMetadata>> consumerGroupOffsets(final Map<String, String> clusterConfigs, final List<String> groups) {
    final Map<String, Map<TopicPartition, OffsetAndMetadata>> groupProgress = new HashMap<>();
    final Map<String, Object> config = new HashMap<>(clusterConfigs);
    try (final AdminClient client = AdminClient.create(config)) {
      log.info("Retrieving consumer group offsets for the following CGs: {}", groups);
      for (final String group : groups) {
        try {
          final Map<TopicPartition, OffsetAndMetadata> groupOffsets = client.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get();
          if (groupOffsets.isEmpty()) {
            log.warn("Could not find topic-partition metadata for consumer group: {}", group);
          } else {
            log.info("{} group topic-partition offsets: {}", group, groupOffsets);
            groupProgress.put(group, groupOffsets);
          }
        } catch (InterruptedException | ExecutionException e) {
          log.error("Unexpected threading error during group offset retrieval for group ({}): {}", group, e.getMessage(), e);
          Thread.currentThread().interrupt();
          throw new AdminClientException(e);
        }
      }
    } catch (Exception e) {
      log.error("Unexpected error occurred during group offset retrieval for group: {}", e.getMessage(), e);
      throw new AdminClientException(e);
    }
    return groupProgress;
  }


  public static void createTopics2(final Map<String, String> clusterConfigs, final List<TopicEntry> topicsToCreate) {
    final Map<String, Object> config = new HashMap<>(clusterConfigs);
    try (final AdminClient client = AdminClient.create(config)) {
      final List<NewTopic> newTopicList = new ArrayList<>();
      for (final TopicEntry entry : topicsToCreate) {
        final NewTopic newTopic = new NewTopic(entry.getTopic(), entry.getPartitions(), entry.getReplicationFactor());
        newTopic.configs(entry.getConfigs());
        newTopicList.add(newTopic);
      }
      client.createTopics(newTopicList).all().get();
    } catch (ExecutionException | InterruptedException e) {
      log.error("Unexpected threading error when creating Kafka Topics : {}", e.getMessage(), e);
      Thread.currentThread().interrupt();
      throw new AdminClientException(e);
    } catch (final Exception e) {
      log.error("Unexpected error when creating Kafka Topics : {}", e.getMessage(), e);
      throw new AdminClientException(e);
    }
  }


  public static List<ClientQuotaAlteration> listQuotas(final Map<String, String> clusterConfigs) {
    final Map<String, Object> config = new HashMap<>(clusterConfigs);
    try (final AdminClient adminClient = AdminClient.create(config)) {
      final DescribeClientQuotasResult result = adminClient.describeClientQuotas(ClientQuotaFilter.all(), new DescribeClientQuotasOptions());
      final Map<ClientQuotaEntity, Map<String, Double>> quotas = result.entities().get();

      final List<ClientQuotaAlteration> clientQuotaAlterations = quotas.entrySet().stream()
              .map(qe -> {
                final List<ClientQuotaAlteration.Op> quotaOps = qe.getValue().entrySet().stream()
                        .map(opEntry -> new ClientQuotaAlteration.Op(opEntry.getKey(), opEntry.getValue()))
                        .collect(Collectors.toList());
                return new ClientQuotaAlteration(qe.getKey(), quotaOps);
              })
              .collect(Collectors.toList());

      log.debug("listQuotas result: {}", clientQuotaAlterations);
      return clientQuotaAlterations;
    } catch (ExecutionException | InterruptedException e) {
      log.error("Unexpected threading error when listQuotas : {}", e.getMessage(), e);
      Thread.currentThread().interrupt();
      throw new AdminClientException(e);
    } catch (final Exception e) {
      log.error("Unexpected error when listQuotas : {}", e.getMessage(), e);
      throw new AdminClientException(e);
    }
  }

  public static List<AclBinding> listAcl(final Map<String, String> clusterConfigs) {
    final Map<String, Object> config = new HashMap<>(clusterConfigs);
    try (final AdminClient client = AdminClient.create(config)) {
      AclBindingFilter any = new AclBindingFilter(
              new ResourcePatternFilter(ResourceType.ANY, null, PatternType.ANY),
              new AccessControlEntryFilter(null, null, AclOperation.ANY, AclPermissionType.ANY));
      return new ArrayList<>(client.describeAcls(any).values().get());
    } catch (InterruptedException | ExecutionException e) {
      log.error("Unexpected threading error while listing ACLs : {}", e.getMessage(), e);
      Thread.currentThread().interrupt();
      throw new AdminClientException(e);
    } catch (final Exception e) {
      log.error("Unexpected error while listing ACLs : {}", e.getMessage(), e);
      throw new AdminClientException(e);
    }
  }


  public static void createQuotas(final Map<String, String> clusterConfigs, final List<ClientQuotaAlteration> quotaList) {
    final Map<String, Object> config = new HashMap<>(clusterConfigs);
    try (final AdminClient client = AdminClient.create(config)) {
      final AlterClientQuotasResult alterClientQuotasResult = client.alterClientQuotas(quotaList);
      alterClientQuotasResult.all().get();
    } catch (ExecutionException | InterruptedException e) {
      log.error("Unexpected threading error occurred while applying quotas : {}", e.getMessage(), e);
      Thread.currentThread().interrupt();
      throw new AdminClientException(e);
    } catch (final Exception e) {
      log.error("Unexpected error occurred while applying quotas : {}", e.getMessage(), e);
      throw new AdminClientException(e);
    }
  }

  public static void createAcls(final Map<String, String> clusterConfigs, List<AclBinding> aclList) {
    final Map<String, Object> config = new HashMap<>(clusterConfigs);
    try (final AdminClient client = AdminClient.create(config)) {
      final CreateAclsResult acls = client.createAcls(aclList);
      acls.all().get();
    } catch (ExecutionException | InterruptedException e) {
      log.error("Unexpected threading error occurred while applying ACLs: {}", e.getMessage(), e);
      Thread.currentThread().interrupt();
      throw new AdminClientException(e);
    } catch (final Exception e) {
      log.error("Unexpected error occurred while applying ACLs: {}", e.getMessage(), e);
      throw new AdminClientException(e);
    }
  }

  public static List<TopicEntry> listTopics2(final Map<String, String> clusterConfigs, final List<String> targetTopics) {
    final Map<String, Object> config = new HashMap<>(clusterConfigs);
    try (final AdminClient client = AdminClient.create(config)) {
      final ListTopicsResult topics = client.listTopics(new ListTopicsOptions().listInternal(false));
      final Set<String> topicStringSet = topics.names().get(60, TimeUnit.SECONDS);

      final Set<String> filteredTopicSet = getFilteredTopicSet(targetTopics, topicStringSet);
      final List<ConfigResource> configResources = filteredTopicSet.stream()
              .map(topic -> new ConfigResource(ConfigResource.Type.TOPIC, topic))
              .collect(Collectors.toList());
      final Map<ConfigResource, Config> configResourceConfigMap = client.describeConfigs(configResources).all().get();
      final Map<String, List<ConfigEntry>> topicConfigMap = extractTopicConfigs(configResourceConfigMap);

      final Map<String, TopicDescription> topicDescriptionMap = client.describeTopics(filteredTopicSet).allTopicNames().get();
      final List<TopicEntry> topicEntryList = composeTopicEntryList(topicConfigMap, topicDescriptionMap);
      log.debug("List topics result: {}", topicEntryList);
      return topicEntryList;

    } catch (ExecutionException | InterruptedException e) {
      log.error("Unexpected threading error while listing topics : {}", e.getMessage(), e);
      Thread.currentThread().interrupt();
      throw new AdminClientException(e);
    } catch (final Exception e) {
      log.error("Unexpected error occurred when listing topics : {}", e.getMessage(), e);
      throw new AdminClientException(e);
    }
  }

  private static List<TopicEntry> composeTopicEntryList(final Map<String, List<ConfigEntry>> topicConfigMap, final Map<String, TopicDescription> topicDescriptionMap) {
    return topicConfigMap.entrySet()
            .stream()
            .map(tce -> {
              final Map<String, String> dynamicConfigs = tce.getValue()
                      .stream()
                      .map(dce -> Map.entry(dce.name(), dce.value()))
                      .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
              final TopicDescription topicDescription = topicDescriptionMap.get(tce.getKey());
              return new TopicEntry(
                      tce.getKey(),
                      topicDescription.partitions().size(),
                      (short) topicDescription.partitions().get(0).replicas().size(),
                      dynamicConfigs
              );
            })
            .collect(Collectors.toList());
  }

  private static Map<String, List<ConfigEntry>> extractTopicConfigs(final Map<ConfigResource, Config> configResourceConfigMap) {
    return configResourceConfigMap.entrySet()
            .stream()
            .map(e -> Map.entry(
                    e.getKey().name(),
                    e.getValue().entries().stream().filter(configEntry -> configEntry.source().equals(ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG)).collect(Collectors.toList())
            ))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  private static Set<String> getFilteredTopicSet(final List<String> targetTopics, final Set<String> topicStringSet) {
    if (!CollectionUtils.isEmpty(targetTopics)) {
      topicStringSet.retainAll(targetTopics);
    }
    return topicStringSet.stream()
            .filter(name -> !name.contains("_confluent"))
            .filter(name -> !name.contains("default_ksql_processing_log"))
            .filter(name -> !name.contains("connect-"))
            .filter(name -> !name.contains("_schemas"))
            .collect(Collectors.toSet());
  }
}
