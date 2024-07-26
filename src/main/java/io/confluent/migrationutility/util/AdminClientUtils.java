package io.confluent.migrationutility.util;

import io.confluent.migrationutility.model.topic.TopicEntry;
import io.confluent.migration.model.TopicMetadata;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterClientQuotasResult;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.CreateAclsResult;
import org.apache.kafka.clients.admin.DescribeClientQuotasOptions;
import org.apache.kafka.clients.admin.DescribeClientQuotasResult;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
    } catch (Exception e) {
      log.error("Unexpected error during consumerGroupIsActive query : {}", e.getMessage(), e);
      throw new RuntimeException(e.getLocalizedMessage());
    }
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
      log.error("Unexpected error when creating Kafka Topics : {}", e.getMessage(), e);
      Thread.currentThread().interrupt();
    }
  }


  public static List<ClientQuotaAlteration>  listQuotas(final Map<String, String> clusterConfigs) {

    final Map<String, Object> config = new HashMap<>(clusterConfigs);
    List<ClientQuotaAlteration> clientQuotaAlterations = new ArrayList<ClientQuotaAlteration>();
    List<ClientQuotaAlteration.Op> qutoOpsList;
    try (final AdminClient adminClient = AdminClient.create(config)) {
      System.out.println("####### FOLLOWING QUOTAS's WERE FOUND  ####### :");

      DescribeClientQuotasResult result =  adminClient.describeClientQuotas(ClientQuotaFilter.all(),new DescribeClientQuotasOptions());
      Map<ClientQuotaEntity, Map<String, Double>> quotas = result.entities().get();
      for (Map.Entry<ClientQuotaEntity, Map<String, Double>> entry : quotas.entrySet()) {
        System.out.println("Entity: " + entry.getKey() + "\n");
        qutoOpsList = new ArrayList<>();
        for (Map.Entry<String, Double> quota : entry.getValue().entrySet()) {
          System.out.println(quota.getKey() + ": " + quota.getValue() + "\n");

          ClientQuotaAlteration.Op quotaOp = new ClientQuotaAlteration.Op(quota.getKey(),  quota.getValue());
          qutoOpsList.add(quotaOp);
        }
        ClientQuotaAlteration alteration = new ClientQuotaAlteration(entry.getKey(), qutoOpsList);
        clientQuotaAlterations.add(alteration);
      }

      System.out.println(clientQuotaAlterations);
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (ExecutionException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return  clientQuotaAlterations;
  }

  public static List<AclBinding>  listAcl(final Map<String, String> clusterConfigs) {

    final Map<String, Object> config = new HashMap<>(clusterConfigs);
    Map< String,TopicMetadata> topicMetadataMap = new HashMap<>();
    Collection<AclBinding> aclBindingList = null;
    try (final AdminClient client = AdminClient.create(config)) {
      System.out.println("####### FOLLOWING ACL's WERE FOUND  ####### :");
      AclBindingFilter any = new AclBindingFilter(
              new ResourcePatternFilter(ResourceType.ANY, null, PatternType.ANY),
              new AccessControlEntryFilter(null, null, AclOperation.ANY, AclPermissionType.ANY));

      aclBindingList = client.describeAcls(any).values().get();


    } catch (InterruptedException | ExecutionException e) {
      log.error("Unexpected error while listing ACLs : {}", e.getMessage(), e);
      Thread.currentThread().interrupt();
    }
    return  new ArrayList<>(aclBindingList);
  }


  public static void  createQuotas(final Map<String, String> clusterConfigs, final List<ClientQuotaAlteration> quotaList ){
    final Map<String, Object> config = new HashMap<>(clusterConfigs);
    try (final AdminClient client = AdminClient.create(config)) {
      final AlterClientQuotasResult alterClientQuotasResult = client.alterClientQuotas(quotaList);
      alterClientQuotasResult.all().get();
    } catch (ExecutionException | InterruptedException e) {
      log.error("Unexpected error occurred while applying quotas : {}", e.getMessage(), e);
      Thread.currentThread().interrupt();
    }
  }

  public static void  createAcls(final Map<String, String> clusterConfigs, List<AclBinding> aclList){
    final Map<String, Object> config = new HashMap<>(clusterConfigs);
    try (final AdminClient client = AdminClient.create(config)) {
      final CreateAclsResult acls = client.createAcls(aclList);
      acls.all().get();
    } catch (ExecutionException | InterruptedException e) {
      log.error("Unexpected error occurred while applying ACLs: {}", e.getMessage(), e);
      Thread.currentThread().interrupt();
    }
  }

  public static List<TopicEntry> listTopics2(final Map<String, String> clusterConfigs, final List<String> targetTopics) {
    final Map<String, Object> config = new HashMap<>(clusterConfigs);
    try (final AdminClient client = AdminClient.create(config)) {
      final ListTopicsResult topics = client.listTopics(new ListTopicsOptions().listInternal(false));
      final Set<String> topicStringSet = topics.names().get(60, TimeUnit.SECONDS);
      if (!CollectionUtils.isEmpty(targetTopics)) {
        topicStringSet.retainAll(targetTopics);
      }

      final Set<String> filteredtopicStringSet = topicStringSet.stream()
              .filter( name-> !name.contains("_confluent-controlcenter") )
              .filter( name-> !name.contains("_confluent") )
              .filter( name-> !name.contains("default_ksql_processing_log"))
              .filter( name-> !name.contains("connect-configs"))
              .filter( name-> !name.contains("connect-statuses"))
              .filter( name-> !name.contains("_schemas"))
              .filter( name-> !name.contains("connect-offsets"))
              .collect(Collectors.toSet());
      final DescribeTopicsResult topicsConfigList = client.describeTopics(filteredtopicStringSet);
      final List<ConfigResource> configResources = filteredtopicStringSet.stream()
              .map(topic -> new ConfigResource(ConfigResource.Type.TOPIC, topic))
              .collect(Collectors.toList());
      final DescribeConfigsResult describeConfigsResult = client.describeConfigs(configResources);
      final Map<ConfigResource, Config> configResourceConfigMap = describeConfigsResult.all().get();
      final Map<String, List<ConfigEntry>> topicConfigMap = new HashMap<>();
      for (final ConfigResource cr : configResourceConfigMap.keySet()) {
        topicConfigMap.put(
                cr.name(),
                configResourceConfigMap.get(cr).entries()
                        .stream()
                        .filter(configEntry -> configEntry.source().equals(ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG))
                        .collect(Collectors.toList())
        );
      }

      final List<TopicEntry> topicEntryList = new ArrayList<>();
      for (final String topic : topicConfigMap.keySet()) {
        final List<ConfigEntry> dynamicConfigEntries = topicConfigMap.get(topic);
        final Map<String, String> dynamicConfigs = new HashMap<>();
        for (final ConfigEntry entry : dynamicConfigEntries) {
          dynamicConfigs.put(entry.name(), entry.value());
        }
        final TopicDescription topicDescription = topicsConfigList.topicNameValues().get(topic).get();
        final TopicEntry topicEntry = new TopicEntry(
                topic,
                topicDescription.partitions().size(),
                (short) topicDescription.partitions().get(0).replicas().size(),
                dynamicConfigs
        );
        topicEntryList.add(topicEntry);
      }
      return topicEntryList;
    } catch (ExecutionException | InterruptedException e) {
      log.error("Unexpected exception while listing topics : {}", e.getMessage(), e);
      Thread.currentThread().interrupt();
    } catch (TimeoutException e) {
      log.error("Timeout exception occurred when listing topics : {}", e.getMessage(), e);
      throw new RuntimeException(e);
    }
    return Collections.emptyList();

  }

  // key == group.id
  public static Map<String, Map<TopicPartition, OffsetAndMetadata>> consumerGroupOffsets(final Map<String, String> clusterConfigs, final List<String> groups) {
    final Map<String, Map<TopicPartition, OffsetAndMetadata>> groupProgress = new HashMap<>();
    final Map<String, Object> config = new HashMap<>(clusterConfigs);
    try (final AdminClient client = AdminClient.create(config)) {
      log.info("Retrieving consumer group offsets for the following CGs : {}", groups);
      for (final String group : groups) {
        try {
          final Map<TopicPartition, OffsetAndMetadata> groupOffsets = client.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get();
          if (groupOffsets.isEmpty()) {
            log.warn("Could not find topic-partition metadata for consumer group : {}.", group);
          } else {
            log.info("{} group topic-partition offsets : {}", group, groupOffsets);
            groupProgress.put(group, groupOffsets);
          }
        } catch (InterruptedException | ExecutionException e) {
          log.error("Error during group offset retrieval for group : {}", group, e);
          Thread.currentThread().interrupt();
        }
      }
    } catch (Exception e) {
      log.error("Unexpected error occured : {}", e.getMessage(), e);
    }
    return groupProgress;
  }
}
