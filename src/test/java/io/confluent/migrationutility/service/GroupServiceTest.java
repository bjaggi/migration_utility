package io.confluent.migrationutility.service;

import io.confluent.migrationutility.framework.Cluster;
import io.confluent.migrationutility.framework.TestKafkaContext;
import io.confluent.migrationutility.model.group.ConsumerGroupMetadata;
import io.confluent.migrationutility.model.group.PostApplyGroupOffsets;
import io.confluent.migrationutility.model.group.TopicPartitionMetadata;
import lombok.SneakyThrows;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.awaitility.Awaitility;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class GroupServiceTest extends TestKafkaContext {
  private static final StringSerializer serializer = new StringSerializer();
  private static final StringDeserializer deserializer = new StringDeserializer();
  private final static Map<String, String> sourceClusterConfig = new HashMap<>();
  private GroupService service;

  @BeforeAll
  static void contextConfig() {
    sourceClusterConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, srcKafkaContainer.getBootstrapServers());
  }



  /**
   * Assert utility tool does not 'muddy' up source cluster on export/fetch
   */
  @AfterAll
  static void groupServiceDoesNotCommitUsingUtilityToolGroupId() {
    try (final AdminClient client = AdminClient.create(producerConfigs(Cluster.SOURCE))) {
      final Collection<ConsumerGroupListing> consumerGroupListings = client.listConsumerGroups().all().get();
      final List<ConsumerGroupListing> consumerGroupsContainingUtilityToolGroupIdForFetching = consumerGroupListings.stream()
              .filter(listing -> listing.groupId().contains("migration_utility_tool"))
              .collect(Collectors.toList());
      assertThat(consumerGroupsContainingUtilityToolGroupIdForFetching)
              .isEmpty();
    } catch (ExecutionException | InterruptedException e) {
      Assertions.fail();
    }
  }



  @BeforeEach
  void init() {
    service = new GroupService();
  }

  @Test
  void testConsumerGroupMetadataList() {
    final String topic = "test_group_service_export";
    final String groupId = topic + "_cg";
    final long startingTs = Instant.now().toEpochMilli();

    final Long sendEventCount = 100L;
    final Long receiveEventCount = 50L;
    final List<ConsumerGroupMetadata> expected = Collections.singletonList(new ConsumerGroupMetadata(
                    groupId,
                    Collections.singletonList(
                            new TopicPartitionMetadata(topic, 0, receiveEventCount, sendEventCount, sendEventCount - receiveEventCount, startingTs+receiveEventCount)
                    )
            )
    );
    produceWithExplicitTS(topic, sendEventCount, startingTs);
    consumeFromTopic(topic, groupId, receiveEventCount, Cluster.SOURCE);
    final List<ConsumerGroupMetadata> actual = service.consumerGroupMetadataList(sourceClusterConfig, Collections.singletonList(groupId));

    System.out.println(actual);
    assertThat(actual)
            .isNotNull()
            .isNotEmpty()
            .isEqualTo(expected);

    // Safe measure validation : utility tool returns correct timestamp
    try (final Consumer<String, String> freeConsumer = new KafkaConsumer<>(consumerConfigs(getShortUUID(), Cluster.SOURCE), new StringDeserializer(), new StringDeserializer())) {
      freeConsumer.assign(Collections.singleton(new TopicPartition(topic, 0)));
      freeConsumer.seek(new TopicPartition(topic, 0), sendEventCount-1);
      final ConsumerRecords<String, String> validationRecord = freeConsumer.poll(Duration.ofSeconds(1));
      assertThat(validationRecord).isNotEmpty()
              .hasSize(1);
      final ConsumerRecord<String, String> lastEventSentByProducer = validationRecord.iterator().next();
      assertThat(lastEventSentByProducer.offset()).isEqualTo(sendEventCount-1);
      assertThat(lastEventSentByProducer.timestamp())
              .isGreaterThan(expected.get(0).getTopicPartitionMetadata().get(0).getTimestamp())
              .isEqualTo(startingTs+sendEventCount-1);
    }
  }

  @ParameterizedTest
  @MethodSource
  void fetchConsumerGroupMetadata(String topic, List<String> groups, Long produceCount, Long consumeCount) {
    final long startingTs = Instant.now().toEpochMilli();

    final List<ConsumerGroupMetadata> expected = new ArrayList<>();
    final long expectedTimestamp = produceCount.equals(consumeCount) ? startingTs+consumeCount-1 : startingTs+consumeCount;
    for (String group : groups) {
      expected.add(new ConsumerGroupMetadata(
              group,
              Collections.singletonList(new TopicPartitionMetadata(topic, 0, consumeCount, produceCount, produceCount - consumeCount, expectedTimestamp))
      ));
    }

    produceWithExplicitTS(topic, produceCount, startingTs);
    for (String group : groups) {
      consumeFromTopic(topic, group, consumeCount, Cluster.SOURCE);
    }

    final List<ConsumerGroupMetadata> actual = service.consumerGroupMetadataList(sourceClusterConfig, groups);
    System.out.println();
    System.out.println(actual);
    System.out.println();

    assertThat(actual)
            .isNotNull()
            .isNotEmpty()
            .hasSameElementsAs(expected);

  }

  public static Stream<Arguments> fetchConsumerGroupMetadata() {
    final String topicPrefix = "groupServiceTestFetch.";
    return Stream.of(
            Arguments.of(topicPrefix+"001", Collections.singletonList(topicPrefix + getShortUUID()), 100L, 99L),
            Arguments.of(topicPrefix+"002", Arrays.asList(topicPrefix + UUID.randomUUID(), topicPrefix + getShortUUID()), 12L, 2L),
            Arguments.of(topicPrefix+"003", Collections.singletonList(topicPrefix + getShortUUID()), 50L, 50L),
            Arguments.of(topicPrefix+"004", Arrays.asList(topicPrefix + getShortUUID(), topicPrefix + getShortUUID()), 21L, 21L)
    );
  }

  @Test
  void applyConsumerGroupMetadata() {
    final String topic = "apply_cg_md_test";
    final String topic2 = topic+"2";
    final String group = topic+"_cg";
    final Long startingTs = Instant.now().toEpochMilli();
    produceWithExplicitTS(topic, 100L, startingTs);
    produceWithExplicitTS(topic2, 10L, startingTs);

    // assume group consumed 50 events from topic1 (out of 100)
    final Long consumedFromSrcTopic1 = 50L;
    // assume group consumed 5 events from topic2 (out of 10)
    final Long consumedFromSrcTopic2 = 5L;


    // expected: after reset, group should be pointing at offset 50 for topic-0 (and 50 lag)
    // expected: after reset, group should be pointing at offset 5 (and 5 lag)
    final ConsumerGroupMetadata mockSrcGroupMetadata = new ConsumerGroupMetadata(group, Arrays.asList(
            new TopicPartitionMetadata(topic, 0, -1L, 100L, -1L, startingTs + consumedFromSrcTopic1),
            new TopicPartitionMetadata(topic2, 0, -1L, 10L, -1L, startingTs + consumedFromSrcTopic2)
    ));

    Map<String, String> mockDestinationCluster = sourceClusterConfig;
    final List<PostApplyGroupOffsets> postApplyGroupOffsets = service.applyConsumerGroupMetadata(mockDestinationCluster, Collections.singletonList(mockSrcGroupMetadata));
    assertThat(postApplyGroupOffsets).isNotNull().isNotNull().hasSize(1);
    final PostApplyGroupOffsets actual = postApplyGroupOffsets.get(0);
    assertThat(actual.getConsumerGroupName()).isEqualTo(group);
    assertThat(actual.getTpOffsets())
            .hasSize(2)
            .containsEntry(topic+"-0", 50L)
            .containsEntry(topic2+"-0", 5L);

    System.out.println();
    System.out.println(actual);
    System.out.println();
  }

  @SneakyThrows
  @Test
  void emptyTopicApplyGroupMetadata() {
    final String topic = "empty_topic_apply_group_metadata_test";
    final String group = topic+"_cg";
    createTopic(topic);

    final long oneHourAgoTS = Instant.now().minusSeconds(60 * 60).toEpochMilli();

    final List<PostApplyGroupOffsets> expected = Collections.singletonList(
            new PostApplyGroupOffsets(group, Collections.singletonMap(topic+"-0", 0L))
    );
    final List<PostApplyGroupOffsets> actual = service.applyConsumerGroupMetadata(
            sourceClusterConfig,
            Collections.singletonList(new ConsumerGroupMetadata(
                    group, Collections.singletonList(new TopicPartitionMetadata(topic, 0, -1L, -1L, -1L, oneHourAgoTS))
            ))
    );

    System.out.println();
    System.out.println(actual);
    System.out.println();
    assertThat(actual).isNotNull().hasSize(1)
            .isEqualTo(expected);

  }

  /*
  Scenario : current offset == 4, logEndOffset == 4, lag==0, all events have expired.
  Result: Set to earliest

  e2e
   */
  @SneakyThrows
  @Test
  void expiredOffsetsTopicApplyGroupMetadata() {
    final String topic = "expired_event_topic_apply_group_md_test";
    final String group = topic+"_cg";
    final String destGroup = group+"2";
    final Long sendCount = 4L;

    final long startingTs = Instant.now().toEpochMilli();
    produceWithExplicitTS(topic, sendCount, startingTs);
    consumeFromTopic(topic, group, sendCount, Cluster.SOURCE);

    final List<ConsumerGroupMetadata> actualFetchListInitial = service.consumerGroupMetadataList(sourceClusterConfig, Collections.singletonList(group));
    assertThat(actualFetchListInitial)
            .isNotNull().hasSize(1)
            .containsOnly(
                    new ConsumerGroupMetadata(group, Collections.singletonList(new TopicPartitionMetadata(topic, 0, sendCount, sendCount, 0L, startingTs+sendCount-1)))
            );
    final List<TopicPartitionMetadata> actualFetchMetadataInitial = actualFetchListInitial.get(0).getTopicPartitionMetadata();
    assertThat(actualFetchMetadataInitial).isNotNull().hasSize(1);

    reduceTopicRetention(topic, 1L);



    final List<ConsumerGroupMetadata> actualFetchListPostExpiry = service.consumerGroupMetadataList(sourceClusterConfig, Collections.singletonList(group));
    assertThat(actualFetchListPostExpiry)
            .isNotNull()
            .hasSize(1);
    final List<TopicPartitionMetadata> actualFetchMetadataPostExpiry = actualFetchListPostExpiry.get(0).getTopicPartitionMetadata();
    assertThat(actualFetchMetadataPostExpiry).isNotNull().hasSize(1);

    assertThat(actualFetchMetadataPostExpiry.get(0).getTopicName()).isEqualTo(actualFetchMetadataInitial.get(0).getTopicName());
    assertThat(actualFetchMetadataPostExpiry.get(0).getPartitionNum()).isEqualTo(actualFetchMetadataInitial.get(0).getPartitionNum());
    assertThat(actualFetchMetadataPostExpiry.get(0).getCurrentOffsetNumber()).isEqualTo(actualFetchMetadataInitial.get(0).getCurrentOffsetNumber());
    assertThat(actualFetchMetadataPostExpiry.get(0).getLogEndOffsetNumber()).isEqualTo(actualFetchMetadataInitial.get(0).getLogEndOffsetNumber());
    assertThat(actualFetchMetadataPostExpiry.get(0).getConsumerLag()).isEqualTo(actualFetchMetadataInitial.get(0).getConsumerLag());

    // When currentOffset - 1 isnt available (ie. currentOffset - n expired events), returns oneMinAgo TS, which returns null on .offsetsForTimes(), which then falls back to earliest available offset (4)
    assertThat(actualFetchMetadataPostExpiry.get(0).getTimestamp())
            .isNotEqualTo(actualFetchMetadataInitial.get(0).getTimestamp())
            .isLessThan(actualFetchMetadataInitial.get(0).getTimestamp());


    // apply offset reset with group2 using group1 metadata to ensure earliest offset has been set (given .offsetsForTimes(historic) will return null (expired events))
    final List<PostApplyGroupOffsets> actualApplyResults = service.applyConsumerGroupMetadata(
            sourceClusterConfig,
            Collections.singletonList(new ConsumerGroupMetadata(destGroup, actualFetchMetadataPostExpiry))
    );
    assertThat(actualApplyResults).isNotNull().hasSize(1);
    assertThat(actualApplyResults.get(0).getTpOffsets()).hasSize(1)
            .containsEntry(topic+"-0", sendCount);
    System.out.println();
    System.out.println(actualApplyResults);
    System.out.println();
  }


  @SneakyThrows
  private void produceWithExplicitTS(final String topic, final Long count, final Long startingTS) {
    try (Producer<String, String> sender = new KafkaProducer<>(producerConfigs(Cluster.SOURCE), serializer, serializer)) {
      int i = 0;
      while (i < count) {
        sender.send(new ProducerRecord<>(topic, 0, startingTS+i, null, count.toString())).get();
        i++;
      }
      sender.flush();
    }
  }


  @SneakyThrows
  private static void createTopic(final String topicName) {
    try (final AdminClient client = AdminClient.create(producerConfigs(Cluster.SOURCE))) {
      client.createTopics(Collections.singleton(new NewTopic(topicName, 1, (short) 1))).all().get();
    }
  }

  @SneakyThrows
  private static void reduceTopicRetention(final String topicName, final Long targetRetention) {
    try (final AdminClient client = AdminClient.create(producerConfigs(Cluster.SOURCE))) {
      final ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
      final ConfigEntry configEntry = new ConfigEntry(TopicConfig.RETENTION_MS_CONFIG, targetRetention.toString());
      final AlterConfigOp reduceRetentionOp = new AlterConfigOp(
              configEntry, AlterConfigOp.OpType.SET
      );
      final Map<ConfigResource, Collection<AlterConfigOp>> configs = new HashMap<>();
      configs.put(configResource, Collections.singletonList(reduceRetentionOp));

      try  {
        final AlterConfigsResult alterConfigsResult = client.incrementalAlterConfigs(configs);
        alterConfigsResult.all().get();
        final Void unused = alterConfigsResult
                .values()
                .get(configResource).get();
      } catch (InterruptedException | ExecutionException e) {
        System.err.println(e.getMessage());
        Thread.currentThread().interrupt();
      }


      try (final KafkaConsumer<String, String> offsetChecker = new KafkaConsumer<>(consumerConfigs(getShortUUID(), Cluster.SOURCE), deserializer, deserializer)) {
        final TopicPartition targetTp = new TopicPartition(topicName, 0);
        Awaitility.waitAtMost(Duration.ofMinutes(2L))
                .pollInterval(Duration.ofSeconds(5))
                .pollInSameThread()
                .untilAsserted(() -> {
                  final Map<TopicPartition, Long> beginningOffsets = offsetChecker.beginningOffsets(Collections.singleton(targetTp));
                  System.out.println("WAITING FOR EVENTS TO EXPIRE : " + beginningOffsets.toString());
                  assertThat(beginningOffsets.get(targetTp)).isNotZero();
                });
      }

    }

  }



  private static String getShortUUID() {
    return UUID.randomUUID().toString().substring(0, 4);
  }
}