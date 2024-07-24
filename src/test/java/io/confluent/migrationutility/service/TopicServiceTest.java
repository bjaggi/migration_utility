package io.confluent.migrationutility.service;

import io.confluent.migrationutility.framework.Cluster;
import io.confluent.migrationutility.framework.TestKafkaContext;
import io.confluent.migrationutility.model.topic.TopicEntry;
import io.confluent.migrationutility.model.topic.TopicMetadataResponse;
import lombok.SneakyThrows;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.util.CollectionUtils;
import org.testcontainers.shaded.org.awaitility.Awaitility;

import java.time.Duration;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class TopicServiceTest extends TestKafkaContext {
  private static final Map<Cluster, Map<String, String>> clusterConfig = new EnumMap<>(Cluster.class);

  private static final String topicPrefix = "topicServiceTest.";
  private final static Map<String, Map<String, String>> testTopicConfigs = new HashMap<>();
  private static final String test001 = topicPrefix + "test001";
  private static final String test002 = topicPrefix + "test002";
  private static final String test003 = topicPrefix + "test003";
  private static final String test004 = topicPrefix + "test004";
  private TopicService service;

  @BeforeAll
  static void contextConfig() {
    clusterConfig.put(Cluster.SOURCE, Collections.singletonMap(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, srcKafkaContainer.getBootstrapServers()));
    clusterConfig.put(Cluster.DESTINATION, Collections.singletonMap(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, destKafkaContainer.getBootstrapServers()));
    final Map<String, String> test002 = new HashMap<>();
    test002.put("retention.ms", "100000");
    test002.put("cleanup.policy", "delete");

    final Map<String, String> test003 = new HashMap<>();
    test003.put("cleanup.policy", "compact");

    final Map<String, String> test004 = new HashMap<>();
    test004.put("cleanup.policy", "compact,delete");
    test004.put("compression.type", "snappy");
    test004.put("flush.messages", "2000");
    test004.put("flush.ms", "20000");

    testTopicConfigs.put(topicPrefix+"test001", Collections.emptyMap());
    testTopicConfigs.put(topicPrefix+"test002", test002);
    testTopicConfigs.put(topicPrefix+"test003", test003);
    testTopicConfigs.put(topicPrefix+"test004", test004);

    // in-case GroupServiceTest gets run before this test suite
    deleteTestTopics(Cluster.SOURCE);
    deleteTestTopics(Cluster.DESTINATION);
  }

  @BeforeEach
  void setUp() {
    service = new TopicService();
    createTestTopics();
  }

  @AfterEach
  void deleteTestTopics() {
    deleteTestTopics(Cluster.SOURCE);
    deleteTestTopics(Cluster.DESTINATION);
  }

  @Test
  void exportAllTopics() {
    final int expectedSrcClusterTopicCount = 4;
    final TopicMetadataResponse actual = service.listTopics(clusterConfig.get(Cluster.SOURCE), Collections.emptyList());
    assertThat(actual.getSourceTopicCount())
            .isEqualTo(expectedSrcClusterTopicCount);
    final List<TopicEntry> actualSourceTopics = actual.getSourceTopics();
    assertThat(actualSourceTopics)
            .hasSize(expectedSrcClusterTopicCount)
            .containsExactly(
                    new TopicEntry(test001, 1, (short) 1, testTopicConfigs.get(test001)),
                    new TopicEntry(test002, 1, (short) 1, testTopicConfigs.get(test002)),
                    new TopicEntry(test003, 1, (short) 1, testTopicConfigs.get(test003)),
                    new TopicEntry(test004, 1, (short) 1, testTopicConfigs.get(test004))
            );
  }

  @ParameterizedTest
  @ValueSource(strings = {test001, test002, test003, test004})
  void applySpecificTopic(final String testTopicKey) {
    final TopicEntry expectedTopicToExist = new TopicEntry(testTopicKey, 1, (short) 1, testTopicConfigs.get(testTopicKey));
    final TopicMetadataResponse expected = new TopicMetadataResponse(
            Collections.singletonList(expectedTopicToExist),
            Collections.singletonList(expectedTopicToExist)
    );
    final TopicMetadataResponse actual = service.applyTopicMetadataRequest(
            clusterConfig.get(Cluster.SOURCE),
            clusterConfig.get(Cluster.DESTINATION),
            Collections.singletonList(testTopicKey));
    assertThat(actual).isEqualTo(expected);
    assertThat(actual.getSrcSubsetExistsInDest()).isTrue();
  }

  @Test
  void applyTopicsIdempotent() {
    applyAllTopics();
    applyAllTopics();
    applyAllTopics();
  }

  @Test
  void applyAllTopics() {
    final int expectedTopicCount = testTopicConfigs.size();
    final TopicMetadataResponse actual = service.applyTopicMetadataRequest(
            clusterConfig.get(Cluster.SOURCE), clusterConfig.get(Cluster.DESTINATION), Collections.emptyList()
    );
    assertThat(actual).isNotNull();
    assertThat(actual.getSourceTopicCount())
            .isEqualTo(expectedTopicCount)
            .isEqualTo(actual.getDestinationTopicCount());
    assertThat(actual.getSourceTopics())
            .isEqualTo(actual.getDestinationTopics());
    assertThat(actual.getSrcSubsetExistsInDest()).isTrue();
    assertThat(actual.getSourceTopics())
            .containsExactly(
                    new TopicEntry(test001, 1, (short) 1, testTopicConfigs.get(test001)),
                    new TopicEntry(test002, 1, (short) 1, testTopicConfigs.get(test002)),
                    new TopicEntry(test003, 1, (short) 1, testTopicConfigs.get(test003)),
                    new TopicEntry(test004, 1, (short) 1, testTopicConfigs.get(test004))
            );
    assertThat(actual.getDestinationTopics())
            .containsExactly(
                    new TopicEntry(test001, 1, (short) 1, testTopicConfigs.get(test001)),
                    new TopicEntry(test002, 1, (short) 1, testTopicConfigs.get(test002)),
                    new TopicEntry(test003, 1, (short) 1, testTopicConfigs.get(test003)),
                    new TopicEntry(test004, 1, (short) 1, testTopicConfigs.get(test004))
            );
  }




  @ParameterizedTest
  @MethodSource
  void exportTopicsNonEmptyList(final String topic, final Map<String, String> configs) {

    final TopicMetadataResponse topicMetadataResponse = service.listTopics(clusterConfig.get(Cluster.SOURCE), Collections.singletonList(topic));
    assertThat(topicMetadataResponse).isNotNull();
    assertThat(topicMetadataResponse.getSourceTopicCount()).isEqualTo(1);
    assertThat(topicMetadataResponse.getSourceTopics())
            .hasSize(1)
            .containsOnly(new TopicEntry(topic, 1, (short) 1, configs));
  }

  public static Stream<Arguments> exportTopicsNonEmptyList() {
    return Stream.of(
            Arguments.of(test001, testTopicConfigs.get(test001)),
            Arguments.of(test002, testTopicConfigs.get(test002)),
            Arguments.of(test003, testTopicConfigs.get(test003)),
            Arguments.of(test004, testTopicConfigs.get(test004))
    );
  }


  @SneakyThrows
  private static void createTopicOnSourceCluster(final String topic, final Map<String, String> configs) {
    try (final AdminClient client = AdminClient.create(producerConfigs(Cluster.SOURCE))) {
      final NewTopic topicToCreate = new NewTopic(topic, 1, (short) 1);
      topicToCreate.configs(configs);
      client.createTopics(Collections.singleton(topicToCreate)).all().get();
    }
  }

  @SneakyThrows
  private static int queryTopicCount(Cluster cluster) {
    final Map<String, Object> config = new HashMap<>(clusterConfig.get(cluster));
    try (final AdminClient client = AdminClient.create(config)) {
      final Set<String> topicSet = client.listTopics(new ListTopicsOptions().listInternal(false)).names().get();
      return topicSet.size();
    }
  }

  @SneakyThrows
  private static void createTestTopics() {
    createTopicOnSourceCluster(test001, testTopicConfigs.get(test001));
    createTopicOnSourceCluster(test002, testTopicConfigs.get(test002));
    createTopicOnSourceCluster(test003, testTopicConfigs.get(test003));
    createTopicOnSourceCluster(test004, testTopicConfigs.get(test004));
    try (final AdminClient client = AdminClient.create(new HashMap<>(clusterConfig.get(Cluster.SOURCE)))) {
      Awaitility.waitAtMost(Duration.ofSeconds(5))
              .pollInterval(Duration.ofSeconds(1))
              .untilAsserted(
                      () -> {
                        final TopicDescription topicDescription = client.describeTopics(Collections.singleton(test004)).allTopicNames().get().get(test004);
                        assertThat(topicDescription).isNotNull();
                      }
              );
    }

  }

  @SneakyThrows
  private static void deleteTestTopics(Cluster cluster) {
    final Map<String, Object> config = new HashMap<>(clusterConfig.get(cluster));
    try (final AdminClient client = AdminClient.create(config)) {
      final Set<String> topics = client.listTopics(new ListTopicsOptions().listInternal(false)).names().get();
      if (!CollectionUtils.isEmpty(topics)) {
        client.deleteTopics(topics).all().get();
      }
      Awaitility.waitAtMost(Duration.ofSeconds(5))
              .pollInterval(Duration.ofSeconds(1))
              .untilAsserted(() -> {
                final Set<String> existingTopics = client.listTopics(new ListTopicsOptions().listInternal(false)).names().get();
                assertThat(existingTopics).isEmpty();
              });
    }
  }

}