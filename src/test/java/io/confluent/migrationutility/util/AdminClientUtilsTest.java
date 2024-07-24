package io.confluent.migrationutility.util;

import io.confluent.migrationutility.framework.Cluster;
import io.confluent.migrationutility.framework.TestKafkaContext;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

class AdminClientUtilsTest extends TestKafkaContext {
  private static final Map<String, String> clusterConfigs = new HashMap<>();

  @BeforeAll
  static void setClusterConfigs() {
    clusterConfigs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, srcKafkaContainer.getBootstrapServers());
  }

  @Test
  void gatherGroupMetadata() {
    final String topic = "gather_group_metadata_test";
    final String group = topic + "_group";
    final long count = 3L;
    produceToTopic(topic, 10L, Cluster.SOURCE);
    consumeFromTopic(topic, group, count, Cluster.SOURCE);

    final Map<String, Map<TopicPartition, OffsetAndMetadata>> actual = AdminClientUtils.consumerGroupOffsets(clusterConfigs, Collections.singletonList(group));
    assertThat(actual).isNotNull().hasSize(1);
    final Map<TopicPartition, OffsetAndMetadata> groupMetadata = actual.get(group);
    assertThat(groupMetadata).isNotNull();
    assertThat(groupMetadata.get(new TopicPartition(topic, 0)).offset()).isEqualTo(count);

    System.out.println();
    System.out.println(actual);
    System.out.println();
  }

  @Test
  void groupDoesNotExist() {
    final String topic = "admin_client_invalid_cg";
    produceToTopic(topic, 10L, Cluster.SOURCE);

    final Map<String, Map<TopicPartition, OffsetAndMetadata>> actual = Assertions.assertDoesNotThrow(
            () -> AdminClientUtils.consumerGroupOffsets(clusterConfigs, Collections.singletonList("invalid_cg_id"))
    );
    assertThat(actual).isNotNull().isEmpty();
  }

  @Test
  void givenExistingGroupAndNonExistingGroupReturnOnlyExisting() {
    final String topic = "admin_existing_and_nonexisting_test";
    final String group = topic+"_cg";
    produceToTopic(topic, 10L, Cluster.SOURCE);
    final long consumeEventCount = 8;
    consumeFromTopic(topic, group, consumeEventCount, Cluster.SOURCE);

    final Map<String, Map<TopicPartition, OffsetAndMetadata>> actual = Assertions.assertDoesNotThrow(
            () -> AdminClientUtils.consumerGroupOffsets(clusterConfigs, Arrays.asList("invalid_cg_id", group))
    );
    assertThat(actual).isNotNull().hasSize(1);
    final Map<TopicPartition, OffsetAndMetadata> validConsumerOffsets = actual.get(group);
    assertThat(validConsumerOffsets).isNotNull().hasSize(1);
    assertThat(validConsumerOffsets.get(new TopicPartition(topic, 0)).offset())
            .isEqualTo(consumeEventCount);

  }

}