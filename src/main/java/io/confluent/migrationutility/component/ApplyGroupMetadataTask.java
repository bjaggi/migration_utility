package io.confluent.migrationutility.component;

import io.confluent.migrationutility.exception.ActiveGroupException;
import io.confluent.migrationutility.exception.GroupServiceException;
import io.confluent.migrationutility.model.group.ConsumerGroupMetadata;
import io.confluent.migrationutility.model.group.PostApplyGroupOffsets;
import io.confluent.migrationutility.model.group.TopicPartitionMetadata;
import io.confluent.migrationutility.util.AdminClientUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;

/**
 * Instance created per group
 */
@Slf4j
public class ApplyGroupMetadataTask implements Callable<PostApplyGroupOffsets> {
  private static final ByteArrayDeserializer deserializer = new ByteArrayDeserializer();

  private final Map<TopicPartition, Long> srcTopicPartitionTimestamps = new HashMap<>();

  private final String group;
  private final Map<String, String> adminConfig;
  private final Map<String, Object> targetClusterConfig;

  public ApplyGroupMetadataTask(final Map<String, String> targetClusterConfig, final ConsumerGroupMetadata groupMetadata) {
    this.group = groupMetadata.getConsumerGroupName();
    this.adminConfig = new HashMap<>(targetClusterConfig);
    this.targetClusterConfig = new HashMap<>(targetClusterConfig);
    this.targetClusterConfig.put(ConsumerConfig.GROUP_ID_CONFIG, group);
    this.targetClusterConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
    this.targetClusterConfig.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");

    if (!AdminClientUtils.consumerGroupIsEmpty(adminConfig, group)) {
      log.error("Cannot apply set consumer group ({}) offsets with active members consuming", group);
      throw new ActiveGroupException("CG is actively consuming : " + group + " , cannot set offsets until no members from this group are consuming.");
    }

    for (final TopicPartitionMetadata tpMetadata : groupMetadata.getTopicPartitionMetadata()) {
      srcTopicPartitionTimestamps.put(
              new TopicPartition(tpMetadata.getTopicName(), tpMetadata.getPartitionNum()),
              tpMetadata.getTimestamp()
      );
    }
  }

  @Override
  public PostApplyGroupOffsets call() throws Exception {
    log.info("Processing consumer group ({}) offsets based on src TS : {}", group, srcTopicPartitionTimestamps);
    try (final Consumer<byte[], byte[]> freeConsumer = new KafkaConsumer<>(targetClusterConfig, deserializer, deserializer)) {
      final Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes = freeConsumer.offsetsForTimes(srcTopicPartitionTimestamps);
      final Map<TopicPartition, Long> firstOffsetByTP = freeConsumer.beginningOffsets(offsetsForTimes.keySet());
      log.info("TopicPartitions for consumer group ({}) yields the following destination/target offsets based on provided timestamps : {}", group, offsetsForTimes);
      freeConsumer.assign(offsetsForTimes.keySet());
      final Map<TopicPartition, OffsetAndMetadata> targetCommitOffsets = new HashMap<>();
      for (final TopicPartition tp : offsetsForTimes.keySet()) {
        final Long tpOffset = Optional.ofNullable(offsetsForTimes.get(tp))
                .map(OffsetAndTimestamp::offset)
                .orElse(firstOffsetByTP.get(tp));
        targetCommitOffsets.put(tp, new OffsetAndMetadata(tpOffset));
        log.info("Seeking to the following TP offset for group ({}) and TP ({}) : {}", group, tp, tpOffset);
        freeConsumer.seek(tp, tpOffset);
      }

      log.info("Executing consumer group offset commit for the following group ({}) TPs : {}", group, targetCommitOffsets);
      freeConsumer.commitSync(targetCommitOffsets);
      log.info("Successfully executed group offset reset for consumer group ({})", group);
    } catch (final Exception e) {
      log.error("Unexpected error occurred while applying group metadata: {}", e.getMessage(), e);
      throw new GroupServiceException(e);
    }

    final Map<String, Map<TopicPartition, OffsetAndMetadata>> groupOffsetsPostApply = AdminClientUtils.consumerGroupOffsets(adminConfig, Collections.singletonList(group));
    assert !groupOffsetsPostApply.isEmpty();
    final Map<TopicPartition, OffsetAndMetadata> metadata = groupOffsetsPostApply.get(group);
    final Map<String, Long> tpOffsetsSimplified = new HashMap<>();
    metadata.forEach((tp, offsetAndMetadata) -> tpOffsetsSimplified.put(tp.topic()+"-"+tp.partition(), offsetAndMetadata.offset()));
    return new PostApplyGroupOffsets(group, tpOffsetsSimplified);
  }
}
