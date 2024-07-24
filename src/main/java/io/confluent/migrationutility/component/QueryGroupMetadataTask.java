package io.confluent.migrationutility.component;

import io.confluent.migrationutility.model.ConsumerGroupMetadata;
import io.confluent.migrationutility.model.TopicPartitionMetadata;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;

@Slf4j
public class QueryGroupMetadataTask implements Callable<ConsumerGroupMetadata> {
  private static final ByteArrayDeserializer deserializer = new ByteArrayDeserializer();

  private final String group;
  private final Map<String, Object> config;
  private final Map<TopicPartition, OffsetAndMetadata> groupTPs;

  public QueryGroupMetadataTask(final String group, final Map<String, Object> config, final Map<TopicPartition, OffsetAndMetadata> groupTPs) {
    this.group = group;
    this.config = new HashMap<>(config);
    this.groupTPs = groupTPs;
    this.config.put(ConsumerConfig.GROUP_ID_CONFIG, config.get(ConsumerConfig.GROUP_ID_CONFIG) + "-" + group);
  }

  @Override
  public ConsumerGroupMetadata call() throws Exception {
    log.info("Retrieving consumer group ({}) metadata.", this.group);
    final List<TopicPartitionMetadata> tpMetadataList = new ArrayList<>();
    try (final Consumer<byte[], byte[]> receiver = new KafkaConsumer<>(config, deserializer, deserializer)) {
      final Map<TopicPartition, Long> groupTPLogEndOffsets = receiver.endOffsets(groupTPs.keySet());
      groupTPs.forEach((tp, offsetAndMetadata) -> {
        receiver.assign(Collections.singletonList(tp));
        log.info("Retrieving consumer group ({}) topic-partition ({}) metadata.", this.group, tp.toString());
        final long currentOffset = offsetAndMetadata.offset();
        final long logEndOffset = groupTPLogEndOffsets.get(tp);
        final long consumerLag = logEndOffset - currentOffset;
        final long seekOffset = determineSeekOffset(currentOffset, consumerLag);
        if (seekOffset > -1) {
          receiver.seek(tp, consumerLag == 0L ? currentOffset - 1 : currentOffset);
          final TopicPartitionMetadata tpMetadata = Optional.of(receiver.poll(Duration.ofSeconds(5)))
                  .filter(cRecords -> !cRecords.isEmpty())
                  .map(cRecords -> cRecords.iterator().next())
                  .map(cRecord -> new TopicPartitionMetadata(
                          tp.topic(),
                          tp.partition(),
                          currentOffset,
                          logEndOffset,
                          consumerLag,
                          cRecord.timestamp())
                  )
                  .orElseGet(() -> {
                    final long epochMilli = Instant.now().minusSeconds(60).toEpochMilli();
                    final TopicPartitionMetadata topicPartitionMetadata = new TopicPartitionMetadata(
                            tp.topic(),
                            tp.partition(),
                            currentOffset,
                            logEndOffset,
                            consumerLag,
                            epochMilli
                    );
                    log.warn("Topic-partition ({}) does not contain any non-expired events - cannot retrieve timestamp. Defaulting to 1min ago TS : {}", tp.topic()+"-"+tp.partition(), epochMilli);
                    return topicPartitionMetadata;
                  });
          tpMetadataList.add(tpMetadata);
        } else {
          // no lag, currentOffset == 0, endOffset == 0, ie. no data
          final long epochMilli = Instant.now().minusSeconds(60).toEpochMilli();
          log.warn("No data has been sent to topic-partition ({}) - cannot retrieve timestamp. Defaulting to 1min ago TS : {}", tp.topic() + "-" + tp.partition(), epochMilli);
          tpMetadataList.add(new TopicPartitionMetadata(
                  tp.topic(),
                  tp.partition(),
                  currentOffset,
                  logEndOffset,
                  consumerLag,
                  epochMilli
          ));
        }
      });
    }
    return new ConsumerGroupMetadata(group, tpMetadataList);
  }

  private long determineSeekOffset(long currentOffset, long consumerLag) {
    log.info("Determining offset to seek to based on current offset ({}) and lag ({})", currentOffset, consumerLag);
    if (consumerLag != 0) {
      return currentOffset;
    } else if (currentOffset == 0) {
      // no lag, ie. endOffset is also == 0
      return -1;
    } else {
      // no lag, so currentOffset is equal to endOffset
      return currentOffset - 1;
    }
  }

}
