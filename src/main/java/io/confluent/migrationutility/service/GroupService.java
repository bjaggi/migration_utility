package io.confluent.migrationutility.service;

import io.confluent.migrationutility.component.ApplyGroupMetadataTask;
import io.confluent.migrationutility.component.QueryGroupMetadataTask;
import io.confluent.migrationutility.exception.GroupServiceException;
import io.confluent.migrationutility.model.group.ConsumerGroupMetadata;
import io.confluent.migrationutility.model.group.PostApplyGroupOffsets;
import io.confluent.migrationutility.util.AdminClientUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

@Slf4j
@Service
public class GroupService {
  private final ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

  public List<ConsumerGroupMetadata> consumerGroupMetadataList(final Map<String, String> config, final List<String> groups) {
    final List<ConsumerGroupMetadata> consumerGroupMetadataList = new ArrayList<>();

    final Map<String, Map<TopicPartition, OffsetAndMetadata>> cgOffsetMap = AdminClientUtils.consumerGroupOffsets(config, groups);
    final Map<String, Object> freeConsumerConfig = new HashMap<>(config);
    freeConsumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "migration_utility_tool");
    freeConsumerConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    freeConsumerConfig.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");

    final List<Future<ConsumerGroupMetadata>> futures = new ArrayList<>();
    for (final Map.Entry<String, Map<TopicPartition, OffsetAndMetadata>> entry : cgOffsetMap.entrySet()) {
      futures.add(
              executorService.submit(
                      new QueryGroupMetadataTask(entry.getKey(), freeConsumerConfig, entry.getValue())
              )
      );
    }

    for (final Future<ConsumerGroupMetadata> future : futures) {
      try {
        consumerGroupMetadataList.add(future.get());
      } catch (ExecutionException | InterruptedException e) {
        log.error("Unexpected thread exception while querying group metadata: {}", e.getMessage(), e);
        Thread.currentThread().interrupt();
      } catch (final Exception e) {
        log.error("Unexpected exception while querying group metadata: {}", e.getMessage(), e);
        throw new GroupServiceException(e);
      }
    }
    return consumerGroupMetadataList;
  }

  public List<PostApplyGroupOffsets> applyConsumerGroupMetadata(final Map<String, String> config, final List<ConsumerGroupMetadata> srcGroupMetadataList) {

    final List<Future<PostApplyGroupOffsets>> futures = new ArrayList<>();
    for (ConsumerGroupMetadata srcGroupMetadata : srcGroupMetadataList) {
      futures.add(executorService.submit(new ApplyGroupMetadataTask(config, srcGroupMetadata)));
    }

    final List<PostApplyGroupOffsets> postApplyGroupOffsets = new ArrayList<>();

    for (final Future<PostApplyGroupOffsets> future : futures) {
      try {
        postApplyGroupOffsets.add(future.get());
      } catch (ExecutionException | InterruptedException e) {
        log.error("Unexpected thread exception while applying consumer group offset reset : {}", e.getMessage(), e);
        Thread.currentThread().interrupt();
      } catch (final Exception e) {
        log.error("Unexpected exception while applying consumer group offset reset : {}", e.getMessage(), e);
        throw new GroupServiceException(e);
      }
    }
    return postApplyGroupOffsets;
  }


}
