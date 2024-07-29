package io.confluent.migrationutility.controller;

import io.confluent.migrationutility.exception.InvalidClusterIdException;
import io.confluent.migrationutility.framework.TestControllerContext;
import io.confluent.migrationutility.model.group.ApplyGroupMetadataRequest;
import io.confluent.migrationutility.model.group.ConsumerGroupMetadata;
import io.confluent.migrationutility.model.group.GroupMetadataRequest;
import io.confluent.migrationutility.model.group.PostApplyGroupOffsets;
import io.confluent.migrationutility.model.group.TopicPartitionMetadata;
import io.confluent.migrationutility.service.GroupService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.util.CollectionUtils;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class GroupControllerTest extends TestControllerContext {

  private GroupService service;
  private GroupController controller;

  @BeforeEach
  void setUp() {
    service = Mockito.mock(GroupService.class);
    controller = new GroupController(clusterConfig, service);
  }

  @Test
  void testInvalidClusterId() {
    final GroupMetadataRequest exportRequest = new GroupMetadataRequest("invalid", Collections.singletonList("tg"), Collections.emptyList());
    final ApplyGroupMetadataRequest applyRequest = new ApplyGroupMetadataRequest("invalid", Collections.emptyList());
    Assertions.assertThrows(InvalidClusterIdException.class, () -> controller.exportConsumerGroupOffsets(exportRequest));
    Assertions.assertThrows(InvalidClusterIdException.class, () -> controller.applyConsumerGroupOffsets(applyRequest));
  }

  @Test
  void testApplyConsumerGroupOffsets() {
    final List<PostApplyGroupOffsets> expected = Collections.singletonList(new PostApplyGroupOffsets("test", Collections.singletonMap("test000-0", 10L)));
    final List<TopicPartitionMetadata> topicPartitionMetadata = generateTPMetadataList(1, 100L);
    final List<ConsumerGroupMetadata> groupMetadataList = Collections.singletonList(new ConsumerGroupMetadata("test", topicPartitionMetadata));
    when(service.applyConsumerGroupMetadata(clusterConfig.getClusters().get("dest"), groupMetadataList))
            .thenReturn(expected);

    final List<PostApplyGroupOffsets> actual = controller.applyConsumerGroupOffsets(new ApplyGroupMetadataRequest("dest", groupMetadataList));

    assertThat(actual)
            .isNotNull()
            .isEqualTo(expected);
  }

  @ParameterizedTest
  @MethodSource
  void testExportConsumerGroupOffsets(String group, List<TopicPartitionMetadata> tpMetadataList, List<String> topicList) {
    final ConsumerGroupMetadata cgMetadata = new ConsumerGroupMetadata(group, tpMetadataList);
    when(service.consumerGroupMetadataList(clusterConfig.getClusters().get("src"), Collections.singletonList(group)))
            .thenReturn(Collections.singletonList(cgMetadata));

    final List<ConsumerGroupMetadata> actual = controller.exportConsumerGroupOffsets(new GroupMetadataRequest("src", Collections.singletonList(group), topicList));
    assertThat(actual)
            .isNotNull()
            .hasSize(1);
    assertThat(actual.get(0).getConsumerGroupName()).isEqualTo(group);
    assertThat(actual.get(0).getTopicPartitionMetadata()).isSorted();
    if (!CollectionUtils.isEmpty(topicList)) {
      final List<TopicPartitionMetadata> actualTPMetadata = actual.get(0).getTopicPartitionMetadata();
      final Set<String> actualTPMetadataTopics = actualTPMetadata.stream().map(TopicPartitionMetadata::getTopicName).collect(Collectors.toSet());
      assertThat(actualTPMetadataTopics)
              .containsExactlyInAnyOrderElementsOf(topicList);
    }
  }

  static Stream<Arguments> testExportConsumerGroupOffsets() {
    return Stream.of(
            Arguments.of("cg001", generateTPMetadataList(1, Instant.now().toEpochMilli()), Collections.emptyList()),
            Arguments.of("cg002", generateTPMetadataList(2, Instant.now().toEpochMilli()), Collections.emptyList()),
            Arguments.of("cg003", generateTPMetadataList(3, Instant.now().toEpochMilli()), Collections.emptyList()),
            Arguments.of("cg004", generateTPMetadataList(4, Instant.now().toEpochMilli()), Collections.emptyList()),
            Arguments.of("cg005", generateTPMetadataList(5, Instant.now().toEpochMilli()), Collections.singletonList("test000")),
            Arguments.of("cg005", generateTPMetadataList(6, Instant.now().toEpochMilli()), Arrays.asList("test000", "test001", "test002"))
    );
  }

  static List<TopicPartitionMetadata> generateTPMetadataList(int count, long startTs) {
    final List<TopicPartitionMetadata> tpMetadataList = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      tpMetadataList.add(
              new TopicPartitionMetadata("test00"+i, i, 0L, 1L, 1L, startTs+i)
      );
    }
    Collections.shuffle(tpMetadataList);
    return tpMetadataList;

  }

}