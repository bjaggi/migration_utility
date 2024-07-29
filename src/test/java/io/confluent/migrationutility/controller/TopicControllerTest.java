package io.confluent.migrationutility.controller;

import io.confluent.migrationutility.exception.InvalidClusterIdException;
import io.confluent.migrationutility.framework.TestControllerContext;
import io.confluent.migrationutility.model.topic.TopicMetadataExtendedRequest;
import io.confluent.migrationutility.model.topic.TopicMetadataRequest;
import io.confluent.migrationutility.service.TopicService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;

@ExtendWith(MockitoExtension.class)
class TopicControllerTest extends TestControllerContext {

  private TopicService service;
  private TopicController controller;

  @BeforeEach
  void setUp() {
    service = Mockito.mock(TopicService.class);
    controller = new TopicController(clusterConfig, service);
  }

  @Test
  void testInvalidClusterId() {
    final TopicMetadataRequest invalidExport = new TopicMetadataRequest("invalid", Collections.emptyList(), null);
    final TopicMetadataRequest invalidApply = new TopicMetadataRequest("src", Collections.emptyList(), "invalid");
    final TopicMetadataExtendedRequest invalidDefinedApply = new TopicMetadataExtendedRequest("invalid", Collections.emptyList());
    final TopicMetadataExtendedRequest invalidDefinedApply2 = new TopicMetadataExtendedRequest(null, Collections.emptyList());

    Assertions.assertThrows(InvalidClusterIdException.class, () -> controller.exportTopics(invalidExport));
    Assertions.assertThrows(InvalidClusterIdException.class, () -> controller.applyTopics(invalidApply));
    Assertions.assertThrows(InvalidClusterIdException.class, () -> controller.applyTopics(invalidExport));
    Assertions.assertThrows(InvalidClusterIdException.class, () -> controller.applyDefinedTopics(invalidDefinedApply));
    Assertions.assertThrows(InvalidClusterIdException.class, () -> controller.applyDefinedTopics(invalidDefinedApply2));
  }

}