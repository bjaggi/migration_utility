package io.confluent.migrationutility.controller;

import io.confluent.migrationutility.exception.InvalidClusterIdException;
import io.confluent.migrationutility.framework.TestControllerContext;
import io.confluent.migrationutility.model.quota.QuotaMetadataExtendedRequest;
import io.confluent.migrationutility.model.quota.QuotaMetadataRequest;
import io.confluent.migrationutility.service.QuotaService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;

@ExtendWith(MockitoExtension.class)
class QuotaControllerTest extends TestControllerContext {

  private QuotaService service;
  private QuotaController controller;

  @BeforeEach
  void setUp() {
    service = Mockito.mock(QuotaService.class);
    controller = new QuotaController(clusterConfig, service);
  }

  @Test
  void testInvalidClusterId() {
    final QuotaMetadataRequest invalidExport = new QuotaMetadataRequest("invalid", null);
    final QuotaMetadataRequest invalidApply = new QuotaMetadataRequest("invalid", null);
    final QuotaMetadataRequest invalidApply2 = new QuotaMetadataRequest("src", "invalid");
    final QuotaMetadataRequest invalidApply3 = new QuotaMetadataRequest("src", null);
    final QuotaMetadataExtendedRequest invalidDefinedApply = new QuotaMetadataExtendedRequest("invalid", Collections.emptyList());

    Assertions.assertThrows(InvalidClusterIdException.class, () -> controller.exportQuotas(invalidExport));
    Assertions.assertThrows(InvalidClusterIdException.class, () -> controller.applyQuotas(invalidApply));
    Assertions.assertThrows(InvalidClusterIdException.class, () -> controller.applyQuotas(invalidApply2));
    Assertions.assertThrows(InvalidClusterIdException.class, () -> controller.applyQuotas(invalidApply3));
    Assertions.assertThrows(InvalidClusterIdException.class, () -> controller.applyDefinedQuotas(invalidDefinedApply));
  }

}