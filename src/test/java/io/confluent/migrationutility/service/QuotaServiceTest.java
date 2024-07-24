package io.confluent.migrationutility.service;

import io.confluent.migrationutility.framework.Cluster;
import io.confluent.migrationutility.framework.TestKafkaContext;
import io.confluent.migrationutility.model.quota.QuotaEntry;
import io.confluent.migrationutility.model.quota.QuotaOp;
import io.confluent.migrationutility.model.quota.QuotaResponse;
import lombok.SneakyThrows;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterClientQuotasResult;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.quota.ClientQuotaAlteration;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;


class QuotaServiceTest extends TestKafkaContext {
  private static final Map<Cluster, Map<String, String>> clusterConfig = new EnumMap<>(Cluster.class);
  private QuotaService service;

  @BeforeAll
  static void init() {
    clusterConfig.put(Cluster.SOURCE, Collections.singletonMap(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, srcKafkaContainer.getBootstrapServers()));
    clusterConfig.put(Cluster.DESTINATION, Collections.singletonMap(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, destKafkaContainer.getBootstrapServers()));
  }

  @BeforeEach
  void setup() {
    service = new QuotaService();
  }

  @Test
  void testApply() {
    final String entityType = "user";
    final String entityName = "confluent";
    final String operationName = "request_percentage";
    final Double operationValue = 10D;
    createQuota(entityType, entityName, operationName, operationValue);
    final QuotaEntry expectedEntryToExist = new QuotaEntry(new ClientQuotaAlteration(new ClientQuotaEntity(Collections.singletonMap(entityType, entityName)), Collections.singleton(new ClientQuotaAlteration.Op(operationName, operationValue))));

    final QuotaResponse expected = new QuotaResponse(
            Collections.singletonList(expectedEntryToExist),
            Collections.singletonList(expectedEntryToExist)
    );
    final QuotaResponse actual = service.applyQuotasRequest(clusterConfig.get(Cluster.SOURCE), clusterConfig.get(Cluster.DESTINATION));
    assertThat(actual).isEqualTo(expected);
    assertThat(actual.getSrcSubsetExistsInDest()).isTrue();
  }

  @Test
  @Order(Order.DEFAULT)
  void testExport() {
    final QuotaResponse quotaResponse = service.listQuotas(clusterConfig.get(Cluster.SOURCE));
    assertThat(quotaResponse.getSourceQuotaCount()).isEqualTo(1);
    final QuotaEntry quotaEntry = quotaResponse.getSourceQuotas().get(0);
    assertThat(quotaEntry).isNotNull();
    assertThat(quotaEntry.getEntities()).containsEntry("user", "confluent");
    assertThat(quotaEntry.getQuotaScope()).contains(new QuotaOp(new ClientQuotaAlteration.Op("request_percentage", 10D)));
  }

  @SneakyThrows
  private void createQuota(String entityType, String entityName, String operationType, Double operationValue) {
    try (final AdminClient client = AdminClient.create(new HashMap<>(clusterConfig.get(Cluster.SOURCE)))) {
      final AlterClientQuotasResult alterClientQuotasResult = client.alterClientQuotas(Collections.singleton(new ClientQuotaAlteration(new ClientQuotaEntity(Collections.singletonMap(entityType, entityName)), Collections.singleton(new ClientQuotaAlteration.Op(operationType, operationValue)))));
      alterClientQuotasResult.all().get();
    }
  }


}