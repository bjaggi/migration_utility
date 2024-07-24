package io.confluent.migrationutility.service;

import io.confluent.migrationutility.framework.Cluster;
import io.confluent.migrationutility.framework.TestKafkaContext;
import io.confluent.migrationutility.model.acl.AclEntry;
import io.confluent.migrationutility.model.acl.AclResponse;
import lombok.SneakyThrows;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Duration;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class AclServiceTest extends TestKafkaContext {
  private static final Map<Cluster, Map<String, String>> clusterConfig = new EnumMap<>(Cluster.class);

  private AclService service;

  @BeforeAll
  static void init() {
    clusterConfig.put(Cluster.SOURCE, Collections.singletonMap(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, srcKafkaContainer.getBootstrapServers()));
    clusterConfig.put(Cluster.DESTINATION, Collections.singletonMap(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, destKafkaContainer.getBootstrapServers()));
  }

  @BeforeEach
  void setup() {
    service = new AclService();
  }

  @AfterEach
  void cleanup() {
    deleteAcls(Cluster.SOURCE);
    deleteAcls(Cluster.DESTINATION);
  }

  @ParameterizedTest
  @MethodSource
  void testApplyAcls(ResourceType resourceType, String resourceName, PatternType patternType, String principal, AclOperation operation, AclPermissionType permissionType) {
    createAcl(resourceType, resourceName, patternType, principal, operation, permissionType);
    final AclResponse expected = new AclResponse(
            Collections.singletonList(new AclEntry(new AclBinding(new ResourcePattern(resourceType, resourceName, patternType), new AccessControlEntry(principal, "*", operation, permissionType)))),
            Collections.singletonList(new AclEntry(new AclBinding(new ResourcePattern(resourceType, resourceName, patternType), new AccessControlEntry(principal, "*", operation, permissionType))))
    );
    final AclResponse actual = service.applyAclMetadataRequest(
            clusterConfig.get(Cluster.SOURCE), clusterConfig.get(Cluster.DESTINATION)
    );
    assertThat(actual).isEqualTo(expected);
    assertThat(actual.getSrcSubsetExistsInDest()).isTrue();
  }

  public static Stream<Arguments> testApplyAcls() {
    return testExportAcls();
  }


  @ParameterizedTest
  @MethodSource
  void testExportAcls(ResourceType resourceType, String resourceName, PatternType patternType, String principal, AclOperation operation, AclPermissionType permissionType) {
    createAcl(resourceType, resourceName, patternType, principal, operation, permissionType);
    final List<AclBinding> expected = Collections.singletonList(
            new AclBinding(new ResourcePattern(resourceType, resourceName, patternType), new AccessControlEntry(principal, "*", operation, permissionType))
    );
    final List<AclBinding> actual = service.listAcl(clusterConfig.get(Cluster.SOURCE));
    assertThat(actual).isEqualTo(expected);
  }

  public static Stream<Arguments> testExportAcls() {
    return Stream.of(
            Arguments.of(ResourceType.CLUSTER, "kafka-cluster", PatternType.LITERAL, "User:confluent", AclOperation.CREATE, AclPermissionType.ALLOW),
            Arguments.of(ResourceType.CLUSTER, "kafka-cluster", PatternType.LITERAL, "Group:confluent", AclOperation.CREATE, AclPermissionType.ALLOW),
            Arguments.of(ResourceType.TOPIC, "test001", PatternType.LITERAL, "User:confluent", AclOperation.READ, AclPermissionType.ALLOW),
            Arguments.of(ResourceType.TOPIC, "test001", PatternType.PREFIXED, "Group:confluent", AclOperation.READ, AclPermissionType.ALLOW),
            Arguments.of(ResourceType.TOPIC, "test001", PatternType.PREFIXED, "User:bad_user", AclOperation.READ, AclPermissionType.DENY),
            Arguments.of(ResourceType.GROUP, "test001", PatternType.PREFIXED, "User:confluent", AclOperation.READ, AclPermissionType.ALLOW),
            Arguments.of(ResourceType.GROUP, "test001", PatternType.LITERAL, "Group:confluent", AclOperation.READ, AclPermissionType.ALLOW),
            Arguments.of(ResourceType.TRANSACTIONAL_ID, "producerTId", PatternType.LITERAL, "Group:confluent", AclOperation.READ, AclPermissionType.ALLOW),
            Arguments.of(ResourceType.TRANSACTIONAL_ID, "producerTId", PatternType.PREFIXED, "User:confluent", AclOperation.READ, AclPermissionType.ALLOW)
    );
  }


  @SneakyThrows
  private static void createAcl(ResourceType resourceType, String resourceName, PatternType patternType, String principal, AclOperation operation, AclPermissionType permissionType) {
    try (final AdminClient client = AdminClient.create(new HashMap<>(clusterConfig.get(Cluster.SOURCE)))) {
      final AclBinding aclToCreateOnSource = new AclBinding(new ResourcePattern(resourceType, resourceName, patternType), new AccessControlEntry(principal, "*", operation, permissionType));
      client.createAcls(Collections.singleton(aclToCreateOnSource)).values().get(aclToCreateOnSource).get();
    }
  }

  @SneakyThrows
  private static void deleteAcls(Cluster cluster) {
    try (final AdminClient client = AdminClient.create(new HashMap<>(clusterConfig.get(cluster)))) {
      client.deleteAcls(Collections.singleton(AclBindingFilter.ANY)).all().get();
      Awaitility.waitAtMost(Duration.ofSeconds(5))
              .pollInterval(Duration.ofMillis(500))
              .untilAsserted(() -> assertThat(client.describeAcls(AclBindingFilter.ANY).values().get()).isEmpty());
    }

  }



}