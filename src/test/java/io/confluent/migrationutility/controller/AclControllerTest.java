package io.confluent.migrationutility.controller;

import io.confluent.migrationutility.exception.InvalidClusterIdException;
import io.confluent.migrationutility.framework.TestControllerContext;
import io.confluent.migrationutility.model.acl.ACLMetadataRequest;
import io.confluent.migrationutility.model.acl.ACLMetadataRequestV2;
import io.confluent.migrationutility.model.acl.AclEntry;
import io.confluent.migrationutility.model.acl.AclResponse;
import io.confluent.migrationutility.service.AclService;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AclControllerTest extends TestControllerContext {

  private AclService aclService;
  private AclController controller;

  @BeforeEach
  void setup() {
    aclService = Mockito.mock(AclService.class);
    controller = new AclController(clusterConfig, aclService);
  }

  @Test
  void testInvalidClusterId() {
    final ACLMetadataRequestV2 definedRequestBody = new ACLMetadataRequestV2("invalid", Collections.emptyList());
    final ACLMetadataRequest invalidSrcExport = new ACLMetadataRequest("invalid", null);
    final ACLMetadataRequest invalidDestApply = new ACLMetadataRequest("src", "invalid");

    Assertions.assertThrows(InvalidClusterIdException.class, () -> controller.exportAllAcl(invalidSrcExport));
    Assertions.assertThrows(InvalidClusterIdException.class, () -> controller.applyAllAcl(invalidDestApply));
    Assertions.assertThrows(InvalidClusterIdException.class, () -> controller.applyDefinedAcl(definedRequestBody));
  }

  @Test
  void testExportAllAcl() {
    final AclEntry expectedEntry = expectedAclEntry();
    final AclResponse expectedResponse = new AclResponse(Collections.singletonList(expectedEntry));
    when(aclService.listAcl(anyMap())).thenReturn(expectedResponse);

    final AclResponse actual = controller.exportAllAcl(new ACLMetadataRequest("src", null));

    assertThat(actual)
            .isNotNull()
            .isEqualTo(expectedResponse);
    assertThat(actual.getSourceAclCount()).isOne();
    assertThat(actual.getDestinationAclCount()).isNull();
    assertThat(actual.getDestinationAclEntries()).isNull();
  }

  @Test
  void testApplyAllAcl() {
    final ACLMetadataRequest request = new ACLMetadataRequest("src", "dest");
    final AclEntry expectedEntry = expectedAclEntry();
    final AclResponse expectedResponse = new AclResponse(Collections.singletonList(expectedEntry), Collections.singletonList(expectedEntry));
    when(aclService.applyAclMetadataRequest(anyMap(), anyMap()))
            .thenReturn(expectedResponse);

    final AclResponse actual = controller.applyAllAcl(request);

    assertThat(actual)
            .isNotNull()
            .isEqualTo(expectedResponse);
    assertThat(actual.getSrcSubsetExistsInDest()).isTrue();
    assertThat(actual.getDestinationAclCount()).isOne()
            .isEqualTo(actual.getSourceAclCount());
  }

  @Test
  void testApplyDefinedAcl() {
    final AclEntry expectedEntry = expectedAclEntry();
    final ACLMetadataRequestV2 request = new ACLMetadataRequestV2("dest", Collections.singletonList(expectedEntry));
    final AclResponse expectedResponse = new AclResponse(Collections.singletonList(expectedEntry), Collections.singletonList(expectedEntry));
    when(aclService.applyAclMetadataRequest(anyMap(), anyList()))
            .thenReturn(expectedResponse);

    final AclResponse actual = controller.applyDefinedAcl(request);

    assertThat(actual)
            .isNotNull()
            .isEqualTo(expectedResponse);
    assertThat(actual.getSrcSubsetExistsInDest()).isTrue();
    assertThat(actual.getSourceAclCount()).isOne()
            .isEqualTo(actual.getDestinationAclCount());
  }

  private static AclEntry expectedAclEntry() {
    return new AclEntry("test-", ResourceType.TOPIC, PatternType.PREFIXED, "*", "User:test", AclOperation.READ, AclPermissionType.ALLOW);
  }
}