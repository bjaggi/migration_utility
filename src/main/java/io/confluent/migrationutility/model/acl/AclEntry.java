package io.confluent.migrationutility.model.acl;

import lombok.Data;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;

@Data
public class AclEntry implements Comparable<AclEntry>{
  final String resourceName;
  final ResourceType resourceType;
  final PatternType resourcePatternType;
  final String host;
  final String principal;
  final AclOperation operation;
  final AclPermissionType access;

  public AclEntry(String resourceName, ResourceType resourceType, PatternType resourcePatternType, String host, String principal, AclOperation operation, AclPermissionType access) {
    this.resourceName = resourceName;
    this.resourceType = resourceType;
    this.resourcePatternType = resourcePatternType;
    this.host = host;
    this.principal = principal;
    this.operation = operation;
    this.access = access;
  }

  public AclEntry(final AclBinding aclBinding) {
    final ResourcePattern pattern = aclBinding.pattern();
    final AccessControlEntry entry = aclBinding.entry();
    resourceName = pattern.name();
    resourceType = pattern.resourceType();
    resourcePatternType = pattern.patternType();
    host = entry.host();
    principal = entry.principal();
    operation = entry.operation();
    access = entry.permissionType();
  }

  @Override
  public int compareTo(AclEntry o) {
    return resourceName.compareTo(o.getResourceName());
  }
}
