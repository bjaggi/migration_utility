package io.confluent.migrationutility.service;

import io.confluent.migrationutility.model.acl.AclEntry;
import io.confluent.migrationutility.model.acl.AclResponse;
import io.confluent.migrationutility.util.AdminClientUtils;
import lombok.extern.slf4j.Slf4j;

import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.resource.ResourcePattern;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@Service
public class AclService {

  public List<AclBinding> listAcl(final Map<String, String> config) {
    return AdminClientUtils.listAcl(config);
  }

  public AclResponse applyAclMetadataRequest(final Map<String, String> srcClusterConfig, final Map<String, String> destClusterConfig) {
    final List<AclBinding> srcAcls = listAcl(srcClusterConfig);
    log.info("Source ACL List : {}", srcAcls);

    log.info("Applying ACLs to destination cluster");
    AdminClientUtils.createAcls(destClusterConfig, srcAcls);

    final List<AclBinding> destAcls = listAcl(destClusterConfig);
    log.info("Destination ACL List : {}", destAcls);
    final List<AclEntry> srcAclEntries = srcAcls.stream().map(AclEntry::new).collect(Collectors.toList());
    final List<AclEntry> destAclEntries = destAcls.stream().map(AclEntry::new).collect(Collectors.toList());

    return new AclResponse(srcAclEntries, destAclEntries);
  }

  public AclResponse applyAclMetadataRequest(final Map<String, String> destClusterConfig, final List<AclEntry> entriesToApply) {

    List<AclBinding> aclBindingsToApply = entriesToApply.stream().map(entry -> new AclBinding(
            new ResourcePattern(entry.getResourceType(), entry.getResourceName(), entry.getResourcePatternType()),
            new AccessControlEntry(entry.getPrincipal(), entry.getHost(), entry.getOperation(), entry.getAccess())
    )).collect(Collectors.toList());

    log.info("Applying ACLs to destination cluster");
    AdminClientUtils.createAcls(destClusterConfig, aclBindingsToApply);

    final List<AclBinding> destAcls = listAcl(destClusterConfig);
    log.info("Destination ACL List : {}", destAcls);
    final List<AclEntry> destAclEntries = destAcls.stream().map(AclEntry::new).collect(Collectors.toList());

    return new AclResponse(entriesToApply, destAclEntries);
  }

}
