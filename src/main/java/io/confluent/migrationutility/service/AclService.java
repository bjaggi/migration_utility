package io.confluent.migrationutility.service;

import io.confluent.migrationutility.model.acl.AclEntry;
import io.confluent.migrationutility.model.acl.AclResponse;
import io.confluent.migrationutility.util.AdminClientUtils;
import lombok.extern.slf4j.Slf4j;

import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.resource.ResourcePattern;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@Service
public class AclService {


  /**
   * Query and return all ACLs
   * @param config cluster config
   * @return query ACL results
   */
  public AclResponse listAcl(final Map<String, String> config) {
    final List<AclBinding> aclBindings = AdminClientUtils.listAcl(config);
    return new AclResponse(
            aclBindings.stream().map(AclEntry::new).collect(Collectors.toList())
    );
  }

  /**
   * Query source cluster ACLs and apply to destination cluster
   * @param srcClusterConfig src cluster config
   * @param destClusterConfig dest cluster config
   * @return AclResponse
   */
  public AclResponse applyAclMetadataRequest(final Map<String, String> srcClusterConfig, final Map<String, String> destClusterConfig) {
    final List<AclBinding> srcAcls = AdminClientUtils.listAcl(srcClusterConfig);

    log.debug("Applying source cluster ACLs to destination cluster : {}", srcAcls);
    AdminClientUtils.createAcls(destClusterConfig, srcAcls);

    final List<AclBinding> destAcls = AdminClientUtils.listAcl(destClusterConfig);
    log.debug("Destination ACL List (post-apply) : {}", destAcls);

    final List<AclEntry> srcAclEntries = srcAcls.stream().map(AclEntry::new).collect(Collectors.toList());
    final List<AclEntry> destAclEntries = destAcls.stream().map(AclEntry::new).collect(Collectors.toList());
    return new AclResponse(srcAclEntries, destAclEntries);
  }

  /**
   * Create ACLs using provided list of ACL entries
   * @param destClusterConfig dest cluster config
   * @param entriesToApply target ACL entries to create
   * @return AclResponse
   */
  public AclResponse applyAclMetadataRequest(final Map<String, String> destClusterConfig, final List<AclEntry> entriesToApply) {

    final List<AclBinding> aclBindingsToApply = mapAclEntryToAclBinding(entriesToApply);

    log.debug("Applying user provided ACLs to destination cluster : {}", aclBindingsToApply);
    AdminClientUtils.createAcls(destClusterConfig, aclBindingsToApply);

    final List<AclBinding> destAcls = AdminClientUtils.listAcl(destClusterConfig);
    log.debug("Destination ACL List (post-definedAcl-apply) : {}", destAcls);
    final List<AclEntry> destAclEntries = destAcls.stream().map(AclEntry::new).collect(Collectors.toList());

    return new AclResponse(entriesToApply, destAclEntries);
  }

  private static List<AclBinding> mapAclEntryToAclBinding(final List<AclEntry> entries) {
    return entries.stream().map(entry -> new AclBinding(
            new ResourcePattern(entry.getResourceType(), entry.getResourceName(), entry.getResourcePatternType()),
            new AccessControlEntry(entry.getPrincipal(), entry.getHost(), entry.getOperation(), entry.getAccess())
    )).collect(Collectors.toList());
  }

}
