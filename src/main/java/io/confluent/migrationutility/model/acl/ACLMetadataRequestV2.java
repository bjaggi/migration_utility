package io.confluent.migrationutility.model.acl;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;


@Data
public class ACLMetadataRequestV2 {
  private String clusterId;
  private List<AclEntry> aclEntries;
}
