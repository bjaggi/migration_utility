package io.confluent.migrationutility.model.acl;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;


@Data
@AllArgsConstructor
@NoArgsConstructor
public class ACLMetadataRequestV2 {
  private String clusterId;
  private List<AclEntry> aclEntries;
}
