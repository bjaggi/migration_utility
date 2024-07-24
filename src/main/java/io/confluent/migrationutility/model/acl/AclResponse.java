package io.confluent.migrationutility.model.acl;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;

import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
public class AclResponse {
  private List<AclEntry> sourceAclEntries;
  private Integer sourceAclCount;
  private List<AclEntry> destinationAclEntries;
  private Integer destinationAclCount;
  private Boolean srcSubsetExistsInDest = null;


  public AclResponse(final List<AclEntry> sourceAcls) {
    this.sourceAclEntries = sourceAcls;
    this.sourceAclCount = sourceAcls.size();
  }

  public AclResponse(final List<AclEntry> sourceAcls, final List<AclEntry> destinationAcls) {
    this.sourceAclEntries = sourceAcls.stream().sorted().collect(Collectors.toList());
    this.sourceAclCount = sourceAcls.size();
    this.destinationAclEntries = destinationAcls.stream().sorted().collect(Collectors.toList());
    this.destinationAclCount = destinationAcls.size();
    this.srcSubsetExistsInDest = new HashSet<>(destinationAclEntries).containsAll(sourceAclEntries);
  }

}
