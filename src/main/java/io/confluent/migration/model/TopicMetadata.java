package io.confluent.migration.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kafka.common.acl.AclOperation;

import java.util.Map;
import java.util.Set;

public class TopicMetadata {

    @JsonProperty("partition")
    private int partitionCount ;
    @JsonProperty("replicationFactor")
    private int replicationFactor;
    private Map<String, String> configs;
    private Set<AclOperation> setAclOperations;

    public TopicMetadata() {
    }

    public TopicMetadata(int partitionCount, int replicationFactor, Map<String, String> configs) {
        this.partitionCount = partitionCount;
        this.replicationFactor = replicationFactor;
        this.configs = configs;
    }

    public int getPartitionCount() {
        return partitionCount;
    }

    public void setPartitionCount(int partitionCount) {
        this.partitionCount = partitionCount;
    }

    public int getReplicationFactor() {
        return replicationFactor;
    }

    public void setReplicationFactor(int replicationFactor) {
        this.replicationFactor = replicationFactor;
    }

    public Map<String, String> getConfigs() {
        return configs;
    }

    public void setConfigs(Map<String, String> configs) {
        this.configs = configs;
    }





    public Set<AclOperation> getSetAclOperations() {
        return setAclOperations;
    }

    public void setSetAclOperations(Set<AclOperation> setAclOperations) {
        this.setAclOperations = setAclOperations;
    }

    @Override
    public String toString() {
        return "TopicMetadata{" +
                "partitionCount=" + partitionCount +
                ", replicationFactor=" + replicationFactor +
                ", configs=" + configs +
                ", setAclOperations=" + setAclOperations +
                '}';
    }
}
