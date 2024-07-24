package io.confluent.migrationutility.model.topic;

import lombok.Data;

import java.util.List;

@Data
public class TopicMetadataExtendedRequest {
    private String clusterId;
    private List<TopicEntry> topicEntries;
}
