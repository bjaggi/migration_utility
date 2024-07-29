package io.confluent.migrationutility.model.topic;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class TopicMetadataExtendedRequest {
    private String clusterId;
    private List<TopicEntry> topicEntries;
}
