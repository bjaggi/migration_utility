package io.confluent.migrationutility.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonAnySetter;

import io.confluent.model.TopicMetadata;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ApplyTopicsMetadataRequest {
  private String clusterId;
  private Map<String,TopicMetadata>  topicMetadataMap = new HashMap<>();

@JsonAnySetter
    public void addtopicMetadataMap(String topicName, TopicMetadata topicMetadata) {
      topicMetadataMap.put(topicName, topicMetadata);
    }

}
