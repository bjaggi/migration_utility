package io.confluent.migrationutility.model.topic;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;

import java.util.List;
import java.util.stream.Collectors;

@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
public class TopicMetadataResponse {
  private List<TopicEntry> sourceTopics;
  private Integer sourceTopicCount;
  private List<TopicEntry> destinationTopics;
  private Integer destinationTopicCount;
  private Boolean srcSubsetExistsInDest = null;

  public TopicMetadataResponse(final List<TopicEntry> sourceTopics) {
    this.sourceTopics = sourceTopics.stream().sorted().collect(Collectors.toList());
    this.sourceTopicCount = sourceTopics.size();
  }

  public TopicMetadataResponse(final List<TopicEntry> sourceTopics, final List<TopicEntry> destinationTopics) {
    this.sourceTopics = sourceTopics.stream().sorted().collect(Collectors.toList());
    this.sourceTopicCount = sourceTopics.size();
    this.destinationTopics = destinationTopics.stream().sorted().collect(Collectors.toList());
    this.destinationTopicCount = destinationTopics.size();
    this.srcSubsetExistsInDest = destinationTopics.stream().map(TopicEntry::getTopic).collect(Collectors.toSet())
            .containsAll(sourceTopics.stream().map(TopicEntry::getTopic).collect(Collectors.toList()));
  }

}
