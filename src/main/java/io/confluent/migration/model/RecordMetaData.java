package io.confluent.migration.model;

import org.apache.kafka.common.TopicPartition;

public class RecordMetaData {
    TopicPartition partition;
    String topicName;
    //String partitionName;
    long timestamp;
    long offset;

    public TopicPartition getPartition() {
        return partition;
    }

    public void setPartition(TopicPartition partition) {
        this.partition = partition;
    }

    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

//    public String getPartitionName() {
//        return partitionName;
//    }
//
//    public void setPartitionName(String partitionName) {
//        this.partitionName = partitionName;
//    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }


    public RecordMetaData(){}

    public RecordMetaData(TopicPartition partition, String topicName, long timestamp, long offset) {
        this.partition = partition;
        this.topicName = topicName;
        //this.partitionName = partitionName;
        this.timestamp = timestamp;
        this.offset = offset;
    }

    @Override
    public String toString() {
        return "RecordMetaData{" +
                "partition=" + partition +
                ", topicName='" + topicName + '\'' +
                ", timestamp=" + timestamp +
                ", offset=" + offset +
                '}';
    }
}
