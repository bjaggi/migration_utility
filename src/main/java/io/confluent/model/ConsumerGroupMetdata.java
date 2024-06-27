package io.confluent.model;

public class ConsumerGroupMetdata {

    String consumerGroupName;
    String topicName;
    int partitionNum;
    long offsetNumber;
    long timestamp ;

    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public int getPartitionNum() {
        return partitionNum;
    }

    public void setPartitionNum(int partitionNum) {
        this.partitionNum = partitionNum;
    }

    public long getOffsetNumber() {
        return offsetNumber;
    }

    public void setOffsetNumber(long offsetNumber) {
        this.offsetNumber = offsetNumber;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
    public String getConsumerGroupName() {
        return consumerGroupName;
    }

    public void setConsumerGroupName(String consumerGroupName) {
        this.consumerGroupName = consumerGroupName;
    }



}
