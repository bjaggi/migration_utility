package io.confluent.migration.model;

public class ConsumerGroupMetdata {

    String consumerGroupName;
    String topicName;
    int partitionNum;
    long currentOffsetNumber;
    long logEndOffsetNumber;

    public long getConsumerLag() {
        return consumerLag;
    }

    public void setConsumerLag(long consumerLag) {
        this.consumerLag = consumerLag;
    }

    long consumerLag;
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

    public long getCurrentOffsetNumber() {
        return currentOffsetNumber;
    }

    public void setCurrentOffsetNumber(long currentOffsetNumber) {
        this.currentOffsetNumber = currentOffsetNumber;
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

    public long getLogEndOffsetNumber() {
        return logEndOffsetNumber;
    }

    public void setLogEndOffsetNumber(long logEndOffsetNumber) {
        this.logEndOffsetNumber = logEndOffsetNumber;
    }

}
