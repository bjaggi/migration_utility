package io.confluent.migration.test;

import io.confluent.migration.utils.KafkaUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConsumerGroupOffsetsResult;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class CGResetTest {

    public static void main(String[] args) {
        Properties destinationProperties = KafkaUtils.getDestinationClusterConfigs("/Users/bjaggi/confluent/OffsetTranslation/src/main/resources/sourcecluster.properties");
        destinationProperties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);

        AdminClient adminClient = AdminClient.create(destinationProperties);
        KafkaConsumer consumer = new KafkaConsumer(destinationProperties);
        Map<TopicPartition, Long> timestamps = new HashMap<>();
        String topicName = "jpmc_topic";
        String cgName = "jpmc_topic_cg";
        long timestamp = 1719508061563L;
        int partitionNumber = 0;
        TopicPartition tp0 = new TopicPartition(topicName, partitionNumber);
        //timestamps.put(tp0, System.currentTimeMillis()-1000*1000);
        timestamps.put(tp0, timestamp);


        Map<TopicPartition, OffsetAndTimestamp> destPartitionOffsetMap = consumer.offsetsForTimes(timestamps);
        System.out.println(destPartitionOffsetMap);
        if( destPartitionOffsetMap != null && destPartitionOffsetMap.get(tp0) != null) {
            long offsetToSeek = destPartitionOffsetMap.get(tp0).offset();


            // For CG API
            Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
            offsets.put(new TopicPartition(topicName, partitionNumber), new OffsetAndMetadata(offsetToSeek));
            AlterConsumerGroupOffsetsResult result = adminClient.alterConsumerGroupOffsets(cgName, offsets);
            try {
                result.all().get();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                System.out.println(" Consumers should not be running during CG reset!");
                throw new RuntimeException(e);
            }
        } else {
            System.err.println(" ERROR : resetting   "+tp0);
            System.out.println("Probably Nothing is read from this partition ?, please reset this manually "+ timestamp);
        }
        // For Seek API Only
/*
        consumer.assign(Arrays.asList(tp0));
        consumer.seek(tp0, offsetToSeek);
        while(true) {
            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record : records) {
                System.out.println("value : "+ record.value() + " , offset : "+record.offset()+ " , timestamp : "+record.timestamp());
            }
        }
*/


//        Map partitionOffsetresetMap = new HashMap();
//        partitionOffsetresetMap.put(topicPartition, consumerGroupMetdata.getTimestamp());
//        Map<TopicPartition, OffsetAndTimestamp> destPartitionOffsetMap = consumer.offsetsForTimes(partitionOffsetresetMap, Duration.ofSeconds(30));


    }
}
