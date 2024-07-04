package io.confluent.migration.test;

import io.confluent.migration.utils.KafkaUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.types.Field;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ConsumerTest {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        Properties destinationProperties = KafkaUtils.getDestinationClusterConfigs("/Users/bjaggi/confluent/migration_utility/src/main/resources/sourcecluster.properties");
        destinationProperties.put("key.deserializer" , "org.apache.kafka.common.serialization.StringDeserializer");
        destinationProperties.put("value.deserializer" ,"org.apache.kafka.common.serialization.StringDeserializer");
        destinationProperties.put("auto.offset.reset", "earliest");
        destinationProperties.put("group.id", "earliest_test3");
        destinationProperties.put("max.poll.records", 1);

        System.out.println(destinationProperties);
        KafkaConsumer<String, String> consumer = new KafkaConsumer(destinationProperties);
        //consumer.subscribe(Arrays.asList("topic"));
        //ConsumerRecords<String, String> records = consumer.poll(1000000);
        //consumer.commitSync();
        //records.forEach(System.out::println);


        TopicPartition tp = new TopicPartition("test", 9);
        consumer.assign(Arrays.asList(tp));
        consumer.seek(tp, 4177320);

        ConsumerRecords<String, String> records = consumer.poll(1000000);
        records.forEach(System.out::println);


//ConsumerRecord(topic = topic, partition = 9, leaderEpoch = 0, offset = 4177320, CreateTime = 1720033291584, serialized key size = -1, serialized value size = 4, headers = RecordHeaders(headers = [], isReadOnly = false), key = null, value = 8587)
//ConsumerRecord(topic = topic, partition = 0, leaderEpoch = 0, offset = 4365651, CreateTime = 1720033291516, serialized key size = -1, serialized value size = 4, headers = RecordHeaders(headers = [], isReadOnly = false), key = null, value = 1458)
//\ConsumerRecord(topic = topic, partition = 2, leaderEpoch = 0, offset = 4235738, CreateTime = 1720033291452, serialized key size = -1, serialized value size = 1, headers = RecordHeaders(headers = [], isReadOnly = false), key = null, value = 1)


    }
}
