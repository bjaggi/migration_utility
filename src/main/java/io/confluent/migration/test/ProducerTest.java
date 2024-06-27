package io.confluent.migration.test;

import io.confluent.migration.utils.KafkaUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerTest {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        Properties destinationProperties = KafkaUtils.getDestinationClusterConfigs("/Users/bjaggi/confluent/OffsetTranslation/src/main/resources/sourcecluster.properties");
        destinationProperties.put("key.serializer" , "org.apache.kafka.common.serialization.StringSerializer");
        destinationProperties.put("value.serializer" ,"org.apache.kafka.common.serialization.StringSerializer");


        AdminClient adminClient = AdminClient.create(destinationProperties);
        KafkaProducer<String, String> producer = new KafkaProducer(destinationProperties);



        ProducerRecord<String, String> record ;
        for (int i=0; i<Integer.MAX_VALUE; i++ ) {



            record = new ProducerRecord<String, String>("jpmc_topic", String.valueOf(i) );

            // send data - asynchronous

            RecordMetadata rm = producer.send(record).get();
            System.out.println("Value : "+ record.value() + " TimeStamp "+rm.timestamp() + " ,  Offset : "+rm.offset()+ " ,  Partition : "+rm.partition());
            try {
                Thread.sleep(2000);// 10 times per second
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }


    }
}
