package io.confluent.migrationutility.framework;

import lombok.SneakyThrows;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Testcontainers
public abstract class TestKafkaContext {
  public static final KafkaContainer srcKafkaContainer;
  public static final KafkaContainer destKafkaContainer;
  static {
    srcKafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"))
//            .withKraft()
            .withListener(() -> "kafka:19092")
            .withNetwork(Network.SHARED)
            .withEnv("KAFKA_NUM_PARTITIONS", "1")
            .withEnv("KAFKA_AUTHORIZER_CLASS_NAME", "kafka.security.authorizer.AclAuthorizer")
            .withEnv("KAFKA_SUPER_USERS", "User:ANONYMOUS")
            .withEnv("KAFKA_LOG_RETENTION_CHECK_INTERVAL_MS", "15000");
    srcKafkaContainer.start();

    destKafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"))
//            .withKraft()
            .withListener(() -> "kafka:19093")
            .withNetwork(Network.SHARED)
            .withEnv("KAFKA_AUTHORIZER_CLASS_NAME", "kafka.security.authorizer.AclAuthorizer")
            .withEnv("KAFKA_SUPER_USERS", "User:ANONYMOUS")
            .withEnv("KAFKA_NUM_PARTITIONS", "1");
    destKafkaContainer.start();


  }

  protected static void consumeFromTopic(final String topic, final String group, final Long count, final Cluster cluster) {
    final StringDeserializer deserializer = new StringDeserializer();
    try (final Consumer<String, String> receiver = new KafkaConsumer<>(consumerConfigs(group, cluster), deserializer, deserializer)) {
      receiver.subscribe(Collections.singleton(topic));
      int i = 0;
      while (i < count) {
        receiver.poll(Duration.ofMillis(3000));
        i++;
      }
      receiver.commitSync();
    }
  }

  @SneakyThrows
  protected static void produceToTopic(final String topic, final Long count, final Cluster cluster) {
    final StringSerializer serializer = new StringSerializer();
    try (final Producer<String, String> sender = new KafkaProducer<>(producerConfigs(cluster), serializer, serializer)) {
      int i = 0;
      while (i < count) {
        sender.send(new ProducerRecord<>(topic, count.toString())).get();
        i++;
      }
      sender.flush();
    }
  }

  protected static Map<String, Object> producerConfigs(Cluster cluster) {
    final Map<String, Object> map = new HashMap<>();
    map.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers(cluster));
    return map;
  }

  protected static Map<String, Object> consumerConfigs(final String group, final Cluster cluster) {
    final Map<String, Object> map = new HashMap<>();
    map.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers(cluster));
    map.put(ConsumerConfig.GROUP_ID_CONFIG, group);
    map.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    map.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    map.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
    return map;
  }

  protected static String bootstrapServers(Cluster cluster) {
    return cluster == Cluster.SOURCE ? srcKafkaContainer.getBootstrapServers() : destKafkaContainer.getBootstrapServers();
  }
}
