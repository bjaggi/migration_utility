package io.confluent.migration;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.migration.utils.KafkaUtils;
import io.confluent.model.ConsumerGroupMetdata;
import io.confluent.model.TopicMetadata;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AclBinding;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static io.confluent.migration.utils.Constants.*;

public class DestinationClusterMetaDataApply {

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        ObjectMapper objectMapper = new ObjectMapper();
        System.out.println(" #### Starting the Utility in Destination Cluster MetaData Application Mode !! ### ");
        System.out.println("Enter absolute path of the destination cluster properties file: ");
        System.out.println();
        System.out.println();
        System.out.println();
        Scanner scanner = new Scanner(System.in);

        String absolutePath = scanner.nextLine();
        System.out.println(" Reading properties from file :  " + absolutePath);
        System.out.println(".");
        System.out.println("..");
        System.out.println("...");
        //Properties destinationProperties = KafkaUtils.getSourceClusterConfigs("/Users/bjaggi/confluent/OffsetTranslation/src/main/resources/sourcecluster.properties");
        Properties destinationProperties = KafkaUtils.getDestinationClusterConfigs(absolutePath);
        AdminClient adminClient = AdminClient.create(destinationProperties);
        System.out.println(" Successfully created Admin Client based on the input file!!");
        System.out.println();
        System.out.println();
        System.out.println();

        System.out.println("Choose one of the following actions ");
        System.out.println("Enter 1 to create all topic's from the JSON file : ");
        System.out.println("Enter 2 to create ACL's  from the JSON file : ");
        System.out.println("Enter 3 to create Consumer Group  from the JSON file : ");
        int optionInt = scanner.nextInt();
        System.out.println(".");
        System.out.println("..");
        System.out.println("...");

        switch (optionInt) {
            case ALL_TOPICS:
                System.out.println("Enter absolute path of the topics file in the JSON format : ");
                scanner.nextLine();
                System.out.println();
                System.out.println();
                System.out.println();
                String topicFilePath = scanner.nextLine();
                System.out.println();
                System.out.println();
                System.out.println();
                System.out.println("Reading topics & configs from file :  " + topicFilePath);
                System.out.println(".");
                System.out.println("..");
                System.out.println("...");
                Map<String, TopicMetadata> topicsMap = objectMapper.readValue(new File(topicFilePath), new TypeReference<Map<String, TopicMetadata>>() {
                });
                System.out.println(" Successfully read all topics & configs from file  :  " + topicFilePath);
                createTopics(adminClient, topicsMap);
                break;
            case ALL_ACLS:
                System.out.println("Enter absolute path of the ACL file in the JSON format : ");
                scanner.nextLine();
                System.out.println();
                System.out.println();
                System.out.println();
                String aclFilePath = scanner.nextLine();
                System.out.println();
                System.out.println();
                System.out.println();
                System.out.println("Reading topics & configs from file :  " + aclFilePath);
                System.out.println(".");
                System.out.println("..");
                System.out.println("...");
                Collection<AclBinding> aclList = objectMapper.readValue(new File(aclFilePath), new TypeReference<Collection<AclBinding>>() {});
                createACLs(adminClient, aclList);


                break;
            case ALL_CG:

                System.out.println("Enter absolute path of the Consumer Group file in the JSON format : ");
                System.out.println();
                System.out.println();
                System.out.println();
                scanner.nextLine();
                String cgFilePath = scanner.nextLine();
                System.out.println(" Reading Consumer Group & configs from file :  " + cgFilePath);
                System.out.println(".");
                System.out.println("..");
                System.out.println("...");
                Map<String, List<ConsumerGroupMetdata>> cgTopicListMap = objectMapper.readValue(new File(cgFilePath), new TypeReference<Map<String, List<ConsumerGroupMetdata>>>() {
                });
                System.out.println(" Successfully read all topics & configs from file  :  " + cgFilePath);

                createConsumerGroups(adminClient, cgTopicListMap, destinationProperties);
                break;
            default:
                System.out.println("Invalid Option, program will now exit");
                System.exit(0);
        }
        System.out.println(" Exiting the script, please review output and errors if any");


        //Properties destinationProperties = KafkaUtils.getDestinationClusterConfigs("/Users/bjaggi/confluent/OffsetTranslation/src/main/resources/destinationcluster.properties");
        //AdminClient adminClient = AdminClient. create(destinationProperties);


    }

    private static void createACLs(AdminClient adminClient, Collection<AclBinding> aclList) {
        adminClient.createAcls(aclList);
    }

    public static void createConsumerGroups(AdminClient adminClient, Map<String, List<ConsumerGroupMetdata>> cgTopicListMap, Properties destinationProperties) {
        KafkaConsumer consumer = new KafkaConsumer(destinationProperties);
        KafkaConsumer consumerSeekLatest = new KafkaConsumer(destinationProperties);

        // TODO : Fix this
        Set<TopicPartition> partitionSet = consumerSeekLatest.assignment();
        List<TopicPartition> topicPartitionList = partitionSet.stream().collect(Collectors.toList());
        consumerSeekLatest.seekToBeginning(topicPartitionList);
        Map<TopicPartition, Long> earliestOffsetMap = consumerSeekLatest.beginningOffsets(topicPartitionList);



        for (Map.Entry<String, List<ConsumerGroupMetdata>> entry : cgTopicListMap.entrySet()) {
            System.out.println(" Resetting Consumer Group " +entry.getKey());
            List<ConsumerGroupMetdata> consumerGroupMetdataList = entry.getValue();


            if (consumerGroupMetdataList != null && !consumerGroupMetdataList.isEmpty()) {
                for (ConsumerGroupMetdata consumerGroupMetdata : consumerGroupMetdataList) {
                    // On all the partitions
                    TopicPartition topicPartition = new TopicPartition(consumerGroupMetdata.getTopicName(), consumerGroupMetdata.getPartitionNum());
                    Map<TopicPartition, Long> timestampsForSearch = new HashMap<>();
                    timestampsForSearch.put(topicPartition, consumerGroupMetdata.getTimestamp());


                    // ***Task 1*** : Fetch offsets based on the timestamp
                    Map<TopicPartition, OffsetAndTimestamp> destPartitionOffsetMap = consumer.offsetsForTimes( timestampsForSearch);
                    if( destPartitionOffsetMap != null && destPartitionOffsetMap.get(topicPartition)!= null ) {
                        System.out.println(" Source Cluster Timestamp of "+ consumerGroupMetdata.getTimestamp()+ " , resulted in following on the destination cluster : " + destPartitionOffsetMap);
                        long offsetToSeek = destPartitionOffsetMap.get(topicPartition).offset();

                        // ***Task 2*** : Resetting the consumer group ID based on the timestamp
                        Map<TopicPartition, OffsetAndMetadata> partitionTimestampMap = new HashMap<>();
                        partitionTimestampMap.put(topicPartition, new OffsetAndMetadata(offsetToSeek));

                        AlterConsumerGroupOffsetsResult result = adminClient.alterConsumerGroupOffsets(consumerGroupMetdata.getConsumerGroupName(), partitionTimestampMap);
                        try {
                            result.all().get();
                            System.out.println("Reset completed for : " + destPartitionOffsetMap);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        } catch (ExecutionException e) {
                            throw new RuntimeException(e);
                        }
                    }else{
                        System.err.println(" ERROR : resetting partition    "+topicPartition + "  to the timestamp "+consumerGroupMetdata.getTimestamp());


                        Map<TopicPartition, OffsetAndMetadata> partitionTimestampMap = new HashMap<>();
                        long earliestOffset = earliestOffsetMap.get(topicPartition)==null?0:earliestOffsetMap.get(topicPartition);
                        partitionTimestampMap.put(topicPartition, new OffsetAndMetadata(earliestOffset));
                        AlterConsumerGroupOffsetsResult result = adminClient.alterConsumerGroupOffsets(consumerGroupMetdata.getConsumerGroupName(), partitionTimestampMap);
                        System.out.println("Probably no data available on this partition ?, please reset this manually "+ consumerGroupMetdata.getTimestamp());
                        System.out.println("Reseting the CG to earliest, this may result in duplicates.  Earliest offset is :  "+ earliestOffset );


                        try {
                            result.all().get();
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        } catch (ExecutionException e) {
                            throw new RuntimeException(e);
                        }

                    }
                }
            }
        }

        // Close the AdminClient
        adminClient.close();
    }

    public static void createTopics(AdminClient adminClient, Map<String, TopicMetadata> topicsMap) throws ExecutionException, InterruptedException {
        TopicMetadata metadata = null;
        KafkaFuture<Void> future = null;

        for (String topicName : topicsMap.keySet()) {
            metadata = topicsMap.get(topicName);
            short replicationFactor = (short) metadata.getReplicationFactor();
            try {

                NewTopic newTopic = new NewTopic(topicName, metadata.getPartitionCount(), replicationFactor);
                newTopic.configs(metadata.getConfigs());

                future = adminClient.createTopics(Collections.singleton(newTopic), new CreateTopicsOptions().timeoutMs(10000)).all();
                System.out.println(" Successfully created topic : " + topicName + " metadata " + metadata);

//                AclBinding newAcl = new AclBinding(new ResourcePattern(ResourceType.TOPIC, "my-secure-topic", PatternType.LITERAL), new AccessControlEntry("test-1", "*", AclOperation.WRITE, AclPermissionType.ALLOW));
//                adminClient.createAcls(Arrays.asList(newAcl));
//                Set<AclOperation> aclOperationSet = metadata.getSetAclOperations();


            } finally {
                try {
                    future.get();
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
            }
        }


    }
}
