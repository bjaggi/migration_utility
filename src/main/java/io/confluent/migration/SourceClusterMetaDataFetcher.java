package io.confluent.migration;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.model.ConsumerGroupMetdata;
import io.confluent.model.TopicMetadata;
import io.confluent.migration.model.RecordMetaData;
import io.confluent.migration.utils.KafkaUtils;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.*;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.SecurityDisabledException;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.resource.ResourcePatternFilter;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static io.confluent.migration.utils.Constants.*;

//import kafka.coordinator.group.OffsetKey;
//import org.apache.kafka.clients.producer.ProducerRecord;
//import org.apache.kafka.clients.producer.RecordMetadata;
//import org.apache.kafka.clients.producer.ProducerConfig;
//import org.apache.kafka.clients.producer.Producer;
//
//
//import org.apache.kafka.common.*;
public class SourceClusterMetaDataFetcher {
    static List<TopicMetadata> topicMetadataList = new ArrayList<>();
    static Map< String,TopicMetadata> topicMetadataMap = new HashMap<>();
    static Map< String, List<ConsumerGroupMetdata>> cgMetadataListMap = new HashMap<>();
    static ObjectMapper objectMapper = new ObjectMapper();



    public static void main(String[] args) throws ExecutionException, InterruptedException, TimeoutException {
        System.out.println(" #### Starting the Source Cluster MetaData Extractor Tool !! ### ");
        System.out.println("Enter absolute path of the source cluster properties file: ");
        System.out.println();
        System.out.println();
        System.out.println();
        Scanner scanner = new Scanner(System.in);

        String absolutePath = scanner.nextLine();
        System.out.println(" Reading properties from file :  " + absolutePath);
        System.out.println(".");
        System.out.println("..");
        System.out.println("...");
        //Properties sourceProperties = KafkaUtils.getSourceClusterConfigs("/Users/bjaggi/confluent/OffsetTranslation/src/main/resources/sourcecluster.properties");
        Properties sourceProperties = KafkaUtils.getSourceClusterConfigs(absolutePath);
        AdminClient adminClient = AdminClient. create(sourceProperties);
        System.out.println(" Successfully created Admin Client based on the input file!!" );
        System.out.println();
        System.out.println();
        System.out.println();

        System.out.println("Choose one of the following actions ");
        System.out.println("Enter 1 to export all topic's as JSON : ");
        System.out.println("Enter 2 to export ACL's as JSON : ");
        System.out.println("Enter 3 to export Consumer Group details as JSON : ");
        int optionInt = scanner.nextInt();


        switch (optionInt) {
            case ALL_TOPICS:
                 exportAllTopics(adminClient);
                 writeTopicsToFile();
                 break;
            case ALL_ACLS:
                 Collection<AclBinding> allAcls = exportAllAcls(adminClient);
                 writeAclsToFile(allAcls);
                 break;
            case ALL_CG:
                System.out.println(" Enter all Consumer Group names(comma seperated) for extraction, enter * for all groups ");
                scanner.nextLine();
                String cgCsv = scanner.nextLine();
                System.out.println(" You chose : "+ cgCsv);

                 exportAllConsumerGroups(adminClient, sourceProperties, cgCsv);
                 writeCgsToFile(cgMetadataListMap);
                break;
            default:
                System.out.println("Invalid Option, program will now exit");
                System.exit(0);
        }
        System.out.println(" Exiting the script, please review output and errors if any");
    }










    private static void exportAllConsumerGroups(AdminClient adminClient, Properties sourceProperties, String cgCsv) throws ExecutionException, InterruptedException {
        System.out.println();
        System.out.println();
        System.out.println("####### ALL CONSUMER GROUPS ##########");
        KafkaConsumer consumerConsumerOffsets = new KafkaConsumer(sourceProperties);
        KafkaConsumer consumer = new KafkaConsumer(sourceProperties);

        consumerConsumerOffsets.subscribe(Arrays.asList("__consumer_offsets"));
        ConsumerRecords<String, String> consumerOffsetRecords = consumerConsumerOffsets.poll(Duration.ofSeconds(5));

        List<String> groupIds = adminClient.listConsumerGroups().all().get().
                stream().map(s -> s.groupId()).collect(Collectors.toList());

        cgCsv = cgCsv.replace("\n", "");
        cgCsv = cgCsv.trim();
        if("*".equals(cgCsv) || "".equals(cgCsv) ){
            // No filter, prin all
        }else {
            String finalCgCsv = cgCsv;
            groupIds = groupIds.stream().filter(s -> s.equals(finalCgCsv)).collect(Collectors.toList());;
        }

        groupIds.forEach(groupId -> {
            System.out.println("#### SUMMARIZING ALL CONSUMER GROUP : " + groupId);

            try {
                Map<TopicPartition, OffsetAndMetadata> cgTopicMetadataMap = adminClient.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().get();
                List<ConsumerGroupMetdata> cgMetadataList = new ArrayList<>();
                Map<TopicPartition, Long>  logEndOffsetMap = consumer.endOffsets(cgTopicMetadataMap.keySet());
                consumer.assign(Arrays.asList(cgTopicMetadataMap.keySet().toArray()));

                cgTopicMetadataMap.forEach((topicPartition, offsetAndMetadata) -> {

                    //System.out.println("Topic Name : "+ topicPartition.topic() + " , Partition :   "+ topicPartition.partition()+ " , Current Offset :   "+offsetAndMetadata.offset() + ", Log end Offset : "+ logEndOffsetMap.get(topicPartition) + " , Metadata :  "+offsetAndMetadata.metadata() );
                    ConsumerGroupMetdata cgMetadata = new ConsumerGroupMetdata();
                    cgMetadata.setConsumerGroupName(groupId);
                    cgMetadata.setTopicName(topicPartition.topic());
                    cgMetadata.setPartitionNum(topicPartition.partition());
                    cgMetadata.setOffsetNumber(offsetAndMetadata.offset());

                    consumer.seek(topicPartition, offsetAndMetadata.offset());
                    //consumer.position(topicPartition);
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(20));
                    //for (ConsumerRecord record : records) {
                    if(records != null && !records.isEmpty()){
                        List<ConsumerRecord> actualList = new ArrayList<ConsumerRecord>();
                        Iterator iterator = records.iterator();
                        while (iterator.hasNext()) {
                            actualList.add((ConsumerRecord)iterator.next());
                        }
                        ConsumerRecord record = actualList.get(actualList.size()-1);
                        System.out.println("Topic Name : "+ topicPartition.topic() + " , Partition :   "+ topicPartition.partition()+ " , Current Offset :   "+offsetAndMetadata.offset() + ", Log end Offset : "+ logEndOffsetMap.get(topicPartition) + " , TimeStamp of last committed offset :  "+record.timestamp() );

//                        RecordMetaData recordMetaData = new RecordMetaData();
//                        recordMetaData.setOffset(offsetAndMetadata.offset());
//                        recordMetaData.setPartition(topicPartition);
//                        recordMetaData.setTimestamp(record.timestamp());

                        cgMetadata.setOffsetNumber(offsetAndMetadata.offset());
                        cgMetadata.setTimestamp(record.timestamp());
                        //recordMetaDataMap.put(record.partition(),recordMetaData );
                        cgMetadataList.add(cgMetadata);
                        System.out.println( "CG Name : "+groupId + " Topic Name : "+topicPartition.topic()+ " Partition : "+topicPartition.partition() + " committed offset "+cgMetadata.getOffsetNumber() + ", timestamp :"+cgMetadata.getTimestamp());

                    }else{
                        //TODO
                        cgMetadata.setOffsetNumber(offsetAndMetadata.offset());
                        cgMetadata.setPartitionNum(topicPartition.partition());
                        cgMetadata.setTimestamp(0);
                        System.out.println("Topic Name : "+ topicPartition.topic() + " , Partition :   "+ topicPartition.partition()+ " , Current Offset :   "+offsetAndMetadata.offset() + ", Log end Offset : "+ logEndOffsetMap.get(topicPartition) + " , TimeStamp of last committed offset :  ?" );


                        cgMetadataList.add(cgMetadata);
                    }

                });
                cgMetadataListMap.put(groupId, cgMetadataList);
                System.out.println();
                System.out.println();
                System.out.println();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
            //System.out.println(list);
        });
    }

    private static void exportAllTopics(AdminClient adminClient) throws ExecutionException, InterruptedException {
        Map<Integer,RecordMetaData>   recordMetaDataMap = new HashMap<Integer, RecordMetaData>();

        System.out.println();
        System.out.println();
        System.out.println("########### FOUND THE FOLLOWING  BUSINESS TOPICS  ################");
        ListTopicsResult topics = adminClient.listTopics(new ListTopicsOptions().listInternal(false));

        Set<String> topicStringSet = topics.names().get().stream()
                .collect(Collectors.toSet());

        Set<String> filteredtopicStringSet = topicStringSet.stream()
                .filter( name-> !name.toString().contains("_confluent-controlcenter") )
                .filter( name-> !name.toString().contains("_confluent") )
                .filter( name-> !name.toString().contains("default_ksql_processing_log"))
                .filter( name-> !name.toString().contains("connect-configs"))
                .filter( name-> !name.toString().contains("connect-statuses"))
                .filter( name-> !name.toString().contains("_schemas"))
                .filter( name-> !name.toString().contains("connect-offsets"))
                .collect(Collectors.toSet());
        //filteredtopicStringSet.forEach(System.out::println);

        filteredtopicStringSet.forEach(System.out::println);
        System.out.println();
        System.out.println();
        System.out.println("###### ALL TOPICS WITH CONFIGS #################");
        TopicDescription topicDescription;
        DescribeTopicsResult topicsConfigList = adminClient.describeTopics(filteredtopicStringSet);


        // TODO
        List<String> topicNameList = new ArrayList<String>();
        topicNameList.addAll(filteredtopicStringSet);


        for( String topicName : topicNameList ) {
            Collection<ConfigResource> cr = Collections.singleton((new ConfigResource(ConfigResource.Type.TOPIC,topicName)));
            DescribeConfigsResult ConfigsResult = adminClient.describeConfigs(cr);
            Config all_configs = (Config) ConfigsResult.all().get().values().toArray()[0];

            // System.out.println(all_configs.entries());
            ArrayList<ConfigEntry> configList = new ArrayList<>(all_configs.entries());
            System.out.println(">> TOPIC NAME & ITS CONFIGS >>>>  : "+topicName);

            int partition_count = topicsConfigList.topicNameValues().get(topicName).get().partitions().size();
            int replicas = topicsConfigList.topicNameValues().get(topicName).get().partitions().get(0).replicas().size();


            boolean isinternal = topicsConfigList.topicNameValues().get(topicName).get().isInternal();
            TopicMetadata topicMetadata = new TopicMetadata();
//            System.out.println("partition.count : "+ partition_count);
//            System.out.println("nun.replicas " + replicas);
//            System.out.println(" ACL Set " + setAclOperations);
//            System.out.println(" isInternal " + isinternal);
            topicMetadata.setPartitionCount(partition_count);
            topicMetadata.setReplicationFactor(replicas);

            Map<String, String> map = new HashMap<String, String>();
            boolean isFilterProperties = true;
            for ( ConfigEntry configEntry : configList){
                if(isFilterProperties   ) {
                    //System.out.println(configEntry.isDefault() + ", " + configEntry.source() + ", " + configEntry.name());
                    //if (configEntry.source().equals(ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG) && importantProperties.contains(configEntry.name())){
                    if (configEntry.source().equals(ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG) ){
                        System.out.println(configEntry.isDefault() + ", " + configEntry.name() + ", " + configEntry.value());
                        map.put(configEntry.name(), configEntry.value());
                    }
                }else {
                    System.out.println(configEntry.isDefault() + ", " + configEntry.name() + ", " + configEntry.value());
                    map.put(configEntry.name(), configEntry.value());
                }
            }
            topicMetadata.setConfigs(map);
            topicMetadataMap.put(topicName, topicMetadata);
            //topicMetadataList.add(topicMetadata);
        }
    }

    private static void writeAclsToFile(Collection<AclBinding> allAcls) {
        try {
            objectMapper
                    .writerWithDefaultPrettyPrinter()
                    .writeValue(new File("source_cluster_acls.json"), allAcls);


        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    private static void writeCgsToFile(Map< String, List<ConsumerGroupMetdata>> cgMetadataListMap) {
        //objectMapper.configure(SerializationConfig.Feature.INDENT_OUTPUT, true);
        try {
            objectMapper
                    .writerWithDefaultPrettyPrinter()
                    .writeValue(new File("source_cluster_CG.json"), cgMetadataListMap);


        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    private static void writeTopicsToFile() {
        //objectMapper.configure(SerializationConfig.Feature.INDENT_OUTPUT, true);
        try {
            objectMapper
                    .writerWithDefaultPrettyPrinter()
                    .writeValue(new File("source_cluster_topics.json"), topicMetadataMap);


        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    private static Collection<AclBinding> exportAllAcls(AdminClient adminClient) {
        try {
//            AclBindingFilter any = new AclBindingFilter(
//                    new ResourcePatternFilter(ResourceType.TOPIC, null, PatternType.ANY),
//                    new AccessControlEntryFilter("User:" + username, null, AclOperation.ANY, AclPermissionType.ANY));
            System.out.println("####### FOLLOWING ACL's WERE FOUND  ####### :");
            AclBindingFilter any = new AclBindingFilter(
                    new ResourcePatternFilter(ResourceType.TOPIC, null, PatternType.ANY),
                    new AccessControlEntryFilter(null, null, AclOperation.ANY, AclPermissionType.ANY));

            return adminClient.describeAcls(any).values().get();
        } catch (SecurityDisabledException sed){
            System.out.println("SecurityDisabledException: No Authorizer is configured on the broker");
            sed.printStackTrace();
        }
        catch (InterruptedException | ExecutionException e) {
            if (e.getCause() != null && e.getCause() instanceof SecurityDisabledException) {
                e.printStackTrace();
            } else {
                e.printStackTrace();
            }
        }
        return Collections.emptyList();
    }


}