package io.confluent.migration;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
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
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.kafka.common.quota.ClientQuotaFilter;
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

import static io.confluent.migration.DestinationClusterMetaDataApply.createACLs;
import static io.confluent.migration.utils.Constants.*;
import static io.confluent.migration.utils.SortCG.sortByCGCommittedOffsetTime;
import static io.confluent.migration.utils.SortCG.sortByCGLag;

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
    static Map<ConsumerGroupMetdata, Long> sortedByCGLagMap = new HashMap<>();
    static Map<ConsumerGroupMetdata, Long> sortedByCGCommittedTimestampMap = new HashMap<>();


    public static void main(String[] args) throws ExecutionException, InterruptedException, TimeoutException {
        System.out.println(" #### Starting the Utility in Source Cluster MetaData Extractor Mode !! ### ");
        System.out.println("Enter absolute path of the source cluster properties file: ");
        System.out.println();
        System.out.println();
        System.out.println();
        Scanner scanner = new Scanner(System.in);

        String sourceClusterAbsolutePath = scanner.nextLine();
        String destinationCLusterAbsolutePath = "";
        if(sourceClusterAbsolutePath.trim().equals("")) {
            sourceClusterAbsolutePath = "/Users/bjaggi/confluent/migration_utility/src/main/resources/sourcecluster.properties";
            destinationCLusterAbsolutePath = "/Users/bjaggi/confluent/migration_utility/src/main/resources/destinationcluster.properties";
        }

        System.out.println(" Reading properties from file :  " + sourceClusterAbsolutePath);
        System.out.println(".");
        System.out.println("..");
        System.out.println("...");
        //Properties sourceProperties = KafkaUtils.getSourceClusterConfigs("/Users/bjaggi/confluent/OffsetTranslation/src/main/resources/sourcecluster.properties");
        Properties sourceProperties = KafkaUtils.getSourceClusterConfigs(sourceClusterAbsolutePath);
        Properties destProperties = KafkaUtils.getSourceClusterConfigs(destinationCLusterAbsolutePath);
        // FOR JPMC
        if(System.getenv("KRBCONFIG") != null && System.getenv("KRB5_CCNAME")!= null) {
            System.setProperty("java.security.krb5.conf", System.getenv("KRB5_CONFIG") );
            System.setProperty("oracle.net.kerberos5_cc_name",System.getenv("KRB5CCNAME"));
        } else {
            System.err.println(" ***KRBCONFIG & KRB5CCNAME are not set! ");
        }


        AdminClient sourceClusterAdminClient = AdminClient. create(sourceProperties);
        AdminClient destClusterAdminClient = AdminClient. create(destProperties);
        System.out.println(" Successfully created Admin Client based on the input file!!" );
        System.out.println();
        System.out.println();
        System.out.println();

        System.out.println("Choose one of the following actions ");
        System.out.println("Enter 1 to export all topic's as JSON : ");
        System.out.println("Enter 2 to export ACL's as JSON & apply to destination cluster : ");
        System.out.println("Enter 3 to export Consumer Group details as JSON : ");
        System.out.println("Enter 4 to export Quotas as JSON & apply to destination cluster : ");

        int optionInt = scanner.nextInt();


        switch (optionInt) {
            case ALL_TOPICS:
                 exportAllTopics(sourceClusterAdminClient);
                 writeTopicsToFile();
                 break;
            case ALL_ACLS:
                 List<AclBinding> allAcls = exportAllAcls(sourceClusterAdminClient);
                 // Print ACL to file and create ACL on destination Clsuter
                 writeAclsToFileAndCreateAcl(allAcls, destClusterAdminClient);
                 break;
            case ALL_QUOTAS:
                Map<ClientQuotaEntity,Map<java.lang.String,java.lang.Double>> quotaList =  sourceClusterAdminClient.describeClientQuotas(ClientQuotaFilter.all()).entities().get();
                for ( Map.Entry<ClientQuotaEntity,Map<String, Double>> entry : quotaList.entrySet()) {
                    System.out.println(entry.getKey() + "/" + entry.getValue());
                }
                break;
            case ALL_CG:
                boolean inclusiveFilterBool = false;
                System.out.println(" Enter all Consumer Group names(comma seperated) for extraction, enter * for all groups ");
                scanner.nextLine();
                String cgCsvFilter = scanner.nextLine();
                System.out.println(" You chose cg filter string as : "+ cgCsvFilter);
                System.out.println();
                System.out.println();
                System.out.println(" Enter 'I' or 'ENTER' for inclusive filter and 'E' for exclusive filter ");
                String inclusiveExclusiveFilter = scanner.nextLine();

                inclusiveExclusiveFilter = inclusiveExclusiveFilter.trim();
                System.out.println(" You filter was : "+ inclusiveExclusiveFilter);
                System.out.println(" You chose CG Filter as : "+ cgCsvFilter);
                if(inclusiveExclusiveFilter.equalsIgnoreCase("I") || inclusiveExclusiveFilter.equalsIgnoreCase("")) {
                    inclusiveFilterBool = true;
                    System.out.println(" Working on a Inclusive Filter with : " + cgCsvFilter);
                }
                else if(inclusiveExclusiveFilter.equalsIgnoreCase("E")) {
                    inclusiveFilterBool = false;
                    System.out.println(" Working on a Exclusive Filter with : " + cgCsvFilter);
                }
                else {
                    System.err.println("Invalid Filter option, valid options are 'I' or 'E' ");
                    System.exit(1);
                }

                 exportAllConsumerGroups(sourceClusterAdminClient, sourceProperties, cgCsvFilter, inclusiveFilterBool);
                 writeCgsToFile(cgMetadataListMap);
                 writeSortedCgsToFile(sortByCGLag(sortedByCGLagMap),"source_cluster_CG_sorted_by_lag.json" );
                 writeSortedCgsToFile(sortByCGCommittedOffsetTime(sortedByCGCommittedTimestampMap),"source_cluster_CG_sorted_by_timestamp.json" );
                break;
            default:
                System.out.println("Invalid Option, program will now exit");
                System.exit(0);
        }
        System.out.println(" Exiting the script, please review output and errors if any");
    }










    private static void exportAllConsumerGroups(AdminClient adminClient, Properties sourceProperties, String filterCgCsv, boolean inclusiveFilter ) throws ExecutionException, InterruptedException {
        System.out.println();
        System.out.println();

        KafkaConsumer consumerConsumerOffsets = new KafkaConsumer(sourceProperties);
        KafkaConsumer consumer = new KafkaConsumer(sourceProperties);

        consumerConsumerOffsets.subscribe(Arrays.asList("__consumer_offsets"));
        ConsumerRecords<String, String> consumerOffsetRecords = consumerConsumerOffsets.poll(Duration.ofSeconds(5));

        List<String> groupIds = adminClient.listConsumerGroups().all().get().
                stream().map(s -> s.groupId()).collect(Collectors.toList());


        filterCgCsv = filterCgCsv.replace("\n", "");
        filterCgCsv = filterCgCsv.trim();
        if("*".equals(filterCgCsv) || "".equals(filterCgCsv) ){
            // No filter, print all
        }else if(inclusiveFilter) {
            String finalCgCsv = filterCgCsv;
            List<String> filtercgArray = Arrays.asList(finalCgCsv.split(","));
            groupIds = groupIds.stream().filter(s -> filtercgArray.contains(s)).collect(Collectors.toList());;
        } else if(!inclusiveFilter){
            String finalCgCsv = filterCgCsv;
            List<String> filtercgArray = Arrays.asList(finalCgCsv.split(","));
            groupIds = groupIds.stream().filter(s -> !filtercgArray.contains(s)).collect(Collectors.toList());;
        }

        System.out.println("####### WORKING ON THE FOLLOWING CONSUMER GROUPS ##########");
        groupIds.forEach(System.out::println);
        System.out.println();
        System.out.println();
        System.out.println();

        groupIds.forEach(groupId -> {
            System.out.println("#### SUMMARIZING CONSUMER GROUP : " + groupId);
            try {
                Map<TopicPartition, OffsetAndMetadata> cgTopicMetadataMap = adminClient.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().get();
                List<ConsumerGroupMetdata> cgMetadataList = new ArrayList<>();
                ArrayList partitionList = new ArrayList<>(cgTopicMetadataMap.keySet());
                Map<TopicPartition, Long>  logEndOffsetMap = consumer.endOffsets(partitionList);
                consumer.assign(partitionList);

                cgTopicMetadataMap.forEach((topicPartition, offsetAndMetadata) -> {
                    //System.out.println("Topic Name : "+ topicPartition.topic() + " , Partition :   "+ topicPartition.partition()+ " , Current Offset :   "+offsetAndMetadata.offset() + ", Log end Offset : "+ logEndOffsetMap.get(topicPartition) + " , Metadata :  "+offsetAndMetadata.metadata() );
                    long currentOffset = offsetAndMetadata.offset();
                    long longEndOffset = logEndOffsetMap.get(topicPartition)==null? 0 : logEndOffsetMap.get(topicPartition);
                    long consumerLag = longEndOffset-currentOffset;

                    ConsumerGroupMetdata cgMetadata = new ConsumerGroupMetdata();
                    ConsumerRecord record ;
                    cgMetadata.setConsumerGroupName(groupId);
                    cgMetadata.setTopicName(topicPartition.topic());
                    cgMetadata.setPartitionNum(topicPartition.partition());
                    cgMetadata.setCurrentOffsetNumber(currentOffset);
                    cgMetadata.setLogEndOffsetNumber(longEndOffset);
                    cgMetadata.setConsumerLag(longEndOffset-currentOffset);
                    if(longEndOffset !=0){
                    consumer.seek(topicPartition, offsetAndMetadata.offset());
                    //consumer.position(topicPartition);
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(20));
                    //for (ConsumerRecord record : records) {
                    if(records != null && !records.isEmpty()) {
                        List<ConsumerRecord> actualList = new ArrayList<ConsumerRecord>();
                        Iterator iterator = records.iterator();
                        while (iterator.hasNext()) {
                            actualList.add((ConsumerRecord) iterator.next());
                        }
                        // take the latest message in the record list
                        record = actualList.get(actualList.size() - 1);
                        cgMetadata.setTimestamp(record.timestamp());
                        System.out.println("Topic Name : "+ topicPartition.topic() + " , Partition :   "+ topicPartition.partition()+ " , Current Offset :   "+offsetAndMetadata.offset() + ", Log end Offset : "+ logEndOffsetMap.get(topicPartition) + ",  Lag : "+ (logEndOffsetMap.get(topicPartition)-offsetAndMetadata.offset()) + " , TimeStamp of last committed offset :  "+record.timestamp() );
                    }

                    //To sort by highest to lowest consumer lag
                        sortedByCGLagMap.put(cgMetadata, consumerLag);
                        //To sort by lowest to highest consumer read timestamp ( ie timestamp from the committed offset)
                        sortedByCGCommittedTimestampMap.put(cgMetadata, cgMetadata.getTimestamp());



//                        RecordMetaData recordMetaData = new RecordMetaData();
//                        recordMetaData.setOffset(offsetAndMetadata.offset());
//                        recordMetaData.setPartition(topicPartition);
//                        recordMetaData.setTimestamp(record.timestamp());

                        cgMetadata.setCurrentOffsetNumber(offsetAndMetadata.offset());

                        //recordMetaDataMap.put(record.partition(),recordMetaData );
                        cgMetadataList.add(cgMetadata);
                        System.out.println( "CG Name : "+groupId + " Topic Name : "+topicPartition.topic()+ " Partition : "+topicPartition.partition() + " committed offset "+cgMetadata.getCurrentOffsetNumber() +  ", Log end Offset : "+ longEndOffset + " , Lag : "+ (consumerLag) + ", timestamp :"+cgMetadata.getTimestamp());

                    }else{
                        //TODO
                        cgMetadata.setCurrentOffsetNumber(offsetAndMetadata.offset());
                        cgMetadata.setPartitionNum(topicPartition.partition());
                        cgMetadata.setTimestamp(0);
                        System.out.println("Topic Name : "+ topicPartition.topic() + " , Partition :   "+ topicPartition+ " , Current Offset :   "+offsetAndMetadata.offset() + ", Log end Offset : "+ longEndOffset+ " , Lag :" + consumerLag + " , TimeStamp of last committed offset :  ?" );
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

    private Map<TopicPartition, Long> computeLags(
            Map<TopicPartition, Long> consumerGrpOffsets,
            Map<TopicPartition, Long> producerOffsets) {
        Map<TopicPartition, Long> lags = new HashMap<>();
        for (Map.Entry<TopicPartition, Long> entry : consumerGrpOffsets.entrySet()) {
            Long producerOffset = producerOffsets.get(entry.getKey());
            Long consumerOffset = consumerGrpOffsets.get(entry.getKey());
            long lag = Math.abs(producerOffset - consumerOffset);
            lags.putIfAbsent(entry.getKey(), lag);
        }
        return lags;
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

    private static void writeAclsToFileAndCreateAcl(List<AclBinding> allAcls, AdminClient adminClient) {
        try {
            JsonNode myObjects = objectMapper.valueToTree(allAcls.toString());
            //List<AclBinding> myObjects = objectMapper.readValue(allAcls.toString() ,new TypeReference<List<AclBinding>>() { } );
            //String jsonArray = objectMapper.writeValueAsString(allAcls);
            //objectMapper.readValue(jsonArray, new TypeReference<List<AclBinding>>() { });

            objectMapper
                    .enable(SerializationFeature.INDENT_OUTPUT)
                    .writerWithDefaultPrettyPrinter()
                    .writeValue(new File("source_cluster_acls.json"), myObjects);
            allAcls.forEach(System.out::println);
            createACLs(adminClient, allAcls);
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

    private static void writeSortedCgsToFile(ArrayList<ConsumerGroupMetdata> sortedcgMetadataList, String fileName) {
        //objectMapper.configure(SerializationConfig.Feature.INDENT_OUTPUT, true);
        try {
            objectMapper
                    .writerWithDefaultPrettyPrinter()
                    .writeValue(new File(fileName), sortedcgMetadataList);
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


    private static List<AclBinding> exportAllAcls(AdminClient adminClient) {
        try {
//            AclBindingFilter any = new AclBindingFilter(
//                    new ResourcePatternFilter(ResourceType.TOPIC, null, PatternType.ANY),
//                    new AccessControlEntryFilter("User:" + username, null, AclOperation.ANY, AclPermissionType.ANY));
            System.out.println("####### FOLLOWING ACL's WERE FOUND  ####### :");
            AclBindingFilter any = new AclBindingFilter(
                    new ResourcePatternFilter(ResourceType.ANY, null, PatternType.ANY),
                    new AccessControlEntryFilter(null, null, AclOperation.ANY, AclPermissionType.ANY));

            Collection<AclBinding> aclBindingList =  adminClient.describeAcls(any).values().get();

            return  new ArrayList<>(aclBindingList);
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