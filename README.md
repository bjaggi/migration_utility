# Migration Utility

[![Build status](https://badge.buildkite.com/59fd0ecf6f9a1dbdf9ec6d7ec2f5bd09eaea1de4e087e033a2.svg)](https://buildkite.com/nerm/migration-utility)

Migration utility is designed to export (query) & apply (migrate) CP/CC resources from one kafka cluster to another.

The following resources are in scope for this tool:

1. ACLs
   - Export all ACLs 
   - Synchronize all ACLs from source to destination kafka cluster 
   - Apply a defined subset of ACLs to create/apply into target kafka cluster
2. Topics
   - Export all topics from and explicit topic configs (DYNAMIC source)  
   - Synchronize topics from source to destination kafka cluster (all topics)
   - Synchronize a subset of topics between source/destination kafka cluster (topic names provided as request input)
   - Apply a defined subset of topics to create/apply into target kafka cluster
3. Client Quotas
   - Export all client quotas
   - Synchronize all client quotas from source to destination kafka cluster (all quotas)
   - Apply a defined subset of client quotas to create/apply into target kafka cluster
4. Consumer Group Progress
   - Export consumer group progress/metadata for a subset of consumer groups (current offset, end offset, lag, last consumed timestamp - across all topic-partitions in CG subscription)
   - Set consumer group offsets/progress in target kafka cluster using export metadata (in particular, timestamp found in each TP) 


## How to Run

### Configuration 

`application.yml` contains the configuration used by the spring-boot restful application. Before running the application, ensure this file contains applicable cluster identifiers with respective cluster configuration.

```yaml
kafka:
  clusters:
    homelab:
      bootstrap.servers: bootstrap.local.nermdev.io:443
      security.protocol: SASL_SSL
      sasl.mechanism: PLAIN
      sasl.jaas.config: org.apache.kafka.common.security.plain.PlainLoginModule required username="apikey" password="secret";
    src:
      bootstrap.servers: localhost:9093
    dest:
      bootstrap.servers: localhost:9095 
```

The above highlights how the tool expects kafka cluster configuration to be defined. 

- `kafka.clusters.CLUSTER001` : contains kafka client connectivity configuration for the kafka cluster identified with name `CLUSTER001`
- `kafka.clusters.CLUSTER002` : contains kafka client connectivity configuration for the kafka cluster identified with name `CLUSTER002`
- `kafka.clusters.CLUSTER_IDENTIFIER` : The `CLUSTER_IDENTIFIER` is the kafka-cluster ID. This tool expects restful requests to contain cluster identifiers found in the `application.yml`. Otherwise, will throw an `InvalidClusterIdException`.

### Starting the application

1. `mvn clean install -DskipTests`
2. `mvn spring-boot:run`
3. Open web browser and navigate to `localhost:8080/swagger-ui.html`


# Run the migration utility (script) 
1: Clone the repo    
2: Ensure download_artifacts/run_migration_utility.sh is executable ( chmod +x ) 
```asciidoc
download_artifacts/run_migration_utility.sh
```






 
