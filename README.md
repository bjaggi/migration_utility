# migration_utility

[![Build status](https://badge.buildkite.com/59fd0ecf6f9a1dbdf9ec6d7ec2f5bd09eaea1de4e087e033a2.svg)](https://buildkite.com/nerm/migration-utility)

This migration utility is designed to export & import.  

. <b>Topics</b> ( ie : partitions, replications and all configs)     
. <b>ACL</b>  (ie : all ACL for topics, CG..)   
. <b>CG</b>   ( CG name, offsets and its curent timestamp), This will Also reset CG based on timestamp.   
. <b>CG Sorted by Lag or oldest timestamp</b> create a list of CG Lag
. <b> Quotas </b>   Export quotas and apply to the target cluster.
. <b>Implemeting log4j logging </b>     



## Run the migration utility
1: Clone the repo    
2: Ensure download_artifacts/run_migration_utility.sh is executable ( chmod +x ) 
```asciidoc
download_artifacts/run_migration_utility.sh
```






 
