#!/bin/sh
echo
echo
echo
echo " Starting Migration Utility!!!"
echo
echo
echo
echo "Enter 'IMPORT' to copy metadata from a Kafka cluster, or 'EXPORT' to apply existing metadata to a Kafka cluster"
read MODE
echo "Running the Utility in $MODE mode"

if [ "$MODE" = "IMPORT" ] || [ "$MODE" = "import" ]
then
  java -cp download_artifacts/MigrationUtility-1-jar-with-dependencies.jar io.confluent.migration.SourceClusterMetaDataFetcher
elif [ "$MODE" = "EXPORT" ] || [ "$MODE" = "export" ]
then
  java -cp download_artifacts/MigrationUtility-1-jar-with-dependencies.jar io.confluent.migration.DestinationClusterMetaDataApply
else
  echo "Existing program, Valid inputs are 'IMPORT' or 'EXPORT' "
  exit
fi

