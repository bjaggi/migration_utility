kafka-topics --create --topic test001 \
  --bootstrap-server localhost:9093 \
  --partitions 10 \
  --replication-factor 1 \
  --config retention.ms=604800000 \
  --config segment.bytes=1073741824

kafka-topics --create --topic test002 \
  --bootstrap-server localhost:9093 \
  --partitions 2 \
  --replication-factor 1

kafka-topics --create --topic test003 \
  --bootstrap-server localhost:9093 \
  --partitions 3 \
  --replication-factor 1

echo "Listing topics in source cluster"
kafka-topics --bootstrap-server localhost:9093 --list