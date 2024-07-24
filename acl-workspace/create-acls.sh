kafka-acls --bootstrap-server localhost:9093 \
  --add \
  --allow-principal User:nerm \
  --operation read \
  --topic test \
  --resource-pattern-type prefixed

kafka-acls --bootstrap-server localhost:9093 \
  --add \
  --allow-principal User:nerm \
  --operation read \
  --group test-cg


kafka-acls --bootstrap-server localhost:9093 \
  --add \
  --allow-principal User:nerm \
  --operation alter \
  --cluster

kafka-acls --bootstrap-server localhost:9093 \
  --add \
  --allow-principal User:nerm \
  --operation all \
  --transactional-id '*'

kafka-acls --bootstrap-server localhost:9093 --list