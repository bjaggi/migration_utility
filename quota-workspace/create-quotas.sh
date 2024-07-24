

kafka-configs --bootstrap-server localhost:9093 --alter --add-config 'producer_byte_rate=1048576' --entity-type users --entity-name nerm
kafka-configs --bootstrap-server localhost:9093 --alter --add-config 'consumer_byte_rate=1048576' --entity-type users --entity-name nerm_consumer
kafka-configs --bootstrap-server localhost:9093 --alter --add-config 'request_percentage=10' --entity-type users --entity-name nerm2
kafka-configs --bootstrap-server localhost:9093 --alter --add-config 'producer_byte_rate=1048576' --entity-type users --entity-name nerm3 --entity-type clients --entity-name nerm.console




echo "Listing user quotas"
kafka-configs --bootstrap-server localhost:9093 --describe --entity-type users

echo "Listing client quotas"
kafka-configs --bootstrap-server localhost:9093 --describe --entity-type users --entity-type clients



