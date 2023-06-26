#To start zookeeper and kafka
#!/bin/bash

# Start ZooKeeper
zookeeper_start_command="./bin/zookeeper-server-start.sh ./config/zookeeper.properties"
$zookeeper_start_command &

# Start Kafka Brokers
kafka_broker_start_command="./bin/kafka-server-start.sh ./config/server.properties"
$kafka_broker_start_command &



------------------------------------


#To Start consumer:

#!/bin/bash

# Start Kafka Consumer 1
kafka_consumer_1="./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic your_topic_name --group consumer_group_1"
$kafka_consumer_1 &


# Start Kafka Consumer 2
#kafka_consumer_2="./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic your_topic_name --group consumer_group_2"
#$kafka_consumer_2 &