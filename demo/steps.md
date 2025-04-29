
# Setup

KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
bin/kafka-storage.sh format --standalone -t $KAFKA_CLUSTER_ID -c config/server.properties
bin/kafka-server-start.sh config/server.properties

bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic events --partitions 6

# Produce

while true; do 
  echo "$(uuidgen),$(uuidgen)"; 
  sleep 0.1;
done | bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic events \
  --property parse.key=true \
  --property key.separator=,

# Consume

bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic events \
    --consumer.config demo/consumer-1.properties \
    --property print.partition=true \
    --property print.offset=true

bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic events \
    --consumer.config demo/consumer-2.properties \
    --property print.partition=true \
    --property print.offset=true

bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic events \
    --consumer.config demo/consumer-3.properties \
    --property print.partition=true \
    --property print.offset=true

# Monitor

watch -n 1 bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group group --describe

watch -n 1 bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group group --describe --members --verbose

# Fault

echo "0" > /tmp/consumer-1/rebalance_delay
echo "60" > /tmp/consumer-1/rebalance_delay