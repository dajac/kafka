#!/bin/bash

bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic events --partitions 6

while true; do 
  echo "$(uuidgen),$(uuidgen)";
done | bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic events \
  --property parse.key=true \
  --property key.separator=,