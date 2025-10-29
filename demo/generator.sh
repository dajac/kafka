#!/bin/bash

bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic events --partitions 6

while true; do 
  echo "$(uuidgen),$(uuidgen)";
done | bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic events \
  --reader-property parse.key=true \
  --reader-property key.separator=, \
  > /dev/null 2>&1