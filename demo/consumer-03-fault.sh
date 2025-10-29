#!/bin/bash

mkdir -p /tmp/consumer-1/
touch /tmp/consumer-1/rebalance_delay

bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic events \
    --group group \
    --command-property client.id=consumer-3 \
    --command-property group.protocol=consumer \
    --formatter-property print.partition=true \
    --formatter-property print.offset=true

rm /tmp/consumer-1/rebalance_delay
