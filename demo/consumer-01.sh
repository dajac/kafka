#!/bin/bash

bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic events \
    --group group \
    --consumer-property client.id=consumer-1 \
    --consumer-property group.protocol=consumer \
    --property print.partition=true \
    --property print.offset=true
