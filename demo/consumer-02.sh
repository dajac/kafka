#!/bin/bash

bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic events \
    --group group \
    --command-property client.id=consumer-2 \
    --command-property group.protocol=consumer \
    --property print.partition=true \
    --property print.offset=true
