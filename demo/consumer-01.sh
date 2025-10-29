#!/bin/bash

bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic events \
    --group group \
    --command-property client.id=consumer-1 \
    --command-property group.protocol=consumer \
    --formatter-property print.partition=true \
    --formatter-property print.offset=true
