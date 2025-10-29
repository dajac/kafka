#!/bin/bash

bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic events \
    --group group \
    --command-property client.id=consumer-2 \
    --command-property group.protocol=classic \
    --command-property partition.assignment.strategy=org.apache.kafka.clients.consumer.RangeAssignor \
    --formatter-property print.partition=true \
    --formatter-property print.offset=true
