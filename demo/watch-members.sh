#!/bin/bash

watch -n 1 bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group group --describe --members --verbose
