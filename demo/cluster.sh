#!/bin/bash

rm /Users/djacot/dev/kafka/core/build/dependant-libs-2.13.15/log4j-slf4j-impl-2.24.3.jar

rm -rf /tmp/kraft-combined-logs

KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
bin/kafka-storage.sh format --standalone -t $KAFKA_CLUSTER_ID -c config/server.properties
bin/kafka-server-start.sh config/server.properties
