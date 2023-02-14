/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.coordinator.group;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatRequestData;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.requests.TransactionResult;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.group.assignor.PartitionAssignor;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.queue.EventQueue;
import org.apache.kafka.queue.KafkaEventQueue;
import org.apache.kafka.server.util.FutureUtils;

import java.util.HashMap;
import java.util.List;
import java.util.OptionalInt;
import java.util.concurrent.CompletableFuture;

/**
 * The group coordinator event loop owns a subset of the __consumer_offsets partitions
 * and therefore manages all the groups assigned to them. This means that all the requests
 * for those groups are serviced by the event loop.
 */
public class GroupCoordinatorEventLoop implements AutoCloseable {

    public static class Builder {
        private final int eventLoopId;
        private Time time = Time.SYSTEM;
        private int consumerGroupSessionTimeoutMs = 45000;
        private int consumerGroupHeartbeatIntervalMs = 5000;
        private int consumerGroupMaxSize = Integer.MAX_VALUE;
        private List<PartitionAssignor> consumerGroupAssignors = null;

        public Builder(int eventLoopId) {
            this.eventLoopId = eventLoopId;
        }

        public Builder setTime(Time time) {
            this.time = time;
            return this;
        }

        public Builder setConsumerGroupSessionTimeoutMs(int consumerGroupSessionTimeoutMs) {
            this.consumerGroupSessionTimeoutMs = consumerGroupSessionTimeoutMs;
            return this;
        }

        public Builder setConsumerGroupHeartbeatIntervalMs(int consumerGroupHeartbeatIntervalMs) {
            this.consumerGroupHeartbeatIntervalMs = consumerGroupHeartbeatIntervalMs;
            return this;
        }

        public Builder setConsumerGroupMaxSize(int consumerGroupMaxSize) {
            this.consumerGroupMaxSize = consumerGroupMaxSize;
            return this;
        }

        public Builder setConsumerGroupAssignors(List<PartitionAssignor> consumerGroupAssignors) {
            this.consumerGroupAssignors = consumerGroupAssignors;
            return this;
        }

        GroupCoordinatorEventLoop build() {
            if (consumerGroupSessionTimeoutMs <= 0) {
                throw new IllegalArgumentException("Consumer group session timeout must be greater than 0.");
            }
            if (consumerGroupHeartbeatIntervalMs <= 0) {
                throw new IllegalArgumentException("Consumer group heartbeat interval must be greater than 0.");
            }
            if (consumerGroupMaxSize <= 0) {
                throw new IllegalArgumentException("Consumer group max size must be greater than 0.");
            }
            if (consumerGroupAssignors == null || consumerGroupAssignors.isEmpty()) {
                throw new IllegalArgumentException("At least one consumer group assignor must be specified.");
            }

            String threadNamePrefix = String.format("GroupCoordinator%d_", eventLoopId);
            LogContext logContext = new LogContext(String.format("[GroupCoordinator %d] ", eventLoopId));

            EventQueue queue = null;

            try {
                queue = new KafkaEventQueue(
                    time,
                    logContext,
                    threadNamePrefix
                );

                return new GroupCoordinatorEventLoop(
                    eventLoopId,
                    time,
                    logContext,
                    consumerGroupSessionTimeoutMs,
                    consumerGroupHeartbeatIntervalMs,
                    consumerGroupMaxSize,
                    consumerGroupAssignors,
                    queue
                );
            } catch (Exception e) {
                Utils.closeQuietly(queue, "event queue");
                throw e;
            }
        }
    }

    /**
     * The id of this event loop.
     */
    private final int eventLoopId;

    /**
     * The Kafka clock object to use.
     */
    private final Time time;

    /**
     * The log context.
     */
    private final LogContext logContext;

    /**
     * The consumer group session timeout in milliseconds.
     */
    private final int consumerGroupSessionTimeoutMs;

    /**
     * The consumer group heartbeat interval in milliseconds.
     */
    private final int consumerGroupHeartbeatIntervalMs;

    /**
     * The consumer group maximum size.
     */
    private final int consumerGroupMaxSize;

    /**
     * The consumer group assignors.
     */
    private final List<PartitionAssignor> consumerGroupAssignors;

    /**
     * The event queue.
     */
    private final EventQueue queue;

    /**
     * The replicated state machines keys by their __consumer_offets partition index.
     */
    private final HashMap<Integer, GroupCoordinatorPartitionState> stateMachines = new HashMap<>();

    GroupCoordinatorEventLoop(
        int eventLoopId,
        Time time,
        LogContext logContext,
        int consumerGroupSessionTimeoutMs,
        int consumerGroupHeartbeatIntervalMs,
        int consumerGroupMaxSize,
        List<PartitionAssignor> consumerGroupAssignors,
        EventQueue queue
    ) {
        this.eventLoopId = eventLoopId;
        this.time = time;
        this.logContext = logContext;
        this.consumerGroupSessionTimeoutMs = consumerGroupSessionTimeoutMs;
        this.consumerGroupHeartbeatIntervalMs = consumerGroupHeartbeatIntervalMs;
        this.consumerGroupMaxSize = consumerGroupMaxSize;
        this.consumerGroupAssignors = consumerGroupAssignors;
        this.queue = queue;
    }

    // Events:
    // - WriteEvent: Generate response & records, write them to the log.
    // - ReadEvent
    // - InternalEvent (load, unload, commit, etc.)

    public int id() {
        return eventLoopId;
    }

    public CompletableFuture<ConsumerGroupHeartbeatResponseData> joinConsumerGroup(
        int partitionId,
        RequestContext context,
        ConsumerGroupHeartbeatRequestData request
    ) {
        return FutureUtils.failedFuture(Errors.UNSUPPORTED_VERSION.exception(
            "This API is not implemented yet."
        ));
    }

    public void onTransactionCompleted(
        long producerId,
        Iterable<TopicPartition> partitions,
        TransactionResult transactionResult
    ) {
        throw new UnsupportedOperationException("This API is not implemented yet.");
    }

    public void onPartitionsDeleted(
        List<TopicPartition> topicPartitions,
        BufferSupplier bufferSupplier
    ) {
        throw new UnsupportedOperationException("This API is not implemented yet.");
    }

    public void onNewMetadataImage(
        MetadataImage newImage,
        MetadataDelta delta
    ) {
        throw new UnsupportedOperationException("This API is not implemented yet.");
    }

    public void onElection(
        int groupMetadataPartitionIndex,
        int groupMetadataPartitionLeaderEpoch
    ) {
        throw new UnsupportedOperationException("This API is not implemented yet.");
    }

    public void onResignation(
        int groupMetadataPartitionIndex,
        OptionalInt groupMetadataPartitionLeaderEpoch
    ) {
        throw new UnsupportedOperationException("This API is not implemented yet.");
    }

    public void beginShutdown() {
        queue.beginShutdown("GroupCoordinatorEventLoop#beginShutdown");
    }

    public void close() throws Exception {
        queue.close();
    }
}
